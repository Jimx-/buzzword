extern crate crossbeam;

#[macro_use]
extern crate rental;

mod nodes;

pub(crate) type NodeID = u64;
pub(crate) const INVALID_NODE_ID: NodeID = 0;

const MAX_INNER_CHAIN_LENGTH: usize = 8;
const MAX_LEAF_CHAIN_LENGTH: usize = 8;

const MAX_INNER_ITEM_COUNT: usize = 128;
const MAX_LEAF_ITEM_COUNT: usize = 128;

use crate::nodes::{Node, TreeNode};

use crossbeam::{
    epoch::{self, Atomic, Guard, Owned, Pointer, Shared},
    queue::SegQueue,
};

use std::sync::atomic::{AtomicU64, Ordering};

type TreePath<'g, K, V> = Vec<(NodeID, Shared<'g, TreeNode<K, V>>)>;

fn binary_search<T, F>(
    items: &Vec<T>,
    val: &T,
    first: usize,
    last: usize,
    upper: bool,
    comparator: F,
) -> usize
where
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let mut low = first;
    let mut high = last;

    let cond = if upper {
        std::cmp::Ordering::Equal
    } else {
        std::cmp::Ordering::Greater
    };

    if low >= high {
        low
    } else {
        while low < high {
            let mid = low + (high - low) / 2;

            let mid_val = &items[mid];
            if comparator(&val, mid_val) >= cond {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        low
    }
}

pub struct BwTree<K, V, KCmp, VEq>
where
    K: 'static + Default + Clone,
    V: 'static + Clone,
    KCmp: Fn(&K, &K) -> std::cmp::Ordering,
    VEq: Fn(&V, &V) -> bool,
{
    mapping_table: Vec<Atomic<TreeNode<K, V>>>,
    key_comparator: KCmp,
    val_eq: VEq,
    root_id: AtomicU64,
    next_node_id: AtomicU64,
    free_node_ids: SegQueue<NodeID>,
}

impl<K, V, KCmp, VEq> BwTree<K, V, KCmp, VEq>
where
    K: Default + Clone + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
    KCmp: Fn(&K, &K) -> std::cmp::Ordering,
    VEq: Fn(&V, &V) -> bool,
{
    pub fn new(mapping_table_size: usize, key_comparator: KCmp, val_eq: VEq) -> Self {
        let mut mapping_table = Vec::new();
        mapping_table.resize_with(mapping_table_size, || Atomic::null());

        let tree = Self {
            mapping_table,
            key_comparator,
            val_eq,
            root_id: AtomicU64::new(1),
            next_node_id: AtomicU64::new(1),
            free_node_ids: SegQueue::new(),
        };

        tree.init_root();
        tree
    }

    fn init_root(&self) {
        let root_id = self.get_next_node_id();
        self.root_id.store(root_id, Ordering::Relaxed);

        let root_node = TreeNode::<K, V>::new_leaf(
            Default::default(),
            Default::default(),
            INVALID_NODE_ID,
            Vec::new(),
        );

        self.store_node(root_id, Some(root_node), Ordering::Relaxed);
    }

    fn get_next_node_id(&self) -> NodeID {
        match self.free_node_ids.pop() {
            Ok(id) => id,
            _ => self.next_node_id.fetch_add(1, Ordering::SeqCst),
        }
    }

    fn load_node_no_check<'g>(
        &self,
        id: NodeID,
        order: Ordering,
        guard: &'g Guard,
    ) -> Shared<'g, TreeNode<K, V>> {
        self.mapping_table[id as usize].load(order, guard)
    }

    fn load_node<'g>(
        &self,
        id: NodeID,
        order: Ordering,
        path: &TreePath<K, V>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, TreeNode<K, V>>, ()> {
        let mut node = self.mapping_table[id as usize].load(order, guard);

        // SMOs and consolidations are done in a cooperative way: every thread that tries to access a node with pending SMOs or
        // needed to be consolidated will perform the computation. However only the first thread that finishes the operation may
        // publish the result and other threads will continue with the old snapshot.
        if let Some(new_node) = self.complete_smo(id, node, path, guard)? {
            node = new_node;
        }

        if let Some(new_node) = self.try_consolidate_node(id, node, guard) {
            node = new_node;
        }

        if let Some(new_node) = self.try_smo(id, node, guard)? {
            node = new_node;
        }

        Ok(node)
    }

    fn store_node(&self, id: NodeID, node: Option<TreeNode<K, V>>, order: Ordering) {
        match node {
            Some(node) => self.mapping_table[id as usize].store(Owned::new(node), order),
            _ => self.mapping_table[id as usize].store(Shared::null(), order),
        }
    }

    fn cas_node<'g, P>(
        &self,
        id: NodeID,
        cur: Shared<TreeNode<K, V>>,
        new: P,
        guard: &'g Guard,
    ) -> Result<Shared<'g, TreeNode<K, V>>, P>
    where
        P: Pointer<TreeNode<K, V>>,
    {
        self.mapping_table[id as usize]
            .compare_and_set(cur, new, Ordering::AcqRel, guard)
            .map_err(|e| e.new)
    }

    fn cas_root_node<'g>(&self, cur: NodeID, new: NodeID) -> bool {
        self.root_id.compare_and_swap(cur, new, Ordering::AcqRel) == cur
    }

    /// Traverse down the tree and search for the leaf node for key.
    fn search<'g>(
        &self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<(NodeID, Shared<'g, TreeNode<K, V>>, TreePath<'g, K, V>), ()> {
        let mut path = TreePath::new();
        let mut node_id = self.root_id.load(Ordering::Acquire);
        let mut node_ptr = self.load_node(node_id, Ordering::Acquire, &path, guard)?;

        loop {
            let child_id = if unsafe { node_ptr.deref() }.is_leaf() {
                break;
            } else {
                self.traverse_inner(node_ptr, key, guard)
            };

            path.push((node_id, node_ptr));

            let child_node_ptr = self.load_node(child_id, Ordering::Acquire, &path, guard)?;

            node_id = child_id;
            node_ptr = child_node_ptr;
        }

        Ok((node_id, node_ptr, path))
    }

    /// Traverse the inner delta chain to locate the downlink for key.
    fn traverse_inner(&self, inner: Shared<TreeNode<K, V>>, key: &K, guard: &Guard) -> NodeID {
        let mut first = 1;
        let mut last = unsafe { inner.deref() }.base_size;
        let mut delta_ptr = inner;

        loop {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Inner(items) => {
                    let slot = self.binary_search_key(items, &key, first, last, true) - 1;
                    let (_, child_id) = items[slot];

                    break child_id;
                }
                Node::InnerInsert((insert_key, insert_id), (next_key, next_id), slot) => {
                    // check if the inserted key is the search key and adjust the range for binary search accordingly
                    if (self.key_comparator)(key, insert_key) >= std::cmp::Ordering::Equal {
                        if *next_id == INVALID_NODE_ID
                            || (self.key_comparator)(key, next_key) == std::cmp::Ordering::Less
                        {
                            break *insert_id;
                        }

                        first = std::cmp::max(first, *slot);
                    } else {
                        last = std::cmp::min(last, *slot);
                    }
                }
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }
    }

    /// Search for the key in inner. If found, return the associated ID and the slot. Otherwise return the slot the key should be
    /// inserted to.
    fn search_inner(
        &self,
        inner: Shared<TreeNode<K, V>>,
        key: &K,
        guard: &Guard,
    ) -> (usize, Option<NodeID>) {
        let mut delta_ptr = inner;

        loop {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Inner(items) => {
                    let slot = self.binary_search_key(items, &key, 0, items.len(), false);

                    if slot == items.len() {
                        return (slot, None);
                    } else {
                        let (k, id) = &items[slot];

                        if (self.key_comparator)(key, k) == std::cmp::Ordering::Equal {
                            return (slot, Some(*id));
                        } else {
                            return (slot, None);
                        }
                    }
                }
                Node::InnerInsert((insert_key, insert_id), _, slot) => {
                    if (self.key_comparator)(insert_key, key) == std::cmp::Ordering::Equal {
                        return (*slot, Some(*insert_id));
                    }
                }
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }
    }

    /// Traverse the delta chain of a leaf node and collect all matching values.
    fn traverse_leaf(&self, leaf: Shared<TreeNode<K, V>>, key: &K, guard: &Guard) -> Vec<V> {
        let mut vals = Vec::new();
        let mut delta_ptr = leaf;

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    if !items.is_empty() {
                        let lower_bound =
                            self.binary_search_key(items, key, 0, items.len() - 1, false);

                        for (k, v) in (&items[lower_bound..]).into_iter() {
                            if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal {
                                vals.push(v.clone());
                            } else {
                                break;
                            }
                        }
                    }
                }
                Node::LeafInsert((k, v), _, _) => {
                    if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal {
                        vals.push(v.clone());
                    }
                }
                Node::LeafSplit(..) => {}
                _ => {}
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        vals
    }

    /// Search for the key-value pair in the leaf node. If the pair is found return its slot otherwise return the slot it will take.
    fn search_leaf<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<'g, TreeNode<K, V>>,
        key: &K,
        value: &V,
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<(NodeID, Shared<'g, TreeNode<K, V>>, bool, usize), ()> {
        // need to move right when the leaf split delta is published but the downlink is not inserted into
        // the parent node so we have to use the right link to navigate to the sibling node
        let (node_id, node_ptr) = self.move_right(key, node_id, node_ptr, path, guard)?;

        let mut delta_ptr = node_ptr;

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    let lower_bound = self.binary_search_key(items, key, 0, items.len(), false);

                    for (i, (k, v)) in (&items[lower_bound..]).into_iter().enumerate() {
                        if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal
                            && (self.val_eq)(v, value)
                        {
                            return Ok((node_id, node_ptr, true, i + lower_bound));
                        }
                    }

                    return Ok((node_id, node_ptr, false, lower_bound));
                }
                Node::LeafInsert((k, v), slot, overwrite) => {
                    if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal
                        && (self.val_eq)(v, value)
                    {
                        return Ok((node_id, node_ptr, *overwrite, *slot));
                    }
                }
                Node::LeafSplit(..) => {}
                _ => {}
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        Err(())
    }

    fn move_right<'g>(
        &self,
        key: &K,
        mut node_id: NodeID,
        mut node_ptr: Shared<'g, TreeNode<K, V>>,
        path: &TreePath<K, V>,
        guard: &'g Guard,
    ) -> Result<(NodeID, Shared<'g, TreeNode<K, V>>), ()> {
        loop {
            let node = unsafe { node_ptr.deref() };

            if node.right_link != INVALID_NODE_ID
                && (self.key_comparator)(key, &node.high_key) >= std::cmp::Ordering::Equal
            {
                node_id = node.right_link;
                node_ptr = self.load_node(node.right_link, Ordering::Acquire, path, guard)?;
            } else {
                break;
            }
        }

        Ok((node_id, node_ptr))
    }

    /// Collect all values on the leaf delta chain.
    fn collect_leaf_values(&self, leaf: Shared<TreeNode<K, V>>, guard: &Guard) -> Vec<(K, V)> {
        self.collect_leaf_values_recur(leaf, Vec::new(), guard)
    }

    fn collect_leaf_values_recur(
        &self,
        leaf: Shared<TreeNode<K, V>>,
        mut deltas: Vec<(K, V, usize, bool, bool)>,
        guard: &Guard,
    ) -> Vec<(K, V)> {
        let mut delta_ptr = leaf;
        let (high_key, right_link) = unsafe {
            let delta = delta_ptr.deref();
            (&delta.high_key, delta.right_link)
        };

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    let mut merged = Vec::new();
                    // merge items and deltas

                    let copy_end = if right_link == INVALID_NODE_ID {
                        items.len()
                    } else {
                        self.binary_search_key(items, high_key, 0, items.len(), false)
                    };
                    let (items, _) = items.split_at(copy_end);
                    let mut item_it = items.into_iter().enumerate();
                    let mut delta_it = deltas.into_iter().peekable();

                    while let Some((kd, vd, cur_slot, mut overwritten, inserted)) = delta_it.next()
                    {
                        // copy from items until we reach cur_slot
                        let cur_item = loop {
                            if let Some((item_slot, (ki, vi))) = item_it.next() {
                                if item_slot < cur_slot {
                                    merged.push((ki.clone(), vi.clone()));
                                } else {
                                    break Some((ki, vi));
                                }
                            } else {
                                break None;
                            }
                        };

                        // handle all deltas that go in this slot
                        if inserted {
                            merged.push((kd, vd));
                        }

                        while let Some((_, _, slot, _, _)) = delta_it.peek() {
                            if *slot != cur_slot {
                                break;
                            }

                            if let Some((kd, vd, _, overwrite, inserted)) = delta_it.next() {
                                overwritten |= overwrite;

                                if inserted {
                                    merged.push((kd, vd));
                                }
                            }
                        }

                        // add the current item if it is not overwritten by the deltas
                        if !overwritten {
                            if let Some((ki, vi)) = cur_item {
                                merged.push((ki.clone(), vi.clone()));
                            }
                        }
                    }

                    // drain the items
                    while let Some((_, (ki, vi))) = item_it.next() {
                        merged.push((ki.clone(), vi.clone()));
                    }

                    return merged;
                }
                Node::LeafInsert((k, v), slot, overwrite) => {
                    let tuple = (k.clone(), v.clone(), *slot, *overwrite, true);
                    let pos = binary_search(
                        &deltas,
                        &tuple,
                        0,
                        deltas.len(),
                        true,
                        |(k1, _, s1, _, _), (k2, _, s2, _, _)| match (self.key_comparator)(k1, k2) {
                            std::cmp::Ordering::Equal => s1.cmp(s2),
                            otherwise => otherwise,
                        },
                    );

                    deltas.insert(pos, tuple);
                }
                Node::LeafSplit(..) => {}
                _ => {}
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        Vec::new()
    }

    fn binary_search_key<T>(
        &self,
        items: &Vec<(K, T)>,
        key: &K,
        first: usize,
        last: usize,
        next_key: bool,
    ) -> usize {
        let mut low = first;
        let mut high = last;

        let cond = if next_key {
            std::cmp::Ordering::Equal
        } else {
            std::cmp::Ordering::Greater
        };

        if low >= high {
            low
        } else {
            while low < high {
                let mid = low + (high - low) / 2;

                let (mid_key, _) = &items[mid];
                if (self.key_comparator)(&key, mid_key) >= cond {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            low
        }
    }

    #[inline(always)]
    fn try_consolidate_node<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        guard: &'g Guard,
    ) -> Option<Shared<'g, TreeNode<K, V>>> {
        let node = unsafe { node_ptr.deref() };

        if !node.is_delta() {
            return None;
        }

        if node.is_leaf() {
            if node.length >= MAX_LEAF_CHAIN_LENGTH {
                return self.consolidate_leaf_node(node_id, node_ptr, guard);
            }
        } else {
            if node.length >= MAX_INNER_CHAIN_LENGTH {}
        }

        None
    }

    /// Consolidate a leaf delta chain into a new base node.
    fn consolidate_leaf_node<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        guard: &'g Guard,
    ) -> Option<Shared<'g, TreeNode<K, V>>> {
        let vals = self.collect_leaf_values(node_ptr, guard);

        let node = unsafe { node_ptr.deref() };
        let new_leaf = Owned::new(TreeNode::new_leaf(
            node.low_key.clone(),
            node.high_key.clone(),
            node.right_link,
            vals,
        ));

        match self.cas_node(node_id, node_ptr, new_leaf, guard) {
            Ok(new) => {
                self.destroy_delta_chain(node_ptr, guard);
                Some(new)
            }
            _ => None,
        }
    }

    /// Schedule to drop the delta chain after all threads get unpinned.
    fn destroy_delta_chain<'g>(&self, mut node_ptr: Shared<'g, TreeNode<K, V>>, guard: &'g Guard) {
        while !node_ptr.is_null() {
            node_ptr = unsafe {
                let next = node_ptr.deref().next.load(Ordering::Relaxed, guard);
                guard.defer_destroy(node_ptr);
                next
            };
        }
    }

    /// Try to publish a delta node for SMOs (node splits, node merges, etc.).
    fn try_smo<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        guard: &'g Guard,
    ) -> Result<Option<Shared<'g, TreeNode<K, V>>>, ()> {
        let node = unsafe { node_ptr.deref() };

        if node.is_delta() {
            return Ok(None);
        }

        if node.is_leaf() {
            if node.item_count >= MAX_LEAF_ITEM_COUNT {
                // need split
                let (split_key, sibling) = match self.get_split_sibling(node_ptr) {
                    None => return Ok(None),
                    Some(sibling) => sibling,
                };

                let sibling_id = self.get_next_node_id();
                let delta = Owned::new(TreeNode::new_leaf_split(
                    &split_key, sibling_id, node, &sibling,
                ));

                delta.next.store(node_ptr, Ordering::Relaxed);

                // install the new sibling
                self.store_node(sibling_id, Some(sibling), Ordering::Relaxed);

                // install the delta
                if let Err(_) = self.cas_node(node_id, node_ptr, delta, guard) {
                    // clean up the sibling node
                    let sibling_ptr = self.load_node_no_check(sibling_id, Ordering::Relaxed, guard);
                    self.store_node(sibling_id, None, Ordering::Relaxed);
                    self.free_node_ids.push(sibling_id);
                    unsafe {
                        guard.defer_destroy(sibling_ptr);
                    }

                    Ok(None)
                } else {
                    // abort and try again to complete the SMO
                    Err(())
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Find the location to split the node evenly into two nodes.
    fn get_split_location(&self, node: &TreeNode<K, V>) -> Option<usize> {
        match &node.node {
            Node::Leaf(items) => {
                if items.len() < 2 {
                    return None;
                }

                let mid = items.len() / 2 - 1;
                let (k, _) = &items[mid];

                // skip duplicate keys
                for (i, (key, _)) in (&items[mid + 1..]).into_iter().enumerate() {
                    if (self.key_comparator)(key, k) != std::cmp::Ordering::Equal {
                        return Some(i + 1 + mid);
                    }
                }

                None
            }
            _ => unreachable!(),
        }
    }

    /// Create the sibling node for node splits.
    fn get_split_sibling(&self, node_ptr: Shared<TreeNode<K, V>>) -> Option<(K, TreeNode<K, V>)> {
        let node = unsafe { node_ptr.deref() };
        let split_location = match self.get_split_location(node) {
            Some(loc) => loc,
            _ => return None,
        };

        match &node.node {
            Node::Leaf(items) => {
                let (split_key, _) = &items[split_location];
                let sibling_items = (&items[split_location..]).to_vec();

                Some((
                    split_key.clone(),
                    TreeNode::new_leaf(
                        split_key.clone(),
                        node.high_key.clone(),
                        node.right_link,
                        sibling_items,
                    ),
                ))
            }
            _ => unreachable!(),
        }
    }

    /// Try to complete a pending SMO and publish the result.
    fn complete_smo<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        path: &TreePath<K, V>,
        guard: &'g Guard,
    ) -> Result<Option<Shared<'g, TreeNode<K, V>>>, ()> {
        let node = unsafe { node_ptr.deref() };

        match &node.node {
            Node::LeafSplit(split_key, right_sibling_id) => {
                //                |  insert_item    |     |    next_item         |
                //                |  split_key      |     |    high_key          |
                //  node      ->  |  right sibling  |  -> |    node.right_link   |
                let insert_item = (split_key, *right_sibling_id);
                let next_node_ptr = node.next.load(Ordering::Relaxed, guard);
                let next_node = unsafe { next_node_ptr.deref() };
                let next_item = (&next_node.high_key, next_node.right_link);

                match self.complete_split_smo(node_id, insert_item, next_item, path, guard) {
                    Ok(true) => {
                        // consolidate the leaf node to remove the split delta and prevent other threads from attempting
                        // the SMO again
                        self.consolidate_leaf_node(node_id, node_ptr, guard);
                        // we cannot return the consolidated node here because the parent node is also updated
                        // so just abort the operation and let the thread traverse the tree again to observe the changes
                        Err(())
                    }
                    Ok(false) => Ok(None),
                    _ => Err(()),
                }
            }
            _ => Ok(None),
        }
    }

    fn complete_split_smo<'g>(
        &self,
        node_id: NodeID,
        insert_item: (&K, NodeID),
        next_item: (&K, NodeID),
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<bool, ()> {
        if path.is_empty() {
            // root split
            // |    /    |  insert_key |
            // | node_id |  insert_id  |
            let (insert_key, insert_id) = insert_item;

            let new_root_id = self.get_next_node_id();
            let new_root = TreeNode::<K, V>::new_inner(
                Default::default(),
                Default::default(),
                node_id,
                INVALID_NODE_ID,
                vec![
                    (Default::default(), node_id),
                    (insert_key.clone(), insert_id),
                ],
            );

            self.store_node(new_root_id, Some(new_root), Ordering::Relaxed);

            if !self.cas_root_node(node_id, new_root_id) {
                // CAS failed, schedule to destroy the new root
                let new_root_ptr = self.load_node_no_check(new_root_id, Ordering::Relaxed, guard);
                self.store_node(new_root_id, None, Ordering::Relaxed);
                self.free_node_ids.push(new_root_id);
                unsafe {
                    guard.defer_destroy(new_root_ptr);
                }
            }

            // abort to load the new root
            Err(())
        } else {
            let (parent_id, parent_node_ptr) = &path[path.len() - 1];

            let (split_key, insert_id) = &insert_item;
            match self.search_inner(*parent_node_ptr, split_key, guard) {
                (_, Some(id)) => {
                    if id == *insert_id {
                        // the split has been completed
                        Ok(false)
                    } else {
                        // abort
                        Err(())
                    }
                }
                (slot, _) => self.insert_parent(
                    *parent_id,
                    *parent_node_ptr,
                    insert_item,
                    next_item,
                    slot,
                    guard,
                ),
            }
        }
    }

    /// Insert a new downlink into the parent node for node splits.
    fn insert_parent<'g>(
        &self,
        parent_id: NodeID,
        parent_node_ptr: Shared<TreeNode<K, V>>,
        insert_item: (&K, NodeID),
        next_item: (&K, NodeID),
        slot: usize,
        guard: &'g Guard,
    ) -> Result<bool, ()> {
        let parent_node = unsafe { parent_node_ptr.deref() };

        let delta = Owned::new(TreeNode::new_inner_insert(
            insert_item,
            next_item,
            slot,
            parent_node,
        ));

        delta.next.store(parent_node_ptr, Ordering::Relaxed);

        self.cas_node(parent_id, parent_node_ptr, delta, guard)
            .map(|_| true)
            .map_err(|_| ())
    }

    /// Insert a key-value pair into the tree.
    pub fn insert(&self, key: &K, mut value: V) {
        let guard = &epoch::pin();
        loop {
            let (leaf_id, leaf_node_ptr, exists, slot) = loop {
                let (leaf_id, leaf_node_ptr, path) = match self.search(key, guard) {
                    Ok(result) => result,
                    _ => continue,
                };

                match self.search_leaf(leaf_id, leaf_node_ptr, key, &value, &path, guard) {
                    Ok(result) => break result,
                    _ => continue,
                }
            };

            // install the insert delta
            let delta = Owned::new(TreeNode::new_leaf_insert(
                key,
                value,
                slot,
                exists,
                unsafe { leaf_node_ptr.deref() },
            ));

            delta.next.store(leaf_node_ptr, Ordering::Relaxed);

            value = match self.cas_node(leaf_id, leaf_node_ptr, delta, guard) {
                Ok(_) => break, // CAS succeeds
                Err(delta) => {
                    // if CAS failed, we need to re-take the ownership of the value from the delta
                    let TreeNode { node, .. } = *delta.into_box();

                    match node {
                        Node::LeafInsert((_, val), _, _) => val,
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    /// Get the list of values associated with the key.
    pub fn get_values(&self, key: &K) -> Vec<V> {
        let guard = &epoch::pin();
        let (_, leaf_node_ptr, _) = loop {
            match self.search(key, guard) {
                Ok(result) => break result,
                _ => continue,
            }
        };

        self.traverse_leaf(leaf_node_ptr, key, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn can_insert_values() {
        let tree = Arc::new(BwTree::new(1024 * 1024, u32::cmp, u32::eq));

        let mut handles = Vec::new();
        for i in 0..100 {
            let tree = tree.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..100 {
                    let key = i * 100 + j;
                    tree.insert(&key, key + 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..10000 {
            let vals = tree.get_values(&i);
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], i + 1);
        }
    }
}
