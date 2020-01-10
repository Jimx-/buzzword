extern crate crossbeam;

mod nodes;

pub(crate) type NodeID = u64;
pub(crate) const INVALID_NODE_ID: NodeID = 0;

const MAX_INNER_CHAIN_LENGTH: usize = 8;
const MAX_LEAF_CHAIN_LENGTH: usize = 8;

const MIN_INNER_ITEM_COUNT: usize = 32;
const MAX_INNER_ITEM_COUNT: usize = 128;
const MIN_LEAF_ITEM_COUNT: usize = 32;
const MAX_LEAF_ITEM_COUNT: usize = 128;

use crate::nodes::{Node, TreeNode};

use crossbeam::{
    epoch::{self, Atomic, Guard, Owned, Pointer, Shared},
    queue::SegQueue,
};

use std::{
    collections::HashSet,
    hash::Hash,
    ops::Deref,
    sync::atomic::{AtomicU64, Ordering},
};

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

pub struct BwTreeImpl<K, V>
where
    K: 'static + Clone + std::fmt::Debug,
    V: Clone + Eq + Hash,
{
    mapping_table: Vec<Atomic<TreeNode<K, V>>>,
    key_comparator: Box<dyn Sync + Send + Fn(&K, &K) -> std::cmp::Ordering>,
    root_id: AtomicU64,
    next_node_id: AtomicU64,
    free_node_ids: SegQueue<NodeID>,
}

impl<K, V> BwTreeImpl<K, V>
where
    K: Clone + std::fmt::Debug,
    V: Clone + Eq + Hash + std::fmt::Debug,
{
    pub fn new(
        mapping_table_size: usize,
        key_comparator: Box<dyn Sync + Send + Fn(&K, &K) -> std::cmp::Ordering>,
    ) -> Self {
        let mut mapping_table = Vec::new();
        mapping_table.resize_with(mapping_table_size, || Atomic::null());

        let tree = Self {
            mapping_table,
            key_comparator,
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

        let root_node = TreeNode::<K, V>::new_leaf(None, None, INVALID_NODE_ID, Vec::new());

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
        path: &TreePath<'g, K, V>,
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

        if let Some(new_node) = self.try_smo(id, node, path, guard)? {
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
            if unsafe { node_ptr.deref() }.is_leaf() {
                break;
            }

            path.push((node_id, node_ptr));

            let child_id = self.traverse_inner(node_id, node_ptr, key, &path, guard)?;
            let child_node_ptr = self.load_node(child_id, Ordering::Acquire, &path, guard)?;

            node_id = child_id;
            node_ptr = child_node_ptr;
        }

        Ok((node_id, node_ptr, path))
    }

    /// Traverse the inner delta chain to locate the downlink for key.
    fn traverse_inner<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        key: &K,
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<NodeID, ()> {
        let (_, node_ptr) = self.move_right(key, node_id, node_ptr, path, guard)?;

        let mut delta_ptr = node_ptr;
        let mut first = 0;
        let mut last = unsafe { delta_ptr.deref() }.base_size;

        Ok(loop {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Inner(items) => {
                    let slot = self.binary_search_key(items, &key, first, last, true);

                    if slot == 0 {
                        break delta.leftmost_child;
                    } else {
                        let (_, child_id) = items[slot - 1];
                        break child_id;
                    }
                }
                Node::InnerInsert((insert_key, insert_id), next_key, slot) => {
                    // check if the inserted key is the search key and adjust the range for binary search accordingly
                    if (self.key_comparator)(key, insert_key) >= std::cmp::Ordering::Equal {
                        if let Some(ref next_key) = next_key {
                            if (self.key_comparator)(key, next_key) == std::cmp::Ordering::Less {
                                break *insert_id;
                            }
                        } else {
                            break *insert_id;
                        }

                        first = std::cmp::max(first, *slot);
                    } else {
                        last = std::cmp::min(last, *slot);
                    }
                }
                Node::InnerDelete((delete_key, _), (prev_key, prev_id), (next_key, _), slot) => {
                    if delta.leftmost_child == *prev_id
                        || (prev_key.is_some()
                            && (self.key_comparator)(key, prev_key.as_ref().unwrap())
                                >= std::cmp::Ordering::Equal)
                    {
                        if let Some(next_key) = next_key {
                            if (self.key_comparator)(key, next_key) == std::cmp::Ordering::Less {
                                break *prev_id; // prev is the merged node
                            }
                        } else {
                            break *prev_id;
                        }
                    }

                    if (self.key_comparator)(key, delete_key) >= std::cmp::Ordering::Equal {
                        first = std::cmp::max(first, *slot);
                    } else {
                        last = std::cmp::min(last, *slot);
                    }
                }
                Node::InnerSplit(..) => {}
                Node::InnerMerge(merge_key, _, removed_node_atomic) => {
                    if (self.key_comparator)(key, merge_key) >= std::cmp::Ordering::Equal {
                        let removed_node_ptr = removed_node_atomic.load(Ordering::Relaxed, guard);

                        delta_ptr = removed_node_ptr;
                        first = 0;
                        last = unsafe { delta_ptr.deref() }.base_size;

                        continue;
                    }
                }
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        })
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
                Node::InnerDelete((delete_key, _), _, _, slot) => {
                    if (self.key_comparator)(delete_key, key) == std::cmp::Ordering::Equal {
                        return (*slot, None);
                    }
                }
                Node::InnerSplit(..) => {}
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }
    }

    /// Traverse the delta chain of a leaf node and collect all matching values.
    fn traverse_leaf<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        key: &K,
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<Vec<V>, ()> {
        // need to move right when the leaf split delta is published but the downlink is not inserted into
        // the parent node so we have to use the right link to navigate to the sibling node
        let (_, node_ptr) = self.move_right(key, node_id, node_ptr, path, guard)?;

        let mut vals = Vec::new();
        let mut delta_ptr = node_ptr;

        let mut first = 0;
        let mut last = unsafe { delta_ptr.deref() }.base_size;

        let mut add_set = HashSet::new();
        let mut remove_set = HashSet::new();

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    if !items.is_empty() {
                        let lower_bound = self.binary_search_key(items, key, first, last, false);

                        for (k, v) in (&items[lower_bound..]).into_iter() {
                            if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal {
                                if !add_set.contains(v) && !remove_set.contains(v) {
                                    vals.push(v.clone());
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                Node::LeafInsert((k, v), slot, _) => match (self.key_comparator)(k, key) {
                    std::cmp::Ordering::Equal => {
                        // item key == search key
                        if !add_set.contains(v) && !remove_set.contains(v) {
                            add_set.insert(v);
                            vals.push(v.clone());
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        // item key > search key
                        last = *slot;
                    }
                    std::cmp::Ordering::Less => {
                        // item key < search key
                        first = *slot;
                    }
                },
                Node::LeafDelete((k, v), slot, _) => match (self.key_comparator)(k, key) {
                    std::cmp::Ordering::Equal => {
                        // item key == search key
                        if !add_set.contains(v) {
                            remove_set.insert(v);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        // item key > search key
                        last = *slot;
                    }
                    std::cmp::Ordering::Less => {
                        // item key < search key
                        first = *slot;
                    }
                },
                Node::LeafSplit(..) => {}
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        Ok(vals)
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
                        if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal && v == value
                        {
                            return Ok((node_id, node_ptr, true, i + lower_bound));
                        }
                    }

                    return Ok((node_id, node_ptr, false, lower_bound));
                }
                Node::LeafInsert((k, v), slot, overwrite) => {
                    if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal && v == value {
                        return Ok((node_id, node_ptr, *overwrite, *slot));
                    }
                }
                Node::LeafDelete((k, v), slot, overwrite) => {
                    if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal && v == value {
                        return Ok((node_id, node_ptr, !*overwrite, *slot));
                    }
                }
                Node::LeafMerge(merge_key, removed_node_id, removed_node_atomic) => {
                    if (self.key_comparator)(key, merge_key) >= std::cmp::Ordering::Equal {
                        let removed_node_ptr = removed_node_atomic.load(Ordering::Relaxed, guard);

                        // go to right branch
                        let (_, _, exists, slot) = self.search_leaf(
                            *removed_node_id,
                            removed_node_ptr,
                            key,
                            value,
                            path,
                            guard,
                        )?;

                        return Ok((node_id, node_ptr, exists, slot));
                    }
                }
                Node::LeafSplit(..) => {}
                _ => unreachable!(),
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
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<(NodeID, Shared<'g, TreeNode<K, V>>), ()> {
        loop {
            let node = unsafe { node_ptr.deref() };

            if let Some(ref high_key) = &node.high_key {
                if (self.key_comparator)(key, high_key) >= std::cmp::Ordering::Equal {
                    node_id = node.right_link;
                    node_ptr = self.load_node(node.right_link, Ordering::Acquire, path, guard)?;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok((node_id, node_ptr))
    }

    /// Collect all downliks on the inner delta chain.
    fn collect_inner_downlinks(
        &self,
        inner: Shared<TreeNode<K, V>>,
        guard: &Guard,
    ) -> Vec<(K, NodeID)> {
        self.collect_inner_downlinks_recur(inner, Vec::new(), guard)
    }

    fn collect_inner_downlinks_recur(
        &self,
        inner: Shared<TreeNode<K, V>>,
        mut deltas: Vec<(K, NodeID, usize, bool)>,
        guard: &Guard,
    ) -> Vec<(K, NodeID)> {
        let mut delta_ptr = inner;
        let high_key = unsafe { &delta_ptr.deref().high_key };

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Inner(ref items) => {
                    let mut merged = Vec::new();

                    // merge items and deltas
                    let copy_end = if let Some(ref high_key) = high_key {
                        self.binary_search_key(items, high_key, 0, items.len(), false)
                    } else {
                        items.len()
                    };
                    let (items, _) = items.split_at(copy_end);
                    let mut item_it = items.into_iter().cloned();
                    let mut delta_it = deltas.into_iter();

                    let mut cur_item = item_it.next();
                    let mut cur_delta = delta_it.next();
                    loop {
                        match (cur_item, cur_delta) {
                            (None, None) => break,
                            (None, Some((k, v, _, insert))) => {
                                // drain deltas
                                if insert {
                                    merged.push((k, v));
                                }
                                merged.extend(
                                    delta_it
                                        .filter(|(_, _, _, insert)| *insert)
                                        .map(|(k, v, _, _)| (k, v)),
                                );
                                break;
                            }
                            (Some(item), None) => {
                                // drain items
                                merged.push(item);
                                merged.extend(item_it);
                                break;
                            }
                            (Some((ki, vi)), Some((kd, vd, slot, insert))) => {
                                match (self.key_comparator)(&ki, &kd) {
                                    std::cmp::Ordering::Greater => {
                                        if insert {
                                            merged.push((kd, vd));
                                        }
                                        cur_delta = delta_it.next();
                                        cur_item = Some((ki, vi));
                                    }
                                    std::cmp::Ordering::Less => {
                                        merged.push((ki, vi));
                                        cur_item = item_it.next();
                                        cur_delta = Some((kd, vd, slot, insert));
                                    }
                                    _ => {
                                        if insert {
                                            merged.push((kd, vd));
                                        }

                                        cur_delta = delta_it.next();
                                        cur_item = item_it.next();
                                    }
                                }
                            }
                        }
                    }

                    return merged;
                }
                Node::InnerInsert((k, id), _, slot) => {
                    let tuple = (k.clone(), *id, *slot, true);
                    let pos = binary_search(
                        &deltas,
                        &tuple,
                        0,
                        deltas.len(),
                        false,
                        |(k1, _, s1, _), (k2, _, s2, _)| match (self.key_comparator)(k1, k2) {
                            std::cmp::Ordering::Equal => s1.cmp(s2),
                            otherwise => otherwise,
                        },
                    );

                    if pos >= deltas.len()
                        || (self.key_comparator)(&deltas[pos].0, k) != std::cmp::Ordering::Equal
                    {
                        deltas.insert(pos, tuple);
                    }
                }
                Node::InnerDelete((k, id), _, _, slot) => {
                    let tuple = (k.clone(), *id, *slot, false);
                    let pos = binary_search(
                        &deltas,
                        &tuple,
                        0,
                        deltas.len(),
                        false,
                        |(k1, _, s1, _), (k2, _, s2, _)| match (self.key_comparator)(k1, k2) {
                            std::cmp::Ordering::Equal => s1.cmp(s2),
                            otherwise => otherwise,
                        },
                    );

                    if pos >= deltas.len()
                        || (self.key_comparator)(&deltas[pos].0, k) != std::cmp::Ordering::Equal
                    {
                        deltas.insert(pos, tuple);
                    }
                }
                Node::InnerSplit(..) => {}
                Node::InnerMerge(merge_key, _, removed_node_atomic) => {
                    let next_node = delta.next.load(Ordering::Relaxed, guard);
                    let split_pos = binary_search(
                        &deltas.iter().map(|x| &x.0).collect::<Vec<&K>>(),
                        &merge_key,
                        0,
                        deltas.len(),
                        false,
                        |x, y| (self.key_comparator)(*x, *y),
                    );
                    let right_deltas = deltas.split_off(split_pos);
                    let mut left_branch =
                        self.collect_inner_downlinks_recur(next_node, deltas, guard);
                    let removed_node_ptr = removed_node_atomic.load(Ordering::Relaxed, guard);
                    let right_branch =
                        self.collect_inner_downlinks_recur(removed_node_ptr, right_deltas, guard);

                    left_branch.extend(right_branch);
                    return left_branch;
                }
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        Vec::new()
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
        let high_key = unsafe { &delta_ptr.deref().high_key };

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    let mut merged = Vec::new();
                    // merge items and deltas

                    let copy_end = if let Some(ref high_key) = high_key {
                        self.binary_search_key(items, high_key, 0, items.len(), false)
                    } else {
                        items.len()
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
                Node::LeafDelete((k, v), slot, overwrite) => {
                    let tuple = (k.clone(), v.clone(), *slot, *overwrite, false);
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
                Node::LeafMerge(merge_key, _, removed_node_atomic) => {
                    let next_node = delta.next.load(Ordering::Relaxed, guard);
                    let split_pos = binary_search(
                        &deltas.iter().map(|x| &x.0).collect::<Vec<&K>>(),
                        &merge_key,
                        0,
                        deltas.len(),
                        false,
                        |x, y| (self.key_comparator)(*x, *y),
                    );
                    let right_deltas = deltas.split_off(split_pos);
                    let mut left_branch = self.collect_leaf_values_recur(next_node, deltas, guard);
                    let removed_node_ptr = removed_node_atomic.load(Ordering::Relaxed, guard);
                    let right_branch =
                        self.collect_leaf_values_recur(removed_node_ptr, right_deltas, guard);

                    left_branch.extend(right_branch);
                    return left_branch;
                }
                Node::LeafSplit(..) => {}
                _ => unreachable!(),
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
            if node.length >= MAX_INNER_CHAIN_LENGTH {
                return self.consolidate_inner_node(node_id, node_ptr, guard);
            }
        }

        None
    }

    fn consolidate_inner_node<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        guard: &'g Guard,
    ) -> Option<Shared<'g, TreeNode<K, V>>> {
        let downlinks = self.collect_inner_downlinks(node_ptr, guard);

        let node = unsafe { node_ptr.deref() };
        let new_inner = Owned::new(TreeNode::new_inner(
            node.low_key.clone(),
            node.high_key.clone(),
            node.leftmost_child,
            node.right_link,
            downlinks,
        ));

        match self.cas_node(node_id, node_ptr, new_inner, guard) {
            Ok(new) => {
                self.destroy_delta_chain(node_ptr, guard);
                Some(new)
            }
            _ => None,
        }
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
        path: &TreePath<'g, K, V>,
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
                    // self.store_node(sibling_id, None, Ordering::Relaxed);
                    // self.free_node_ids.push(sibling_id);
                    unsafe {
                        guard.defer_destroy(sibling_ptr);
                    }

                    Ok(None)
                } else {
                    // abort and try again to complete the SMO
                    Err(())
                }
            } else if node.item_count < MIN_LEAF_ITEM_COUNT {
                if path.is_empty() {
                    // root underflow
                    return Ok(None);
                }

                let (_, parent_node_ptr) = &path[path.len() - 1];
                // do not merge leftmost leaf
                if unsafe { parent_node_ptr.deref().leftmost_child } == node_id {
                    return Ok(None);
                }

                // remove the node
                let delta = Owned::new(TreeNode::new_leaf_remove(node_id, node));
                delta.next.store(node_ptr, Ordering::Relaxed);

                match self.cas_node(node_id, node_ptr, delta, guard) {
                    // need to abort anyway
                    _ => Err(()),
                }
            } else {
                Ok(None)
            }
        } else {
            if node.item_count >= MAX_INNER_ITEM_COUNT {
                let (split_key, sibling) = match self.get_split_sibling(node_ptr) {
                    None => return Ok(None),
                    Some(sibling) => sibling,
                };

                let sibling_id = self.get_next_node_id();
                let delta = Owned::new(TreeNode::new_inner_split(
                    &split_key, sibling_id, node, &sibling,
                ));

                delta.next.store(node_ptr, Ordering::Relaxed);

                // install the new sibling
                self.store_node(sibling_id, Some(sibling), Ordering::Relaxed);

                // install the delta
                if let Err(_) = self.cas_node(node_id, node_ptr, delta, guard) {
                    // clean up the sibling node
                    let sibling_ptr = self.load_node_no_check(sibling_id, Ordering::Relaxed, guard);
                    // self.store_node(sibling_id, None, Ordering::Relaxed);
                    // self.free_node_ids.push(sibling_id);
                    unsafe {
                        guard.defer_destroy(sibling_ptr);
                    }

                    Ok(None)
                } else {
                    // abort and try again to complete the SMO
                    Err(())
                }
            } else if node.item_count < MIN_INNER_ITEM_COUNT {
                if path.is_empty() {
                    // root underflow
                    return Ok(None);
                }

                let (_, parent_node_ptr) = &path[path.len() - 1];
                // do not merge leftmost leaf
                if unsafe { parent_node_ptr.deref().leftmost_child } == node_id {
                    return Ok(None);
                }

                // remove the node
                let delta = Owned::new(TreeNode::new_inner_remove(node_id, node));
                delta.next.store(node_ptr, Ordering::Relaxed);

                match self.cas_node(node_id, node_ptr, delta, guard) {
                    // need to abort anyway
                    _ => Err(()),
                }
            } else {
                Ok(None)
            }
        }
    }

    /// Find the location to split the node evenly into two nodes.
    fn get_split_location(&self, node: &TreeNode<K, V>) -> Option<usize> {
        match &node.node {
            Node::Leaf(ref items) => {
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
            Node::Inner(ref items) => {
                if items.len() < 2 {
                    return None;
                }

                Some(items.len() / 2)
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
                        Some(split_key.clone()),
                        node.high_key.clone(),
                        node.right_link,
                        sibling_items,
                    ),
                ))
            }
            Node::Inner(items) => {
                let (split_key, leftmost_child) = &items[split_location];
                let sibling_items = (&items[split_location..]).to_vec();

                Some((
                    split_key.clone(),
                    TreeNode::new_inner(
                        Some(split_key.clone()),
                        node.high_key.clone(),
                        *leftmost_child,
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
                let next_key = &next_node.high_key;

                match self.complete_split_smo(node_id, insert_item, next_key, path, guard) {
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
            Node::InnerSplit(split_key, right_sibling_id) => {
                let insert_item = (split_key, *right_sibling_id);
                let next_node_ptr = node.next.load(Ordering::Relaxed, guard);
                let next_node = unsafe { next_node_ptr.deref() };
                let next_key = &next_node.high_key;

                match self.complete_split_smo(node_id, insert_item, next_key, path, guard) {
                    Ok(true) => {
                        self.consolidate_inner_node(node_id, node_ptr, guard);
                        Err(())
                    }
                    Ok(false) => Ok(None),
                    _ => Err(()),
                }
            }
            Node::LeafRemove(removed_node_id) => {
                let removed_node_ptr = node.next.load(Ordering::Relaxed, guard);
                let removed_node = unsafe { removed_node_ptr.deref() };

                // try to remove this node by publishing a node merge delta with the left sibling
                let (left_sibling_id, left_sibling_ptr) =
                    self.move_left(node_id, node_ptr, path, guard)?;

                let left_sibling = unsafe { left_sibling_ptr.deref() };
                let merge_key = match &left_sibling.high_key {
                    Some(high_key) => high_key,
                    _ => unreachable!(),
                };

                let delta = Owned::new(TreeNode::<K, V>::new_leaf_merge(
                    &merge_key,
                    *removed_node_id,
                    removed_node_ptr,
                    left_sibling,
                    removed_node,
                ));

                delta.next.store(left_sibling_ptr, Ordering::Relaxed);

                match self.cas_node(left_sibling_id, left_sibling_ptr, delta, guard) {
                    Ok(new_left_sibling) => {
                        // CAS succeeds, this also prevents other threads from completing this remove SMO because the right link
                        // of the left sibling will no longer match the removed node

                        // continue to complete the merge SMO
                        self.complete_smo(left_sibling_id, new_left_sibling, path, guard)
                    }
                    _ => Err(()),
                }
            }
            Node::InnerRemove(removed_node_id) => {
                let removed_node_ptr = node.next.load(Ordering::Relaxed, guard);
                let removed_node = unsafe { removed_node_ptr.deref() };

                // try to remove this node by publishing a node merge delta with the left sibling
                let (left_sibling_id, left_sibling_ptr) =
                    self.move_left(node_id, node_ptr, path, guard)?;

                let left_sibling = unsafe { left_sibling_ptr.deref() };
                let merge_key = match &left_sibling.high_key {
                    Some(high_key) => high_key,
                    _ => unreachable!(),
                };

                let delta = Owned::new(TreeNode::<K, V>::new_inner_merge(
                    &merge_key,
                    *removed_node_id,
                    removed_node_ptr,
                    left_sibling,
                    removed_node,
                ));

                delta.next.store(left_sibling_ptr, Ordering::Relaxed);

                match self.cas_node(left_sibling_id, left_sibling_ptr, delta, guard) {
                    Ok(new_left_sibling) => {
                        // CAS succeeds, this also prevents other threads from completing this remove SMO because the right link
                        // of the left sibling will no longer match the removed node

                        // continue to complete the merge SMO
                        self.complete_smo(left_sibling_id, new_left_sibling, path, guard)
                    }
                    _ => Err(()),
                }
            }
            Node::LeafMerge(merge_key, removed_node_id, removed_node_atomic) => {
                let delete_item = (merge_key, *removed_node_id);
                let remove_node_ptr = removed_node_atomic.load(Ordering::Relaxed, guard);

                match self.complete_merge_smo(
                    node_id,
                    node_ptr,
                    delete_item,
                    remove_node_ptr, // removed node
                    path,
                    guard,
                ) {
                    Ok(true) => {
                        self.consolidate_leaf_node(node_id, node_ptr, guard);
                        Err(())
                    }
                    Ok(false) => Ok(None),
                    _ => Err(()),
                }
            }
            Node::InnerMerge(merge_key, removed_node_id, removed_node_atomic) => {
                let delete_item = (merge_key, *removed_node_id);
                let remove_node_ptr = removed_node_atomic.load(Ordering::Relaxed, guard);

                match self.complete_merge_smo(
                    node_id,
                    node_ptr,
                    delete_item,
                    remove_node_ptr, // removed node
                    path,
                    guard,
                ) {
                    Ok(true) => {
                        self.consolidate_inner_node(node_id, node_ptr, guard);
                        Err(())
                    }
                    Ok(false) => Ok(None),
                    _ => Err(()),
                }
            }
            _ => Ok(None),
        }
    }

    fn move_left<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<(NodeID, Shared<'g, TreeNode<K, V>>), ()> {
        let (parent_node_id, parent_node_ptr) = &path[path.len() - 1];
        let leftmost_child = unsafe { parent_node_ptr.deref().leftmost_child };
        // do not merge leftmost leaf
        if leftmost_child == node_id {
            return Err(());
        }

        if parent_node_ptr != &self.load_node_no_check(*parent_node_id, Ordering::Acquire, guard) {
            // parent has changed
            return Err(());
        }

        let node = unsafe { node_ptr.deref() };
        let low_key = match &node.low_key {
            Some(low_key) => low_key,
            None => unreachable!(), // the node is not the leftmost leaf so low key must be valid
        };

        // refind the left sibling in parent node
        let mut delta_ptr = *parent_node_ptr;
        // collect all items on the delta chain whose keys are <= low_key
        let mut delta_items = Vec::new();
        let left_sibling_id = loop {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Inner(ref items) => {
                    // use upper bound to avoid underflow in case the left sibling is the leftmost child
                    // +1 to avoid underflow
                    let low_slot = self.binary_search_key(items, low_key, 0, items.len(), true);

                    // the last item of (items[..low_key] U delta_items) is the removed node that we are looking for
                    // we need to take one step back and take the second item
                    let mut remaining = 2; // one step back + the second item
                    let mut item_it = items.iter().take(low_slot).rev().cloned();
                    let mut delta_it = delta_items.into_iter().rev();
                    let mut left_sibling_id = 0;

                    let mut item = item_it.next();
                    let mut delta_item = delta_it.next();

                    while remaining > 0 {
                        match (item, delta_item) {
                            (None, None) => {
                                // both streams are drain -- use the leftmost child
                                left_sibling_id = leftmost_child;
                                remaining -= 1;
                                item = None;
                                delta_item = None;
                            }
                            (Some((_, v)), None) => {
                                // take from base node
                                left_sibling_id = v;
                                delta_item = None;
                                item = item_it.next();
                                remaining -= 1;
                            }
                            (None, Some((_, v, insert))) => {
                                if insert {
                                    left_sibling_id = v;
                                    remaining -= 1;
                                }
                                delta_item = delta_it.next();
                                item = None;
                            }
                            (Some((cur_key, cur_id)), Some((delta_key, delta_id, insert))) => {
                                // compare the last items on delta chain and in the base node
                                match (self.key_comparator)(&delta_key, &cur_key) {
                                    std::cmp::Ordering::Equal => {
                                        // deleted or overwritten item in the base node
                                        if insert {
                                            left_sibling_id = delta_id;
                                            remaining -= 1;
                                        }

                                        item = item_it.next();
                                        delta_item = delta_it.next();
                                    }
                                    std::cmp::Ordering::Less => {
                                        // take from the base node
                                        left_sibling_id = cur_id;
                                        item = item_it.next();
                                        delta_item = Some((delta_key, delta_id, insert));
                                        remaining -= 1;
                                    }
                                    std::cmp::Ordering::Greater => {
                                        // take from delta chain
                                        if insert {
                                            left_sibling_id = delta_id;
                                            remaining -= 1;
                                        }
                                        item = Some((cur_key.clone(), cur_id));
                                        delta_item = delta_it.next();
                                    }
                                }
                            }
                        }
                    }

                    break left_sibling_id;
                }
                Node::InnerInsert((key, node_id), _, _) => {
                    if (self.key_comparator)(key, low_key) <= std::cmp::Ordering::Equal {
                        let tuple = (key.clone(), *node_id, true);
                        let pos = binary_search(
                            &delta_items,
                            &tuple,
                            0,
                            delta_items.len(),
                            false,
                            |(k1, _, _), (k2, _, _)| (self.key_comparator)(k1, k2),
                        );

                        if pos >= delta_items.len()
                            || (self.key_comparator)(&delta_items[pos].0, key)
                                != std::cmp::Ordering::Equal
                        {
                            delta_items.insert(pos, tuple);
                        }
                    }
                }
                Node::InnerDelete((key, node_id), _, _, _) => {
                    if (self.key_comparator)(key, low_key) <= std::cmp::Ordering::Equal {
                        let tuple = (key.clone(), *node_id, false);
                        let pos = binary_search(
                            &delta_items,
                            &tuple,
                            0,
                            delta_items.len(),
                            false,
                            |(k1, _, _), (k2, _, _)| (self.key_comparator)(k1, k2),
                        );

                        if pos >= delta_items.len()
                            || (self.key_comparator)(&delta_items[pos].0, key)
                                != std::cmp::Ordering::Equal
                        {
                            delta_items.insert(pos, tuple);
                        }
                    }
                }
                Node::InnerSplit(..) => {}
                Node::InnerMerge(..) => {
                    match self.consolidate_inner_node(node_id, delta_ptr, guard) {
                        Some(node_ptr) => {
                            delta_ptr = node_ptr;
                            delta_items.clear();
                            continue;
                        }
                        _ => return Err(()),
                    }
                }
                _ => unreachable!(),
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        };

        let left_sibling = self.load_node(left_sibling_id, Ordering::Acquire, path, guard)?;

        if unsafe { left_sibling.deref().right_link } != node_id {
            Err(())
        } else {
            Ok((left_sibling_id, left_sibling))
        }
    }

    fn complete_split_smo<'g>(
        &self,
        node_id: NodeID,
        insert_item: (&K, NodeID),
        next_item: &Option<K>,
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
                None,
                None,
                node_id,
                INVALID_NODE_ID,
                vec![(insert_key.clone(), insert_id)],
            );

            self.store_node(new_root_id, Some(new_root), Ordering::Relaxed);

            if !self.cas_root_node(node_id, new_root_id) {
                // CAS failed, schedule to destroy the new root
                let new_root_ptr = self.load_node_no_check(new_root_id, Ordering::Relaxed, guard);
                // self.store_node(new_root_id, None, Ordering::Relaxed);
                // self.free_node_ids.push(new_root_id);
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
        next_key: &Option<K>,
        slot: usize,
        guard: &'g Guard,
    ) -> Result<bool, ()> {
        let parent_node = unsafe { parent_node_ptr.deref() };

        let delta = Owned::new(TreeNode::new_inner_insert(
            insert_item,
            next_key,
            slot,
            parent_node,
        ));

        delta.next.store(parent_node_ptr, Ordering::Relaxed);

        self.cas_node(parent_id, parent_node_ptr, delta, guard)
            .map(|_| true)
            .map_err(|_| ())
    }

    fn complete_merge_smo<'g>(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        delete_item: (&K, NodeID),
        right_sibling_ptr: Shared<TreeNode<K, V>>,
        path: &TreePath<'g, K, V>,
        guard: &'g Guard,
    ) -> Result<bool, ()> {
        let (parent_id, parent_node_ptr) = &path[path.len() - 1];
        let (merge_key, delete_id) = &delete_item;

        if *parent_node_ptr != self.load_node_no_check(*parent_id, Ordering::Acquire, guard) {
            return Err(());
        }

        let node = unsafe { node_ptr.deref() };
        let parent_node = unsafe { parent_node_ptr.deref() };
        let right_sibling = unsafe { right_sibling_ptr.deref() };

        match self.search_inner(*parent_node_ptr, merge_key, guard) {
            (slot, Some(_)) => {
                // delete the removed node from parent
                let delta = Owned::new(TreeNode::<K, V>::new_inner_delete(
                    merge_key,
                    *delete_id,
                    &node.low_key,
                    node_id,
                    &right_sibling.high_key,
                    right_sibling.right_link,
                    slot,
                    parent_node,
                ));

                delta.next.store(*parent_node_ptr, Ordering::Relaxed);

                match self.cas_node(*parent_id, *parent_node_ptr, delta, guard) {
                    Ok(_) => {
                        // CAS succeeds, all accesses to the removed node will be redirected to the left sibling
                        // from now on the removed node is only accessed through the guarded pointer in the node merge delta
                        // so we can schedule to destroy the removed node and reclaim its node ID now
                        let removed_node_ptr =
                            self.load_node_no_check(*delete_id, Ordering::Relaxed, guard);
                        self.destroy_delta_chain(removed_node_ptr, guard);
                        // self.store_node(*delete_id, None, Ordering::Release);
                        // self.free_node_ids.push(*delete_id);
                        Ok(true)
                    }
                    _ => Err(()),
                }
            }
            (_, None) => Ok(false),
        }
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
        loop {
            let (leaf_node_id, leaf_node_ptr, path) = match self.search(key, guard) {
                Ok(result) => result,
                _ => continue,
            };

            match self.traverse_leaf(leaf_node_id, leaf_node_ptr, key, &path, guard) {
                Ok(result) => return result,
                _ => continue,
            }
        }
    }

    /// Delete a key-value pair from the tree.
    pub fn delete(&self, key: &K, value: &V) -> bool {
        let guard = &epoch::pin();
        loop {
            let (leaf_id, leaf_node_ptr, exists, slot) = loop {
                let (leaf_id, leaf_node_ptr, path) = match self.search(key, guard) {
                    Ok(result) => result,
                    _ => continue,
                };

                match self.search_leaf(leaf_id, leaf_node_ptr, key, value, &path, guard) {
                    Ok(result) => break result,
                    _ => continue,
                }
            };

            if !exists {
                return false;
            }

            // install the insert delta
            let delta = Owned::new(TreeNode::new_leaf_delete(
                key,
                value.clone(),
                slot,
                exists,
                unsafe { leaf_node_ptr.deref() },
            ));

            delta.next.store(leaf_node_ptr, Ordering::Relaxed);

            if self.cas_node(leaf_id, leaf_node_ptr, delta, guard).is_ok() {
                break;
            }
        }

        true
    }
}

pub struct BwTree<K, V>(BwTreeImpl<K, V>)
where
    K: 'static + Clone + Ord + std::fmt::Debug,
    V: Clone + Eq + Hash + std::fmt::Debug;

impl<K, V> BwTree<K, V>
where
    K: 'static + Clone + Ord + std::fmt::Debug,
    V: Clone + Eq + Hash + std::fmt::Debug,
{
    pub fn new(mapping_table_size: usize) -> Self {
        Self(BwTreeImpl::new(mapping_table_size, Box::new(K::cmp)))
    }
}

impl<K, V> Deref for BwTree<K, V>
where
    K: Clone + Ord + std::fmt::Debug,
    V: Clone + Eq + Hash + std::fmt::Debug,
{
    type Target = BwTreeImpl<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Instant};

    #[test]
    fn can_insert_and_delete_values() {
        let tree = Arc::new(BwTree::new(1024 * 1024 * 10));

        const THREADS: u32 = 20;
        const SCALE: u32 = 5000000;

        let mut handles = Vec::new();
        let start = Instant::now();
        for i in 0..THREADS {
            let tree = tree.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..SCALE {
                    let key = i * SCALE + j;
                    tree.insert(&key, key + 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        println!(
            "Inserted {} data points in {} millisec(s), throughput: {} Mop/s",
            THREADS * SCALE,
            start.elapsed().as_millis(),
            (THREADS * SCALE) as f32 / (start.elapsed().as_millis()) as f32 / 1000.0
        );

        let mut handles = Vec::new();
        let start = Instant::now();
        for i in 0..THREADS {
            let tree = tree.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..SCALE {
                    let key = i * SCALE + j;
                    let vals = tree.get_values(&key);
                    assert_eq!(vals.len(), 1);
                    assert_eq!(vals[0], key + 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        println!(
            "Queried {} data points in {} millisec(s), throughput: {} Mop/s",
            THREADS * SCALE,
            start.elapsed().as_millis(),
            (THREADS * SCALE) as f32 / (start.elapsed().as_millis()) as f32 / 1000.0
        );

        let mut handles = Vec::new();
        let start = Instant::now();
        for i in 0..THREADS {
            let tree = tree.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..SCALE {
                    let key = i * SCALE + j;
                    assert!(tree.delete(&key, &(key + 1)));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let vals = tree.get_values(&1);
        assert_eq!(vals.len(), 0);

        println!(
            "Deleted {} data points in {} millisec(s), throughput: {} Mop/s",
            THREADS * SCALE,
            start.elapsed().as_millis(),
            (THREADS * SCALE) as f32 / (start.elapsed().as_millis()) as f32 / 1000.0
        );
    }
}
