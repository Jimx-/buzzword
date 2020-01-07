extern crate crossbeam;

#[macro_use]
extern crate rental;

mod nodes;

pub(crate) type NodeID = u64;
pub(crate) const INVALID_NODE_ID: NodeID = 0;

const MAX_INNER_CHAIN_LENGTH: usize = 8;
const MAX_LEAF_CHAIN_LENGTH: usize = 8;

use crate::nodes::{GuardedTreeNode, Node, TreeNode};

use crossbeam::{
    epoch::{self, Atomic, Guard, Owned, Pointer, Shared},
    queue::SegQueue,
};

use std::sync::atomic::{AtomicU64, Ordering};

enum Either<L, R> {
    Left(L),
    Right(R),
}

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

type TreePath<K, V> = Vec<GuardedTreeNode<K, V>>;

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
    K: Default + Clone,
    V: Clone,
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

        tree.init_nodes();
        tree
    }

    fn init_nodes(&self) {
        let root_id = self.get_next_node_id();
        self.root_id.store(root_id, Ordering::Relaxed);
        let first_leaf_id = self.get_next_node_id();

        let root_node = TreeNode::<K, V>::new_inner(
            Default::default(),
            Default::default(),
            first_leaf_id,
            INVALID_NODE_ID,
            vec![(Default::default(), first_leaf_id)],
        );

        let first_leaf_node = TreeNode::<K, V>::new_leaf(
            Default::default(),
            Default::default(),
            INVALID_NODE_ID,
            Vec::new(),
        );

        self.store_node(root_id, root_node, Ordering::Relaxed);
        self.store_node(first_leaf_id, first_leaf_node, Ordering::Relaxed);
    }

    fn get_next_node_id(&self) -> NodeID {
        match self.free_node_ids.pop() {
            Ok(id) => id,
            _ => self.next_node_id.fetch_add(1, Ordering::SeqCst),
        }
    }

    fn load_node(&self, id: NodeID, order: Ordering) -> GuardedTreeNode<K, V> {
        let guard = Box::new(epoch::pin());
        GuardedTreeNode::new(guard, |guard| {
            let node = self.mapping_table[id as usize].load(order, guard);

            // once the consolidation starts, any further accesses to the same node will cause the thread to consolidate the node
            // and only the first thread that finishes the consolidation can publish the new base node
            self.try_consolidate_node(id, node, guard);

            node
        })
    }

    fn store_node(&self, id: NodeID, node: TreeNode<K, V>, order: Ordering) {
        self.mapping_table[id as usize].store(Owned::new(node), order);
    }

    fn cas_node<P>(
        &self,
        id: NodeID,
        cur: Shared<TreeNode<K, V>>,
        new: P,
        guard: &Guard,
    ) -> Either<(), P>
    where
        P: Pointer<TreeNode<K, V>>,
    {
        match self.mapping_table[id as usize].compare_and_set(cur, new, Ordering::AcqRel, guard) {
            Ok(_) => Either::Left(()),
            Err(e) => Either::Right(e.new),
        }
    }

    fn search(&self, key: &K) -> (NodeID, GuardedTreeNode<K, V>, TreePath<K, V>) {
        let mut path = TreePath::<K, V>::new();
        let mut child_id = self.root_id.load(Ordering::Acquire);
        let mut node_ptr = self.load_node(child_id, Ordering::Acquire);
        let guard = &epoch::pin();

        loop {
            child_id = match node_ptr.rent(|node| {
                if unsafe { node.deref() }.is_leaf() {
                    None
                } else {
                    let child_id = self.search_inner(*node, key, guard);
                    Some(child_id)
                }
            }) {
                None => break,
                Some(child_id) => child_id,
            };

            let child_node_ptr = self.load_node(child_id, Ordering::Acquire);

            path.push(node_ptr);
            node_ptr = child_node_ptr;
        }

        (child_id, node_ptr, path)
    }

    fn search_inner(&self, inner: Shared<TreeNode<K, V>>, key: &K, guard: &Guard) -> NodeID {
        let mut first = 1;
        let mut last = unsafe { inner.deref() }.base_size;
        let mut delta_ptr = inner;

        loop {
            if delta_ptr.is_null() {
                break INVALID_NODE_ID;
            }

            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Inner(items) => {
                    let slot = self.binary_search_key(items, &key, first, last, true) - 1;
                    let (_, child_id) = items[slot];

                    break child_id;
                }
                _ => {}
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
                    let lower_bound = self.binary_search_key(items, key, 0, items.len() - 1, false);

                    for (k, v) in (&items[lower_bound..]).into_iter() {
                        if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal {
                            vals.push(v.clone());
                        } else {
                            break;
                        }
                    }
                }
                Node::LeafInsert((k, v), _, _) => {
                    if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal {
                        vals.push(v.clone());
                    }
                }
                _ => {}
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        vals
    }

    /// Search for the key-value pair in the leaf node. If the pair is found return its slot otherwise return the slot it will take.
    fn search_leaf(
        &self,
        leaf: Shared<TreeNode<K, V>>,
        key: &K,
        value: &V,
        guard: &Guard,
    ) -> (bool, usize) {
        let mut delta_ptr = leaf;

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    let lower_bound = self.binary_search_key(items, key, 0, items.len(), false);

                    for (i, (k, v)) in (&items[lower_bound..]).into_iter().enumerate() {
                        if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal
                            && (self.val_eq)(v, value)
                        {
                            return (true, i + lower_bound);
                        }
                    }

                    return (false, lower_bound);
                }
                Node::LeafInsert((k, v), slot, overwrite) => {
                    if (self.key_comparator)(k, key) == std::cmp::Ordering::Equal
                        && (self.val_eq)(v, value)
                    {
                        return (*overwrite, *slot);
                    }
                }
                _ => {}
            }

            delta_ptr = delta.next.load(Ordering::Relaxed, guard);
        }

        (false, 0)
    }

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

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::Leaf(items) => {
                    let mut merged = Vec::new();
                    // merge items and deltas
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
    fn try_consolidate_node(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        guard: &Guard,
    ) {
        let node = unsafe { node_ptr.deref() };

        if !node.is_delta() {
            return;
        }

        if node.is_leaf() {
            if node.length >= MAX_LEAF_CHAIN_LENGTH {
                self.consolidate_leaf_node(node_id, node_ptr, guard);
            }
        } else {
            if node.length >= MAX_INNER_CHAIN_LENGTH {}
        }
    }

    fn consolidate_leaf_node(
        &self,
        node_id: NodeID,
        node_ptr: Shared<TreeNode<K, V>>,
        guard: &Guard,
    ) {
        let vals = self.collect_leaf_values(node_ptr, guard);

        let node = unsafe { node_ptr.deref() };
        let new_leaf = Owned::new(TreeNode::new_leaf(
            node.low_key.clone(),
            node.high_key.clone(),
            node.right_link,
            vals,
        ));

        if let Either::Left(_) = self.cas_node(node_id, node_ptr, new_leaf, guard) {
            self.destroy_delta_chain(node_ptr, guard);
        }
    }

    fn destroy_delta_chain<'g>(&self, mut node_ptr: Shared<'g, TreeNode<K, V>>, guard: &'g Guard) {
        while !node_ptr.is_null() {
            node_ptr = unsafe {
                let next = node_ptr.deref().next.load(Ordering::Relaxed, guard);
                guard.defer_destroy(node_ptr);
                next
            };
        }
    }

    pub fn insert(&self, key: &K, mut value: V) {
        let guard = &epoch::pin();
        loop {
            let (leaf_id, leaf_node_ptr, _) = self.search(key);

            // install the insert delta
            value = match leaf_node_ptr.rent(|leaf_node| {
                let (exists, slot) = self.search_leaf(*leaf_node, key, &value, guard);
                let delta = Owned::new(TreeNode::new_leaf_insert(
                    key,
                    value,
                    slot,
                    exists,
                    &leaf_node_ptr,
                ));

                delta.next.store(*leaf_node, Ordering::Relaxed);

                self.cas_node(leaf_id, *leaf_node, delta, guard)
            }) {
                Either::Left(_) => break, // CAS succeeds
                Either::Right(delta) => {
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

    pub fn get_values(&self, key: &K) -> Vec<V> {
        let guard = &epoch::pin();
        let (_, leaf_node_ptr, _) = self.search(key);

        leaf_node_ptr.rent(|leaf_node| self.traverse_leaf(*leaf_node, key, guard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_insert_values() {
        let tree = BwTree::<u32, u32, _, _>::new(1024 * 1024, u32::cmp, u32::eq);

        for i in 0..10 {
            tree.insert(&i, i + 1);
        }

        for i in 0..10 {
            let vals = tree.get_values(&i);
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], i + 1);
        }
    }
}
