extern crate crossbeam;

#[macro_use]
extern crate rental;

mod nodes;

pub(crate) type NodeID = u64;
pub(crate) const INVALID_NODE_ID: NodeID = 0;

use crate::nodes::{GuardedTreeNode, Node, TreeNode};

use crossbeam::{
    epoch::{self, Atomic, Guard, Owned, Pointer, Shared},
    queue::SegQueue,
};

use std::sync::atomic::{AtomicU64, Ordering};

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

type TreePath<K, V> = Vec<GuardedTreeNode<K, V>>;

pub struct BwTree<K, V>
where
    K: 'static + Default + Clone,
    V: 'static + Clone,
{
    mapping_table: Vec<Atomic<TreeNode<K, V>>>,
    root_id: AtomicU64,
    next_node_id: AtomicU64,
    free_node_ids: SegQueue<NodeID>,
}

impl<K, V> BwTree<K, V>
where
    K: Default + Clone,
    V: Clone,
{
    pub fn new(mapping_table_size: usize) -> Self {
        let mut mapping_table = Vec::new();
        mapping_table.resize_with(mapping_table_size, || Atomic::null());

        let tree = Self {
            mapping_table,
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
            self.mapping_table[id as usize].load(order, guard)
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

    fn search<F>(
        &self,
        key: &K,
        key_comparator: &F,
    ) -> (NodeID, GuardedTreeNode<K, V>, TreePath<K, V>)
    where
        F: Fn(&K, &K) -> std::cmp::Ordering,
    {
        let mut path = TreePath::<K, V>::new();
        let mut child_id = self.root_id.load(Ordering::Acquire);
        let mut node_ptr = self.load_node(child_id, Ordering::Acquire);

        loop {
            child_id = match node_ptr.rent(|node| {
                let node = unsafe { node.deref() };

                if node.is_leaf() {
                    None
                } else {
                    let child_id = Self::search_inner(node, key, key_comparator);
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

    fn search_inner<F>(inner: &TreeNode<K, V>, key: &K, key_comparator: &F) -> NodeID
    where
        F: Fn(&K, &K) -> std::cmp::Ordering,
    {
        let mut first = 1;
        let mut last = inner.base_size;

        match &inner.node {
            Node::Inner(items) => {
                let slot = Self::binary_search_key(items, &key, first, last, key_comparator) - 1;
                let (_, child_id) = items[slot];

                child_id
            }
            _ => unreachable!(),
        }
    }

    /// Traverse the delta chain of a leaf node and collect all matching values.
    fn traverse_leaf<F>(
        leaf: Shared<TreeNode<K, V>>,
        key: &K,
        key_comparator: &F,
        guard: &Guard,
    ) -> Vec<V>
    where
        F: Fn(&K, &K) -> std::cmp::Ordering,
    {
        let mut vals = Vec::new();
        let mut delta_ptr = leaf;

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };

            match &delta.node {
                Node::LeafInsert(k, v) => {
                    if key_comparator(k, key) == std::cmp::Ordering::Equal {
                        vals.push(v.clone());
                    }
                }
                _ => {}
            }

            delta_ptr = delta.next.load(Ordering::Acquire, guard);
        }

        vals
    }

    fn binary_search_key<T, F>(
        items: &Vec<(K, T)>,
        key: &K,
        first: usize,
        last: usize,
        key_comparator: F,
    ) -> usize
    where
        F: Fn(&K, &K) -> std::cmp::Ordering,
    {
        let mut low = first;
        let mut high = last;

        if low >= high {
            low
        } else {
            while low < high {
                let mid = low + (high - low) / 2;

                let (mid_key, _) = &items[mid];
                if key_comparator(&key, mid_key) == std::cmp::Ordering::Greater {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            low
        }
    }

    pub fn insert<F>(&self, key: &K, mut value: V, key_comparator: &F)
    where
        F: Fn(&K, &K) -> std::cmp::Ordering,
    {
        let guard = &epoch::pin();
        loop {
            let (leaf_id, leaf_node_ptr, _) = self.search(key, key_comparator);

            let delta = Owned::new(TreeNode::new_leaf_insert(key, value, &leaf_node_ptr));

            // install the insert delta
            value = match leaf_node_ptr.rent(|leaf_node| {
                delta.next.store(*leaf_node, Ordering::Relaxed);

                self.cas_node(leaf_id, *leaf_node, delta, guard)
            }) {
                Either::Left(_) => break, // CAS succeeds
                Either::Right(delta) => {
                    // if CAS failed, we need to re-take the ownership of the value from the delta
                    let TreeNode { node, .. } = *delta.into_box();

                    match node {
                        Node::LeafInsert(_, val) => val,
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    pub fn get_values<F>(&self, key: &K, key_comparator: &F) -> Vec<V>
    where
        F: Fn(&K, &K) -> std::cmp::Ordering,
    {
        let guard = &epoch::pin();
        let (_, leaf_node_ptr, _) = self.search(key, key_comparator);

        leaf_node_ptr.rent(|leaf_node| Self::traverse_leaf(*leaf_node, key, key_comparator, guard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_insert_values() {
        let tree = BwTree::<u32, u32>::new(1024 * 1024);
        let key_comparator = &u32::cmp;

        tree.insert(&1, 2, key_comparator);
        tree.insert(&2, 3, key_comparator);

        let vals = tree.get_values(&1, key_comparator);
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0], 2);
    }
}
