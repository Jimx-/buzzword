use crate::{NodeID, INVALID_NODE_ID};

use crossbeam::epoch::{Atomic, Guard, Shared};

rental! {
    mod rentals {
        use super::*;

        #[rental]
        /// A shared pointer to an undo node protected by the epoch GC and the pin guard with
        /// which it is loaded.
        pub struct GuardedTreeNode<K, V>
        where
            K: 'static + Clone,
            V: 'static,
        {
            guard: Box<Guard>,
            node: Shared<'guard, TreeNode<K, V>>,
        }
    }
}

pub use self::rentals::GuardedTreeNode;

pub enum Node<K, V>
where
    K: 'static + Clone,
    V: 'static,
{
    Inner(Vec<(K, NodeID)>),
    InnerInsert(
        (K, NodeID), /* inserted item */
        (K, NodeID), /* next item */
    ),
    InnerDelete(
        (K, NodeID), /* deleted item */
        (K, NodeID), /* previous item */
        (K, NodeID), /* next item */
    ),
    InnerSplit(K, NodeID /* deleted item */),
    InnerRemove(NodeID /* removed node ID */),
    InnerMerge(
        K,
        NodeID,                /* deleted item */
        GuardedTreeNode<K, V>, /* physical pointer to the deleted node */
    ),

    Leaf(Vec<(K, V)>),
    LeafInsert(K, V /* inserted item */),
    LeafDelete(K, V /* deleted item */),
    LeafSplit(K /* split key */, NodeID /* right sibling ID */),
    LeafRemove(NodeID /* removed node ID */),
    LeafMerge(
        K,                     /* delete key */
        NodeID,                /* deleted node ID */
        GuardedTreeNode<K, V>, /* physical pointer to the deleted node */
    ),
}

pub struct TreeNode<K, V>
where
    K: 'static + Clone,
    V: 'static,
{
    pub(super) low_key: K,
    pub(super) high_key: K,
    pub(super) leftmost_child: NodeID,
    pub(super) right_link: NodeID,
    pub(super) base_size: usize,
    pub(super) node: Node<K, V>,
    pub(super) next: Atomic<TreeNode<K, V>>,
}

impl<K, V> TreeNode<K, V>
where
    K: Clone,
{
    pub fn is_leaf(&self) -> bool {
        match self.node {
            Node::Leaf(..) => true,
            Node::LeafInsert(..) => true,
            Node::LeafDelete(..) => true,
            Node::LeafSplit(..) => true,
            Node::LeafRemove(..) => true,
            Node::LeafMerge(..) => true,
            _ => false,
        }
    }

    pub fn new_inner(
        low_key: K,
        high_key: K,
        leftmost_child: NodeID,
        right_link: NodeID,
        items: Vec<(K, NodeID)>,
    ) -> Self {
        Self {
            low_key,
            high_key,
            leftmost_child,
            right_link,
            base_size: items.len(),
            node: Node::Inner(items),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf(low_key: K, high_key: K, right_link: NodeID, items: Vec<(K, V)>) -> Self {
        Self {
            low_key,
            high_key,
            leftmost_child: INVALID_NODE_ID,
            right_link,
            base_size: items.len(),
            node: Node::Leaf(items),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf_insert(key: &K, value: V, next: &GuardedTreeNode<K, V>) -> Self {
        next.rent(|base_ptr| {
            let base = unsafe { base_ptr.deref() };
            Self {
                low_key: base.low_key.clone(),
                high_key: base.high_key.clone(),
                leftmost_child: base.leftmost_child,
                right_link: base.right_link,
                base_size: base.base_size,
                node: Node::LeafInsert(key.clone(), value),
                next: Atomic::null(),
            }
        })
    }
}
