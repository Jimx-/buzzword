use crate::{NodeID, INVALID_NODE_ID};

use crossbeam::epoch::{Atomic, Shared};

use std::sync::atomic::Ordering;

pub enum Node<K, V>
where
    K: Clone,
{
    Inner(Vec<(K, NodeID)>),
    InnerInsert(
        (K, NodeID), /* inserted item */
        Option<K>,   /* next key */
        usize,       /* insert location */
    ),
    InnerDelete(
        (K, NodeID),         /* deleted item */
        (Option<K>, NodeID), /* previous item */
        (Option<K>, NodeID), /* next item */
        usize,               /* slot in base node */
    ),
    InnerSplit(K /* split key */, NodeID /* right sibling ID */),
    InnerRemove(NodeID /* removed node ID */),
    InnerMerge(
        K,                      /* deleted key */
        NodeID,                 /* deleted node ID */
        Atomic<TreeNode<K, V>>, /* physical pointer to the merged node */
    ),

    Leaf(Vec<(K, V)>),
    LeafInsert(
        (K, V), /* inserted item */
        usize,  /* slot in base node */
        bool,   /* overwrite the existing value? */
    ),
    LeafDelete(
        (K, V), /* deleted item */
        usize,  /* slot in base node */
        bool,   /* deleted value exists? */
    ),
    LeafSplit(K /* split key */, NodeID /* right sibling ID */),
    LeafRemove(NodeID /* removed node ID */),
    LeafMerge(
        K,                      /* delete key */
        NodeID,                 /* deleted node ID */
        Atomic<TreeNode<K, V>>, /* physical pointer to the merged node */
    ),
}

pub struct TreeNode<K, V>
where
    K: Clone,
{
    pub(super) low_key: Option<K>,
    pub(super) high_key: Option<K>,
    pub(super) leftmost_child: NodeID,
    pub(super) right_link: NodeID,
    /// Size of the base node. Used for adjusting the range of binary search during delta chain traversal.
    pub(super) base_size: usize,
    /// Logical item count of the delta node.
    pub(super) item_count: usize,
    /// Length of the delta chain following this node.
    pub(super) length: usize,
    pub(super) node: Node<K, V>,
    /// Next node on the delta chain.
    pub(super) next: Atomic<TreeNode<K, V>>,
}

impl<K, V> TreeNode<K, V>
where
    K: Clone,
{
    #[inline(always)]
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

    #[inline(always)]
    pub fn is_delta(&self) -> bool {
        match self.node {
            Node::Leaf(..) => false,
            Node::Inner(..) => false,
            _ => true,
        }
    }

    pub fn new_inner(
        low_key: Option<K>,
        high_key: Option<K>,
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
            item_count: items.len(),
            length: 0,
            node: Node::Inner(items),
            next: Atomic::null(),
        }
    }

    pub fn new_inner_insert(
        insert_item: (&K, NodeID),
        next_key: &Option<K>,
        slot: usize,
        next: &TreeNode<K, V>,
    ) -> Self {
        let (insert_key, insert_id) = insert_item;

        Self {
            low_key: next.low_key.clone(),
            high_key: next.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: next.right_link,
            base_size: next.base_size,
            item_count: next.item_count + 1,
            length: next.length + 1,
            node: Node::InnerInsert((insert_key.clone(), insert_id), next_key.clone(), slot),
            next: Atomic::null(),
        }
    }

    pub fn new_inner_delete(
        delete_key: &K,
        delete_id: NodeID,
        prev_key: &Option<K>,
        prev_id: NodeID,
        next_key: &Option<K>,
        next_id: NodeID,
        slot: usize,
        next: &TreeNode<K, V>,
    ) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: next.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: next.right_link,
            base_size: next.base_size,
            item_count: next.item_count - 1,
            length: next.length + 1,
            node: Node::InnerDelete(
                (delete_key.clone(), delete_id),
                (prev_key.clone(), prev_id),
                (next_key.clone(), next_id),
                slot,
            ),
            next: Atomic::null(),
        }
    }

    pub fn new_inner_split(
        split_key: &K,
        sibling_id: NodeID,
        next: &TreeNode<K, V>,
        sibling: &TreeNode<K, V>,
    ) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: Some(split_key.clone()),
            leftmost_child: next.leftmost_child,
            right_link: sibling_id,
            base_size: next.base_size,
            item_count: next.item_count - sibling.item_count,
            length: next.length,
            node: Node::InnerSplit(split_key.clone(), sibling_id),
            next: Atomic::null(),
        }
    }

    pub fn new_inner_remove(node_id: NodeID, next: &TreeNode<K, V>) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: next.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: next.right_link,
            base_size: next.base_size,
            item_count: next.item_count,
            length: next.length,
            node: Node::InnerRemove(node_id),
            next: Atomic::null(),
        }
    }

    pub fn new_inner_merge<'g>(
        merge_key: &K,
        removed_node_id: NodeID,
        removed_node_ptr: Shared<TreeNode<K, V>>,
        next: &TreeNode<K, V>,
        removed_node: &TreeNode<K, V>,
    ) -> Self {
        let link = Atomic::null();
        link.store(removed_node_ptr, Ordering::Relaxed);

        Self {
            low_key: next.low_key.clone(),
            high_key: removed_node.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: removed_node.right_link,
            base_size: next.base_size, // unused in leaf merge node
            item_count: next.item_count + removed_node.item_count,
            length: next.length + removed_node.length,
            node: Node::InnerMerge(merge_key.clone(), removed_node_id, link),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf(
        low_key: Option<K>,
        high_key: Option<K>,
        right_link: NodeID,
        items: Vec<(K, V)>,
    ) -> Self {
        Self {
            low_key,
            high_key,
            leftmost_child: INVALID_NODE_ID,
            right_link,
            base_size: items.len(),
            item_count: items.len(),
            length: 0,
            node: Node::Leaf(items),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf_insert(
        key: &K,
        value: V,
        slot: usize,
        overwrite: bool,
        next: &TreeNode<K, V>,
    ) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: next.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: next.right_link,
            base_size: next.base_size,
            item_count: next.item_count + 1,
            length: next.length + 1,
            node: Node::LeafInsert((key.clone(), value), slot, overwrite),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf_delete(
        key: &K,
        value: V,
        slot: usize,
        overwrite: bool,
        next: &TreeNode<K, V>,
    ) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: next.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: next.right_link,
            base_size: next.base_size,
            item_count: next.item_count - 1,
            length: next.length + 1,
            node: Node::LeafDelete((key.clone(), value), slot, overwrite),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf_split(
        split_key: &K,
        sibling_id: NodeID,
        next: &TreeNode<K, V>,
        sibling: &TreeNode<K, V>,
    ) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: Some(split_key.clone()),
            leftmost_child: next.leftmost_child,
            right_link: sibling_id,
            base_size: next.base_size,
            item_count: next.item_count - sibling.item_count,
            length: next.length,
            node: Node::LeafSplit(split_key.clone(), sibling_id),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf_remove(node_id: NodeID, next: &TreeNode<K, V>) -> Self {
        Self {
            low_key: next.low_key.clone(),
            high_key: next.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: next.right_link,
            base_size: next.base_size,
            item_count: next.item_count,
            length: next.length,
            node: Node::LeafRemove(node_id),
            next: Atomic::null(),
        }
    }

    pub fn new_leaf_merge<'g>(
        merge_key: &K,
        removed_node_id: NodeID,
        removed_node_ptr: Shared<TreeNode<K, V>>,
        next: &TreeNode<K, V>,
        removed_node: &TreeNode<K, V>,
    ) -> Self {
        let link = Atomic::null();
        link.store(removed_node_ptr, Ordering::Relaxed);

        Self {
            low_key: next.low_key.clone(),
            high_key: removed_node.high_key.clone(),
            leftmost_child: next.leftmost_child,
            right_link: removed_node.right_link,
            base_size: next.base_size, // unused in leaf merge node
            item_count: next.item_count + removed_node.item_count,
            length: next.length + removed_node.length,
            node: Node::LeafMerge(merge_key.clone(), removed_node_id, link),
            next: Atomic::null(),
        }
    }
}
