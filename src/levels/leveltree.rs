use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;

const MAX_TREE_NODE_SIZE: usize = 512;
const MIN_TREE_NODE_SIZE: usize = 64;
const SPLIT_NODE_SIZE: usize = 384;
const MIN_MERGE_TREE_NODE_SIZE: usize = 256;

pub enum LevelOperation<T: ComparableNode> {
    Insert(T),
    Delete { key: Bytes, id: u64 },
}

pub trait ComparableNode: Clone {
    fn smallest(&self) -> &Bytes;
    fn largest(&self) -> &Bytes;
    fn id(&self) -> u64;
}

#[derive(Clone, Default)]
pub struct LeafNode<T: ComparableNode> {
    data: Vec<T>,
    key: Bytes,
}

#[derive(Clone, Default)]
pub struct LeafNodeBuilder<T: ComparableNode> {
    data: Vec<T>,
    delMap: HashSet<u64>,
    key: Bytes,
}

impl<T: ComparableNode> LeafNode<T> {
    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn merge(&self, other: Arc<LeafNode<T>>) -> Arc<LeafNode<T>> {
        let mut data = self.data.clone();
        for d in other.data.iter() {
            data.push(d.clone());
        }
        Arc::new(LeafNode {
            data,
            key: self.key.clone(),
        })
    }

    pub fn create_builder(&self) -> LeafNodeBuilder<T> {
        LeafNodeBuilder {
            data: self.data.clone(),
            key: self.key.clone(),
            delMap: HashSet::default(),
        }
    }
}

impl<T: ComparableNode> LeafNodeBuilder<T> {
    fn split_node(mut data: Vec<T>, left: Bytes) -> Vec<Arc<LeafNode<T>>> {
        let split_count = (data.len() + SPLIT_NODE_SIZE - 1) / SPLIT_NODE_SIZE;
        let split_size = data.len() / split_count;
        let mut last_idx = data.len() - split_size;
        let mut end_idx = data.len();
        let mut nodes = Vec::with_capacity(split_count);
        while last_idx >= split_size {
            let new_data = data[last_idx..end_idx].to_vec();
            let key = data[last_idx].smallest().clone();
            nodes.push(Arc::new(LeafNode {
                data: new_data,
                key,
            }));
            end_idx = last_idx;
            last_idx -= split_size;
        }
        data.truncate(end_idx);
        nodes.push(Arc::new(LeafNode { data, key: left }));
        nodes.reverse();
        nodes
    }

    pub fn build(mut self) -> Vec<Arc<LeafNode<T>>> {
        if self.delMap.is_empty() {
            self.data.sort_by(|a, b| a.smallest().cmp(b.smallest()));
            if self.data.len() <= MAX_TREE_NODE_SIZE {
                return vec![Arc::new(LeafNode {
                    data: self.data,
                    key: self.key,
                })];
            } else {
                return Self::split_node(self.data, self.key);
            }
        }
        let mut data = Vec::with_capacity(self.data.len());
        for d in self.data {
            if self.delMap.contains(&d.id()) {
                continue;
            }
            data.push(d);
        }
        data.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        if data.len() <= MAX_TREE_NODE_SIZE {
            vec![Arc::new(LeafNode {
                data,
                key: self.key,
            })]
        } else {
            Self::split_node(data, self.key)
        }
    }

    pub fn insert(&mut self, v: T) {
        self.data.push(v);
    }

    pub fn delete(&mut self, id: u64) {
        self.delMap.insert(id);
    }
}

#[derive(Clone)]
pub struct LevelTree<T: ComparableNode> {
    nodes: Vec<Arc<LeafNode<T>>>,
    prefix_sum: Vec<usize>,
}

pub struct LevelTreeIterator<T: ComparableNode> {
    tree: Arc<LevelTree<T>>,
    cursor_node: usize,
}

impl<T: ComparableNode> LevelTree<T> {
    pub fn new() -> Self {
        LevelTree::<T> {
            nodes: vec![Arc::new(LeafNode {
                data: vec![],
                key: Bytes::new(),
            })],
            prefix_sum: vec![0],
        }
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn at(&self, idx: usize) -> Option<T> {
        match self.prefix_sum.binary_search_by(|x| x.cmp(&idx)) {
            Ok(found) => {
                if found + 1 == self.prefix_sum.len() {
                    None
                } else {
                    Some(self.nodes[found + 1].data[idx - self.prefix_sum[found]].clone())
                }
            }
            Err(upper) => {
                if upper >= self.prefix_sum.len() {
                    None
                } else {
                    Some(self.nodes[upper].data[idx - self.prefix_sum[upper - 1]].clone())
                }
            }
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<T> {
        let mut leaf_idx = match self.nodes.binary_search_by(|node| node.key.cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper - 1,
        };
        match self.nodes[leaf_idx]
            .data
            .binary_search_by(|t| t.smallest().cmp(key))
        {
            Ok(idx) => Some(self.nodes[leaf_idx].data[idx].clone()),
            Err(upper) => {
                if upper > 0 && self.nodes[leaf_idx].data[upper - 1].largest() >= key {
                    Some(self.nodes[leaf_idx].data[upper - 1].clone())
                } else {
                    None
                }
            }
        }
    }

    pub fn update(&self, mut changes: Vec<LevelOperation<T>>) -> Self {
        changes.sort_by(|a, b| {
            let x = match a {
                LevelOperation::Insert(t) => t.smallest(),
                LevelOperation::Delete { key, .. } => key,
            };
            let y = match b {
                LevelOperation::Insert(t) => t.smallest(),
                LevelOperation::Delete { key, .. } => key,
            };
            x.cmp(y)
        });
        let change = changes.first().unwrap();
        let key = match change {
            LevelOperation::Insert(t) => t.smallest(),
            LevelOperation::Delete { key, .. } => key,
        };
        let mut idx = match self.nodes.binary_search_by(|node| node.key.cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper - 1,
        };
        let mut nodes = self.nodes.clone();
        nodes.truncate(idx);
        let mut leaf = self.nodes[idx].create_builder();
        let mut start = idx;
        for operation in changes {
            match operation {
                LevelOperation::Delete { key, id } => {
                    while idx + 1 < self.nodes.len() && key.ge(&self.nodes[idx + 1].key) {
                        nodes.append(&mut leaf.build());
                        leaf = self.nodes[idx + 1].create_builder();
                        idx += 1;
                    }
                    leaf.delete(id);
                }
                LevelOperation::Insert(table) => {
                    while idx + 1 < self.nodes.len()
                        && table.smallest().ge(&self.nodes[idx + 1].key)
                    {
                        nodes.append(&mut leaf.build());
                        leaf = self.nodes[idx + 1].create_builder();
                        idx += 1;
                    }
                    leaf.insert(table);
                }
            }
        }
        nodes.append(&mut leaf.build());
        for i in idx + 1..self.nodes.len() {
            nodes.push(self.nodes[i].clone());
        }
        while start <= idx && start < nodes.len() {
            Self::try_merge_tree_node(&mut nodes, start);
            start += 1;
        }
        let mut prefix_sum = vec![0; nodes.len()];
        prefix_sum[0] = nodes[0].size();
        for i in 1..prefix_sum.len() {
            prefix_sum[i] = prefix_sum[i - 1] + nodes[i].size();
        }
        LevelTree { nodes, prefix_sum }
    }

    fn try_merge_tree_node(nodes: &mut Vec<Arc<LeafNode<T>>>, mut idx: usize) {
        let node_size = nodes[idx].size();
        if idx > 0
            && ((node_size < MIN_TREE_NODE_SIZE
                && nodes[idx - 1].size() + node_size < MAX_TREE_NODE_SIZE)
                || nodes[idx - 1].size() + node_size < MIN_MERGE_TREE_NODE_SIZE)
        {
            let cur = nodes[idx].clone();
            for i in idx + 1..nodes.len() {
                nodes[i - 1] = nodes[i].clone();
            }
            nodes[idx] = cur.merge(nodes[idx].clone());
            nodes.pop();
        } else if idx + 1 < nodes.len()
            && ((node_size < MIN_TREE_NODE_SIZE
                && nodes[idx + 1].size() + node_size < MAX_TREE_NODE_SIZE)
                || nodes[idx + 1].size() + node_size < MIN_MERGE_TREE_NODE_SIZE)
        {
            nodes[idx] = nodes[idx].merge(nodes[idx + 1].clone());
            idx += 1;
            for i in idx + 1..nodes.len() {
                nodes[i - 1] = nodes[i].clone();
            }
            nodes.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct FakeTable {
        id: u64,
        smallest: Bytes,
        largest: Bytes,
    }

    impl ComparableNode for FakeTable {
        fn smallest(&self) -> &Bytes {
            &self.smallest
        }

        fn largest(&self) -> &Bytes {
            &self.largest
        }

        fn id(&self) -> u64 {
            self.id
        }
    }

    fn insert_to_tree(
        tree: LevelTree<FakeTable>,
        left: u64,
        right: u64,
        gap: u64,
    ) -> LevelTree<FakeTable> {
        let mut ops = vec![];
        for i in left..right {
            let smallest = i * gap;
            let largest = (i + 1) * gap;
            ops.push(LevelOperation::Insert(FakeTable {
                id: i,
                smallest: Bytes::from(smallest.to_string()),
                largest: Bytes::from(largest.to_string()),
            }));
        }
        tree.update(ops)
    }

    #[test]
    fn test_leveltree() {
        let tree = LevelTree::<FakeTable>::new();
        let tree = insert_to_tree(tree, 100, 228, 100);
        let t = tree.get(&Bytes::from("20000"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 200);
        let t = tree.get(&Bytes::from("20099"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 200);

        let tree = insert_to_tree(tree, 228, 700, 100);
        assert_eq!(tree.nodes.len(), 2);
        let t = tree.get(&Bytes::from("69999"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 699);

        let mut ops = vec![];
        for i in 100..=400 {
            let left = i * 100;
            ops.push(LevelOperation::Delete {
                id: i,
                key: Bytes::from(left.to_string()),
            });
        }
        let mut tree = tree.update(ops);
        let t = tree.get(&Bytes::from("20000"));
        assert!(t.is_none());

        let mut start = 1000;
        while start < 3000 {
            tree = insert_to_tree(tree, start, start + 400, 10);
            start += 400;
        }
        let t = tree.get(&Bytes::from("20000"));
        assert!(t.is_some());
    }
}
