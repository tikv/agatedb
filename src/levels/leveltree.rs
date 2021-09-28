use bytes::Bytes;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::sync::{Arc, Mutex};

const MAX_TREE_NODE_SIZE: usize = 512;
const MIN_TREE_NODE_SIZE: usize = 64;
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

#[derive(Clone)]
pub struct TreeNode<T: ComparableNode> {
    data: Vec<NodeType<T>>,
    key: Bytes,
}


impl<T: ComparableNode> LeafNode<T> {
    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn insert(&mut self, v: T) {
        self.data.push(v);
    }

    pub fn merge(&self, other: Arc<LeafNode<T>>) -> Arc<LeafNode<T>> {
        let mut data = self.data.clone();
        for d in other.data.iter() {
            data.push(d.clone());
        }
        Arc::new(LeafNode {
            data,
            key: self.key.clone()
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
    pub fn build(mut self) -> Vec<Arc<LeafNode<T>>> {
        if self.delMap.is_empty() {
            self.data.sort_by(|a, b| a.smallest().cmp(b.smallest()));
            if self.data.len() <= MAX_TREE_NODE_SIZE {
                return vec![Arc::new(LeafNode {
                    data: self.data,
                    key: self.key,
                })];
            } else {
                let mid = self.data.len() / 2;
                let right_data = self.data[mid..].to_vec();
                let right_key = self.data[mid].smallest().clone();
                self.data.truncate(mid);
                return vec![
                    Arc::new(LeafNode {
                        data: self.data,
                        key: self.key,
                    }),
                    Arc::new(LeafNode {
                        data: right_data,
                        key: right_key,
                    }),
                ];
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

        if data.len() > MAX_TREE_NODE_SIZE {
            vec![Arc::new(LeafNode {
                data,
                key: self.key,
            })]
        } else {
            let mid = data.len() / 2;
            let right_data = data[mid..].to_vec();
            let right_key = data[mid].smallest().clone();
            data.truncate(mid);
            vec![
                Arc::new(LeafNode {
                    data,
                    key: self.key,
                }),
                Arc::new(LeafNode {
                    data: right_data,
                    key: right_key,
                }),
            ];
        }
    }

    pub fn delete(&mut self, id: u64) {
        self.delMap.insert(id);
    }
}

#[derive(Clone)]
pub struct LevelTree<T: ComparableNode> {
    nodes: Vec<Arc<LeafNode<T>>>,
}

impl<T: ComparableNode> LevelTree<T> {
    pub fn new() -> Self {
        LevelTree::<T> {
            nodes: vec![Arc::new(LeafNode {
                data: vec![],
                key: Bytes::new(),
            })],
        }
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn get(&self, key: &Bytes) -> Option<T> {
        let mut leaf_idx = match self.nodes.binary_search_by(|node| node.key.cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper - 1,
        };
        match self.nodes[leaf_idx].data.binary_search_by(|t| t.smallest().cmp(key)) {
            Ok(idx) => Some(self.nodes[leaf_idx].data[idx].clone()),
            Err(upper) => {
                if self.nodes[leaf_idx].data[upper - 1].largest() >= key {
                    Some(self.nodes[leaf_idx].data[upper - 1].clone())
                } else {
                    None
                }
            }
        }
    }

    pub fn update(&self, mut changes: Vec<LevelOperation<T>>) -> Self {
        changes.sort_by_key(|op| match op {
            LevelOperation::Insert(t) => t.smallest(),
            LevelOperation::Delete { key, id } => key,
        });
        let change = changes.first().unwrap();
        let key = match change {
            LevelOperation::Insert(t) => t.smallest(),
            LevelOperation::Delete { key, id } => key,
        };
        let mut idx = match self.nodes.binary_search_by(|node| node.key.cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper - 1,
        };
        let mut nodes = self.nodes[0..idx].to_vec();
        let mut leaf = self.nodes[idx].create_builder();
        let mut start = idx;
        for operation in changes {
            match operation {
                LevelOperation::Delete { key, id } => {
                    while idx + 1 < self.nodes.len() && key.ge(&self.nodes[cur + 1].key) {
                        nodes.append(&mut leaf.build());
                        leaf = self.nodes[idx + 1].create_builder();
                        idx += 1;
                    }
                    leaf.delete(id);
                }
                LevelOperation::Insert(table) => {
                    while idx + 1 < self.nodes.len() && table.smallest().ge(&self.nodes[cur + 1].key) {
                        nodes.append(&mut leaf.build());
                        leaf = self.nodes[idx + 1].create_builder();
                        idx += 1;
                    }
                    leaf.insert(table);
                }
            }
        }
        nodes.append(&mut leaf.build());
        for i in idx+1..self.nodes.len() {
            nodes.push(self.nodes[i].clone());
        }
        for i in start..=idx {
            Self::try_merge_tree_node(&mut nodes, i);
        }
        LevelTree {
            nodes,
        }
    }

    fn try_merge_tree_node(nodes: &mut Vec<Arc<LeafNode<T>>>, mut idx: usize) {
        let node_size = nodes[idx].size();
        if idx > 0
            && ((node_size < MIN_MERGE_TREE_NODE_SIZE
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
            && ((node_size < MIN_MERGE_TREE_NODE_SIZE
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

    #[test]
    fn test_manifest_recover() {
        let mut tree = LevelTree::<FakeTable>::new();
        let mut ops = vec![];
        for i in 0..128 {
            let left = i * 100;
            let right = (i + 1) * 100;
            ops.push(LevelOperation::Insert(FakeTable {
                id: i,
                smallest: Bytes::from(left.to_string()),
                largest: Bytes::from(right.to_string()),
            }));
        }
        let tree = tree.update(ops);
        let t = tree.get(&Bytes::from("200"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 2);
        let t = tree.get(&Bytes::from("299"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 2);
        let mut ops = vec![];
        for i in 128..600 {
            let left = i * 100;
            let right = (i + 1) * 100;
            ops.push(LevelOperation::Insert(FakeTable {
                id: i,
                smallest: Bytes::from(left.to_string()),
                largest: Bytes::from(right.to_string()),
            }));
        }
        let tree = tree.update(ops);
        assert_eq!(tree.nodes.len(), 2);
    }
}

