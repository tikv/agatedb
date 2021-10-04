use bytes::Bytes;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

pub trait ComparableRecord: Clone {
    fn smallest(&self) -> &Bytes;
    fn largest(&self) -> &Bytes;
    fn id(&self) -> u64;
}

pub trait TreePage<T: ComparableRecord>: Clone {
    fn seek(&self, key: &Bytes) -> Option<T>;
    fn smallest(&self) -> &Bytes;
    fn largest(&self) -> &Bytes;
    fn split(&self) -> Vec<Arc<Self>>;
    fn merge(&self, other: &Self) -> Arc<Self>;
    fn size(&self) -> usize;
    fn record_number(&self) -> usize;
    fn insert(&mut self, records: Vec<T>);
    fn delete(&mut self, records: Vec<T>);
    fn max_page_size(&self) -> usize;
    fn min_merge_size(&self) -> usize {
        self.max_page_size() / 4
    }
    fn split_page_size(&self) -> usize {
        self.max_page_size() / 2
    }
}

#[derive(Clone, Default)]
pub struct LeafNode<T: ComparableRecord> {
    data: Vec<T>,
    smallest: Bytes,
    largest: Bytes,
    max_page_size: usize,
}

impl<T: ComparableRecord> TreePage<T> for LeafNode<T> {
    fn seek(&self, key: &Bytes) -> Option<T> {
        if self.data.is_empty() {
            return None;
        }

        let idx = match self.data.binary_search_by(|node| node.largest().cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper,
        };
        if idx >= self.data.len() {
            None
        } else {
            Some(self.data[idx].clone())
        }
    }

    fn smallest(&self) -> &Bytes {
        &self.smallest
    }

    fn largest(&self) -> &Bytes {
        &self.largest
    }

    fn split(&self) -> Vec<Arc<LeafNode<T>>> {
        let split_count = (self.data.len() + self.split_page_size() - 1) / self.split_page_size();
        let split_size = self.data.len() / split_count;
        let mut start_idx = 0;
        let mut end_idx = split_size;
        let mut nodes = vec![];
        while start_idx < self.data.len() {
            let new_data = self.data[start_idx..end_idx].to_vec();
            let key = if start_idx == 0 {
                self.smallest.clone()
            } else {
                self.data[start_idx].smallest().clone()
            };
            nodes.push(Arc::new(Self {
                data: new_data,
                smallest: key,
                largest: self.data[end_idx - 1].largest().clone(),
                max_page_size: self.max_page_size,
            }));
            start_idx += split_size;
            end_idx += split_size;
            if end_idx > self.data.len() {
                end_idx = self.data.len();
            }
        }
        nodes
    }

    fn merge(&self, other: &LeafNode<T>) -> Arc<LeafNode<T>> {
        let mut data = self.data.clone();
        for d in other.data.iter() {
            data.push(d.clone());
        }
        Arc::new(LeafNode {
            data,
            smallest: self.smallest.clone(),
            largest: other.largest.clone(),
            max_page_size: self.max_page_size,
        })
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn record_number(&self) -> usize {
        self.data.len()
    }

    fn insert(&mut self, mut tables: Vec<T>) {
        self.data.append(&mut tables);
        self.data.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        self.largest = self.data.last().unwrap().largest().clone();
    }

    fn delete(&mut self, tables: Vec<T>) {
        let mut del_map = HashSet::with_capacity(tables.len());
        for t in tables {
            del_map.insert(t.id());
        }
        let mut new_idx = 0;
        for cur in 0..self.data.len() {
            if del_map.contains(&self.data[cur].id()) {
                continue;
            }
            self.data[new_idx] = self.data[cur].clone();
            new_idx += 1;
        }
        self.data.truncate(new_idx);
    }

    fn max_page_size(&self) -> usize {
        self.max_page_size
    }
}

#[derive(Clone)]
pub struct LevelTreePage<R: ComparableRecord, P: TreePage<R>> {
    son: Vec<Arc<P>>,
    smallest: Bytes,
    largest: Bytes,
    record_number: usize,
    max_page_size: usize,
    _phantom: PhantomData<R>,
}

impl<R, P> TreePage<R> for LevelTreePage<R, P>
where
    R: ComparableRecord,
    P: TreePage<R>,
{
    fn seek(&self, key: &Bytes) -> Option<R> {
        if self.son.is_empty() {
            return None;
        }
        let idx = match self.son.binary_search_by(|node| node.smallest().cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => {
                if upper > 0 {
                    upper - 1
                } else {
                    upper
                }
            }
        };
        println!(
            "{},[{}, {}]",
            idx,
            String::from_utf8(self.son[idx].smallest().as_ref().to_vec()).unwrap(),
            String::from_utf8(self.son[idx].largest().as_ref().to_vec()).unwrap()
        );
        if let Some(t) = self.son[idx].seek(key) {
            return Some(t);
        }
        self.son[idx + 1].seek(key)
    }

    fn smallest(&self) -> &Bytes {
        &self.smallest
    }
    fn largest(&self) -> &Bytes {
        &self.largest
    }

    fn split(&self) -> Vec<Arc<Self>> {
        let split_count = (self.son.len() + self.split_page_size() - 1) / self.split_page_size();
        let split_size = self.son.len() / split_count;
        let mut start_idx = 0;
        let mut end_idx = split_size;
        let mut nodes = vec![];
        while start_idx < self.son.len() {
            let new_data = self.son[start_idx..end_idx].to_vec();
            let mut record_number = 0;
            for page in &new_data {
                record_number += page.record_number();
            }
            let key = if start_idx == 0 {
                self.smallest.clone()
            } else {
                self.son[start_idx].smallest().clone()
            };
            nodes.push(Arc::new(LevelTreePage {
                son: new_data,
                smallest: key,
                largest: self.son[end_idx - 1].largest().clone(),
                max_page_size: self.max_page_size,
                record_number,
                _phantom: Default::default(),
            }));
            start_idx += split_size;
            end_idx += split_size;
            if end_idx > self.son.len() {
                end_idx = self.son.len();
            }
        }
        nodes
    }

    fn merge(&self, other: &Self) -> Arc<Self> {
        let mut son = self.son.clone();
        for d in other.son.iter() {
            son.push(d.clone());
        }
        Arc::new(Self {
            son,
            smallest: self.smallest.clone(),
            largest: other.largest.clone(),
            record_number: self.record_number + other.record_number,
            max_page_size: self.max_page_size,
            _phantom: Default::default(),
        })
    }

    fn size(&self) -> usize {
        self.son.len()
    }

    fn record_number(&self) -> usize {
        self.record_number
    }

    fn insert(&mut self, records: Vec<R>) {
        if records.is_empty() {
            return;
        }
        let key = records.first().unwrap().smallest();
        let mut idx = match self.son.binary_search_by(|node| node.smallest().cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper - 1,
        };
        let mut cur_page = self.son[idx].as_ref().clone();
        let mut cur_records = Vec::with_capacity(records.len());
        let mut processed_count = records.len();
        for r in records {
            if idx + 1 < self.son.len() && r.smallest().ge(self.son[idx + 1].smallest()) {
                if !cur_records.is_empty() {
                    self.record_number -= cur_page.record_number();
                    cur_page.insert(cur_records);
                    self.record_number += cur_page.record_number();
                    cur_records = Vec::with_capacity(processed_count);
                    self.son[idx] = Arc::new(cur_page);
                    while idx + 1 < self.son.len() && r.smallest().ge(self.son[idx + 1].smallest())
                    {
                        idx += 1;
                    }
                    cur_page = self.son[idx].as_ref().clone();
                }
            }
            cur_records.push(r);
            processed_count -= 1;
        }
        if !cur_records.is_empty() {
            self.record_number -= cur_page.record_number();
            cur_page.insert(cur_records);
            self.record_number += cur_page.record_number();
            self.son[idx] = Arc::new(cur_page);
        }
        let mut idx = 0;
        let mut unsorted = false;
        let size = self.son.len();
        while idx < size {
            if self.son[idx].size() > self.son[idx].max_page_size() {
                let mut new_pages = self.son[idx].split();
                assert!(new_pages.len() > 1);
                self.son.append(&mut new_pages);
                let p = self.son.pop().unwrap();
                self.son[idx] = p;
                unsorted = true;
            }
            idx += 1;
        }
        if unsorted {
            self.son.sort_by(|a, b| a.smallest().cmp(b.smallest()));
            if self.son.first().unwrap().smallest().cmp(self.smallest()) == std::cmp::Ordering::Less
            {
                self.smallest = self.son.first().unwrap().smallest().clone();
            }
            self.largest = self.son.last().unwrap().largest().clone();
        }
    }

    fn delete(&mut self, records: Vec<R>) {
        if records.is_empty() {
            return;
        }
        let key = records.first().unwrap().smallest();
        let mut idx = match self.son.binary_search_by(|node| node.smallest().cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper - 1,
        };
        let mut cur_page = self.son[idx].as_ref().clone();
        let mut cur_records = Vec::with_capacity(records.len());
        let mut processed_count = records.len();
        for r in records {
            if idx + 1 < self.son.len() && r.smallest().ge(self.son[idx + 1].smallest()) {
                if !cur_records.is_empty() {
                    self.record_number -= cur_page.record_number();
                    cur_page.delete(cur_records);
                    self.record_number += cur_page.record_number();
                    cur_records = Vec::with_capacity(processed_count);
                    self.son[idx] = Arc::new(cur_page);
                    while idx + 1 < self.son.len() && r.smallest().ge(self.son[idx + 1].smallest())
                    {
                        idx += 1;
                    }
                    cur_page = self.son[idx].as_ref().clone();
                }
            }
            cur_records.push(r);
            processed_count -= 1;
        }
        if !cur_records.is_empty() {
            self.record_number -= cur_page.record_number();
            cur_page.delete(cur_records);
            self.record_number += cur_page.record_number();
            self.son[idx] = Arc::new(cur_page);
        }
        let mut new_idx = 1;
        let mut cur_idx = 1;
        let size = self.son.len();
        while cur_idx < size {
            if self.son[new_idx - 1].size() + self.son[cur_idx].size()
                < self.son[cur_idx].min_merge_size()
                || self.son[new_idx - 1].size() == 0
                || self.son[cur_idx].size() == 0
            {
                self.son[new_idx - 1] = self.son[new_idx - 1].merge(self.son[cur_idx].as_ref());
                cur_idx += 1;
            } else {
                self.son[new_idx] = self.son[cur_idx].clone();
                new_idx += 1;
                cur_idx += 1;
            }
        }
        if new_idx < self.son.len() {
            self.son.truncate(new_idx);
        }
        self.largest = self.son.last().unwrap().largest().clone();
    }

    fn max_page_size(&self) -> usize {
        self.max_page_size
    }
}

#[derive(Clone)]
pub struct LevelTree<T: ComparableRecord> {
    node: LevelTreePage<T, LevelTreePage<T, LeafNode<T>>>,
}

impl<T: ComparableRecord> LevelTree<T> {
    pub fn new(max_page_size: usize, leaf_max_page_size: usize) -> Self {
        Self {
            node: LevelTreePage::<T, LevelTreePage<T, LeafNode<T>>> {
                son: vec![Arc::new(LevelTreePage::<T, LeafNode<T>> {
                    son: vec![Arc::new(LeafNode::<T> {
                        data: vec![],
                        smallest: Bytes::new(),
                        largest: Bytes::new(),
                        max_page_size: leaf_max_page_size,
                    })],
                    smallest: Bytes::new(),
                    largest: Bytes::new(),
                    record_number: 0,
                    max_page_size,
                    _phantom: Default::default(),
                })],
                largest: Bytes::new(),
                smallest: Bytes::new(),
                max_page_size: 32,
                record_number: 0,
                _phantom: Default::default(),
            },
        }
    }

    pub fn size(&self) -> usize {
        self.node.record_number()
    }

    pub fn get(&self, key: &Bytes) -> Option<T> {
        self.node.seek(key)
    }

    pub fn replace(&self, mut to_del: Vec<T>, mut to_add: Vec<T>) -> Self {
        let mut node = self.node.clone();
        if !to_del.is_empty() {
            to_del.sort_by(|a, b| a.smallest().cmp(b.smallest()));
            node.delete(to_del);
        }
        if !to_add.is_empty() {
            to_add.sort_by(|a, b| a.smallest().cmp(b.smallest()));
            node.insert(to_add);
        }
        LevelTree { node }
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

    impl ComparableRecord for FakeTable {
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

    fn update_page<P: TreePage<FakeTable>>(
        page: &mut P,
        left: u64,
        right: u64,
        gap: u64,
        is_insert: bool,
    ) {
        let mut ops = vec![];
        for i in left..right {
            let smallest = i * gap;
            let largest = (i + 1) * gap - 1;
            ops.push(FakeTable {
                id: i,
                smallest: Bytes::from(smallest.to_string()),
                largest: Bytes::from(largest.to_string()),
            });
        }
        if is_insert {
            page.insert(ops);
        } else {
            page.delete(ops);
        }
    }

    #[test]
    fn test_leaf_page() {
        let mut page = LeafNode {
            data: vec![],
            smallest: Default::default(),
            largest: Default::default(),
            max_page_size: 120,
        };
        update_page(&mut page, 200, 300, 100, true);
        let p = page.seek(&Bytes::from("0".to_string()));
        assert_eq!(p.unwrap().id, 200);
        assert_eq!(page.record_number(), 100);
        assert_eq!(page.size(), 100);
        update_page(&mut page, 100, 200, 100, true);
        let p = page.seek(&Bytes::from("0".to_string()));
        assert_eq!(p.unwrap().id, 100);
        let p = page.seek(&Bytes::from("10099".to_string()));
        assert_eq!(p.unwrap().id, 100);
        let p = page.seek(&Bytes::from("29999".to_string()));
        assert_eq!(p.unwrap().id, 299);
        let p = page.seek(&Bytes::from("30000".to_string()));
        assert!(p.is_none());

        assert_eq!(page.record_number(), 200);
        assert_eq!(page.size(), 200);
        let pages = page.split();
        assert_eq!(pages.len(), 4);
        assert_eq!(pages[0].size(), 50);
        assert_eq!(pages[1].size(), 50);
        assert_eq!(pages[2].size(), 50);
        assert_eq!(pages[3].size(), 50);
        let mut page2 = pages[2].as_ref().clone();
        let mut page3 = pages[3].as_ref().clone();
        update_page(&mut page2, 215, 250, 100, false);
        update_page(&mut page3, 250, 290, 100, false);
        let page = page2.merge(&page3);
        assert_eq!(page.size(), 25);
        let p = page.seek(&Bytes::from("250".to_string()));
        assert_eq!(p.unwrap().id, 290);
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
            let largest = (i + 1) * gap - 1;
            ops.push(FakeTable {
                id: i,
                smallest: Bytes::from(smallest.to_string()),
                largest: Bytes::from(largest.to_string()),
            });
        }
        tree.replace(vec![], ops)
    }

    fn delete_from_tree(
        tree: LevelTree<FakeTable>,
        left: u64,
        right: u64,
        gap: u64,
    ) -> LevelTree<FakeTable> {
        let mut ops = vec![];
        for i in left..right {
            let smallest = i * gap;
            let largest = (i + 1) * gap - 1;
            ops.push(FakeTable {
                id: i,
                smallest: Bytes::from(smallest.to_string()),
                largest: Bytes::from(largest.to_string()),
            });
        }
        tree.replace(ops, vec![])
    }

    #[test]
    fn test_leveltree() {
        let tree = LevelTree::<FakeTable>::new(32, 64);
        let tree = insert_to_tree(tree, 100, 228, 100);
        let t = tree.get(&Bytes::from("20000"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 200);
        let t = tree.get(&Bytes::from("20099"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 200);

        let tree = insert_to_tree(tree, 228, 100 + 640, 100);
        assert_eq!(tree.node.son[0].record_number(), 640);
        assert_eq!(tree.node.son[0].size(), 20);
        let t = tree.get(&Bytes::from("69999"));
        assert!(t.is_some());
        assert_eq!(t.unwrap().id, 699);

        let mut tree = delete_from_tree(tree, 100, 400, 100);
        let t = tree.get(&Bytes::from("20000"));
        assert!(t.is_some());
        assert_eq!(tree.node.son[0].size(), 11);
        // 640 - 300
        assert_eq!(tree.node.son[0].record_number(), 340);
        assert_eq!(t.unwrap().id, 400);

        let mut start = 1000;
        while start < 3000 {
            tree = insert_to_tree(tree, start, start + 400, 10);
            start += 400;
        }

        // 640 + 2000 - 300
        assert_eq!(tree.node.size(), 7);
        assert_eq!(tree.node.record_number(), 2340);
        assert_eq!(tree.node.son[0].size(), 12);
        let t = tree.get(&Bytes::from("20000"));
        assert!(t.is_some());
    }
}
