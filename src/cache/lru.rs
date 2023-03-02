use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::Mutex,
};

use hashbrown::raw::RawTable;

use super::{utils::hash_key, Cache, CacheHandle, CacheShard, Options};

const FLAG_IN_CACHE: u8 = 1;
const FLAG_DUMMY: u8 = 1 << 1;

struct LRUNode<K, V> {
    hash: u64,
    charge: usize,
    ref_count: u32,
    flag: u8,
    next: NodePtr<K, V>,
    prev: NodePtr<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<K, V> LRUNode<K, V> {
    fn dummy() -> Self {
        unsafe {
            let mut node = MaybeUninit::assume_init(MaybeUninit::<LRUNode<K, V>>::uninit());
            node.flag = FLAG_DUMMY;
            node
        }
    }

    fn new(key: K, value: V, charge: usize, hash: u64) -> Self {
        Self {
            hash,
            charge,
            ref_count: 1,
            flag: 0,
            next: NodePtr(NonNull::dangling()),
            prev: NodePtr(NonNull::dangling()),
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }

    fn in_cache(&self) -> bool {
        self.flag & FLAG_IN_CACHE != 0
    }

    fn set_in_cache(&mut self, in_cache: bool) {
        if in_cache {
            self.flag |= FLAG_IN_CACHE;
        } else {
            self.flag &= !FLAG_IN_CACHE;
        }
    }

    fn incr_ref(&mut self) -> u32 {
        self.ref_count += 1;
        self.ref_count
    }

    fn decr_ref(&mut self) -> u32 {
        self.ref_count -= 1;
        self.ref_count
    }
}

impl<K, V> Drop for LRUNode<K, V> {
    fn drop(&mut self) {
        if self.flag & FLAG_DUMMY == 0 {
            unsafe {
                ManuallyDrop::drop(&mut self.key);
                ManuallyDrop::drop(&mut self.value);
            }
        }
    }
}

impl<K, V> PartialEq for LRUNode<K, V>
where
    K: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.key.eq(&other.key)
    }
}

struct NodePtr<K, V>(NonNull<LRUNode<K, V>>);

impl<K, V> PartialEq for NodePtr<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 // cmp the address
    }
}

impl<K, V> From<*mut LRUNode<K, V>> for NodePtr<K, V> {
    fn from(n: *mut LRUNode<K, V>) -> Self {
        unsafe { Self(NonNull::new_unchecked(n)) }
    }
}

impl<K, V> Clone for NodePtr<K, V> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<K, V> Deref for NodePtr<K, V> {
    type Target = LRUNode<K, V>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl<K, V> DerefMut for NodePtr<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

pub struct LRUHandle<'a, K, V, S: Send + Sync> {
    node: NodePtr<K, V>,
    lru: &'a LRU<K, V, S>,
}

impl<K, V, S> CacheHandle for LRUHandle<'_, K, V, S>
where
    S: Send + Sync,
{
    type Value = V;

    fn value(&self) -> &Self::Value {
        &self.node.value
    }
}

impl<K, V, S> Drop for LRUHandle<'_, K, V, S>
where
    S: Send + Sync,
{
    fn drop(&mut self) {
        let mut lru = self.lru.inner.lock().unwrap();
        lru.unref_node(self.node.clone());
    }
}

pub struct LRU<K, V, S = RandomState> {
    inner: Mutex<LRUInner<K, V>>,
    hash_builder: S,
}

struct LRUInner<K, V> {
    cap: usize,
    usage: usize,
    map: RawTable<NodePtr<K, V>>,
    dummy: NodePtr<K, V>,
    in_use_dummy: NodePtr<K, V>,
}

///! this might not be safe......
unsafe impl<K, V> Send for LRUInner<K, V> {}

impl<K, V> LRUInner<K, V> {
    fn new(capacity: usize) -> Self {
        let mut dummy = NodePtr::from(Box::into_raw(Box::new(LRUNode::dummy())));
        dummy.next = dummy.clone();
        dummy.prev = dummy.clone();
        let mut in_use_dummy = NodePtr::from(Box::into_raw(Box::new(LRUNode::dummy())));
        in_use_dummy.next = in_use_dummy.clone();
        in_use_dummy.prev = in_use_dummy.clone();
        Self {
            cap: capacity,
            usage: 0,
            map: RawTable::new(),
            dummy,
            in_use_dummy,
        }
    }

    fn list_remove_node(mut node: NodePtr<K, V>) {
        node.next.prev = node.prev.clone();
        node.prev.next = node.next.clone();
    }

    fn append(list: NodePtr<K, V>, mut node: NodePtr<K, V>) {
        node.next = list.clone();
        node.prev = list.prev.clone();
        node.prev.next = node.clone();
        node.next.prev = node.clone();
    }

    fn ref_node(&mut self, mut node: NodePtr<K, V>) {
        if node.ref_count == 1 && node.in_cache() {
            LRUInner::list_remove_node(node.clone());
            LRUInner::append(self.in_use_dummy.clone(), node.clone());
        }
        node.incr_ref();
    }

    fn unref_node(&mut self, mut node: NodePtr<K, V>) {
        node.decr_ref();
        if node.ref_count == 0 {
            debug_assert!(!node.in_cache());
            unsafe { Box::from_raw(node.0.as_ptr()) };
        } else if node.in_cache() && node.ref_count == 1 {
            LRUInner::list_remove_node(node.clone());
            LRUInner::append(self.dummy.clone(), node.clone());
        }
    }

    fn finish_remove_entry(&mut self, mut node: NodePtr<K, V>) {
        Self::list_remove_node(node.clone());
        node.set_in_cache(false);
        self.usage -= node.charge;
        self.unref_node(node);
    }
}

impl<K, V> Drop for LRUInner<K, V> {
    fn drop(&mut self) {
        debug_assert!(self.in_use_dummy == self.in_use_dummy.next);
        let mut node = self.dummy.next.clone();
        let mut next;
        while node != self.dummy {
            next = node.next.clone();
            debug_assert!(node.in_cache());
            node.set_in_cache(false);
            debug_assert!(node.ref_count == 1);
            self.unref_node(node);
            node = next;
        }
        unsafe {
            Box::from_raw(self.dummy.0.as_ptr());
            Box::from_raw(self.in_use_dummy.0.as_ptr());
        }
    }
}

impl<K, V> LRU<K, V, RandomState> {
    pub fn new(capacity: usize) -> Self {
        Self::with_hasher(capacity, RandomState::default())
    }
}

impl<K, V, S> LRU<K, V, S> {
    pub fn with_hasher(capacity: usize, hash_builder: S) -> Self {
        Self {
            inner: Mutex::new(LRUInner::new(capacity)),
            hash_builder,
        }
    }
}

impl<K, V, S> CacheShard for LRU<K, V, S>
where
    S: Send + Sync,
{
    type Key = K;
    type Value = V;
    type HashBuilder = S;

    fn shard_with_options(opts: &Options<S>) -> Self
    where
        Self::HashBuilder: Clone,
    {
        Self::with_hasher(opts.capacity, opts.hash_builder.clone())
    }

    fn shard_insert(
        &self,
        key: Self::Key,
        value: Self::Value,
        hash: u64,
        charge: usize,
    ) -> Box<dyn CacheHandle<Value = Self::Value> + '_>
    where
        Self::Key: Eq,
    {
        let mut node = unsafe {
            NodePtr(NonNull::new_unchecked(Box::into_raw(Box::new(
                LRUNode::new(key, value, charge, hash),
            ))))
        };

        let mut lru = self.inner.lock().unwrap();

        if lru.cap > 0 {
            node.incr_ref(); // lru has 1, returned handle has 1
            node.set_in_cache(true);
            LRUInner::append(lru.in_use_dummy.clone(), node.clone());
            lru.usage += charge;
            // remove if node with same key already in lru
            if let Some(rn) = lru
                .map
                .remove_entry(hash, |e| e.hash == hash && e.key.deref().eq(&node.key))
            {
                lru.finish_remove_entry(rn);
            }
            lru.map.insert(hash, node.clone(), |e| e.hash);
        }

        while lru.usage > lru.cap && lru.dummy != lru.dummy.next {
            let old = lru.dummy.next.clone();
            debug_assert_eq!(old.ref_count, 1);
            if let Some(rn) = lru.map.remove_entry(old.hash, |e| {
                e.hash == old.hash && e.key.deref().eq(&old.key)
            }) {
                lru.finish_remove_entry(rn);
            }
        }

        Box::new(LRUHandle { node, lru: self })
    }

    fn shard_lookup(
        &self,
        key: &Self::Key,
        hash: u64,
    ) -> Option<Box<dyn CacheHandle<Value = Self::Value> + '_>>
    where
        Self::Key: Eq,
    {
        let mut lru = self.inner.lock().unwrap();
        let node = lru
            .map
            .get(hash, |e| e.hash == hash && e.key.deref().eq(key))?
            .clone();

        lru.ref_node(node.clone());
        Some(Box::new(LRUHandle { node, lru: self }))
    }

    fn shard_erase(&self, key: &Self::Key, hash: u64)
    where
        Self::Key: Eq,
    {
        let mut lru = self.inner.lock().unwrap();
        if let Some(node) = lru
            .map
            .remove_entry(hash, |e| e.hash == hash && e.key.deref().eq(key))
        {
            lru.finish_remove_entry(node);
        }
    }

    fn shard_prune(&self)
    where
        Self::Key: Eq,
    {
        let mut lru = self.inner.lock().unwrap();
        while lru.dummy != lru.dummy.next {
            let node_to_remove = lru.dummy.next.clone();
            debug_assert_eq!(node_to_remove.ref_count, 1);
            let rn = lru.map.remove_entry(node_to_remove.hash, |e| {
                e.hash == node_to_remove.hash && e.key.deref().eq(&node_to_remove.key)
            });
            debug_assert!(rn.is_some());
            if let Some(node) = rn {
                lru.finish_remove_entry(node);
            }
        }
    }
}

impl<K, V, S> Cache for LRU<K, V, S>
where
    S: Send + Sync,
{
    type Key = K;
    type Value = V;
    type HashBuilder = S;

    fn with_options(opts: &Options<Self::HashBuilder>) -> Self
    where
        Self::HashBuilder: Clone,
    {
        Self::with_hasher(opts.capacity, opts.hash_builder.clone())
    }

    fn insert(
        &self,
        key: Self::Key,
        value: Self::Value,
        charge: usize,
    ) -> Box<dyn CacheHandle<Value = Self::Value> + '_>
    where
        Self::Key: Hash + Eq,
        Self::HashBuilder: BuildHasher,
    {
        let hash = hash_key(&self.hash_builder, &key);
        self.shard_insert(key, value, hash, charge)
    }

    fn lookup(&self, key: &K) -> Option<Box<dyn CacheHandle<Value = Self::Value> + '_>>
    where
        Self::Key: Hash + Eq,
        Self::HashBuilder: BuildHasher,
    {
        let hash = hash_key(&self.hash_builder, key);
        self.shard_lookup(key, hash)
    }

    fn erase(&self, key: &K)
    where
        Self::Key: Hash + Eq,
        Self::HashBuilder: BuildHasher,
    {
        let hash = hash_key(&self.hash_builder, key);
        self.shard_erase(key, hash)
    }

    fn prune(&self)
    where
        Self::Key: Eq,
    {
        self.shard_prune()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{Cache, LRU};

    // protect DROPPED_KEY and DROPPED_VALUE.
    // cargo test will run in parallel mode by default
    static THREAD_LOCK: Mutex<()> = Mutex::new(());
    static mut DROPPED_KEY: Vec<i32> = vec![];
    static mut DROPPED_VALUE: Vec<i32> = vec![];

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct TestKey(i32);
    #[derive(Debug, PartialEq, Eq)]
    struct TestValue(i32);

    impl TestKey {
        fn set(&mut self, v: i32) -> &Self {
            self.0 = v;
            self
        }
    }

    impl Drop for TestKey {
        fn drop(&mut self) {
            unsafe { DROPPED_KEY.push(self.0) }
        }
    }

    impl Drop for TestValue {
        fn drop(&mut self) {
            unsafe { DROPPED_VALUE.push(self.0) }
        }
    }

    fn lookup(lru: &LRU<TestKey, TestValue>, key: i32) -> Option<i32> {
        let key = TestKey(key);
        let r = lru.lookup(&key).map(|h| h.value().0);
        std::mem::forget(key);
        r
    }

    fn insert(lru: &LRU<TestKey, TestValue>, key: i32, value: i32, charge: usize) {
        lru.insert(TestKey(key), TestValue(value), charge);
    }

    fn erase(lru: &LRU<TestKey, TestValue>, key: i32) {
        let key = TestKey(key);
        lru.erase(&key);
        std::mem::forget(key);
    }

    fn clear() {
        unsafe {
            DROPPED_KEY.clear();
            DROPPED_VALUE.clear();
        }
    }

    #[test]
    fn simple() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();
        let lru = LRU::<TestKey, TestValue>::new(100);
        assert_eq!(lookup(&lru, 100), None);

        insert(&lru, 100, 100, 1);
        assert_eq!(lookup(&lru, 100), Some(100));
        assert_eq!(lookup(&lru, 200), None);
        assert_eq!(lookup(&lru, 300), None);

        insert(&lru, 200, 200, 1);
        assert_eq!(lookup(&lru, 100), Some(100));
        assert_eq!(lookup(&lru, 200), Some(200));
        assert_eq!(lookup(&lru, 300), None);

        insert(&lru, 100, 101, 1);
        assert_eq!(lookup(&lru, 100), Some(101));
        assert_eq!(lookup(&lru, 200), Some(200));
        assert_eq!(lookup(&lru, 300), None);

        assert_eq!(unsafe { DROPPED_KEY.len() }, 1);
        assert_eq!(unsafe { DROPPED_KEY[0] }, 100);
        assert_eq!(unsafe { DROPPED_VALUE[0] }, 100);

        drop(lru);
        assert_eq!(unsafe { DROPPED_KEY.len() }, 3);
    }

    #[test]
    fn remove() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();
        let lru = LRU::<TestKey, TestValue>::new(100);
        erase(&lru, 200);
        assert_eq!(unsafe { DROPPED_KEY.len() }, 0);
        insert(&lru, 100, 100, 1);
        insert(&lru, 200, 200, 1);
        erase(&lru, 100);
        assert_eq!(lookup(&lru, 100), None);
        assert_eq!(lookup(&lru, 200), Some(200));
        assert_eq!(unsafe { DROPPED_KEY.len() }, 1);
        assert_eq!(unsafe { DROPPED_KEY[0] }, 100);
        assert_eq!(unsafe { DROPPED_VALUE[0] }, 100);
        erase(&lru, 100);
        assert_eq!(lookup(&lru, 100), None);
        assert_eq!(lookup(&lru, 200), Some(200));
        assert_eq!(unsafe { DROPPED_KEY.len() }, 1);

        drop(lru);
        assert_eq!(unsafe { DROPPED_KEY.len() }, 2);
    }

    #[test]
    fn pinned() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();

        let mut lookup_key = TestKey(0);

        let lru = LRU::<TestKey, TestValue>::new(100);
        insert(&lru, 100, 100, 1);
        let h1 = lru.lookup(lookup_key.set(100));
        assert_eq!(Some(100), h1.as_ref().map(|h| h.value().0));

        insert(&lru, 100, 101, 1);
        let h2 = lru.lookup(lookup_key.set(100));
        assert_eq!(Some(101), h2.as_ref().map(|h| h.value().0));
        assert_eq!(unsafe { DROPPED_KEY.len() }, 0);

        drop(h1);
        assert_eq!(unsafe { DROPPED_KEY.len() }, 1);
        assert_eq!(unsafe { DROPPED_KEY[0] }, 100);
        assert_eq!(unsafe { DROPPED_VALUE[0] }, 100);

        erase(&lru, 100);
        assert_eq!(lookup(&lru, 100), None);
        assert_eq!(unsafe { DROPPED_KEY.len() }, 1);

        drop(h2);
        assert_eq!(unsafe { DROPPED_KEY.len() }, 2);
        assert_eq!(unsafe { DROPPED_KEY[1] }, 100);
        assert_eq!(unsafe { DROPPED_VALUE[1] }, 101);
        std::mem::forget(lookup_key);
    }

    #[test]
    fn evict() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();

        let mut lookup_key = TestKey(0);

        let lru = LRU::<TestKey, TestValue>::new(100);
        insert(&lru, 100, 100, 1);
        insert(&lru, 200, 200, 1);
        insert(&lru, 300, 300, 1);
        let h = lru.lookup(lookup_key.set(300));
        assert!(h.is_some());

        for i in 0..200 {
            insert(&lru, 1000 + i, 1000 + i, 1);
            assert_eq!(lookup(&lru, 1000 + i), Some(1000 + i));
            assert_eq!(lookup(&lru, 100), Some(100));
        }

        assert_eq!(lookup(&lru, 100), Some(100));
        assert_eq!(lookup(&lru, 300), Some(300));
        assert_eq!(lookup(&lru, 200), None);
        drop(h);
        std::mem::forget(lookup_key);
    }

    #[test]
    fn hang() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();
        let lru = LRU::<TestKey, TestValue>::new(100);
        let mut handles = vec![];
        for i in 0..200 {
            handles.push(lru.insert(TestKey(i), TestValue(i), 1));
        }
        for i in 0..200 {
            assert_eq!(lookup(&lru, i), Some(i));
        }
        drop(handles);
    }

    #[test]
    fn diff_charge() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();
        let lru = LRU::<TestKey, TestValue>::new(100);
        for i in 0..200 {
            insert(&lru, i, i, if i & 1 != 0 { 1 } else { 2 });
        }
        let mut usage = 0;
        for i in 0..200 {
            if let Some(v) = lookup(&lru, i) {
                assert_eq!(v, i);
                usage += if i & 1 != 0 { 1 } else { 2 };
            }
        }
        assert!(usage <= 100);
    }

    #[test]
    fn multi_thread() {
        let _g = THREAD_LOCK.lock().unwrap();
        clear();
        let lru = Arc::new(LRU::<TestKey, TestValue>::new(10000));
        let mut thread_handles = vec![];
        for i in 0..10 {
            let l = lru.clone();
            thread_handles.push(std::thread::spawn(move || {
                for j in 0..1000 {
                    insert(&l, i * 1000 + j, j, 1);
                }
            }));
        }
        thread_handles.into_iter().for_each(|h| {
            h.join().unwrap();
        });
        for i in 0..10000 {
            assert_eq!(lookup(&lru, i), Some(i % 1000));
        }
    }
}
