use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
};

use super::{utils::hash_key, Cache, CacheHandle, CacheShard, Options};

pub struct ShardCache<T: CacheShard, S = RandomState> {
    shards: Vec<T>,
    hash_builder: S,
}

impl<T> ShardCache<T, RandomState>
where
    T: CacheShard<HashBuilder = RandomState>,
{
    pub fn new(capacity: usize, shard_bits: usize) -> Self {
        Self::with_hasher(capacity, shard_bits, RandomState::default())
    }
}

impl<T, S> ShardCache<T, S>
where
    T: CacheShard<HashBuilder = S>,
    S: Clone,
{
    pub fn with_hasher(capacity: usize, shard_bits: usize, hash_builder: S) -> Self {
        let num_shards = 1 << shard_bits;
        let per_shard = (capacity + (num_shards - 1)) / num_shards;
        Self {
            shards: (0..num_shards)
                .map(|_| {
                    T::shard_with_options(&Options {
                        capacity: per_shard,
                        shard_bits: 0,
                        hash_builder: hash_builder.clone(),
                    })
                })
                .collect(),
            hash_builder,
        }
    }
}

impl<T, S> Cache for ShardCache<T, S>
where
    T: CacheShard<HashBuilder = S>,
    S: Send + Sync,
{
    type Key = T::Key;
    type Value = T::Value;
    type HashBuilder = S;

    fn with_options(opts: &Options<S>) -> Self
    where
        Self::HashBuilder: Clone,
        Self: Sized,
    {
        Self::with_hasher(opts.capacity, opts.shard_bits, opts.hash_builder.clone())
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
        self.shards[hash as usize & (self.shards.len() - 1)].shard_insert(key, value, hash, charge)
    }

    fn lookup(&self, key: &Self::Key) -> Option<Box<dyn CacheHandle<Value = Self::Value> + '_>>
    where
        Self::Key: Hash + Eq,
        Self::HashBuilder: BuildHasher,
    {
        let hash = hash_key(&self.hash_builder, &key);
        self.shards[hash as usize & (self.shards.len() - 1)].shard_lookup(key, hash)
    }

    fn erase(&self, key: &Self::Key)
    where
        Self::Key: Hash + Eq,
        Self::HashBuilder: BuildHasher,
    {
        let hash = hash_key(&self.hash_builder, &key);
        self.shards[hash as usize & (self.shards.len() - 1)].shard_erase(key, hash)
    }

    fn prune(&self)
    where
        Self::Key: Eq,
    {
        self.shards.iter().for_each(|s| s.shard_prune())
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{Cache, ShardCache, LRU};

    #[test]
    fn simple() {
        let cache = ShardCache::<LRU<i32, i32>>::new(4096, 4);
        {
            cache.insert(10, 10, 1);
        }
        {
            let h1 = cache.lookup(&10);
            assert!(h1.is_some());
            assert_eq!(h1.as_ref().unwrap().value(), &10);
            {
                cache.insert(10, 11, 1);
            }
            assert_eq!(h1.unwrap().value(), &10);
            let h2 = cache.lookup(&10);
            assert!(h2.is_some());
            assert_eq!(h2.unwrap().value(), &11);
        }
    }
}
