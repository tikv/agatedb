use std::hash::{BuildHasher, Hash, Hasher};

pub(crate) fn hash_key<S, Q>(s: &S, k: &Q) -> u64
where
    S: BuildHasher,
    Q: Hash + ?Sized,
{
    let mut hasher = s.build_hasher();
    k.hash(&mut hasher);
    hasher.finish()
}
