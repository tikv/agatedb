use bitvec::prelude::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct Bloom<'a> {
    filter: &'a BitSlice<Lsb0, u8>,
    k: u32,
}

impl<'a> Bloom<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        let filter = &buf[..buf.len() - 4];
        let k = (&buf[buf.len() - 4..]).get_u32();
        Self {
            filter: &BitSlice::from_slice(filter).unwrap(),
            k,
        }
    }

    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / (0.69314718056 as f64).powi(2);
        let locs = 0.69314718056 * size / (entries as f64);
        locs as usize
    }

    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Bytes {
        let k = ((bits_per_key as f64) * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) >> 3;
        // nbits is always multiplication of 8
        let nbits = nbytes << 3;
        let mut filter = BitVec::<Lsb0, u8>::with_capacity(nbits);
        filter.resize(nbits, false);
        for h in keys {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        let mut buf = BytesMut::new();
        buf.put_slice(filter.as_slice());
        buf.put_u32(k);
        buf.freeze()
    }

    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.len();
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.filter.get(bit_pos as usize).unwrap() {
                    return false;
                }
                h = h.wrapping_add(delta);
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_bloom_filter() {
        let hash: Vec<u32> = vec![b"hello".to_vec(), b"world".to_vec()]
            .into_iter()
            .map(|x| farmhash::fingerprint32(&x))
            .collect();
        let buf = Bloom::build_from_key_hashes(&hash, 10);

        let check_hash: Vec<u32> = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"x".to_vec(),
            b"fool".to_vec(),
        ]
        .into_iter()
        .map(|x| farmhash::fingerprint32(&x))
        .collect();

        let f = Bloom::new(&buf);

        assert!(f.may_contain(check_hash[0]));
        assert!(f.may_contain(check_hash[1]));
        assert!(!f.may_contain(check_hash[2]));
        assert!(!f.may_contain(check_hash[3]));
    }
}
