use bit_vec::BitVec;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct Bloom {
    filter: BitVec,
    nbits: u64,
    k: u32,
}

impl Bloom {
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / (0.69314718056 as f64).powi(2);
        let locs = 0.69314718056 * size / (entries as f64);
        locs as usize
    }

    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = ((bits_per_key as f64) * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) >> 3;
        let nbits = nbytes << 3;
        let mut filter = BitVec::from_elem(nbits, false);
        for h in keys {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        Self {
            filter,
            nbits: nbits as u64,
            k,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u64(self.nbits as u64);
        let data = self.filter.to_bytes();
        buf.put_u64(data.len() as u64);
        buf.put_slice(&data);
        buf.put_u32(self.k);
        buf.freeze()
    }

    pub fn decode(mut buf: Bytes) -> Self {
        let nbits = buf.get_u64();
        let data_len = buf.get_u64() as usize;
        let data = buf.slice(..data_len);
        buf.advance(data_len);
        let k = buf.get_u32();
        let filter = BitVec::from_bytes(&data);
        Self { filter, nbits, k }
    }

    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (self.nbits as u32);
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
    use rand::prelude::*;

    #[test]
    fn test_small_bloom_filter() {
        let hash: Vec<u32> = vec![b"hello".to_vec(), b"world".to_vec()]
            .into_iter()
            .map(|x| farmhash::fingerprint32(&x))
            .collect();
        let f = Bloom::build_from_key_hashes(&hash, 10);

        let check_hash: Vec<u32> = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"x".to_vec(),
            b"fool".to_vec(),
        ]
        .into_iter()
        .map(|x| farmhash::fingerprint32(&x))
        .collect();

        assert!(f.may_contain(check_hash[0]));
        assert!(f.may_contain(check_hash[1]));
        assert!(!f.may_contain(check_hash[2]));
        assert!(!f.may_contain(check_hash[3]));
    }

    #[test]
    fn bloom_test_encode_decode() {
        let hash: Vec<u32> = vec![b"hello".to_vec(), b"world".to_vec()]
            .into_iter()
            .map(|x| farmhash::fingerprint32(&x))
            .collect();
        let f = Bloom::build_from_key_hashes(&hash, 10);
        let f = Bloom::decode(f.encode());
        let check_hash: Vec<u32> = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"x".to_vec(),
            b"fool".to_vec(),
        ]
        .into_iter()
        .map(|x| farmhash::fingerprint32(&x))
        .collect();

        assert!(f.may_contain(check_hash[0]));
        assert!(f.may_contain(check_hash[1]));
        assert!(!f.may_contain(check_hash[2]));
        assert!(!f.may_contain(check_hash[3]));
    }
}
