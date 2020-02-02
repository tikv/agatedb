use std::cmp::Ordering;

pub trait KeyComparitor {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FixedLengthSuffixComparitor {
    len: usize,
}

impl FixedLengthSuffixComparitor {
    pub fn new(len: usize) -> FixedLengthSuffixComparitor {
        FixedLengthSuffixComparitor { len }
    }
}

impl KeyComparitor for FixedLengthSuffixComparitor {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let (l_p, l_s) = lhs.split_at(lhs.len() - self.len);
        let (r_p, r_s) = rhs.split_at(rhs.len() - self.len);
        let res = l_p.cmp(r_p);
        match res {
            Ordering::Greater | Ordering::Less => res,
            Ordering::Equal => l_s.cmp(r_s),
        }
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        let (l_p, _) = lhs.split_at(lhs.len() - self.len);
        let (r_p, _) = rhs.split_at(rhs.len() - self.len);
        l_p == r_p
    }
}
