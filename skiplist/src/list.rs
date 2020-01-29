use crate::arena::Arena;
use crate::MAX_HEIGHT;
use rand::Rng;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::{mem, ptr, slice, u32};

const HEIGHT_INCREASE: u32 = u32::MAX / 3;

// Uses C layout to make sure tower is at the bottom
#[derive(Debug)]
#[repr(C)]
pub struct Node {
    value: AtomicU64,
    key_offset: u32,
    key_len: u16,
    height: u16,
    tower: [AtomicU32; MAX_HEIGHT as usize],
}

impl Node {
    fn alloc(arena: &Arena, key: &[u8], value: &[u8], height: u32) -> u32 {
        let align = mem::align_of::<Node>();
        let size = mem::size_of::<Node>();
        // Not all values in Node::tower will be utilized.
        let not_used = (MAX_HEIGHT as usize - height as usize - 1) * mem::size_of::<AtomicU32>();
        let node_offset = arena.alloc(align, size - not_used);
        unsafe {
            let node_ptr: *mut Node = arena.get_mut(node_offset);
            let node = &mut *node_ptr;
            node.key_offset = arena.put_bytes(key);
            node.key_len = key.len() as u16;
            node.height = height as u16;
            let value_offset = arena.put_bytes(value) as u64;
            node.value
                .store((value.len() as u64) << 32 | value_offset, Ordering::SeqCst);
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height as usize + 1);
        }
        node_offset
    }

    fn value_offset(&self) -> (u32, usize) {
        let value = self.value.load(Ordering::SeqCst);
        (value as u32, (value >> 32) as usize)
    }

    unsafe fn key<'a>(&self, arena: &'a Arena) -> &'a [u8] {
        slice::from_raw_parts(arena.get_mut(self.key_offset), self.key_len as usize)
    }

    unsafe fn value<'a>(&self, arena: &'a Arena) -> &'a [u8] {
        let (value_offset, value_len) = self.value_offset();
        slice::from_raw_parts(arena.get_mut(value_offset), value_len)
    }

    fn set_value(&self, arena: &Arena, value: &[u8]) {
        let val_offset = arena.put_bytes(value);
        let value = (value.len() as u64) << 32 | val_offset as u64;
        self.value.store(value, Ordering::SeqCst);
    }

    fn next_offset(&self, height: u32) -> u32 {
        self.tower[height as usize].load(Ordering::SeqCst)
    }
}

struct SkiplistCore {
    height: AtomicU32,
    head: NonNull<Node>,
    arena: Arena,
}

#[derive(Clone)]
pub struct Skiplist {
    core: Arc<SkiplistCore>,
}

impl Skiplist {
    pub fn with_capacity(arena_size: u32) -> Skiplist {
        let arena = Arena::with_capacity(arena_size);
        let head_offset = Node::alloc(&arena, &[], &[], MAX_HEIGHT - 1);
        let head = unsafe { NonNull::new_unchecked(arena.get_mut(head_offset)) };
        Skiplist {
            core: Arc::new(SkiplistCore {
                height: AtomicU32::new(0),
                head,
                arena,
            }),
        }
    }

    fn random_height(&self) -> u32 {
        let mut rng = rand::thread_rng();
        for h in 0..(MAX_HEIGHT - 1) {
            if !rng.gen_ratio(HEIGHT_INCREASE, u32::MAX) {
                return h;
            }
        }
        MAX_HEIGHT - 1
    }

    fn height(&self) -> u32 {
        self.core.height.load(Ordering::SeqCst)
    }

    unsafe fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> *const Node {
        let mut cursor: *const Node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            let next_offset = (&*cursor).next_offset(level);
            if next_offset == 0 {
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if !less || cursor == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            let next_ptr: *mut Node = self.core.arena.get_mut(next_offset);
            let next = &*next_ptr;
            let next_key = next.key(&self.core.arena);
            let res = key.cmp(next_key);
            if res == std::cmp::Ordering::Greater {
                cursor = next_ptr;
                continue;
            }
            if res == std::cmp::Ordering::Equal {
                if allow_equal {
                    return next;
                }
                if !less {
                    let offset = next.next_offset(0);
                    if offset != 0 {
                        return self.core.arena.get_mut(offset);
                    } else {
                        return ptr::null();
                    }
                }
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if cursor == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            if level > 0 {
                level -= 1;
                continue;
            }
            if !less {
                return next;
            }
            if cursor == self.core.head.as_ptr() {
                return ptr::null();
            }
            return cursor;
        }
    }

    unsafe fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: u32,
    ) -> (*mut Node, *mut Node) {
        loop {
            let next_offset = (&*before).next_offset(level);
            if next_offset == 0 {
                return (before, ptr::null_mut());
            }
            let next_ptr: *mut Node = self.core.arena.get_mut(next_offset);
            let next_node = &*next_ptr;
            let next_key = next_node.key(&self.core.arena);
            match key.cmp(next_key) {
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                std::cmp::Ordering::Less => return (before, next_ptr),
                _ => before = next_ptr,
            }
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let mut list_height = self.height();
        let mut prev = [ptr::null_mut(); MAX_HEIGHT as usize + 1];
        let mut next = [ptr::null_mut(); MAX_HEIGHT as usize + 1];
        prev[list_height as usize + 1] = self.core.head.as_ptr();
        next[list_height as usize + 1] = ptr::null_mut();
        for i in (0..=list_height as usize).rev() {
            let (p, n) = unsafe { self.find_splice_for_level(key, prev[i + 1], i as u32) };
            prev[i] = p;
            next[i] = n;
            if p == n {
                unsafe {
                    (&*p).set_value(&self.core.arena, value);
                }
                return;
            }
        }

        let height = self.random_height();
        let node_offset = Node::alloc(&self.core.arena, key, value, height);
        while height > list_height {
            match self.core.height.compare_exchange_weak(
                list_height,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(h) => list_height = h,
            }
        }
        let x: &mut Node = unsafe { &mut *self.core.arena.get_mut(node_offset) };
        for i in 0..=height as usize {
            loop {
                if prev[i].is_null() {
                    assert!(i > 1);
                    let (p, n) = unsafe {
                        self.find_splice_for_level(key, self.core.head.as_ptr(), i as u32)
                    };
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }
                let next_offset = self.core.arena.offset(next[i]);
                x.tower[i].store(next_offset, Ordering::SeqCst);
                match unsafe { (&*prev[i]) }.tower[i].compare_exchange(
                    next_offset,
                    node_offset,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(_) => {
                        let (p, n) = unsafe { self.find_splice_for_level(key, prev[i], i as u32) };
                        if p == n {
                            assert_eq!(i, 0);
                            unsafe {
                                (&*p).set_value(&self.core.arena, value);
                            }
                            return;
                        }
                        prev[i] = p;
                        next[i] = n;
                    }
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        let node = self.core.head.as_ptr();
        let next_offset = unsafe { (&*node).next_offset(0) };
        next_offset == 0
    }

    pub fn len(&self) -> usize {
        let mut node = self.core.head.as_ptr();
        let mut count = 0;
        loop {
            let next = unsafe { (&*node).next_offset(0) };
            if next != 0 {
                count += 1;
                node = unsafe { self.core.arena.get_mut(next) };
                continue;
            }
            return count;
        }
    }

    fn find_last(&self) -> *const Node {
        let mut node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            let next = unsafe { (&*node).next_offset(level) };
            if next != 0 {
                node = unsafe { self.core.arena.get_mut(next) };
                continue;
            }
            if level == 0 {
                if node == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return node;
            }
            level -= 1;
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let node = unsafe { self.find_near(key, false, true) };
        if node.is_null() {
            return None;
        }
        let next_key = unsafe { (&*node).key(&self.core.arena) };
        if next_key != key {
            return None;
        }
        unsafe { Some((&*node).value(&self.core.arena)) }
    }

    pub fn iter_ref(&self) -> IterRef {
        IterRef {
            list: self,
            cursor: ptr::null(),
        }
    }

    pub fn mem_size(&self) -> u32 {
        self.core.arena.len()
    }
}

unsafe impl Send for Skiplist {}
unsafe impl Sync for Skiplist {}

pub struct IterRef<'a> {
    list: &'a Skiplist,
    cursor: *const Node,
}

impl<'a> IterRef<'a> {
    pub fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (&*self.cursor).key(&self.list.core.arena) }
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (&*self.cursor).value(&self.list.core.arena) }
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            let cursor_offset = (&*self.cursor).next_offset(0);
            self.cursor = self.list.core.arena.get_mut(cursor_offset);
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = self.list.find_near(self.key(), true, false);
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.find_near(target, false, true);
        }
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.find_near(target, true, true);
        }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            let cursor_offset = (&*self.list.core.head.as_ptr()).next_offset(0);
            self.cursor = self.list.core.arena.get_mut(cursor_offset);
        }
    }

    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.find_last();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_near() {
        let list = Skiplist::with_capacity(1 << 20);
        for i in 0..1000 {
            let key = format!("{:05}", i * 10 + 5);
            let value = format!("{:05}", i);
            list.put(key.as_bytes(), value.as_bytes());
        }
        let mut cases = vec![
            (
                b"00001" as &'static [u8],
                false,
                false,
                Some(b"00005" as &'static [u8]),
            ),
            (b"00001", false, true, Some(b"00005")),
            (b"00001", true, false, None),
            (b"00001", true, true, None),
            (b"00005", false, false, Some(b"00015")),
            (b"00005", false, true, Some(b"00005")),
            (b"00005", true, false, None),
            (b"00005", true, true, Some(b"00005")),
            (b"05555", false, false, Some(b"05565")),
            (b"05555", false, true, Some(b"05555")),
            (b"05555", true, false, Some(b"05545")),
            (b"05555", true, true, Some(b"05555")),
            (b"05558", false, false, Some(b"05565")),
            (b"05558", false, true, Some(b"05565")),
            (b"05558", true, false, Some(b"05555")),
            (b"05558", true, true, Some(b"05555")),
            (b"09995", false, false, None),
            (b"09995", false, true, Some(b"09995")),
            (b"09995", true, false, Some(b"09985")),
            (b"09995", true, true, Some(b"09995")),
            (b"59995", false, false, None),
            (b"59995", false, true, None),
            (b"59995", true, false, Some(b"09995")),
            (b"59995", true, true, Some(b"09995")),
        ];
        for (i, (key, less, allow_equal, exp)) in cases.drain(..).enumerate() {
            let res = unsafe { list.find_near(key, less, allow_equal) };
            if exp.is_none() {
                assert!(res.is_null(), "{}", i);
                continue;
            }
            let e = exp.unwrap();
            let val = unsafe { (&*res).key(&list.core.arena) };
            assert_eq!(val, e, "{}", i);
        }
    }
}
