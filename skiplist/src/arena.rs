use std::{
    cell::Cell,
    mem, ptr,
    sync::atomic::{AtomicUsize, Ordering},
};

const ADDR_ALIGN_MASK: usize = 7;

struct ArenaCore {
    len: AtomicUsize,
    cap: Cell<usize>,
    ptr: Cell<*mut u8>,
}

impl Drop for ArenaCore {
    fn drop(&mut self) {
        let ptr = self.ptr.get() as *mut u64;
        let cap = self.cap.get() / 8;
        unsafe {
            Vec::from_raw_parts(ptr, 0, cap);
        }
    }
}

// Thread-local Arena
pub struct Arena {
    core: ArenaCore,
}

impl Arena {
    pub fn with_capacity(cap: usize) -> Arena {
        let mut buf: Vec<u64> = Vec::with_capacity(cap / 8);
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * 8;
        mem::forget(buf);
        Arena {
            core: ArenaCore {
                // Offset 0 is invalid value for func `offset` and `get_mut`, initialize the
                // len 8 to guarantee the allocated memory addr is always align with 8 bytes.
                len: AtomicUsize::new(8),
                cap: Cell::new(cap),
                ptr: Cell::new(ptr),
            },
        }
    }

    pub fn len(&self) -> usize {
        self.core.len.load(Ordering::SeqCst)
    }

    /// Alloc 8-byte aligned memory.
    pub fn alloc(&self, size: usize) -> usize {
        // Leave enough padding for alignment.
        let size = (size + ADDR_ALIGN_MASK) & !ADDR_ALIGN_MASK;
        let offset = self.core.len.fetch_add(size, Ordering::SeqCst);

        // Grow the arena if there is no enough space
        if offset + size > self.core.cap.get() {
            // Alloc new buf and copy data to new buf
            let mut grow_by = self.core.cap.get();
            if grow_by > 1 << 30 {
                grow_by = 1 << 30;
            }
            if grow_by < size {
                grow_by = size;
            }
            let mut new_buf: Vec<u64> =
                Vec::with_capacity((self.core.cap.get() + grow_by) as usize / 8);
            let new_ptr = new_buf.as_mut_ptr() as *mut u8;
            unsafe {
                ptr::copy_nonoverlapping(new_ptr, self.core.ptr.get(), self.core.cap.get());
            }

            // Release old buf
            let old_ptr = self.core.ptr.get() as *mut u64;
            unsafe {
                Vec::from_raw_parts(old_ptr, 0, self.core.cap.get() / 8);
            }

            // Use new buf
            self.core.ptr.set(new_ptr);
            self.core.cap.set(new_buf.capacity() * 8);
            mem::forget(new_buf);
        }
        offset
    }

    pub unsafe fn get_mut<N>(&self, offset: usize) -> *mut N {
        if offset == 0 {
            return ptr::null_mut();
        }

        self.core.ptr.get().add(offset) as _
    }

    pub fn offset<N>(&self, ptr: *const N) -> usize {
        let ptr_addr = ptr as usize;
        let self_addr = self.core.ptr.get() as usize;
        if ptr_addr > self_addr && ptr_addr < self_addr + self.core.cap.get() {
            ptr_addr - self_addr
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena() {
        // There is enough space
        let arena = Arena::with_capacity(128);
        let offset = arena.alloc(8);
        assert_eq!(offset, 8);
        assert_eq!(arena.len(), 16);
        unsafe {
            let ptr = arena.get_mut::<u64>(offset);
            let offset = arena.offset::<u64>(ptr);
            assert_eq!(offset, 8);
        }

        // There is not enough space, grow buf and then return the offset
        let offset = arena.alloc(256);
        assert_eq!(offset, 16);
    }
}
