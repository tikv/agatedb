use std::{
    mem, ptr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

const ADDR_ALIGN_MASK: usize = 7;

struct ArenaCore {
    len: AtomicU32,
    cap: usize,
    ptr: *mut u8,
}

impl Drop for ArenaCore {
    fn drop(&mut self) {
        let ptr = self.ptr as *mut u64;
        let cap = self.cap / 8;
        unsafe {
            Vec::from_raw_parts(ptr, 0, cap);
        }
    }
}

pub struct Arena {
    core: Arc<ArenaCore>,
}

impl Arena {
    pub fn with_capacity(cap: u32) -> Arena {
        let mut buf: Vec<u64> = Vec::with_capacity(cap as usize / 8);
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * 8;
        mem::forget(buf);
        Arena {
            core: Arc::new(ArenaCore {
                // Reserve 8 bytes at the beginning, because func offset return 0 means invalid value
                len: AtomicU32::new(8),
                cap,
                ptr,
            }),
        }
    }

    pub fn len(&self) -> u32 {
        self.core.len.load(Ordering::SeqCst)
    }

    pub fn alloc(&self, size: usize) -> u32 {
        // Leave enough padding for alignment.
        let size = (size + ADDR_ALIGN_MASK) & !ADDR_ALIGN_MASK;
        let offset = self.core.len.fetch_add(size as u32, Ordering::SeqCst);

        // Return 0 if there is not enough space to allocate.
        if offset as usize + size > self.core.cap {
            let _ = self.core.len.fetch_sub(size as u32, Ordering::SeqCst);
            return 0;
        }
        offset as u32
    }

    pub unsafe fn get_mut<N>(&self, offset: u32) -> *mut N {
        if offset == 0 {
            return ptr::null_mut();
        }
        self.core.ptr.add(offset as usize) as _
    }

    pub fn offset<N>(&self, ptr: *const N) -> u32 {
        let ptr_addr = ptr as usize;
        let self_addr = self.core.ptr as usize;
        if ptr_addr > self_addr && ptr_addr < self_addr + self.core.cap {
            (ptr_addr - self_addr) as u32
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

        // Align with 8 bytes
        let offset = arena.alloc(3);
        assert_eq!(offset, 16);
        assert_eq!(arena.len(), 24);

        // There is not enough space, return 0
        let offset = arena.alloc(256);
        assert_eq!(offset, 0);
    }
}
