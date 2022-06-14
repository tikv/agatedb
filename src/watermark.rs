use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crossbeam_channel::{bounded, select, Receiver, Sender};
use yatp::task::callback::Handle;

use crate::closer::Closer;

#[derive(Debug)]
enum Index {
    Single(u64),
    Multiple(Vec<u64>),
}

// Contains one or more indices, along with a done boolean to indicate the
// status of the index: begin or done. It also contains waiters, who could be
// waiting for the watermark to reach >= a certain index.
#[derive(Debug)]
struct Mark {
    index: Index,
    waiter: Option<Sender<()>>,
    done: bool,
}

struct WaterMarkInner {
    done_until: AtomicU64,
    last_index: AtomicU64,
    pub name: String,
    mark_ch: Receiver<Mark>,
}

impl WaterMarkInner {
    /// Processes the `mark_ch`. This is not thread-safe, so only run one thread for `process`.
    /// One is sufficient, because all thread ops use purely memory and cpu.
    ///
    /// Each index has to emit at least one begin watermark in serial order otherwise waiters
    /// can get blocked idefinitely. Example: We had an watermark at 100 and a waiter at 101,
    /// if no watermark is emitted at index 101 then waiter would get stuck indefinitely as it
    /// can't decide whether the task at 101 has decided not to emit watermark or it didn't get
    /// scheduled yet.
    fn process(&self, closer: Closer) {
        let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();

        let pending: RefCell<HashMap<u64, usize>> = RefCell::new(HashMap::new());
        let waiters: RefCell<HashMap<u64, Vec<Sender<()>>>> = RefCell::new(HashMap::new());

        let mut process_one = |index: u64, done: bool| {
            let mut pending = pending.borrow_mut();
            let mut waiters = waiters.borrow_mut();

            if !pending.contains_key(&index) {
                indices.push(Reverse(index));
            }

            let prev = pending.entry(index).or_default();
            if done {
                (*prev) = (*prev).saturating_sub(1);
            } else {
                *prev += 1;
            }

            // Update mark by going through all indices in order; and checking if they have
            // been done. Stop at the first index, which isn't done.
            let done_until = self.done_until.load(Ordering::SeqCst);
            if done_until > index {
                panic!(
                    "name: {}, done_until: {}, index: {}",
                    self.name, done_until, index
                );
            }

            let mut until = done_until;

            while !indices.is_empty() {
                let min = indices.peek().unwrap().0;
                if let Some(done) = pending.get(&min) {
                    if *done > 0 {
                        break;
                    }
                }
                indices.pop();
                pending.remove(&min);
                until = min;
            }

            if until != done_until {
                assert_eq!(
                    self.done_until.compare_exchange(
                        done_until,
                        until,
                        Ordering::SeqCst,
                        Ordering::Acquire
                    ),
                    Ok(done_until)
                );
            }

            if until - done_until <= waiters.len() as u64 {
                // Close channel and remove from waiters.
                for idx in done_until + 1..=until {
                    if let Some(to_notify) = waiters.remove(&idx) {
                        drop(to_notify);
                    }
                }
            } else {
                // Close and drop idx <= util channels.
                waiters.retain(|idx, _| *idx > until);
            }
        };

        loop {
            select! {
                recv(self.mark_ch) -> mark => {
                    let mut mark: Mark = mark.unwrap();
                    if let Some(ref waiter) = mark.waiter {
                        if let Index::Single(index) = mark.index {
                            let done_until = self.done_until.load(Ordering::SeqCst);
                            if done_until >= index {
                                mark.waiter.take(); // Close channel.
                            } else{
                                let mut waiters = waiters.borrow_mut();
                                if let Some(ws) = waiters.get_mut(&index) {
                                    ws.push(waiter.clone());
                                } else {
                                    waiters.insert(index, vec![waiter.clone()]);
                                }
                            }
                        } else {
                            panic!("waiter used without index");
                        }
                    } else {
                        match &mark.index {
                            Index::Single(index) => process_one(*index, mark.done),
                            Index::Multiple(indices) => for index in indices {
                                process_one(*index, mark.done);
                            }
                        }

                    }
                },
                recv(closer.get_receiver()) -> _ => return
            }
        }
    }
}

/// Used to keep track of the minimum un-finished index. Typically,
/// an index k becomes finished or done according to a [`WaterMark`] once `done(k)` has been called
///
/// 1. as many times as `begin(k)` has, AND
/// 2. a positive number of times.
///
/// An index may also become done by calling `set_done_until` at a time such that it is not
/// inter-mingled with `begin`/`done` calls.
///
/// TODO: one watermark spawns a thread. We may later use async and futures to reduce
/// number of threads needed.
#[derive(Clone)]
pub struct WaterMark {
    inner: Arc<WaterMarkInner>,
    tx: Sender<Mark>,
}

impl WaterMark {
    pub fn new(name: String) -> Self {
        let (tx, rx) = bounded(100);
        Self {
            inner: Arc::new(WaterMarkInner {
                mark_ch: rx,
                name,
                done_until: AtomicU64::new(0),
                last_index: AtomicU64::new(0),
            }),
            tx,
        }
    }

    /// Initializes a [`WaterMark`] struct. MUST be called before using it.
    pub fn init(&self, pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>, closer: Closer) {
        let inner = self.inner.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            inner.process(closer);
        });
    }

    /// Sets the last index to the given value.
    pub fn begin(&self, index: u64) {
        self.inner.last_index.store(index, Ordering::SeqCst);
        self.tx
            .send(Mark {
                index: Index::Single(index),
                done: false,
                waiter: None,
            })
            .unwrap();
    }

    /// Works like `begin` but accepts multiple indices.
    pub fn begin_many(&self, indices: Vec<u64>) {
        self.inner
            .last_index
            .store(*indices.last().unwrap(), Ordering::SeqCst);
        self.tx
            .send(Mark {
                index: Index::Multiple(indices),
                done: false,
                waiter: None,
            })
            .unwrap();
    }

    /// Sets a single index as done.
    pub fn done(&self, index: u64) {
        self.tx
            .send(Mark {
                index: Index::Single(index),
                done: true,
                waiter: None,
            })
            .unwrap();
    }

    /// Works like done but accepts multiple indices.
    pub fn done_many(&self, indices: Vec<u64>) {
        self.tx
            .send(Mark {
                index: Index::Multiple(indices),
                done: true,
                waiter: None,
            })
            .unwrap();
    }

    /// Returns the maximum index that has the property that all indices
    /// less than or equal to it are done.
    pub fn done_until(&self) -> u64 {
        self.inner.done_until.load(Ordering::SeqCst)
    }

    /// Sets the maximum index that has the property that all indices
    /// less than or equal to it are done.
    pub fn set_done_until(&self, val: u64) {
        self.inner.done_until.store(val, Ordering::SeqCst);
    }

    /// Returns the last index for which `begin` has been called.
    pub fn last_index(&self) -> u64 {
        self.inner.last_index.load(Ordering::SeqCst)
    }

    /// Waits until the given index is marked as done.
    pub fn wait_for_mark(&self, index: u64) {
        if self.done_until() >= index {
            return;
        }

        let (tx, rx) = bounded(1);

        self.tx
            .send(Mark {
                index: Index::Single(index),
                waiter: Some(tx),
                done: false,
            })
            .unwrap();

        matches!(rx.recv(), Err(crossbeam_channel::RecvError));
    }
}

#[cfg(test)]
mod tests {
    use yatp::Builder;

    use super::*;
    use crate::closer::Closer;

    fn init_and_close<F>(f: F)
    where
        F: FnOnce(&WaterMark),
    {
        let pool = Builder::new("watermark-basic").build_callback_pool();
        let closer = Closer::new();

        let watermark = WaterMark::new("watermark".into());
        watermark.init(&pool, closer.clone());

        f(&watermark);

        closer.close();
        pool.shutdown();
    }

    #[test]
    fn test_basic() {
        init_and_close(|_| {});
    }

    #[test]
    fn test_begin_done() {
        init_and_close(|watermark| {
            watermark.begin(1);
            watermark.begin_many(vec![2, 3]);

            watermark.done(1);
            watermark.done_many(vec![2, 3]);
        });
    }

    #[test]
    fn test_wait_for_mark() {
        init_and_close(|watermark| {
            watermark.begin_many(vec![1, 2, 3]);
            watermark.done_many(vec![2, 3]);

            assert_eq!(watermark.done_until(), 0);

            watermark.done(1);

            watermark.wait_for_mark(1);
            assert_eq!(watermark.done_until(), 3);
        });
    }

    #[test]
    fn test_last_index() {
        init_and_close(|watermark| {
            watermark.begin_many(vec![1, 2, 3]);
            watermark.done_many(vec![2, 3]);

            assert_eq!(watermark.last_index(), 3);
        });
    }
}
