use crate::closer::Closer;
use crossbeam_channel::{bounded, select, Receiver, Sender};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use yatp::task::callback::Handle;

enum Index {
    Single(u64),
    Multiple(Vec<u64>),
}

struct Mark {
    index: Index,
    waiter: Option<Sender<()>>,
    done: bool,
}

struct Core {
    done_until: AtomicU64,
    last_index: AtomicU64,
    pub name: String,
    mark_ch: Receiver<Mark>,
}

impl Core {
    fn process(&self, closer: Closer) {
        let mut pending: HashMap<u64, usize> = HashMap::new();
        let mut waiters: HashMap<u64, Vec<Sender<()>>> = HashMap::new();

        let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();

        let process_one = |waiters: &mut HashMap<u64, Vec<Sender<()>>>,
                           pending: &mut HashMap<u64, usize>,
                           indices: &mut BinaryHeap<Reverse<u64>>,
                           index: u64,
                           done: bool| {
            if !pending.contains_key(&index) {
                indices.push(Reverse(index));
            }

            let prev = pending.entry(index).or_default();
            if done {
                *prev -= 1;
            } else {
                *prev += 1;
            }

            let done_until = self.done_until.load(Ordering::SeqCst);

            if done_until > index {
                panic!(
                    "name: {}, done_until: {}, index: {}",
                    self.name, done_until, index
                );
            }

            let mut until = done_until;
            // let mut loops = 0;

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
                // loops += 1;
            }

            if until != done_until {
                assert_eq!(
                    self.done_until
                        .compare_and_swap(done_until, until, Ordering::SeqCst),
                    done_until
                );
            }

            if until - done_until <= waiters.len() as u64 {
                for idx in done_until + 1..=until {
                    if let Some(to_notify) = waiters.remove(&idx) {
                        drop(to_notify); // close channel and remove from waiters
                    }
                }
            } else {
                waiters.drain_filter(|idx, _| *idx <= until); // close and drop idx <= util channels
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
                                mark.waiter.take(); // close channel
                            } else {
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
                            Index::Single(index) => process_one(&mut waiters, &mut pending, &mut indices, *index, mark.done),
                            Index::Multiple(index) => for index in index {
                                process_one(&mut waiters, &mut pending, &mut indices, *index, mark.done);
                            }
                        }

                    }
                },
                recv(closer.has_been_closed()) -> _ => return
            }
        }
    }
}

/// Watermark
///
/// TODO: one watermark spawns a thread. We may later use async and futures to reduce
/// number of threads needed.
#[derive(Clone)]
pub struct WaterMark {
    core: Arc<Core>,
    tx: Sender<Mark>,
}

impl WaterMark {
    pub fn new(name: String) -> Self {
        let (tx, rx) = bounded(100);
        Self {
            core: Arc::new(Core {
                mark_ch: rx,
                name,
                done_until: AtomicU64::new(0),
                last_index: AtomicU64::new(0),
            }),
            tx,
        }
    }

    pub fn init(&self, pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>, closer: Closer) {
        let core = self.core.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            core.process(closer);
        });
    }

    pub fn begin(&self, index: u64) {
        self.core.last_index.store(index, Ordering::SeqCst);
        self.tx
            .send(Mark {
                index: Index::Single(index),
                done: false,
                waiter: None,
            })
            .unwrap();
    }

    pub fn begin_many(&self, indices: Vec<u64>) {
        self.core
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

    pub fn done(&self, index: u64) {
        self.tx
            .send(Mark {
                index: Index::Single(index),
                done: true,
                waiter: None,
            })
            .unwrap();
    }

    pub fn done_many(&self, indices: Vec<u64>) {
        self.tx
            .send(Mark {
                index: Index::Multiple(indices),
                done: true,
                waiter: None,
            })
            .unwrap();
    }

    pub fn done_until(&self) -> u64 {
        self.core.done_until.load(Ordering::SeqCst)
    }

    pub fn set_done_until(&self, val: u64) {
        self.core.done_until.store(val, Ordering::SeqCst);
    }

    pub fn last_index(&self) -> u64 {
        self.core.last_index.load(Ordering::SeqCst)
    }

    pub fn wait_for_mark(&self, index: u64) -> Option<Receiver<()>> {
        if self.done_until() >= index {
            return None;
        }

        let (tx, rx) = bounded(1);

        self.tx
            .send(Mark {
                index: Index::Single(index),
                waiter: Some(tx),
                done: false,
            })
            .unwrap();

        Some(rx)
    }
}
