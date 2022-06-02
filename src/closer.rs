use std::sync::{Arc, Mutex};

use crossbeam_channel::{unbounded, Receiver, Sender};

type CloserChan = Arc<(Mutex<Option<Sender<()>>>, Receiver<()>)>;

// TODO: review closer implementation
#[derive(Clone)]
pub struct Closer {
    chan: CloserChan,
}

impl Closer {
    pub fn new() -> Self {
        let (tx, rx) = unbounded::<()>();
        Self {
            chan: Arc::new((Mutex::new(Some(tx)), rx)),
        }
    }

    pub fn close(&self) {
        self.chan.0.lock().unwrap().take();
    }

    pub fn get_receiver(&self) -> &Receiver<()> {
        &self.chan.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closer() {
        let closer = Closer::new();

        let (tx, rx) = unbounded::<()>();

        let mut handles = vec![];

        for _i in 0..10 {
            let closer = closer.clone();
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                assert!(closer.get_receiver().recv().is_err());
                tx.send(()).unwrap();
            }));
        }

        closer.close();

        for _ in 0..10 {
            rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        }

        handles.into_iter().for_each(|h| h.join().unwrap());
    }
}
