use std::sync::{Arc, Mutex};

use crossbeam_channel::{unbounded, Receiver, Sender};

type CloserChanType = Arc<(Mutex<Option<Sender<()>>>, Receiver<()>)>;

// TODO: review closer implementation
#[derive(Clone)]
pub struct Closer {
    chan: CloserChanType,
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

    pub fn has_been_closed(&self) -> &Receiver<()> {
        &self.chan.1
    }
}

#[cfg(test)]
mod tests {
    use yatp::task::callback::Handle;

    use super::*;

    #[test]
    fn test_closer() {
        let pool = yatp::Builder::new("test_closer").build_callback_pool();
        let closer = Closer::new();

        let (tx, rx) = unbounded::<()>();

        for _i in 0..10 {
            let closer = closer.clone();
            let tx = tx.clone();
            pool.spawn(move |_: &mut Handle<'_>| {
                assert!(closer.has_been_closed().recv().is_err());
                tx.send(()).unwrap();
            });
        }

        closer.close();

        for _ in 0..10 {
            rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        }
    }
}
