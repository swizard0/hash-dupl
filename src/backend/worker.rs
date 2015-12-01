use std::fmt;
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver, SendError};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

pub enum Req<D> {
    SaveState(Arc<State>),
    LoadState,
    Insert(Arc<Signature>, Arc<D>),
    Lookup(Arc<Signature>, Box<CandidatesFilter>),
    Rotate,
    Terminate,
}

pub enum Rep<D> {
    Ok,
    State(Option<Arc<State>>),
    LookupResult(f64, Arc<D>),
    LookupFinish,
    TerminateAck,
}

impl<D> fmt::Debug for Rep<D> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(match self {
            &Rep::Ok => "Rep::Ok",
            &Rep::State(None) => "Rep::State(None)",
            &Rep::State(Some(..)) => "Rep::State(Some(..))",
            &Rep::LookupResult(..) => "Rep::LookupResult(..)",
            &Rep::LookupFinish => "Rep::LookupFinish",
            &Rep::TerminateAck => "Rep::TerminateAck",
        })
    }
}

pub struct Worker<B> where B: Backend, B::Error: fmt::Debug {
    pub tx: Sender<Req<B::Document>>,
    pub rx: Receiver<Result<Rep<B::Document>, B::Error>>,
    slave: Option<JoinHandle<()>>,
}

impl<B> Worker<B> where B: Backend + Send + 'static, B::Document: Send + Sync, B::Error: Send + Sync + fmt::Debug {
    pub fn run(backend: B) -> Worker<B> {
        let (master_tx, slave_rx) = channel();
        let (slave_tx, master_rx) = channel();
        let slave = Builder::new().name("hash dupl backend worker".to_owned())
            .spawn(move || {
                worker_loop(backend, &slave_tx, &slave_rx).unwrap();
                slave_tx.send(Ok(Rep::TerminateAck)).unwrap();
            }).unwrap();
        Worker {
            tx: master_tx,
            rx: master_rx,
            slave: Some(slave),
        }
    }

    pub fn shutdown(&mut self) {
        if let Some(slave) = self.slave.take() {
            slave.join().unwrap();
        }
    }
}

impl<B> Drop for Worker<B> where B: Backend, B::Error: fmt::Debug {
    fn drop(&mut self) {
        if let Some(slave) = self.slave.take() {
            self.tx.send(Req::Terminate).unwrap();
            match self.rx.recv() {
                Ok(Ok(Rep::TerminateAck)) => slave.join().unwrap(),
                other => panic!("unexpected rep while joining hash dupl backend worker: {:?}", other),
            }
        }
    }
}

impl<B> Backend for Worker<B> where B: Backend, B::Error: fmt::Debug {
    type Error = B::Error;
    type Document = B::Document;

    fn save_state(&mut self, state: Arc<State>) -> Result<(), B::Error> {
        self.tx.send(Req::SaveState(state)).unwrap();
        match self.rx.recv() {
            Ok(Ok(Rep::Ok)) => Ok(()),
            Ok(Err(e)) => Err(e),
            other => panic!("unexpected rep: {:?}", other),
        }
    }

    fn load_state(&mut self) -> Result<Option<Arc<State>>, B::Error> {
        self.tx.send(Req::LoadState).unwrap();
        match self.rx.recv() {
            Ok(Ok(Rep::State(maybe_state))) => Ok(maybe_state),
            Ok(Err(e)) => Err(e),
            other => panic!("unexpected rep: {:?}", other),
        }
    }

    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<B::Document>) -> Result<(), B::Error> {
        self.tx.send(Req::Insert(signature, doc)).unwrap();
        match self.rx.recv() {
            Ok(Ok(Rep::Ok)) => Ok(()),
            Ok(Err(e)) => Err(e),
            other => panic!("unexpected rep: {:?}", other),
        }
    }

    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, filter: F, mut collector: C) -> Result<CR, LookupError<B::Error, CE>>
        where F: CandidatesFilter, C: CandidatesCollector<Error = CE, Document = B::Document, Result = CR>
    {
        self.tx.send(Req::Lookup(signature, Box::new(filter))).unwrap();
        loop {
            match self.rx.recv() {
                Ok(Ok(Rep::LookupResult(similarity, doc))) =>
                    try!(collector.receive(similarity, doc).map_err(|e| LookupError::Collector(e))),
                Ok(Ok(Rep::LookupFinish)) =>
                    return collector.finish().map_err(|e| LookupError::Collector(e)),
                Ok(Err(e)) =>
                    return Err(LookupError::Backend(e)),
                other =>
                    panic!("unexpected rep: {:?}", other),
            }
        }
    }

    fn rotate(&mut self) -> Result<(), Self::Error> {
        self.tx.send(Req::Rotate).unwrap();
        match self.rx.recv() {
            Ok(Ok(Rep::Ok)) => Ok(()),
            Ok(Err(e)) => Err(e),
            other => panic!("unexpected rep: {:?}", other),
        }
    }

}

fn worker_loop<B>(mut backend: B, tx: &Sender<Result<Rep<B::Document>, B::Error>>, rx: &Receiver<Req<B::Document>>) -> Result<(), B::Error>
    where B: Backend
{
    loop {
        match rx.recv().unwrap() {
            Req::SaveState(state) =>
                tx.send(backend.save_state(state).map(|()| Rep::Ok)).unwrap(),
            Req::LoadState =>
                tx.send(backend.load_state().map(|s| Rep::State(s))).unwrap(),
            Req::Insert(signature, document) =>
                tx.send(backend.insert(signature, document).map(|()| Rep::Ok)).unwrap(),
            Req::Lookup(signature, filter) => {
                struct Transmitter<'a, B>(&'a Sender<Result<Rep<B::Document>, B::Error>>) where B: Backend + 'a;
                impl<'a, B> CandidatesCollector for Transmitter<'a, B> where B: Backend {
                    type Error = SendError<Result<Rep<B::Document>, B::Error>>;
                    type Document = B::Document;
                    type Result = ();

                    fn receive(&mut self, similarity: f64, doc: Arc<B::Document>) -> Result<(), Self::Error> {
                        self.0.send(Ok(Rep::LookupResult(similarity, doc)))
                    }

                    fn finish(self) -> Result<(), Self::Error> {
                        self.0.send(Ok(Rep::LookupFinish))
                    }
                }

                match backend.lookup(signature, filter, Transmitter::<B>(&tx)) {
                    Ok(()) => (),
                    Err(LookupError::Backend(e)) => tx.send(Err(e)).unwrap(),
                    Err(LookupError::Collector(e)) => panic!("{:?}", e),
                }
            }
            Req::Rotate =>
                tx.send(backend.rotate().map(|()| Rep::Ok)).unwrap(),
            Req::Terminate =>
                return Ok(()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use super::Worker;
    use super::super::in_memory::InMemory;
    use super::super::super::{Backend, SimilarityThresholdFilter, Signature, State, Config};

    #[test]
    fn save_load_state() {
        let state = Arc::new(State::new(Config::default()));
        let bg_backend = InMemory::<String>::new();
        let mut backend = Worker::run(bg_backend);
        assert_eq!(backend.load_state().unwrap(), None);
        backend.save_state(state.clone()).unwrap();
        assert_eq!(backend.load_state().unwrap().unwrap(), state.clone());
    }

    #[test]
    fn insert_lookup() {
        let bg_backend = InMemory::<String>::new();
        let mut backend = Worker::run(bg_backend);
        let doc_a = Arc::new("some text".to_owned());
        let doc_b = Arc::new("some other text".to_owned());
        backend.insert(Arc::new(Signature { minhash: vec![1, 2, 3], bands: vec![100, 300, 400], }), doc_a.clone()).unwrap();
        backend.insert(Arc::new(Signature { minhash: vec![4, 5, 6], bands: vec![200, 300, 500], }), doc_b.clone()).unwrap();

        let results = backend.lookup(Arc::new(Signature { minhash: vec![1, 2, 3], bands: vec![100, 400], }),
                                     Box::new(SimilarityThresholdFilter(0.0)),
                                     Vec::new()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].similarity, 1.0);
        assert_eq!(results[0].document, doc_a.clone());
        let results = backend.lookup(Arc::new(Signature { minhash: vec![4, 5, 6], bands: vec![200, 500, 600, 700], }),
                                     Box::new(SimilarityThresholdFilter(0.0)),
                                     Vec::new()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].similarity, 1.0);
        assert_eq!(results[0].document, doc_b.clone());
        let results = backend.lookup(Arc::new(Signature { minhash: vec![1, 2, 4], bands: vec![300], }),
                                     Box::new(SimilarityThresholdFilter(0.0)),
                                     Vec::new()).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document, doc_a.clone());
        assert_eq!(results[1].document, doc_b.clone());
    }
}
