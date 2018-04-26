use std::mem;
use std::sync::Arc;
use std::thread::spawn;
use std::sync::mpsc::{channel, Receiver, TryRecvError};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use super::worker::{Worker, Req, Rep};
use super::{in_memory, pile_lookup, pile_compile};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

enum RunState<D> where D: Serialize + Deserialize<'static> + Send + Sync + 'static {
    Invalid,
    Filling { in_memory: Worker<in_memory::InMemory<D>, ()>, compile: Worker<pile_compile::PileCompile<D>, pile_compile::Error>, },
    Freezing { in_memory: Worker<in_memory::InMemory<D>, ()>, compile: Worker<pile_compile::PileCompile<D>, pile_compile::Error>, },
    Switching { in_memory: Worker<in_memory::InMemory<D>, ()>, rx: Receiver<Result<pile_lookup::PileLookup<D>, pile_lookup::Error>>, },
    Freezed { lookup: pile_lookup::PileLookup<D>, },
}

pub struct PileRw<D> where D: Serialize + Deserialize<'static> + Send + Sync + 'static {
    database_dir: Arc<PathBuf>,
    lookup_params: pile_lookup::Params,
    state: RunState<D>,
}

#[derive(Debug)]
pub enum Error {
    UnsupportedForCurrentMode,
    InMemory,
    PileCompile(pile_compile::Error),
    PileLookup(pile_lookup::Error),
}

impl<D> PileRw<D> where D: Serialize + Deserialize<'static> + Send + Sync + 'static {
    pub fn new<P>(database_dir: P, lookup_params: pile_lookup::Params, compile_params: pile_compile::Params) ->
        Result<PileRw<D>, Error> where P: AsRef<Path>
    {
        let mut base_dir = PathBuf::new();
        base_dir.push(&database_dir);

        let in_memory = in_memory::InMemory::new();
        let compile = try!(pile_compile::PileCompile::new(&base_dir, compile_params).map_err(Error::PileCompile));
        Ok(PileRw {
            database_dir: Arc::new(base_dir),
            lookup_params: lookup_params,
            state: RunState::Filling {
                in_memory: Worker::run(in_memory),
                compile: Worker::run(compile),
            }
        })
    }

    pub fn is_freezed(&self) -> bool {
        if let RunState::Freezed { .. } = self.state { true } else { false }
    }
}

impl<D> Drop for PileRw<D> where D: Serialize + Deserialize<'static> + Send + Sync + 'static {
    fn drop(&mut self) {
        match mem::replace(&mut self.state, RunState::Invalid) {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { .. } | RunState::Freezed { .. } =>
                (),
            RunState::Freezing { compile: mut pile_backend, .. } =>
                loop {
                    if let Ok(Rep::TerminateAck) = pile_backend.rx.recv().unwrap() {
                        pile_backend.shutdown();
                        break
                    }
                },
            RunState::Switching { rx: pile_rx, .. } => {
                let _ = pile_rx.recv().unwrap();
            },
        }
    }
}

impl<D> Backend for PileRw<D> where D: Serialize + Deserialize<'static> + Send + Sync + 'static {
    type Error = Error;
    type Document = D;

    fn save_state(&mut self, state: Arc<State>) -> Result<(), Error> {
        match self.state {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { in_memory: ref mut mem_backend, compile: ref mut pile_backend } => {
                try!(mem_backend.save_state(state.clone()).map_err(|_| Error::InMemory));
                try!(pile_backend.save_state(state).map_err(Error::PileCompile));
                Ok(())
            },
            RunState::Freezing { .. } | RunState::Switching { .. } | RunState::Freezed { .. } =>
                Err(Error::UnsupportedForCurrentMode),
        }
    }

    fn load_state(&mut self) -> Result<Option<Arc<State>>, Error> {
        match self.state {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { in_memory: ref mut backend, .. } =>
                backend.load_state().map_err(|_| Error::InMemory),
            RunState::Freezing { in_memory: ref mut backend, .. } =>
                backend.load_state().map_err(|_| Error::InMemory),
            RunState::Switching { in_memory: ref mut backend, .. } =>
                backend.load_state().map_err(|_| Error::InMemory),
            RunState::Freezed { lookup: ref mut backend } =>
                backend.load_state().map_err(Error::PileLookup),
        }
    }

    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<D>) -> Result<(), Error> {
        match self.state {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { in_memory: ref mem_backend, compile: ref pile_backend } => {
                mem_backend.tx.send(Req::Insert(signature.clone(), doc.clone())).unwrap();
                pile_backend.tx.send(Req::Insert(signature, doc)).unwrap();
                match (mem_backend.rx.recv().unwrap(), pile_backend.rx.recv().unwrap()) {
                    (Ok(Rep::Ok), Ok(Rep::Ok)) => Ok(()),
                    (Err(..), _) => Err(Error::InMemory),
                    (_, Err(e)) => Err(Error::PileCompile(e)),
                    other @ (Ok(..), Ok(..)) => panic!("unexpected rep pair: {:?}", other),
                }
            },
            RunState::Freezing { .. } | RunState::Switching { .. } | RunState::Freezed { .. } =>
                Err(Error::UnsupportedForCurrentMode),
        }
    }

    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, filter: F, collector: C) -> Result<CR, LookupError<Error, CE>>
        where F: CandidatesFilter + Clone, C: CandidatesCollector<Error = CE, Document = D, Result = CR>
    {
        // check if freezing is done
        if let RunState::Freezing { .. } = self.state {
            if let RunState::Freezing { in_memory: mem_backend, compile: mut pile_compile, } = mem::replace(&mut self.state, RunState::Invalid) {
                loop {
                    match pile_compile.rx.try_recv() {
                        Ok(Ok(Rep::TerminateAck)) => {
                            pile_compile.shutdown();
                            let database_dir_copy = self.database_dir.clone();
                            let lookup_params_copy = self.lookup_params.clone();
                            let (tx, rx) = channel();
                            spawn(move || {
                                mem::drop(pile_compile);
                                tx.send(pile_lookup::PileLookup::new(&**database_dir_copy, lookup_params_copy)).unwrap();
                            });
                            self.state = RunState::Switching { in_memory: mem_backend, rx: rx, };
                        },
                        Ok(..) =>
                            continue,
                        Err(TryRecvError::Empty) =>
                            self.state = RunState::Freezing { in_memory: mem_backend, compile: pile_compile, },
                        Err(..) =>
                            panic!("compile backend died unexpectedly"),
                    }
                    break
                }
            }
        }

        // check if switching is done
        if let RunState::Switching { .. } = self.state {
            if let RunState::Switching { in_memory: mem_backend, rx: pile_rx, } = mem::replace(&mut self.state, RunState::Invalid) {
                match pile_rx.try_recv() {
                    Ok(Ok(pile_lookup)) =>
                        self.state = RunState::Freezed { lookup: pile_lookup, },
                    Ok(Err(e)) =>
                        return Err(LookupError::Backend(Error::PileLookup(e))),
                    Err(TryRecvError::Empty) =>
                        self.state = RunState::Switching { in_memory: mem_backend, rx: pile_rx, },
                    Err(..) =>
                        panic!("switching thread died unexpectedly"),
                }
            }
        }

        // process lookup
        match self.state {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { in_memory: ref mut backend, .. } =>
                backend.lookup(signature, filter, collector).map_err(|err| match err {
                    LookupError::Collector(e) => LookupError::Collector(e),
                    LookupError::Backend(..) => LookupError::Backend(Error::InMemory),
                }),
            RunState::Freezing { in_memory: ref mut backend, .. } =>
                backend.lookup(signature, filter, collector).map_err(|err| match err {
                    LookupError::Collector(e) => LookupError::Collector(e),
                    LookupError::Backend(..) => LookupError::Backend(Error::InMemory),
                }),
            RunState::Switching { in_memory: ref mut backend, .. } =>
                backend.lookup(signature, filter, collector).map_err(|err| match err {
                    LookupError::Collector(e) => LookupError::Collector(e),
                    LookupError::Backend(..) => LookupError::Backend(Error::InMemory),
                }),
            RunState::Freezed { lookup: ref mut backend } =>
                backend.lookup(signature, filter, collector).map_err(|err| match err {
                    LookupError::Collector(e) => LookupError::Collector(e),
                    LookupError::Backend(e) => LookupError::Backend(Error::PileLookup(e)),
                }),
        }
    }

    fn rotate(&mut self) -> Result<(), Error> {
        match mem::replace(&mut self.state, RunState::Invalid) {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { in_memory: mem_backend, compile: pile_backend, } => {
                pile_backend.tx.send(Req::Terminate).unwrap();
                self.state = RunState::Freezing { in_memory: mem_backend, compile: pile_backend, };
                Ok(())
            },
            state @ RunState::Freezing { .. } => {
                self.state = state;
                Err(Error::UnsupportedForCurrentMode)
            },
            state @ RunState::Switching { .. } => {
                self.state = state;
                Err(Error::UnsupportedForCurrentMode)
            },
            state @ RunState::Freezed { .. } => {
                self.state = state;
                Err(Error::UnsupportedForCurrentMode)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use bin_merge_pile::merge::ParallelConfig;
    use super::PileRw;
    use super::super::{pile_compile, pile_lookup};
    use super::super::super::{Backend, SimilarityThresholdFilter, Signature, State, Config};

    #[test]
    fn save_load_state() {
        let state = Arc::new(State::new(Config::default()));
        let mut backend = PileRw::<String>::new(
            "/tmp/pile_rw_a",
            Default::default(),
            pile_compile::Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            }).unwrap();
        assert_eq!(backend.load_state().unwrap(), None);
        backend.save_state(state.clone()).unwrap();
        assert_eq!(backend.load_state().unwrap().unwrap(), state.clone());
    }

    #[test]
    fn insert_lookup_rotate() {
        let doc_a = Arc::new("some text".to_owned());
        let doc_b = Arc::new("some other text".to_owned());
        {
            let mut backend = PileRw::<String>::new(
                "/tmp/pile_rw_b",
                Default::default(),
                pile_compile::Params {
                    min_tree_height: 1,
                    max_block_size: 32,
                    memory_limit_power: 13,
                    parallel_config: ParallelConfig::SingleThread,
                }).unwrap();
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

            backend.rotate().unwrap();

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
        {
            let mut backend = pile_lookup::PileLookup::<String>::new("/tmp/pile_rw_b", Default::default()).unwrap();
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
}
