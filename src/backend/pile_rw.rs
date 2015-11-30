use std::sync::Arc;
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use super::worker::Worker;
use super::{in_memory, pile_lookup, pile_compile};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

enum RunState<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    Invalid,
    Filling { in_memory: Worker<in_memory::InMemory<D>>, compile: Worker<pile_compile::PileCompile<D>>, },
    Freezing { in_memory: Worker<in_memory::InMemory<D>>, compile: Worker<pile_compile::PileCompile<D>>, },
    Freezed { lookup: Worker<pile_lookup::PileLookup<D>>, },
}

pub struct PileRw<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    database_dir: PathBuf,
    state: RunState<D>,
}

pub enum Error {
    UnsupportedForCurrentMode,
    InMemory,
    PileCompile(pile_compile::Error),
    PileLookup(pile_lookup::Error),
}

impl<D> PileRw<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    pub fn new<P>(database_dir: P, compile_params: pile_compile::Params) -> Result<PileRw<D>, Error> where P: AsRef<Path> {
        let mut base_dir = PathBuf::new();
        base_dir.push(&database_dir);

        let in_memory = in_memory::InMemory::new();
        let compile = try!(pile_compile::PileCompile::new(&base_dir, compile_params).map_err(|e| Error::PileCompile(e)));
        Ok(PileRw {
            database_dir: base_dir,
            state: RunState::Filling {
                in_memory: Worker::run(in_memory),
                compile: Worker::run(compile),
            }
        })
    }
}

impl<D> Backend for PileRw<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    type Error = Error;
    type Document = D;

    fn save_state(&mut self, state: Arc<State>) -> Result<(), Error> {
        match self.state {
            RunState::Invalid =>
                unreachable!(),
            RunState::Filling { in_memory: ref mut mem_backend, compile: ref mut pile_backend } => {
                try!(mem_backend.save_state(state.clone()).map_err(|_| Error::InMemory));
                try!(pile_backend.save_state(state).map_err(|e| Error::PileCompile(e)));
                Ok(())
            },
            RunState::Freezing { .. } =>
                Err(Error::UnsupportedForCurrentMode),
            RunState::Freezed { .. } =>
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
            RunState::Freezed { lookup: ref mut backend } =>
                backend.load_state().map_err(|e| Error::PileLookup(e)),
        }
    }

    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<D>) -> Result<(), Error> {
        Err(Error::UnsupportedForCurrentMode)
    }

    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, filter: F, collector: C) -> Result<CR, LookupError<Error, CE>>
        where F: CandidatesFilter, C: CandidatesCollector<Error = CE, Document = D, Result = CR>
    {
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
            RunState::Freezed { lookup: ref mut backend } =>
                backend.lookup(signature, filter, collector).map_err(|err| match err {
                    LookupError::Collector(e) => LookupError::Collector(e),
                    LookupError::Backend(e) => LookupError::Backend(Error::PileLookup(e)),
                }),
        }
    }
}
