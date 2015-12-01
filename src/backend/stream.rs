use std::{io, fs};
use std::sync::Arc;
use std::convert::From;
use std::default::Default;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender, Receiver};
use time;
use serde::{Serialize, Deserialize};
use rmp_serde;
use super::worker::{Worker, Req, Rep};
use super::{pile_rw, pile_lookup, pile_compile};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

#[derive(Clone, Copy)]
pub struct Params {
    pub compile_params: pile_compile::Params,
    pub windows_count: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params { compile_params: Default::default(), windows_count: 12, }
    }
}

#[derive(Debug)]
pub enum Error {
    WindowsDatabaseNotADir(PathBuf),
    CreateWindowsDir(PathBuf, io::Error),
    StatWindowsDir(PathBuf, io::Error),
    ReadWindowsDir(PathBuf, io::Error),
    ReadWindowsDirEntry(PathBuf, io::Error),
    StatWindowsDirEntry(PathBuf, io::Error),
    StatWindowRWDir(PathBuf, io::Error),
    RemoveWindowDir(PathBuf, io::Error),
    CreateSavedAtFile(PathBuf, io::Error),
    OpenSavedAtFile(PathBuf, io::Error),
    SerializeSavedAt(rmp_serde::encode::Error),
    DeserializeSavedAt(rmp_serde::decode::Error),
    PileRw(pile_rw::Error),
    PileLookup(pile_lookup::Error),
    PileCompile(pile_compile::Error),
    MissingStateForWindow(PathBuf),
    WindowsStatesInconsistency(PathBuf),
    UnsupportedForROWindow,
    WindowRWNotCreated,
    StateIsAlreadySaved,
}

impl From<pile_rw::Error> for Error {
    fn from(err: pile_rw::Error) -> Error {
        Error::PileRw(err)
    }
}

impl From<pile_lookup::Error> for Error {
    fn from(err: pile_lookup::Error) -> Error {
        Error::PileLookup(err)
    }
}

struct InnerWindow<B> {
    saved_at: time::Timespec,
    database_dir: PathBuf,
    backend: B,
}

enum Window<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    Rw(InnerWindow<Worker<pile_rw::PileRw<D>, Error>>),
    Ro(InnerWindow<Worker<pile_lookup::PileLookup<D>, Error>>),
}

pub struct Stream<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    state: Option<Arc<State>>,
    windows_dir: PathBuf,
    windows: Vec<Window<D>>,
    rw_window: Option<InnerWindow<Worker<pile_rw::PileRw<D>, Error>>>,
    params: Params,
    lookup_tx: Sender<Result<Rep<D>, Error>>,
    lookup_rx: Receiver<Result<Rep<D>, Error>>,
}

impl<D> Stream<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    pub fn new<P>(windows_dir: P, params: Params) -> Result<Stream<D>, Error> where P: AsRef<Path> {
        let mut base_dir = PathBuf::new();
        base_dir.push(&windows_dir);

        // make windows dir if need to
        match fs::metadata(&base_dir) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(..) => return Err(Error::WindowsDatabaseNotADir(base_dir.clone())),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
                try!(fs::create_dir(&base_dir).map_err(|e| Error::CreateWindowsDir(base_dir.clone(), e))),
            Err(e) => return Err(Error::StatWindowsDir(base_dir.clone(), e)),
        }

        // create lookup channel
        let (worker_tx, stream_rx) = channel();

        let mut windows = Vec::new();
        // open existing windows
        for entry in try!(fs::read_dir(&base_dir).map_err(|e| Error::ReadWindowsDir(base_dir.clone(), e))) {
            let database = try!(entry.map_err(|e| Error::ReadWindowsDirEntry(base_dir.clone(), e))).path();
            let stat = try!(fs::metadata(&*database).map_err(|e| Error::StatWindowsDirEntry(database.clone(), e)));
            if !stat.is_dir() {
                continue;
            }

            let mut pile_lookup = try!(pile_lookup::PileLookup::new(&database));
            if !try!(pile_lookup.load_state()).is_some() {
                return Err(Error::MissingStateForWindow(database.clone()))
            }

            let mut saved_at_filename = database.clone();
            saved_at_filename.push("saved_at.bin");
            let mut saved_at_file = try!(fs::File::open(&saved_at_filename).map_err(|e| Error::OpenSavedAtFile(saved_at_filename, e)));
            let mut deserializer = rmp_serde::Deserializer::new(&mut saved_at_file);
            let saved_at = time::Timespec {
                sec: try!(Deserialize::deserialize(&mut deserializer).map_err(|e| Error::DeserializeSavedAt(e))),
                nsec: try!(Deserialize::deserialize(&mut deserializer).map_err(|e| Error::DeserializeSavedAt(e))),
            };

            windows.push(Window::Ro(InnerWindow {
                saved_at: saved_at,
                database_dir: database.clone(),
                backend: Worker::run_redirect_lookup(pile_lookup, worker_tx.clone()),
            }));
        }

        let mut stream = Stream {
            state: None,
            windows_dir: base_dir,
            windows: windows,
            rw_window: None,
            params: params,
            lookup_tx: worker_tx,
            lookup_rx: stream_rx,
        };
        try!(stream.create_rw_window());
        Ok(stream)
    }

    fn create_rw_window(&mut self) -> Result<(), Error> {
        let mut common_state = self.state.as_ref().map(|s| s.clone());
        for win in self.windows.iter_mut() {
            let (database_dir, maybe_state) = match win {
                &mut Window::Rw(InnerWindow { database_dir: ref dir, backend: ref mut win, .. }) => (dir, try!(win.load_state())),
                &mut Window::Ro(InnerWindow { database_dir: ref dir, backend: ref mut win, .. }) => (dir, try!(win.load_state())),
            };

            match (maybe_state, &mut common_state) {
                (Some(ref state), &mut Some(ref common)) if state.minhash_seeds == common.minhash_seeds && state.bands_seeds == common.bands_seeds =>
                    (),
                (Some(..), &mut Some(..)) =>
                    return Err(Error::WindowsStatesInconsistency(database_dir.clone())),
                (None, _) =>
                    return Err(Error::MissingStateForWindow(database_dir.clone())),
                (state @ Some(..), place) =>
                    *place = state,
            }
        }

        let mut database_dir;
        let mut index = 0;
        loop {
            let now = time::now_utc();
            database_dir = PathBuf::new();
            database_dir.push(&self.windows_dir);
            database_dir.push(format!("{}_{}", now.rfc3339(), index));
            match fs::metadata(&database_dir) {
                Ok(..) => (),
                Err(ref e) if e.kind() == io::ErrorKind::NotFound => break,
                Err(e) => return Err(Error::StatWindowRWDir(database_dir, e)),
            }
            index += 1;
        }

        let mut pile_rw = try!(pile_rw::PileRw::new(&database_dir, self.params.compile_params.clone()));
        if let Some(state) = common_state.take() {
            try!(pile_rw.save_state(state.clone()));
            if self.state.is_none() {
                self.state = Some(state)
            }
        }

        let saved_at = time::get_time();
        {
            let mut saved_at_filename = database_dir.clone();
            saved_at_filename.push("saved_at.bin");
            let mut saved_at_file = try!(fs::File::create(&saved_at_filename).map_err(|e| Error::CreateSavedAtFile(saved_at_filename, e)));
            let mut serializer = rmp_serde::Serializer::new(&mut saved_at_file);
            try!(saved_at.sec.serialize(&mut serializer).map_err(|e| Error::SerializeSavedAt(e)));
            try!(saved_at.nsec.serialize(&mut serializer).map_err(|e| Error::SerializeSavedAt(e)));
        }

        self.rw_window = Some(InnerWindow {
            saved_at: saved_at,
            database_dir: database_dir,
            backend: Worker::run_redirect_lookup(pile_rw, self.lookup_tx.clone()),
        });
        Ok(())
    }
}

impl<D> Backend for Stream<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    type Error = Error;
    type Document = D;

    fn save_state(&mut self, state: Arc<State>) -> Result<(), Error> {
        if self.state.is_none() {
            if let Some(InnerWindow { backend: ref mut bkd, ..}) = self.rw_window {
                self.state = Some(state.clone());
                Ok(try!(bkd.save_state(state)))
            } else {
                Err(Error::WindowRWNotCreated)
            }
        } else {
            Err(Error::StateIsAlreadySaved)
        }
    }

    fn load_state(&mut self) -> Result<Option<Arc<State>>, Error> {
        Ok(self.state.as_ref().map(|s| s.clone()))
    }

    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<D>) -> Result<(), Error> {
        if let Some(InnerWindow { backend: ref mut bkd, ..}) = self.rw_window {
            Ok(try!(bkd.insert(signature, doc)))
        } else {
            Err(Error::WindowRWNotCreated)
        }
    }

    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, filter: F, mut collector: C) -> Result<CR, LookupError<Error, CE>>
        where F: CandidatesFilter + Clone, C: CandidatesCollector<Error = CE, Document = D, Result = CR>
    {
        let boxed_filter = Box::new(filter);
        let mut waiting_replies = 0;
        for win in self.windows.iter() {
            match win {
                &Window::Rw(InnerWindow { backend: ref win, .. }) =>
                    win.tx.send(Req::Lookup(signature.clone(), boxed_filter.clone())).unwrap(),
                &Window::Ro(InnerWindow { backend: ref win, .. }) =>
                    win.tx.send(Req::Lookup(signature.clone(), boxed_filter.clone())).unwrap(),
            }
            waiting_replies += 1;
        }
        if let Some(InnerWindow { backend: ref win, ..}) = self.rw_window {
            win.tx.send(Req::Lookup(signature.clone(), boxed_filter.clone())).unwrap();
            waiting_replies += 1;
        }

        let mut error_occurred = None;
        while waiting_replies > 0 {
            match self.lookup_rx.recv() {
                Ok(Ok(Rep::LookupResult(similarity, doc))) =>
                    try!(collector.receive(similarity, doc).map_err(|e| LookupError::Collector(e))),
                Ok(Ok(Rep::LookupFinish)) =>
                    waiting_replies -= 1,
                Ok(Err(e)) => {
                    waiting_replies -= 1;
                    error_occurred = Some(LookupError::Backend(e));
                },
                other =>
                    panic!("unexpected rep: {:?}", other),
            }
        }

        if let Some(error) = error_occurred {
            Err(error)
        } else {
            collector.finish().map_err(|e| LookupError::Collector(e))
        }
    }

    fn rotate(&mut self) -> Result<(), Error> {
        if let Some(mut window) = self.rw_window.take() {
            try!(window.backend.rotate());
            self.windows.push(Window::Rw(window));
            try!(self.create_rw_window());
            if self.windows.len() > self.params.windows_count {
                // find eldest window
                let mut eldest = None;
                for (i, win) in self.windows.iter().enumerate() {
                    let saved_at = match win {
                        &Window::Ro(InnerWindow { saved_at: ref value, .. }) => value,
                        &Window::Rw(InnerWindow { saved_at: ref value, .. }) => value,
                    };
                    match &mut eldest {
                        &mut Some((_, ref best_saved_at)) if best_saved_at <= saved_at => (),
                        &mut Some(ref mut best) => *best = (i, *saved_at),
                        place => *place = Some((i, *saved_at)),
                    }
                }
                if let Some((i, _)) = eldest {
                    let database = match self.windows.swap_remove(i) {
                        Window::Ro(InnerWindow { database_dir: database, .. }) => database,
                        Window::Rw(InnerWindow { database_dir: database, .. }) => database,
                    };
                    try!(fs::remove_dir_all(&database).map_err(|e| Error::RemoveWindowDir(database, e)));
                }
            }
            Ok(())
        } else {
            Err(Error::WindowRWNotCreated)
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use bin_merge_pile::merge::ParallelConfig;
    use super::{Params, Stream};
    use super::super::pile_compile;
    use super::super::super::{Backend, SimilarityThresholdFilter, Signature, State, Config};

    #[test]
    fn save_load_state() {
        let _ = fs::remove_dir_all("/tmp/stream_a");
        let state = Arc::new(State::new(Config::default()));
        let mut backend = Stream::<String>::new("/tmp/stream_a", Params {
            compile_params: pile_compile::Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            },
            windows_count: 3,
        }).unwrap();
        assert_eq!(backend.load_state().unwrap(), None);
        backend.save_state(state.clone()).unwrap();
        assert_eq!(backend.load_state().unwrap().unwrap(), state.clone());
    }

    #[test]
    fn insert_lookup_rotate() {
        let doc_a = Arc::new("some text".to_owned());
        let doc_b = Arc::new("some other text".to_owned());
        let doc_c = Arc::new("and finally not a text at all".to_owned());
        {
            let _ = fs::remove_dir_all("/tmp/stream_b");
            let mut backend = Stream::<String>::new("/tmp/stream_b", Params {
                compile_params: pile_compile::Params {
                    min_tree_height: 1,
                    max_block_size: 32,
                    memory_limit_power: 13,
                    parallel_config: ParallelConfig::SingleThread,
                },
                windows_count: 3,
            }).unwrap();
            backend.save_state(Arc::new(State::new(Config::default()))).unwrap();
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
            backend.insert(Arc::new(Signature { minhash: vec![7, 8, 9], bands: vec![300, 800, 900], }), doc_c.clone()).unwrap();

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
            let mut results = backend.lookup(Arc::new(Signature { minhash: vec![1, 2, 4], bands: vec![300], }),
                                             Box::new(SimilarityThresholdFilter(0.0)),
                                             Vec::new()).unwrap();
            assert_eq!(results.len(), 3);
            results.sort_by(|a, b| a.document.cmp(&b.document));
            assert_eq!(results[0].document, doc_c.clone());
            assert_eq!(results[1].document, doc_b.clone());
            assert_eq!(results[2].document, doc_a.clone());
            let results = backend.lookup(Arc::new(Signature { minhash: vec![7, 8, 9], bands: vec![800, 900], }),
                                         Box::new(SimilarityThresholdFilter(0.0)),
                                         Vec::new()).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].similarity, 1.0);
            assert_eq!(results[0].document, doc_c.clone());
        }
        {
            let mut backend = Stream::<String>::new("/tmp/stream_b", Params {
                compile_params: pile_compile::Params {
                    min_tree_height: 1,
                    max_block_size: 32,
                    memory_limit_power: 13,
                    parallel_config: ParallelConfig::SingleThread,
                },
                windows_count: 3,
            }).unwrap();
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
            let mut results = backend.lookup(Arc::new(Signature { minhash: vec![1, 2, 4], bands: vec![300], }),
                                             Box::new(SimilarityThresholdFilter(0.0)),
                                             Vec::new()).unwrap();
            assert_eq!(results.len(), 3);
            results.sort_by(|a, b| a.document.cmp(&b.document));
            assert_eq!(results[0].document, doc_c.clone());
            assert_eq!(results[1].document, doc_b.clone());
            assert_eq!(results[2].document, doc_a.clone());
        }
    }
}
