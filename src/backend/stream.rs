use std::{io, fs};
use std::sync::Arc;
use std::convert::From;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender, Receiver};
use time;
use serde::{Serialize, Deserialize};
use super::worker::{Worker, Req, Rep};
use super::{pile_rw, pile_lookup, pile_compile};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

#[derive(Debug)]
pub enum Error {
    WindowsDatabaseNotADir(PathBuf),
    CreateWindowsDir(PathBuf, io::Error),
    StatWindowsDir(PathBuf, io::Error),
    ReadWindowsDir(PathBuf, io::Error),
    ReadWindowsDirEntry(PathBuf, io::Error),
    StatWindowsDirEntry(PathBuf, io::Error),
    PileRw(pile_rw::Error),
    PileLookup(pile_lookup::Error),
    PileCompile(pile_compile::Error),
    MissingStateForWindow(PathBuf),
    WindowsStatesInconsistency(PathBuf),
    UnsupportedForROWindow,
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
    database_dir: PathBuf,
    backend: B,
}

enum Window<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    Rw(InnerWindow<Worker<pile_rw::PileRw<D>, Error>>),
    Ro(InnerWindow<Worker<pile_lookup::PileLookup<D>, Error>>),
}

pub struct Stream<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    windows_dir: PathBuf,
    windows: Vec<Window<D>>,
    rw_window: Option<InnerWindow<Worker<pile_rw::PileRw<D>, Error>>>,
    compile_params: pile_compile::Params,
    lookup_tx: Sender<Result<Rep<D>, Error>>,
    lookup_rx: Receiver<Result<Rep<D>, Error>>,
}

impl<D> Stream<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    pub fn new<P>(windows_dir: P, compile_params: pile_compile::Params) -> Result<Stream<D>, Error> where P: AsRef<Path> {
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

            windows.push(Window::Ro(InnerWindow {
                database_dir: database.clone(),
                backend: Worker::run_redirect_lookup(pile_lookup, worker_tx.clone()),
            }));
        }

        let mut stream = Stream {
            windows_dir: base_dir,
            windows: windows,
            rw_window: None,
            compile_params: compile_params,
            lookup_tx: worker_tx,
            lookup_rx: stream_rx,
        };
        try!(stream.create_rw_window());
        Ok(stream)
    }

    fn create_rw_window(&mut self) -> Result<(), Error> {
        let mut common_state: Option<Arc<State>> = None;
        for win in self.windows.iter_mut() {
            let (database_dir, maybe_state) = match win {
                &mut Window::Rw(InnerWindow { database_dir: ref dir, backend: ref mut win, }) => (dir, try!(win.load_state())),
                &mut Window::Ro(InnerWindow { database_dir: ref dir, backend: ref mut win, }) => (dir, try!(win.load_state())),
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

        let now = time::now_utc();
        let mut database_dir = PathBuf::new();
        database_dir.push(&self.windows_dir);
        database_dir.push(format!("{}", now.rfc3339()));

        let mut pile_rw = try!(pile_rw::PileRw::new(&database_dir, self.compile_params.clone()));
        if let Some(state) = common_state.take() {
            try!(pile_rw.save_state(state));
        }
        self.rw_window = Some(InnerWindow {
            database_dir: database_dir,
            backend: Worker::run_redirect_lookup(pile_rw, self.lookup_tx.clone()),
        });
        Ok(())
    }
}
