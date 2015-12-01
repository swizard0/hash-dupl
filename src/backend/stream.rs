use std::{io, fs};
use std::sync::Arc;
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use super::worker::{Worker, Req, Rep};
use super::{pile_rw, pile_lookup, pile_compile};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

enum Window<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    Rw(Worker<pile_rw::PileRw<D>>),
    Ro(Worker<pile_lookup::PileLookup<D>>),
}

pub struct Stream<D> where D: Serialize + Deserialize + Send + Sync + 'static {
    windows_dir: PathBuf,
    windows: Vec<Window<D>>,
    rw_window: Option<Worker<pile_rw::PileRw<D>>>,
    // lookup_rx: Receiver<Result<Rep<D>, B::Error>>
}

#[derive(Debug)]
pub enum Error {
    WindowsDatabaseNotADir(PathBuf),
    CreateWindowsDir(PathBuf, io::Error),
    StatWindowsDir(PathBuf, io::Error),
    ReadWindowsDir(PathBuf, io::Error),
    ReadWindowsDirEntry(PathBuf, io::Error),
    StatWindowsDirEntry(PathBuf, io::Error),
    PileLookup(pile_lookup::Error),
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

        let mut windows = Vec::new();
        // open existing windows
        for entry in try!(fs::read_dir(&base_dir).map_err(|e| Error::ReadWindowsDir(base_dir.clone(), e))) {
            let database = try!(entry.map_err(|e| Error::ReadWindowsDirEntry(base_dir.clone(), e))).path();
            let stat = try!(fs::metadata(&*database).map_err(|e| Error::StatWindowsDirEntry(database.clone(), e)));
            if !stat.is_dir() {
                continue;
            }

            let pile_lookup = try!(pile_lookup::PileLookup::new(database).map_err(|e| Error::PileLookup(e)));
            windows.push(Window::Ro(Worker::run(pile_lookup)));
        }

        Ok(Stream {
            windows_dir: base_dir,
            windows: windows,
            rw_window: None,
        })
    }
}
