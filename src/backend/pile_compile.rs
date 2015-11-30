use std::{io, fs};
use std::sync::Arc;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::thread::{Builder, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver};
use bin_merge_pile::{merge, reduce, ntree_bkd};
use bin_merge_pile::bkd::{file, file_cache};
use serde::Serialize;
use rmp_serde;
use super::pile_common::{BandEntry, BandEntriesReducer};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

pub struct Params {
    pub min_tree_height: usize,
    pub memory_limit_power: usize,
    pub parallel_config: merge::ParallelConfig,
}

enum IndexCommand<D> {
    Insert(Arc<Signature>, Arc<D>),
    Finish,
}

pub struct PileCompile<D> {
    tx: Sender<IndexCommand<D>>,
    rx: Receiver<()>,
    slave: JoinHandle<()>,
}

#[derive(Debug)]
pub enum Error {
    RemoveDatabaseDir(PathBuf),
    CreateDatabaseDir(PathBuf),
    CreateDocsFile(PathBuf),
    SeekDocsFile(io::Error),
    CreateBandsFile(ntree_bkd::file::Error),
    TapeAdd(file::Error),
    SerializeDoc(rmp_serde::encode::Error),
    ReadUnsupported,
}

impl<D> PileCompile<D> where D: Serialize + Send + Sync + 'static {
    pub fn new<P>(database_dir: P, params: Params) -> Result<PileCompile<D>, Error> where P: AsRef<Path> {
        let mut base_dir = PathBuf::new();
        base_dir.push(&database_dir);

        match fs::remove_dir_all(&database_dir) {
            Ok(()) => (),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(_) => return Err(Error::RemoveDatabaseDir(base_dir)),
        }

        try!(fs::create_dir(&database_dir).map_err(|_| Error::CreateDatabaseDir(base_dir.clone())));
        let mut docs_filename = base_dir.clone();
        docs_filename.push("docs.bin");
        let docs_file = try!(fs::File::create(&docs_filename).map_err(|_| Error::CreateDocsFile(docs_filename)));

        let mut index_filename = base_dir.clone();
        index_filename.push("bands.bin");
        let index_ntree = try!(ntree_bkd::file::FileWriter::new(index_filename).map_err(|e| Error::CreateBandsFile(e)));

        let index_tape = merge::BinMergeTape::with_params(
            reduce::ReducerTapesCreator::new(
                file_cache::FileCacheTapesCreator::new(params.memory_limit_power, database_dir),
                BandEntriesReducer::new()),
            params.parallel_config);

        let (master_tx, slave_rx) = channel();
        let (slave_tx, master_rx) = channel();
        let slave = Builder::new().name("pile_compile indexer".to_owned())
            .spawn(move || indexer_loop(index_tape, index_ntree, docs_file, slave_tx, slave_rx).unwrap()).unwrap();

        Ok(PileCompile {
            tx: master_tx,
            rx: master_rx,
            slave: slave,
        })
    }
}

fn indexer_loop<D, PC>(mut index_tape: merge::BinMergeTape<PC>,
                       mut index_ntree: ntree_bkd::file::FileWriter<BandEntry>,
                       docs_file: fs::File,
                       tx: Sender<()>,
                       rx: Receiver<IndexCommand<D>>) -> Result<(), Error>
    where D: Serialize, PC: merge::TapesCreator<Item = BandEntry, Error = file::Error>
{
    let mut docs_file_writer = io::BufWriter::new(docs_file);
    loop {
        match rx.recv().unwrap() {
            IndexCommand::Insert(signature, document) => {
                let doc_offset = try!(docs_file_writer.seek(SeekFrom::Current(0)).map_err(|e| Error::SeekDocsFile(e)));
                try!(document.serialize(&mut rmp_serde::Serializer::new(&mut docs_file_writer)).map_err(|e| Error::SerializeDoc(e)));
                for &band in signature.bands.iter() {
                    try!(index_tape.add(BandEntry { band: band, docs: vec![doc_offset], }).map_err(|e| Error::TapeAdd(e)))
                }
            },
            IndexCommand::Finish => {
                return Ok(())
            },
        }
    }
}
