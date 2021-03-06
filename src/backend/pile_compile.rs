use std::{io, fs};
use std::sync::Arc;
use std::default::Default;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::thread::{Builder, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver};
use bin_merge_pile::{merge, reduce, ntree, ntree_bkd};
use bin_merge_pile::ntree::NTreeWriter;
use bin_merge_pile::bkd::{file, file_cache};
use serde::Serialize;
use rmp_serde;
use super::pile_common::{BandEntry, BandEntriesReducer};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

#[derive(Clone, Copy)]
pub struct Params {
    pub min_tree_height: usize,
    pub max_block_size: usize,
    pub memory_limit_power: usize,
    pub parallel_config: merge::ParallelConfig,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            min_tree_height: 3,
            max_block_size: 64,
            memory_limit_power: 16,
            parallel_config: merge::ParallelConfig::SingleThread,
        }
    }
}

enum IndexCommand<D> {
    Insert(Arc<Signature>, Arc<D>),
    Finish,
}

pub struct PileCompile<D> {
    state_filename: PathBuf,
    tx: Sender<IndexCommand<D>>,
    rx: Receiver<()>,
    slave: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub enum Error {
    RemoveDatabaseDir(PathBuf, io::Error),
    CreateDatabaseDir(PathBuf, io::Error),
    CreateDocsFile(PathBuf, io::Error),
    CreateMinhashFile(PathBuf, io::Error),
    SeekMinhashFile(io::Error),
    SeekDocsFile(io::Error),
    CreateStateFile(PathBuf, io::Error),
    CreateBandsFile(ntree_bkd::file::Error),
    TapeAdd(reduce::Error<file::Error, ()>),
    TapeFinish(reduce::Error<file::Error, ()>),
    TapeIter(reduce::Error<file::Error, ()>),
    NTreeBuild(ntree_bkd::file::Error),
    SerializeState(rmp_serde::encode::Error),
    SerializeDoc(rmp_serde::encode::Error),
    SerializeMinhash(rmp_serde::encode::Error),
    ReadUnsupported,
}

impl<D> PileCompile<D> where D: Serialize + Send + Sync + 'static {
    pub fn new<P>(database_dir: P, params: Params) -> Result<PileCompile<D>, Error> where P: AsRef<Path> {
        let mut base_dir = PathBuf::new();
        base_dir.push(&database_dir);

        match fs::remove_dir_all(&database_dir) {
            Ok(()) => (),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(Error::RemoveDatabaseDir(base_dir, e)),
        }

        fs::create_dir(&database_dir).map_err(|e| Error::CreateDatabaseDir(base_dir.clone(), e))?;
        let mut docs_filename = base_dir.clone();
        docs_filename.push("docs.bin");
        let docs_file = fs::File::create(&docs_filename).map_err(|e| Error::CreateDocsFile(docs_filename, e))?;

        let mut minhash_filename = base_dir.clone();
        minhash_filename.push("minhash.bin");
        let minhash_file = fs::File::create(&minhash_filename).map_err(|e| Error::CreateMinhashFile(minhash_filename, e))?;

        let mut index_filename = base_dir.clone();
        index_filename.push("bands.bin");
        let index_ntree = ntree_bkd::file::FileWriter::new(index_filename).map_err(Error::CreateBandsFile)?;

        let index_tape = merge::BinMergeTape::with_params(
            reduce::ReducerTapesCreator::new(
                file_cache::FileCacheTapesCreator::new(params.memory_limit_power, database_dir),
                BandEntriesReducer::new()),
            params.parallel_config);

        let mut state_filename = base_dir.clone();
        state_filename.push("state.bin");

        let (master_tx, slave_rx) = channel();
        let (slave_tx, master_rx) = channel();
        let slave = Builder::new().name("pile_compile indexer".to_owned())
            .spawn(move || indexer_loop(index_tape,
                                        index_ntree,
                                        params.min_tree_height,
                                        params.max_block_size,
                                        docs_file,
                                        minhash_file,
                                        slave_tx,
                                        slave_rx).unwrap()).unwrap();

        Ok(PileCompile {
            state_filename: state_filename,
            tx: master_tx,
            rx: master_rx,
            slave: Some(slave),
        })
    }
}

impl<D> Drop for PileCompile<D> {
    fn drop(&mut self) {
        if let Some(slave) = self.slave.take() {
            self.tx.send(IndexCommand::Finish).unwrap();
            match self.rx.recv() {
                Ok(()) => slave.join().unwrap(),
                other => panic!("unexpected rep while joining pile_compile indexer: {:?}", other),
            }
        }
    }
}

impl<D> Backend for PileCompile<D> {
    type Error = Error;
    type Document = D;

    fn save_state(&mut self, state: Arc<State>) -> Result<(), Error> {
        let mut state_file = fs::File::create(&self.state_filename).map_err(|e| Error::CreateStateFile(self.state_filename.clone(), e))?;
        state.serialize(&mut rmp_serde::Serializer::new(&mut state_file)).map_err(Error::SerializeState)
    }

    fn load_state(&mut self) -> Result<Option<Arc<State>>, Error> {
        Err(Error::ReadUnsupported)
    }

    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<D>) -> Result<(), Error> {
        self.tx.send(IndexCommand::Insert(signature, doc)).unwrap();
        Ok(())
    }

    fn lookup<F, C, CR, CE>(&mut self, _signature: Arc<Signature>, _filter: F, _collector: C) -> Result<CR, LookupError<Error, CE>>
        where F: CandidatesFilter + Clone, C: CandidatesCollector<Error = CE, Document = D, Result = CR>
    {
        Err(LookupError::Backend(Error::ReadUnsupported))
    }
}


fn indexer_loop<D, PC>(mut index_tape: merge::BinMergeTape<PC>,
                       index_ntree: ntree_bkd::file::FileWriter<BandEntry>,
                       min_tree_height: usize,
                       max_block_size: usize,
                       docs_file: fs::File,
                       minhash_file: fs::File,
                       tx: Sender<()>,
                       rx: Receiver<IndexCommand<D>>) -> Result<(), Error>
    where D: Serialize, PC: merge::TapesCreator<Item = BandEntry, Error = reduce::Error<file::Error, ()>>
{
    let mut docs_file_writer = io::BufWriter::new(docs_file);
    let mut minhash_file_writer = io::BufWriter::new(minhash_file);
    loop {
        match rx.recv().unwrap() {
            IndexCommand::Insert(signature, document) => {
                let minhash_offset = minhash_file_writer.seek(SeekFrom::Current(0)).map_err(Error::SeekMinhashFile)?;
                let doc_offset = docs_file_writer.seek(SeekFrom::Current(0)).map_err(Error::SeekDocsFile)?;
                signature.minhash.serialize(&mut rmp_serde::Serializer::new(&mut minhash_file_writer)).map_err(Error::SerializeMinhash)?;
                document.serialize(&mut rmp_serde::Serializer::new(&mut docs_file_writer)).map_err(Error::SerializeDoc)?;
                for &band in signature.bands.iter() {
                    index_tape.add(BandEntry::entry(band, minhash_offset, doc_offset)).map_err(Error::TapeAdd)?;
                }
            },
            IndexCommand::Finish => {
                match try!(index_tape.finish().map_err(Error::TapeFinish)) {
                    Some((index_iter, index_len)) =>
                        index_ntree.build(index_iter, index_len, min_tree_height, max_block_size),
                    None =>
                        index_ntree.build(None.into_iter(), 0, min_tree_height, max_block_size),
                }.map_err(|err| match err {
                    ntree::BuildError::Iter(e) => Error::TapeIter(e),
                    ntree::BuildError::NTree(e) => Error::NTreeBuild(e),
                })?;
                tx.send(()).unwrap();
                return Ok(())
            },
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use std::io::{Seek, SeekFrom};
    use serde::Deserialize;
    use rmp_serde::Deserializer;
    use bin_merge_pile::ntree_bkd;
    use bin_merge_pile::ntree::NTreeReader;
    use bin_merge_pile::merge::ParallelConfig;
    use super::{Params, PileCompile, Error};
    use super::super::pile_common::BandEntry;
    use super::super::super::{Backend, State, Config, Signature};

    #[test]
    fn save_check_state() {
        let state = Arc::new(State::new(Config::default()));
        {
            let mut backend = PileCompile::<String>::new("/tmp/pile_compile_a", Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            }).unwrap();

            match backend.load_state() { Err(Error::ReadUnsupported) => (), other => panic!("unexpected resut: {:?}", other), }
            backend.save_state(state.clone()).unwrap();
        }
        {
            let mut state_file = fs::File::open("/tmp/pile_compile_a/state.bin").unwrap();
            let mut deserializer = Deserializer::new(&mut state_file);
            let state_read = Arc::new(Deserialize::deserialize(&mut deserializer).unwrap());
            assert_eq!(state, state_read);
        }
    }

    #[test]
    fn insert_check() {
        let doc_a = Arc::new("some text".to_owned());
        let doc_b = Arc::new("some other text".to_owned());
        {
            let mut backend = PileCompile::<String>::new("/tmp/pile_compile_b", Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            }).unwrap();
            backend.insert(Arc::new(Signature { minhash: vec![1, 2, 3], bands: vec![100, 300, 400], }), doc_a.clone()).unwrap();
            backend.insert(Arc::new(Signature { minhash: vec![4, 5, 6], bands: vec![200, 300, 500], }), doc_b.clone()).unwrap();
        }
        {
            let mut checker: ntree_bkd::file::FileReader<BandEntry> =
                ntree_bkd::file::FileReader::new("/tmp/pile_compile_b/bands.bin").unwrap();
            let doc_a_offsets = {
                let keys = [100];
                let mut iter = checker.lookup_iter(keys.iter());
                match iter.next() {
                    Ok(Some((_, &BandEntry { offsets: Some(ref o), .. }))) => o.get(0).unwrap().clone(),
                    other => panic!("{:?}", other),
                }
            };
            let doc_b_offsets = {
                let keys = [200];
                let mut iter = checker.lookup_iter(keys.iter());
                match iter.next() {
                    Ok(Some((_, &BandEntry { offsets: Some(ref o), .. }))) => o.get(0).unwrap().clone(),
                    other => panic!("{:?}", other),
                }
            };
            let mut both_offsets = {
                let keys = [300];
                let mut iter = checker.lookup_iter(keys.iter());
                match iter.next() {
                    Ok(Some((_, &BandEntry { offsets: Some(ref o), .. }))) => o.clone(),
                    other => panic!("{:?}", other),
                }
            };
            both_offsets.sort();
            assert_eq!(both_offsets, vec![doc_a_offsets, doc_b_offsets]);

            let mut docs_file = fs::File::open("/tmp/pile_compile_b/docs.bin").unwrap();
            docs_file.seek(SeekFrom::Start(doc_a_offsets.doc_offset)).unwrap();
            let restored_doc_a: Arc<String> = Arc::new(Deserialize::deserialize(&mut Deserializer::new(&mut docs_file)).unwrap());
            assert_eq!(restored_doc_a, doc_a);
            docs_file.seek(SeekFrom::Start(doc_b_offsets.doc_offset)).unwrap();
            let restored_doc_b: Arc<String> = Arc::new(Deserialize::deserialize(&mut Deserializer::new(&mut docs_file)).unwrap());
            assert_eq!(restored_doc_b, doc_b);

            let mut minhash_file = fs::File::open("/tmp/pile_compile_b/minhash.bin").unwrap();
            minhash_file.seek(SeekFrom::Start(doc_a_offsets.minhash_offset)).unwrap();
            let restored_minhash_a: Vec<u64> = Deserialize::deserialize(&mut Deserializer::new(&mut minhash_file)).unwrap();
            assert_eq!(restored_minhash_a, vec![1, 2, 3]);
            minhash_file.seek(SeekFrom::Start(doc_b_offsets.minhash_offset)).unwrap();
            let restored_minhash_b: Vec<u64> = Deserialize::deserialize(&mut Deserializer::new(&mut minhash_file)).unwrap();
            assert_eq!(restored_minhash_b, vec![4, 5, 6]);
        }
    }
}
