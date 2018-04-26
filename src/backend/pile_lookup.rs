use std::{io, fs};
use std::sync::Arc;
use std::marker::PhantomData;
use std::io::{Seek, SeekFrom, BufReader};
use std::path::{Path, PathBuf};
use bin_merge_pile::ntree_bkd;
use bin_merge_pile::ntree::NTreeReader;
use slices_merger::SlicesMerger;
use serde::Deserialize;
use rmp_serde;
use rmp_serde::Deserializer;
use super::pile_common::{Offset, BandEntry};
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

#[derive(Clone, Copy)]
pub struct Params {
    pub mmap_type: ntree_bkd::mmap::MmapType,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            mmap_type: ntree_bkd::mmap::MmapType::TrueMmap { madv_willneed: true, },
        }
    }
}

pub struct PileLookup<D> {
    state: Option<Arc<State>>,
    state_filename: PathBuf,
    minhash_file_reader: BufReader<fs::File>,
    docs_file_reader: BufReader<fs::File>,
    bands_index: ntree_bkd::mmap::MmapReader<BandEntry>,
    merger: SlicesMerger<Offset>,
    _marker: PhantomData<D>,
}

#[derive(Debug)]
pub enum Error {
    OpenDatabaseNotADir(PathBuf),
    OpenDatabase(PathBuf, io::Error),
    OpenStateFile(PathBuf, io::Error),
    OpenMinhashFile(PathBuf, io::Error),
    OpenDocsFile(PathBuf, io::Error),
    SeekMinhashFile(io::Error),
    SeekDocsFile(io::Error),
    OpenBandsFile(ntree_bkd::mmap::Error),
    BandsLookup(ntree_bkd::mmap::Error),
    DeserializeState(rmp_serde::decode::Error),
    DeserializeMinhash(rmp_serde::decode::Error),
    DeserializeDoc(rmp_serde::decode::Error),
    WriteUnsupported,
}

impl<D> PileLookup<D> {
    pub fn new<P>(database_dir: P, params: Params) -> Result<PileLookup<D>, Error> where P: AsRef<Path> {
        let mut base_dir = PathBuf::new();
        base_dir.push(&database_dir);

        match fs::metadata(database_dir) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(..) => return Err(Error::OpenDatabaseNotADir(base_dir.clone())),
            Err(e) => return Err(Error::OpenDatabase(base_dir.clone(), e)),
        }

        let mut minhash_filename = base_dir.clone();
        minhash_filename.push("minhash.bin");
        let minhash_file = try!(fs::File::open(&minhash_filename).map_err(|e| Error::OpenMinhashFile(minhash_filename, e)));

        let mut docs_filename = base_dir.clone();
        docs_filename.push("docs.bin");
        let docs_file = try!(fs::File::open(&docs_filename).map_err(|e| Error::OpenDocsFile(docs_filename, e)));

        let mut index_filename = base_dir.clone();
        index_filename.push("bands.bin");
        let bands_index = try!(ntree_bkd::mmap::MmapReader::new(index_filename, params.mmap_type).map_err(Error::OpenBandsFile));

        let mut state_filename = base_dir.clone();
        state_filename.push("state.bin");

        Ok(PileLookup {
            state: None,
            state_filename: state_filename,
            minhash_file_reader: BufReader::new(minhash_file),
            docs_file_reader: BufReader::new(docs_file),
            bands_index: bands_index,
            merger: SlicesMerger::new(),
            _marker: PhantomData,
        })
    }
}

impl<'a, D> Backend for PileLookup<D> where D: Deserialize<'a> {
    type Error = Error;
    type Document = D;

    fn save_state(&mut self, _state: Arc<State>) -> Result<(), Error> {
        Err(Error::WriteUnsupported)
    }

    fn load_state(&mut self) -> Result<Option<Arc<State>>, Error> {
        if let Some(ref state) = self.state {
            Ok(Some(state.clone()))
        } else {
            match fs::File::open(&self.state_filename) {
                Ok(mut state_file) => {
                    let mut deserializer = rmp_serde::Deserializer::new(&mut state_file);
                    let state = Arc::new(try!(Deserialize::deserialize(&mut deserializer).map_err(Error::DeserializeState)));
                    self.state = Some(state);
                    Ok(self.state.as_ref().map(|s| s.clone()))
                },
                Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
                    Ok(None),
                Err(e) =>
                    Err(Error::OpenStateFile(self.state_filename.clone(), e)),
            }
        }
    }

    fn insert(&mut self, _signature: Arc<Signature>, _doc: Arc<D>) -> Result<(), Error> {
        Err(Error::WriteUnsupported)
    }

    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, mut filter: F, mut collector: C) -> Result<CR, LookupError<Error, CE>>
        where F: CandidatesFilter + Clone, C: CandidatesCollector<Error = CE, Document = D, Result = CR>
    {
        self.merger.reset();
        let mut index_iter = self.bands_index.lookup_iter(signature.bands.iter());
        while let Some((_, entry)) = try!(index_iter.next().map_err(|e| LookupError::Backend(Error::BandsLookup(e)))) {
            if let Some(ref offsets) = entry.offsets {
                self.merger.add(offsets.iter().cloned())
            }
        }

        for offsets in self.merger.iter() {
            try!(self.minhash_file_reader.seek(
                SeekFrom::Start(offsets.minhash_offset)).map_err(|e| LookupError::Backend(Error::SeekMinhashFile(e))));
            let minhash: Vec<_> = try!(Deserialize::deserialize(
                &mut Deserializer::new(&mut self.minhash_file_reader)).map_err(|e| LookupError::Backend(Error::DeserializeMinhash(e))));
            if let Some(similarity) = filter.accept_minhash_similarity(&signature.minhash, &minhash) {
                try!(self.docs_file_reader.seek(
                    SeekFrom::Start(offsets.doc_offset)).map_err(|e| LookupError::Backend(Error::SeekDocsFile(e))));
                let doc: Arc<D> = Arc::new(try!(Deserialize::deserialize(
                    &mut Deserializer::new(&mut self.docs_file_reader)).map_err(|e| LookupError::Backend(Error::DeserializeDoc(e)))));
                try!(collector.receive(similarity, doc).map_err(LookupError::Collector));
            }
        }

        collector.finish().map_err(LookupError::Collector)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use bin_merge_pile::merge::ParallelConfig;
    use super::PileLookup;
    use super::super::pile_compile::{Params, PileCompile};
    use super::super::super::{Backend, SimilarityThresholdFilter, Signature, State, Config};

    #[test]
    fn save_load_state() {
        let state = Arc::new(State::new(Config::default()));
        {
            let mut backend = PileCompile::<String>::new("/tmp/pile_lookup_a", Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            }).unwrap();
            backend.save_state(state.clone()).unwrap();
        }
        {
            let mut backend = PileLookup::<String>::new("/tmp/pile_lookup_a", Default::default()).unwrap();
            assert_eq!(backend.load_state().unwrap().unwrap(), state.clone());
        }
    }

    #[test]
    fn insert_lookup() {
        let doc_a = Arc::new("some text".to_owned());
        let doc_b = Arc::new("some other text".to_owned());
        {
            let mut backend = PileCompile::<String>::new("/tmp/pile_lookup_b", Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            }).unwrap();
            backend.insert(Arc::new(Signature { minhash: vec![1, 2, 3], bands: vec![100, 300, 400], }), doc_a.clone()).unwrap();
            backend.insert(Arc::new(Signature { minhash: vec![4, 5, 6], bands: vec![200, 300, 500], }), doc_b.clone()).unwrap();
        }
        {
            let mut backend = PileLookup::<String>::new("/tmp/pile_lookup_b", Default::default()).unwrap();
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
