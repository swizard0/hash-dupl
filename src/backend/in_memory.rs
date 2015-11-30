use std::sync::Arc;
use std::clone::Clone;
use std::collections::HashMap;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use slices_merger::SlicesMerger;
use super::super::{Backend, CandidatesFilter, CandidatesCollector, Signature, State, LookupError};

struct DocEntry<D> {
    minhash: Box<[u64]>,
    doc: Arc<D>,
}

struct DocWithId<D> {
    id: u64,
    entry: Arc<DocEntry<D>>,
}

impl<D> Clone for DocWithId<D> {
    fn clone(&self) -> DocWithId<D> {
        DocWithId {
            id: self.id,
            entry: self.entry.clone(),
        }
    }
}

impl<D> PartialEq for DocWithId<D> {
    fn eq(&self, other: &DocWithId<D>) -> bool {
        self.id.eq(&other.id)
    }
}

impl<D> PartialOrd for DocWithId<D> {
    fn partial_cmp(&self, other: &DocWithId<D>) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

pub struct InMemory<D> {
    serial: u64,
    bands_index: HashMap<u64, Vec<DocWithId<D>>>,
    state: Option<Arc<State>>,
    merger: SlicesMerger<DocWithId<D>>,
}

impl<D> InMemory<D> {
    pub fn new() -> InMemory<D> {
        InMemory {
            serial: 0,
            bands_index: HashMap::new(),
            state: None,
            merger: SlicesMerger::new(),
        }
    }
}

impl<D> Backend for InMemory<D> {
    type Error = ();
    type Document = D;

    fn save_state(&mut self, state: Arc<State>) -> Result<(), ()> {
        self.state = Some(state.clone());
        Ok(())
    }

    fn load_state(&mut self) -> Result<Option<Arc<State>>, ()> {
        Ok(self.state.as_ref().map(|s| s.clone()))
    }

    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<D>) -> Result<(), ()> {
        let id = self.serial;
        self.serial += 1;
        let entry = Arc::new(DocEntry {
            minhash: signature.minhash.clone().into_boxed_slice(),
            doc: doc,
        });

        for &band in signature.bands.iter() {
            let docs = self.bands_index.entry(band).or_insert(Vec::new());
            docs.push(DocWithId { id: id, entry: entry.clone(), });
        }
        Ok(())
    }

    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, mut filter: F, mut collector: C) -> Result<CR, LookupError<(), CE>>
        where F: CandidatesFilter, C: CandidatesCollector<Error = CE, Document = D, Result = CR>
    {
        self.merger.reset();
        for band in signature.bands.iter() {
            if let Some(docs) = self.bands_index.get(band) {
                self.merger.add(&docs);
            }
        }

        for &DocWithId { entry: ref e, .. } in self.merger.iter() {
            if let Some(similarity) = filter.accept_minhash_similarity(&signature.minhash, &e.minhash) {
                try!(collector.receive(similarity, e.doc.clone()).map_err(|e| LookupError::Collector(e)));
            }
        }

        collector.finish().map_err(|e| LookupError::Collector(e))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use super::InMemory;
    use super::super::super::{Backend, SimilarityThresholdFilter, Signature, State, Config};

    #[test]
    fn save_load_state() {
        let state = Arc::new(State::new(Config::default()));
        let mut backend = InMemory::<String>::new();
        assert_eq!(backend.load_state().unwrap(), None);
        backend.save_state(state.clone()).unwrap();
        assert_eq!(backend.load_state().unwrap().unwrap(), state.clone());
    }

    #[test]
    fn insert_lookup() {
        let mut backend = InMemory::<String>::new();
        let doc_a = Arc::new("some text".to_owned());
        let doc_b = Arc::new("some other text".to_owned());
        backend.insert(Arc::new(Signature { minhash: vec![1, 2, 3], bands: vec![100, 300, 400], }), doc_a.clone()).unwrap();
        backend.insert(Arc::new(Signature { minhash: vec![4, 5, 6], bands: vec![200, 300, 500], }), doc_b.clone()).unwrap();

        let results = backend.lookup(Arc::new(Signature { minhash: vec![1, 2, 3], bands: vec![100, 400], }),
                                     SimilarityThresholdFilter(0.0),
                                     Vec::new()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].similarity, 1.0);
        assert_eq!(results[0].document, doc_a.clone());
        let results = backend.lookup(Arc::new(Signature { minhash: vec![4, 5, 6], bands: vec![200, 500, 600, 700], }),
                                     SimilarityThresholdFilter(0.0),
                                     Vec::new()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].similarity, 1.0);
        assert_eq!(results[0].document, doc_b.clone());
        let results = backend.lookup(Arc::new(Signature { minhash: vec![1, 2, 4], bands: vec![300], }),
                                     SimilarityThresholdFilter(0.0),
                                     Vec::new()).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document, doc_a.clone());
        assert_eq!(results[1].document, doc_b.clone());
    }
}
