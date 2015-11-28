use std::sync::Arc;
use std::clone::Clone;
use std::collections::HashMap;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use slices_merger::SlicesMerger;
use super::super::{Backend, CandidatesCollector, Signature, State, LookupError};

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
    state: Option<State>,
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

    fn save_state(&mut self, state: &State) -> Result<(), ()> {
        self.state = Some(state.clone());
        Ok(())
    }

    fn load_state(&mut self) -> Result<Option<State>, ()> {
        Ok(self.state.clone())
    }

    fn insert(&mut self, signature: &Signature, doc: D) -> Result<(), ()> {
        let id = self.serial;
        self.serial += 1;
        let entry = Arc::new(DocEntry {
            minhash: signature.minhash.clone().into_boxed_slice(),
            doc: Arc::new(doc),
        });

        for &band in signature.bands.iter() {
            let docs = self.bands_index.entry(band).or_insert(Vec::new());
            docs.push(DocWithId { id: id, entry: entry.clone(), });
        }
        Ok(())
    }

    fn lookup<C, CE>(&mut self, signature: &Signature, collector: &mut C) -> Result<(), LookupError<(), CE>>
        where C: CandidatesCollector<Error = CE, Document = D>
    {
        self.merger.reset();
        for band in signature.bands.iter() {
            if let Some(docs) = self.bands_index.get(band) {
                self.merger.add(&docs);
            }
        }

        for &DocWithId { entry: ref e, .. } in self.merger.iter() {
            if collector.accept_minhash(&signature.minhash, &e.minhash) {
                try!(collector.receive(&signature.minhash, &e.minhash, e.doc.clone()).map_err(|e| LookupError::Collector(e)));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use super::InMemory;
    use super::super::super::{Backend, Signature, State, Config};

    #[test]
    fn save_load_state() {
        let state = State::new(Config::default());
        let mut backend = InMemory::<String>::new();
        assert_eq!(backend.load_state().unwrap(), None);
        backend.save_state(&state).unwrap();
        assert_eq!(backend.load_state().unwrap().unwrap(), state);
    }

    #[test]
    fn insert_lookup() {
        let mut backend = InMemory::<String>::new();
        backend.insert(&Signature { minhash: vec![1, 2, 3], bands: vec![100, 300, 400], },
                       "some text".to_owned()).unwrap();
        backend.insert(&Signature { minhash: vec![4, 5, 6], bands: vec![200, 300, 500], },
                       "some other text".to_owned()).unwrap();

        let mut results = Vec::new();
        backend.lookup(&Signature { minhash: vec![1, 2, 3], bands: vec![100, 400], }, &mut results).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].similarity, 1.0);
        assert_eq!(results[0].document, Arc::new("some text".to_owned()));
        results.clear();
        backend.lookup(&Signature { minhash: vec![4, 5, 6], bands: vec![200, 500, 600, 700], }, &mut results).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].similarity, 1.0);
        assert_eq!(results[0].document, Arc::new("some other text".to_owned()));
        results.clear();
        backend.lookup(&Signature { minhash: vec![1, 2, 4], bands: vec![300], }, &mut results).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document, Arc::new("some text".to_owned()));
        assert_eq!(results[1].document, Arc::new("some other text".to_owned()));
    }
}
