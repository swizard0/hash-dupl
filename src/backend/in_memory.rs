use std::sync::Arc;
use std::clone::Clone;
use std::vec::IntoIter;
use std::collections::HashMap;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use slices_merger::SlicesMerger;
use super::super::{Backend, CandidateFilter, Document, State};

struct DocWithId<UD> {
    id: u64,
    doc: Arc<Document<UD>>,
}

impl<UD> Clone for DocWithId<UD> {
    fn clone(&self) -> DocWithId<UD> {
        DocWithId {
            id: self.id,
            doc: self.doc.clone(),
        }
    }
}

impl<UD> PartialEq for DocWithId<UD> {
    fn eq(&self, other: &DocWithId<UD>) -> bool {
        self.id.eq(&other.id)
    }
}

impl<UD> PartialOrd for DocWithId<UD> {
    fn partial_cmp(&self, other: &DocWithId<UD>) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

pub struct InMemory<UD> {
    serial: u64,
    bands_index: HashMap<u64, Vec<DocWithId<UD>>>,
    state: Option<State>,
    merger: SlicesMerger<DocWithId<UD>>,
}

impl<UD> InMemory<UD> {
    pub fn new() -> InMemory<UD> {
        InMemory {
            serial: 0,
            bands_index: HashMap::new(),
            state: None,
            merger: SlicesMerger::new(),
        }
    }
}

impl<UD> Backend for InMemory<UD> {
    type Error = ();
    type UserData = UD;
    type Iter = IntoIter<Arc<Document<UD>>>;

    fn save_state(&mut self, state: &State) -> Result<(), ()> {
        self.state = Some(state.clone());
        Ok(())
    }

    fn load_state(&mut self) -> Result<Option<State>, ()> {
        Ok(self.state.clone())
    }

    fn insert(&mut self, doc: Document<UD>, bands: &[u64]) -> Result<(), ()> {
        let id = self.serial;
        self.serial += 1;
        let shared_doc = Arc::new(doc);
        for &band in bands.iter() {
            let docs = self.bands_index.entry(band).or_insert(Vec::new());
            docs.push(DocWithId { id: id, doc: shared_doc.clone(), });
        }
        Ok(())
    }

    fn lookup<F>(&mut self, bands: &[u64], filter: &mut F) -> Result<Self::Iter, ()> where F: CandidateFilter<UD> {
        self.merger.reset();
        for band in bands {
            if let Some(docs) = self.bands_index.get(band) {
                self.merger.add(&docs);
            }
        }

        Err(())
    }
}

#[cfg(test)]
mod test {
    use super::InMemory;
    use super::super::super::{Backend, Document, State, Config};

    #[test]
    fn save_load_state() {
        let state = State::new(Config::default());
        let mut backend = InMemory::<String>::new();
        assert_eq!(backend.load_state().unwrap(), None);
        backend.save_state(&state).unwrap();
        assert_eq!(backend.load_state().unwrap().unwrap(), state);
    }
}
