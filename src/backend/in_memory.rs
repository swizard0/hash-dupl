use std::sync::Arc;
use std::clone::Clone;
use std::collections::HashMap;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use slices_merger::SlicesMerger;
use super::super::{Backend, CandidatesCollector, Document, State, LookupError};

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

    fn lookup<C, CE>(&mut self, bands: &[u64], collector: &mut C) -> Result<(), LookupError<(), CE>>
        where C: CandidatesCollector<Error = CE, UserData = UD>
    {
        self.merger.reset();
        for band in bands {
            if let Some(docs) = self.bands_index.get(band) {
                self.merger.add(&docs);
            }
        }

        for &DocWithId { doc: ref candidate_doc, .. } in self.merger.iter() {
            if collector.accept_signature(&candidate_doc.signature) {
                try!(collector.receive(candidate_doc.clone()).map_err(|e| LookupError::Collector(e)));
            }
        }

        Ok(())
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

    #[test]
    fn insert_lookup() {
        let mut backend = InMemory::<String>::new();
        backend.insert(Document {
            signature: vec![1, 2, 3],
            user_data: "some text".to_owned(),
        }, &[100, 300, 400]).unwrap();
        backend.insert(Document {
            signature: vec![4, 5, 6, 7],
            user_data: "some other text".to_owned(),
        }, &[200, 300, 500]).unwrap();

        let mut results = Vec::new();
        backend.lookup(&[100, 400], &mut results).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(&results[0].signature, &[1, 2, 3]);
        assert_eq!(&results[0].user_data, "some text");
        results.clear();
        backend.lookup(&[200, 500, 600, 700], &mut results).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(&results[0].signature, &[4, 5, 6, 7]);
        assert_eq!(&results[0].user_data, "some other text");
        results.clear();
        backend.lookup(&[300], &mut results).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(&results[0].signature, &[1, 2, 3]);
        assert_eq!(&results[0].user_data, "some text");
        assert_eq!(&results[1].signature, &[4, 5, 6, 7]);
        assert_eq!(&results[1].user_data, "some other text");
    }
}
