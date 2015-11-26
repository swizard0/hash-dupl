use std::rc::Rc;
use std::collections::HashMap;
use super::super::{Backend, Document, State};

struct DocWithId<UD> {
    id: u64,
    doc: Document<UD>,
}

pub struct InMemory<UD> {
    serial: u64,
    bands_index: HashMap<u64, Vec<Rc<DocWithId<UD>>>>,
    state: Option<State>,
}

impl<UD> InMemory<UD> {
    pub fn new() -> InMemory<UD> {
        InMemory {
            serial: 0,
            bands_index: HashMap::new(),
            state: None,
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
        let doc = Rc::new(DocWithId { id: id, doc: doc, });
        for &band in bands.iter() {
            let docs = self.bands_index.entry(band).or_insert(Vec::new());
            docs.push(doc.clone());
        }
        Ok(())
    }
}
