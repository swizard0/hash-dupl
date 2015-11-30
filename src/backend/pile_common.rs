use std::marker::PhantomData;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use bin_merge_pile::reduce::Reducer;
use slices_merger::SlicesMerger;

pub struct BandEntry {
    pub band: u64,
    pub docs: Vec<u64>,
}

impl PartialEq for BandEntry {
    fn eq(&self, other: &BandEntry) -> bool {
        self.band.eq(&other.band)
    }
}

impl PartialOrd for BandEntry {
    fn partial_cmp(&self, other: &BandEntry) -> Option<Ordering> {
        self.band.partial_cmp(&other.band)
    }
}

impl Serialize for BandEntry {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        (self.band, &self.docs).serialize(serializer)
    }
}

impl Deserialize for BandEntry {
    fn deserialize<D>(deserializer: &mut D) -> Result<BandEntry, D::Error> where D: Deserializer {
        let (band, docs) = try!(Deserialize::deserialize(deserializer));
        Ok(BandEntry { band: band, docs: docs, })
    }
}

pub struct BandEntriesReducer<E>(PhantomData<E>);

impl<E> BandEntriesReducer<E> {
    pub fn new() -> BandEntriesReducer<E> {
        BandEntriesReducer(PhantomData)
    }
}

impl<E> Reducer<BandEntry, E> for BandEntriesReducer<E> where E: Send + Sync + 'static {
    fn reduce(&self, existing: &mut BandEntry, incoming: BandEntry) -> Result<(), E> {
        let mut merger = SlicesMerger::from(incoming.docs);
        merger.add(&existing.docs);
        existing.docs = merger.finish();
        Ok(())
    }
}
