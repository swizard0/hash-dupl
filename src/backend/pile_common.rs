use std::borrow::Borrow;
use std::marker::PhantomData;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use time::Timespec;
use serde::de::{self, Deserialize, Deserializer};
use serde::ser::{Serialize, SerializeTuple, Serializer};
use bin_merge_pile::reduce::Reducer;
use slices_merger::SlicesMerger;
use super::super::{Config, State};

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Debug)]
pub struct Offset {
    pub minhash_offset: u64,
    pub doc_offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BandEntry {
    pub band: u64,
    pub offsets: Option<Vec<Offset>>,
}

impl Borrow<u64> for BandEntry {
    fn borrow(&self) -> &u64 {
        &self.band
    }
}

impl BandEntry {
    pub fn entry(band: u64, minhash_offset: u64, doc_offset: u64) -> BandEntry {
        BandEntry {
            band: band,
            offsets: Some(vec![Offset {
                minhash_offset: minhash_offset,
                doc_offset: doc_offset,
            }]),
        }
    }
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

const STATE_MAGIC: u64 = 0xb6e05a82649d271f;

impl Serialize for State {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut ts = serializer.serialize_tuple(10)?;
        ts.serialize_element(&STATE_MAGIC)?;
        ts.serialize_element(&self.config.shingle_length)?;
        ts.serialize_element(&self.config.signature_length)?;
        ts.serialize_element(&self.config.similarity_threshold)?;
        ts.serialize_element(&self.config.band_min_probability)?;
        ts.serialize_element(&self.created_at.sec)?;
        ts.serialize_element(&self.created_at.nsec)?;
        ts.serialize_element(&self.band_length)?;
        ts.serialize_element(&self.minhash_seeds)?;
        ts.serialize_element(&self.bands_seeds)?;
        ts.end()
    }
}

impl<'a> Deserialize<'a> for State {
    fn deserialize<D>(deserializer: D) -> Result<State, D::Error> where D: Deserializer<'a> {
        let (magic, shingle_length, signature_length, similarity_threshold,
             band_min_probability, sec, nsec, band_length, minhash_seeds, bands_seeds)
            : (u64, _, _, _, _, _, _, _, _, _)
            = Deserialize::deserialize(deserializer)?;
        if magic != STATE_MAGIC {
            return Err(de::Error::custom(format!("invalid state magic: {}, expected: {}", magic, STATE_MAGIC)))
        }
        Ok(State {
            config: Config {
                shingle_length: shingle_length,
                signature_length: signature_length,
                similarity_threshold: similarity_threshold,
                band_min_probability: band_min_probability,
            },
            created_at: Timespec { sec: sec, nsec: nsec, },
            band_length: band_length,
            minhash_seeds: minhash_seeds,
            bands_seeds: bands_seeds,
        })
    }
}

pub struct BandEntriesReducer<E>(PhantomData<E>);

impl<E> BandEntriesReducer<E> {
    pub fn new() -> BandEntriesReducer<E> {
        BandEntriesReducer(PhantomData)
    }
}

impl<E> Reducer<BandEntry> for BandEntriesReducer<E> where E: Send + Sync + 'static {
    type Error = E;

    fn reduce(&self, existing: &mut BandEntry, incoming: BandEntry) -> Result<(), Self::Error> {
        existing.offsets =
            match (existing.offsets.take(), incoming.offsets) {
                (Some(e_offsets), Some(i_offsets)) => {
                    let mut merger = SlicesMerger::from(i_offsets);
                    merger.add(e_offsets.into_iter());
                    Some(merger.finish())
                },
                (Some(e_offsets), None) =>
                    Some(e_offsets),
                (None, Some(i_offsets)) =>
                    Some(i_offsets),
                (None, None) =>
                    None,
            };
        Ok(())
    }
}
