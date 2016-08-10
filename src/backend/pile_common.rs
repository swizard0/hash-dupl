use std::borrow::Borrow;
use std::marker::PhantomData;
use std::cmp::{PartialEq, PartialOrd, Ordering};
use time::Timespec;
use serde::{de, Serialize, Serializer, Deserialize, Deserializer};
use bin_merge_pile::reduce::Reducer;
use slices_merger::SlicesMerger;
use super::super::{Config, State};

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct Offset {
    pub minhash_offset: u64,
    pub doc_offset: u64,
}

#[derive(Debug)]
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

impl Serialize for Offset {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        try!(self.minhash_offset.serialize(serializer));
        try!(self.doc_offset.serialize(serializer));
        Ok(())
    }
}

impl Serialize for BandEntry {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        try!(self.band.serialize(serializer));
        try!(self.offsets.serialize(serializer));
        Ok(())
    }
}

impl Deserialize for Offset {
    fn deserialize<D>(deserializer: &mut D) -> Result<Offset, D::Error> where D: Deserializer {
        Ok(Offset {
            minhash_offset: try!(Deserialize::deserialize(deserializer)),
            doc_offset: try!(Deserialize::deserialize(deserializer)),
        })
    }
}

impl Deserialize for BandEntry {
    fn deserialize<D>(deserializer: &mut D) -> Result<BandEntry, D::Error> where D: Deserializer {
        Ok(BandEntry {
            band: try!(Deserialize::deserialize(deserializer)),
            offsets: try!(Deserialize::deserialize(deserializer)),
        })
    }
}

const STATE_MAGIC: u64 = 0xb5e05a82649d271f;

impl Serialize for State {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        try!(STATE_MAGIC.serialize(serializer));
        try!(self.config.shingle_length.serialize(serializer));
        try!(self.config.signature_length.serialize(serializer));
        try!(self.config.similarity_threshold.serialize(serializer));
        try!(self.config.band_min_probability.serialize(serializer));
        try!(self.created_at.sec.serialize(serializer));
        try!(self.created_at.nsec.serialize(serializer));
        try!(self.band_length.serialize(serializer));
        try!(self.minhash_seeds.serialize(serializer));
        try!(self.bands_seeds.serialize(serializer));
        Ok(())
    }
}

impl Deserialize for State {
    fn deserialize<D>(deserializer: &mut D) -> Result<State, D::Error> where D: Deserializer {
        let magic: u64 = try!(Deserialize::deserialize(deserializer));
        if magic != STATE_MAGIC {
            return Err(de::Error::custom(format!("invalid state magic: {}, expected: {}", magic, STATE_MAGIC)))
        }
        let shingle_length = try!(Deserialize::deserialize(deserializer));
        let signature_length = try!(Deserialize::deserialize(deserializer));
        let similarity_threshold = try!(Deserialize::deserialize(deserializer));
        let band_min_probability = try!(Deserialize::deserialize(deserializer));
        let sec = try!(Deserialize::deserialize(deserializer));
        let nsec = try!(Deserialize::deserialize(deserializer));
        let band_length = try!(Deserialize::deserialize(deserializer));
        let minhash_seeds = try!(Deserialize::deserialize(deserializer));
        let bands_seeds = try!(Deserialize::deserialize(deserializer));
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
