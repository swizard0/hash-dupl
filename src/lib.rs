extern crate fnv;
extern crate rand;
extern crate time;
extern crate tokenizer;
extern crate token_ngrams;
extern crate slices_merger;
extern crate bin_merge_pile;

use std::sync::Arc;
use std::ops::Deref;
use std::default::Default;
use std::hash::{Hash, Hasher};
use rand::{thread_rng, Rng};
use time::Timespec;
use fnv::FnvHasher;

pub mod shingler;
pub mod backend;

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Config {
    pub shingle_length: usize,
    pub signature_length: usize,
    pub similarity_threshold: f64,
    pub band_min_probability: f64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            shingle_length: 3,
            signature_length: 128,
            similarity_threshold: 0.4,
            band_min_probability: 0.94,
        }
    }
}

pub type Signature = Vec<u64>;

pub struct Document<UD> {
    signature: Signature,
    user_data: UD,
}

impl<UD> Deref for Document<UD> {
    type Target = UD;

    fn deref(&self) -> &UD {
        &self.user_data
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct State {
    config: Config,
    created_at: Timespec,
    band_length: usize,
    minhash_seeds: Vec<u64>,
    bands_seeds: Vec<u64>,
}

impl State {
    pub fn new(config: Config) -> State {
        let band_length = maximum_band_length(config.signature_length, config.similarity_threshold, config.band_min_probability);
        let bands_count = ((band_length as f64) / band_length as f64).ceil() as usize;
        let minhash_seeds = random_seeds(config.signature_length);
        let bands_seeds = random_seeds(bands_count);
        State {
            config: config,
            created_at: time::get_time(),
            band_length: band_length,
            minhash_seeds: minhash_seeds,
            bands_seeds: bands_seeds,
        }
    }
}

pub trait CandidatesCollector {
    type Error;
    type UserData;

    fn accept_signature(&mut self, signature: &Signature) -> bool;
    fn receive(&mut self, doc: Arc<Document<Self::UserData>>) -> Result<(), Self::Error>;
}

impl<T> CandidatesCollector for Vec<Arc<Document<T>>> {
    type Error = ();
    type UserData = T;

    fn accept_signature(&mut self, _signature: &Signature) -> bool {
        true
    }

    fn receive(&mut self, doc: Arc<Document<T>>) -> Result<(), ()> {
        self.push(doc);
        Ok(())
    }
}

#[derive(Debug)]
pub enum LookupError<BE, CE> {
    Backend(BE),
    Collector(CE),
}

pub trait Backend {
    type Error;
    type UserData;

    fn save_state(&mut self, &State) -> Result<(), Self::Error>;
    fn load_state(&mut self) -> Result<Option<State>, Self::Error>;
    fn insert(&mut self, doc: Document<Self::UserData>, bands: &[u64]) -> Result<(), Self::Error>;
    fn lookup<C, CE>(&mut self, bands: &[u64], collector: &mut C) -> Result<(), LookupError<Self::Error, CE>>
        where C: CandidatesCollector<Error = CE, UserData = Self::UserData>;
    fn rotate(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait Shingler {
    type Error;

    fn shinglify(&mut self, text: &str, shingle_length: usize, hashed_shingles: &mut Vec<u64>) -> Result<(), Self::Error>;
}

pub struct HashDupl<S, B> {
    shingler: S,
    backend: B,
    state: State,
    shingles: Vec<u64>,
    bands: Vec<u64>,
}

#[derive(Debug)]
pub enum ConfigError {
    ZeroShingleLength,
    ZeroSignatureLength,
    InvalidSimilarityThresholdRange,
    InvalidBandMinProbabilityRange,
}

#[derive(Debug)]
pub enum Error<SE, BE> {
    Config(ConfigError),
    Shingler(SE),
    Backend(BE),
    NoShinglesBuilt,
}

fn random_seeds(len: usize) -> Vec<u64> {
    let mut rng = thread_rng();
    (0 .. len).map(|_| rng.gen()).collect()
}

fn lsh_prob(signature_length: usize, band_length: usize, jaccard_similarity: f64) -> f64 {
    let p = 1.0 - jaccard_similarity.powi(band_length as i32);
    let b = ((signature_length as f64) / band_length as f64).ceil();
    1.0 - p.powf(b)
}

pub fn maximum_band_length(signature_length: usize, expected_min_jaccard_similarity: f64, expected_match_probability: f64) -> usize {
    for band_length in (1 .. signature_length).rev() {
        let prob = lsh_prob(signature_length, band_length, expected_min_jaccard_similarity);
        if prob >= expected_match_probability {
            return band_length
        }
    }

    1
}

impl<UD, S, B, SE, BE> HashDupl<S, B> where S: Shingler<Error = SE>, B: Backend<Error = BE, UserData = UD> {
    pub fn new(shingler: S, backend: B, user_config: Config) -> Result<HashDupl<S, B>, Error<SE, BE>> {
        match user_config {
            Config { shingle_length: l, .. } if l == 0 =>
                Err(Error::Config(ConfigError::ZeroShingleLength)),
            Config { signature_length: l, .. } if l == 0 =>
                Err(Error::Config(ConfigError::ZeroSignatureLength)),
            Config { similarity_threshold: s, .. } if s < 0.0 || s > 1.0 =>
                Err(Error::Config(ConfigError::InvalidSimilarityThresholdRange)),
            Config { band_min_probability: p, .. } if p < 0.0 || p > 1.0 =>
                Err(Error::Config(ConfigError::InvalidBandMinProbabilityRange)),
            config => {
                Ok(HashDupl {
                    shingler: shingler,
                    backend: backend,
                    state: State::new(config),
                    shingles: Vec::new(),
                    bands: Vec::new(),
                })
            },
        }
    }

    pub fn sign<T>(&mut self, text: T, signature: &mut Signature) -> Result<(), Error<SE, BE>> where T: Deref<Target = str> {
        try!(self.shingler.shinglify(&text, self.state.config.shingle_length, &mut self.shingles).map_err(|e| Error::Shingler(e)));

        signature.clear();
        for &seed in self.state.minhash_seeds.iter() {
            let maybe_minhash = self.shingles
                .iter()
                .map(|shingle_hash| {
                    let mut hasher = FnvHasher::default();
                    seed.hash(&mut hasher);
                    shingle_hash.hash(&mut hasher);
                    hasher.finish()
                })
                .min()
                .ok_or(Error::NoShinglesBuilt);
            signature.push(try!(maybe_minhash));
        }

        Ok(())
    }

    fn build_bands(&mut self, signature: &Signature) {
        self.bands.clear();
        let mut start = 0;
        for &seed in self.state.bands_seeds.iter() {
            let mut hasher = FnvHasher::default();
            seed.hash(&mut hasher);

            let mut end = start + self.state.band_length;
            if end > self.state.config.signature_length {
                end = self.state.config.signature_length;
            }

            for minhash in signature[start .. end].iter() {
                minhash.hash(&mut hasher);
            }

            self.bands.push(hasher.finish());
            start += self.state.band_length;
        }
    }
}

#[cfg(test)]
mod test {
    use super::{HashDupl, Config, Signature, maximum_band_length};
    use super::shingler::tokens::Tokens;
    use super::backend::in_memory::InMemory;

    #[test]
    fn lsh_prob() {
        assert_eq!(maximum_band_length(128, 0.4, 0.94), 3);
        assert_eq!(maximum_band_length(128, 0.5, 0.85), 4);
        assert_eq!(maximum_band_length(256, 0.7, 0.99), 6);
    }

    #[test]
    fn sign_basic() {
        let mut hd = HashDupl::<_, InMemory<String>>::new(Tokens::new(), InMemory::new(), Config::default()).unwrap();
        let mut signature = Signature::new();
        hd.sign("some text to sign and check", &mut signature).unwrap();
        assert_eq!(signature.len(), 128);
    }
}
