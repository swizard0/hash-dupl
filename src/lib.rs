extern crate fnv;
extern crate rand;
extern crate time;
extern crate tokenizer;
extern crate token_ngrams;
extern crate slices_merger;
extern crate bin_merge_pile;

use std::sync::Arc;
use rand::{thread_rng, Rng};
use time::Timespec;

pub mod shingler;
pub mod backend;

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Config {
    pub shingle_length: usize,
    pub signature_length: usize,
    pub similarity_threshold: f64,
    pub band_min_probability: f64,
}

impl Config {
    pub fn default() -> Config {
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
}

pub trait Shingler {
    type Error;

    fn shinglify(&mut self, text: &str, shingle_length: usize) -> Result<Vec<u64>, Self::Error>;
}

pub struct HashDupl<S, B> {
    shingler: S,
    backend: B,
    state: State,
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
                })
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::maximum_band_length;

    #[test]
    fn lsh_prob() {
        assert_eq!(maximum_band_length(128, 0.4, 0.94), 3);
        assert_eq!(maximum_band_length(128, 0.5, 0.85), 4);
        assert_eq!(maximum_band_length(256, 0.7, 0.99), 6);
    }
}
