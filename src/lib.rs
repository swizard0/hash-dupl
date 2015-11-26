extern crate fnv;
extern crate rand;
extern crate time;
extern crate tokenizer;
extern crate token_ngrams;
extern crate bin_merge_pile;

use rand::{thread_rng, Rng};
use time::Timespec;

pub mod lexer;
pub mod backend;

pub struct Config {
    pub shingle_length: usize,
    pub signature_length: usize,
    pub similarity_threshold: f64,
    pub band_min_probability: f64,
}

pub type Signature = Vec<u64>;

pub struct Document<UD> {
    similarity: f64,
    signature: Signature,
    user_data: UD,
}

pub struct State {
    config: Config,
    created_at: Timespec,
    band_length: usize,
    minhash_seeds: Vec<u64>,
    bands_seeds: Vec<u64>,
}

pub trait Backend {
    type Error;
    type UserData;

    fn save_state(&mut self, &State) -> Result<(), Self::Error>;
    fn load_state(&mut self) -> Result<State, Self::Error>;
    fn insert(&mut self, doc: Document<Self::UserData>, bands: &[u64]) -> Result<(), Self::Error>;
}

pub trait Lexer {
    type Error;
    type Iter: Iterator<Item = Result<u64, Self::Error>>;

    fn lexify(&mut self, text: &str) -> Result<Self::Iter, Self::Error>;
}

pub struct HashDupl<L, B> {
    lexer: L,
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
pub enum Error<LE, BE> {
    Config(ConfigError),
    Lexer(LE),
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

impl<UD, L, B, LE, BE> HashDupl<L, B> where L: Lexer<Error = LE>, B: Backend<Error = BE, UserData = UD> {
    pub fn new(lexer: L, backend: B, user_config: Config) -> Result<HashDupl<L, B>, Error<LE, BE>> {
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
                let band_length = maximum_band_length(config.signature_length, config.similarity_threshold, config.band_min_probability);
                let bands_count = ((band_length as f64) / band_length as f64).ceil() as usize;
                let minhash_seeds = random_seeds(config.signature_length);
                let bands_seeds = random_seeds(bands_count);

                Ok(HashDupl {
                    lexer: lexer,
                    backend: backend,
                    state: State {
                        config: config,
                        created_at: time::get_time(),
                        band_length: band_length,
                        minhash_seeds: minhash_seeds,
                        bands_seeds: bands_seeds,
                    },
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
