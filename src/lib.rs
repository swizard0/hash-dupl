extern crate fnv;
extern crate rand;
extern crate time;
extern crate bin_merge_pile;

use rand::{thread_rng, Rng};
use time::Timespec;

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

pub struct HashDupl {
    created_at: Timespec,
    config: Config,
    band_length: usize,
    bands_count: usize,
    minhash_seeds: Vec<u64>,
    bands_seeds: Vec<u64>,
}

#[derive(Debug)]
pub enum ConfigError {
    ZeroShingleLength,
    ZeroSignatureLength,
    InvalidSimilarityThresholdRange,
    InvalidBandMinProbabilityRange,
}

#[derive(Debug)]
pub enum Error {
    Config(ConfigError),
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

impl HashDupl {
    pub fn new(user_config: Config) -> Result<HashDupl, Error> {
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
                    created_at: time::get_time(),
                    config: config,
                    band_length: band_length,
                    bands_count: bands_count,
                    minhash_seeds: minhash_seeds,
                    bands_seeds: bands_seeds,
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
