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

pub struct Signature {
    minhash: Vec<u64>,
    bands: Vec<u64>,
}

impl Signature {
    pub fn new() -> Signature {
        Signature {
            minhash: Vec::new(),
            bands: Vec::new(),
        }
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
        let bands_count = ((config.signature_length as f64) / band_length as f64).ceil() as usize;
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
    type Document;

    fn accept_minhash(&mut self, sample_minhash: &[u64], minhash: &[u64]) -> bool;
    fn receive(&mut self, sample_minhash: &[u64], minhash: &[u64], doc: Arc<Self::Document>) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct LookupResult<D> {
    pub similarity: f64,
    pub document: Arc<D>,
}

impl<D> CandidatesCollector for Vec<LookupResult<D>> {
    type Error = ();
    type Document = D;

    fn accept_minhash(&mut self, _sample_minhash: &[u64], _minhash: &[u64]) -> bool {
        true
    }

    fn receive(&mut self, sample_minhash: &[u64], minhash: &[u64], doc: Arc<D>) -> Result<(), ()> {
        self.push(LookupResult {
            similarity: minhash_distance(sample_minhash, minhash),
            document: doc,
        });
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
    type Document;

    fn save_state(&mut self, &State) -> Result<(), Self::Error>;
    fn load_state(&mut self) -> Result<Option<State>, Self::Error>;
    fn insert(&mut self, signature: &Signature, doc: Self::Document) -> Result<(), Self::Error>;
    fn lookup<C, CE>(&mut self, signature: &Signature, collector: &mut C) -> Result<(), LookupError<Self::Error, CE>>
        where C: CandidatesCollector<Error = CE, Document = Self::Document>;
    fn rotate(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub struct Shingles(Vec<u64>);

impl Shingles {
    pub fn new() -> Shingles {
        Shingles(Vec::new())
    }
}

pub trait Shingler {
    type Error;

    fn shinglify<'a>(&mut self, text: &str, shingle_length: usize, hashed_shingles: &'a mut Shingles) -> Result<&'a Shingles, Self::Error>;
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

pub fn minhash_distance(minhash_a: &[u64], minhash_b: &[u64]) -> f64 {
    assert!(!minhash_a.is_empty());
    assert_eq!(minhash_a.len(), minhash_b.len());
    let matched = minhash_a.iter().zip(minhash_b.iter()).filter(|&(a, b)| a == b).count();
    matched as f64 / minhash_a.len() as f64
}

impl<D, S, B, SE, BE> HashDupl<S, B> where S: Shingler<Error = SE>, B: Backend<Error = BE, Document = D> {
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

    pub fn shinglify<'a, T>(&mut self, text: T, shingles: &'a mut Shingles) -> Result<&'a Shingles, Error<SE, BE>> where T: Deref<Target = str> {
        self.shingler.shinglify(&text, self.state.config.shingle_length, shingles).map_err(|e| Error::Shingler(e))
    }

    pub fn sign<'a>(&mut self, shingles: &Shingles, signature: &'a mut Signature) -> Result<&'a Signature, Error<SE, BE>> {
        signature.minhash.clear();
        for &seed in self.state.minhash_seeds.iter() {
            let maybe_minhash = shingles.0.iter()
                .map(|shingle_hash| {
                    let mut hasher = FnvHasher::default();
                    seed.hash(&mut hasher);
                    shingle_hash.hash(&mut hasher);
                    hasher.finish()
                })
                .min()
                .ok_or(Error::NoShinglesBuilt);
            signature.minhash.push(try!(maybe_minhash));
        }

        signature.bands.clear();
        let mut start = 0;
        for &seed in self.state.bands_seeds.iter() {
            let mut hasher = FnvHasher::default();
            seed.hash(&mut hasher);

            let mut end = start + self.state.band_length;
            if end > self.state.config.signature_length {
                end = self.state.config.signature_length;
            }

            for minhash in signature.minhash[start .. end].iter() {
                minhash.hash(&mut hasher);
            }

            signature.bands.push(hasher.finish());
            start += self.state.band_length;
        }

        Ok(signature)
    }

    pub fn insert(&mut self, signature: &Signature, document: D) -> Result<(), Error<SE, BE>> {
        self.backend.insert(signature, document).map_err(|e| Error::Backend(e))
    }

    pub fn lookup_best(&mut self, signature: &Signature) -> Result<Option<LookupResult<D>>, Error<SE, BE>> {
        struct BestDocFinder<D> {
            last_sim: Option<f64>,
            best: Option<LookupResult<D>>,
        }

        impl<D> CandidatesCollector for BestDocFinder<D> {
            type Error = ();
            type Document = D;

            fn accept_minhash(&mut self, sample_minhash: &[u64], minhash: &[u64]) -> bool {
                let next_best = match (minhash_distance(sample_minhash, minhash), &self.best) {
                    (sim, &None) => Some(sim),
                    (sim, &Some(LookupResult { similarity: best_sim, .. })) if sim > best_sim => Some(sim),
                    _ => None,
                };
                self.last_sim = next_best;
                self.last_sim.is_some()
            }

            fn receive(&mut self, _sample_minhash: &[u64], _minhash: &[u64], doc: Arc<D>) -> Result<(), ()> {
                if let Some(sim) = self.last_sim {
                    self.best = Some(LookupResult {
                        similarity: sim,
                        document: doc,
                    });
                }
                Ok(())
            }
        }

        let mut collector = BestDocFinder {
            last_sim: None,
            best: None,
        };

        match self.backend.lookup(signature, &mut collector) {
            Ok(()) => Ok(collector.best),
            Err(LookupError::Backend(e)) => Err(Error::Backend(e)),
            Err(LookupError::Collector(())) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use super::{HashDupl, Config, Shingles, Signature, maximum_band_length};
    use super::shingler::tokens::Tokens;
    use super::backend::in_memory::InMemory;

    #[test]
    fn lsh_prob() {
        assert_eq!(maximum_band_length(128, 0.4, 0.94), 3);
        assert_eq!(maximum_band_length(128, 0.5, 0.85), 4);
        assert_eq!(maximum_band_length(256, 0.7, 0.99), 6);
    }

    #[test]
    fn shinglify_sign_basic() {
        let mut hd = HashDupl::<_, InMemory<String>>::new(Tokens::new(), InMemory::new(), Config::default()).unwrap();
        let mut shingles = Shingles::new();
        let mut signature = Signature::new();
        hd.shinglify("some text to sign and check", &mut shingles).unwrap();
        hd.sign(&shingles, &mut signature).unwrap();
        assert_eq!(shingles.0.len(), 15);
        assert_eq!(signature.minhash.len(), 128);
    }

    #[test]
    fn insert_lookup_best_basic() {
        let mut hd = HashDupl::new(Tokens::new(), InMemory::new(), Config::default()).unwrap();
        let mut shingles = Shingles::new();
        let mut signature = Signature::new();
        hd.shinglify("some text to sign and check", &mut shingles).unwrap();
        hd.sign(&shingles, &mut signature).unwrap();
        hd.insert(&signature, 177).unwrap();

        hd.shinglify("then some other text to sign and maybe check", &mut shingles).unwrap();
        hd.sign(&shingles, &mut signature).unwrap();
        hd.insert(&signature, 277).unwrap();

        hd.shinglify("text to sign and check", &mut shingles).unwrap();
        hd.sign(&shingles, &mut signature).unwrap();
        let found_a = hd.lookup_best(&signature).unwrap().unwrap();
        assert_eq!(found_a.document, Arc::new(177));

        hd.shinglify("some other text to sign and", &mut shingles).unwrap();
        hd.sign(&shingles, &mut signature).unwrap();
        let found_b = hd.lookup_best(&signature).unwrap().unwrap();

        assert_eq!(found_b.document, Arc::new(277));
    }
}
