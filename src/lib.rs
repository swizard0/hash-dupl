extern crate fnv;
extern crate rand;
extern crate time;
extern crate serde;
extern crate rmp_serde;
extern crate tokenizer;
extern crate token_ngrams;
extern crate slices_merger;
extern crate bin_merge_pile;
#[macro_use] extern crate serde_derive;

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

#[derive(PartialEq, Debug)]
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

pub trait CandidatesFilter: Send + 'static {
    fn accept_minhash_similarity(&mut self, sample_minhash: &[u64], minhash: &[u64]) -> Option<f64>;
}

impl<T: ?Sized> CandidatesFilter for Box<T> where T: CandidatesFilter {
    fn accept_minhash_similarity(&mut self, sample_minhash: &[u64], minhash: &[u64]) -> Option<f64> {
        (**self).accept_minhash_similarity(sample_minhash, minhash)
    }
}

trait CandidatesFilterBoxClone {
    fn clone_box(&self) -> Box<CandidatesFilter>;
}

impl<T> CandidatesFilterBoxClone for T where T: CandidatesFilter + Clone + 'static {
    fn clone_box(&self) -> Box<CandidatesFilter> {
        Box::new(self.clone())
    }
}

impl Clone for Box<CandidatesFilter> {
    fn clone(&self) -> Box<CandidatesFilter> {
        self.clone_box()
    }
}

#[derive(Clone, Copy)]
pub struct SimilarityThresholdFilter(pub f64);

impl CandidatesFilter for SimilarityThresholdFilter {
    fn accept_minhash_similarity(&mut self, sample_minhash: &[u64], minhash: &[u64]) -> Option<f64> {
        match minhash_distance(sample_minhash, minhash) {
            similarity if similarity >= self.0 => Some(similarity),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub struct TrackBestFilter {
    threshold: f64,
    best: Option<f64>,
}

impl TrackBestFilter {
    pub fn new(threshold: f64) -> TrackBestFilter {
        TrackBestFilter {
            threshold: threshold,
            best: None,
        }
    }
}

impl CandidatesFilter for TrackBestFilter {
    fn accept_minhash_similarity(&mut self, sample_minhash: &[u64], minhash: &[u64]) -> Option<f64> {
        match (minhash_distance(sample_minhash, minhash), &mut self.best) {
            (sim, _) if sim < self.threshold =>
                None,
            (sim, &mut Some(ref mut best_sim)) if sim > *best_sim => {
                *best_sim = sim;
                Some(sim)
            },
            (_, &mut Some(..)) =>
                None,
            (sim, no_best) => {
                *no_best = Some(sim);
                Some(sim)
            },
        }
    }
}

#[derive(Debug)]
pub struct LookupResult<D> {
    pub similarity: f64,
    pub document: Arc<D>,
}

pub trait CandidatesCollector {
    type Error;
    type Document;
    type Result;

    fn receive(&mut self, similarity: f64, doc: Arc<Self::Document>) -> Result<(), Self::Error>;
    fn finish(self) -> Result<Self::Result, Self::Error>;
}

impl<D> CandidatesCollector for Vec<LookupResult<D>> {
    type Error = ();
    type Document = D;
    type Result = Vec<LookupResult<D>>;

    fn receive(&mut self, similarity: f64, doc: Arc<D>) -> Result<(), ()> {
        self.push(LookupResult { similarity: similarity, document: doc, });
        Ok(())
    }

    fn finish(self) -> Result<Vec<LookupResult<D>>, ()> {
        Ok(self)
    }
}

pub struct TrackBestCollector<D>(Option<LookupResult<D>>);

impl<D> TrackBestCollector<D> {
    pub fn new() -> TrackBestCollector<D> {
        TrackBestCollector(None)
    }
}

impl<D> CandidatesCollector for TrackBestCollector<D> {
    type Error = ();
    type Document = D;
    type Result = Option<LookupResult<D>>;

    fn receive(&mut self, similarity: f64, doc: Arc<D>) -> Result<(), ()> {
        match &mut self.0 {
            &mut Some(ref mut best) if similarity > best.similarity =>
                *best = LookupResult { similarity: similarity, document: doc, },
            &mut Some(..) =>
                (),
            no_best =>
                *no_best = Some(LookupResult { similarity: similarity, document: doc, }),
        };
        Ok(())
    }

    fn finish(self) -> Result<Option<LookupResult<D>>, ()> {
        Ok(self.0)
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

    fn save_state(&mut self, state: Arc<State>) -> Result<(), Self::Error>;
    fn load_state(&mut self) -> Result<Option<Arc<State>>, Self::Error>;
    fn insert(&mut self, signature: Arc<Signature>, doc: Arc<Self::Document>) -> Result<(), Self::Error>;
    fn lookup<F, C, CR, CE>(&mut self, signature: Arc<Signature>, filter: F, collector: C) -> Result<CR, LookupError<Self::Error, CE>>
        where F: CandidatesFilter + Clone, C: CandidatesCollector<Error = CE, Document = Self::Document, Result = CR>;
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

impl Deref for Shingles {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait Shingler {
    type Error;

    fn shinglify<'a>(&mut self, text: &str, shingle_length: usize, hashed_shingles: &'a mut Shingles) -> Result<&'a Shingles, Self::Error>;
}

pub struct HashDupl<S, B> {
    shingler: S,
    backend: B,
    state: Arc<State>,
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
    pub fn new(shingler: S, mut backend: B, user_config: Config) -> Result<HashDupl<S, B>, Error<SE, BE>> {
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
                let state = match try!(backend.load_state().map_err(Error::Backend)) {
                    Some(state) => state,
                    None => {
                        let new_state = Arc::new(State::new(config));
                        try!(backend.save_state(new_state.clone()).map_err(Error::Backend));
                        new_state
                    }
                };

                Ok(HashDupl {
                    shingler: shingler,
                    backend: backend,
                    state: state,
                })
            },
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    pub fn shinglify<'a, T>(&mut self, text: T, shingles: &'a mut Shingles) -> Result<&'a Shingles, Error<SE, BE>> where T: Deref<Target = str> {
        self.shingler.shinglify(&text, self.state.config.shingle_length, shingles).map_err(Error::Shingler)
    }

    pub fn sign<'a>(&mut self, shingles: &Shingles) -> Result<Arc<Signature>, Error<SE, BE>> {
        let mut signature = Signature {
            minhash: Vec::with_capacity(self.state.minhash_seeds.len()),
            bands: Vec::with_capacity(self.state.bands_seeds.len()),
        };

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

        Ok(Arc::new(signature))
    }

    pub fn insert(&mut self, signature: Arc<Signature>, document: Arc<D>) -> Result<(), Error<SE, BE>> {
        self.backend.insert(signature, document).map_err(Error::Backend)
    }

    pub fn lookup_best(&mut self, signature: Arc<Signature>) -> Result<Option<LookupResult<D>>, Error<SE, BE>> {
        let filter = TrackBestFilter::new(self.state.config.similarity_threshold);
        let collector = TrackBestCollector::new();
        self.backend.lookup(signature, filter, collector)
            .map_err(|err| match err {
                LookupError::Backend(e) => Error::Backend(e),
                LookupError::Collector(()) => unreachable!(),
            })
    }

    pub fn lookup_all(&mut self, signature: Arc<Signature>) -> Result<Vec<LookupResult<D>>, Error<SE, BE>> {
        let filter = SimilarityThresholdFilter(self.state.config.similarity_threshold);
        let collector = Vec::new();
        self.backend.lookup(signature, filter, collector)
            .map_err(|err| match err {
                LookupError::Backend(e) => Error::Backend(e),
                LookupError::Collector(()) => unreachable!(),
            })
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use rand::{thread_rng, Rng};
    use bin_merge_pile::merge::ParallelConfig;
    use super::{HashDupl, Config, Shingles, LookupResult, Backend, maximum_band_length};
    use super::shingler::tokens::Tokens;
    use super::backend::in_memory::InMemory;
    use super::backend::worker::Worker;
    use super::backend::pile_rw::PileRw;
    use super::backend::pile_compile;
    use super::backend::stream::{Stream, Params};

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
        hd.shinglify("some text to sign and check", &mut shingles).unwrap();
        let signature = hd.sign(&shingles).unwrap();
        assert_eq!(shingles.0.len(), 15);
        assert_eq!(signature.minhash.len(), 128);
    }

    fn backend_insert_lookup_basic<B, E>(backend: B) where B: Backend<Document = i32, Error = E>, E: ::std::fmt::Debug {
        let mut hd = HashDupl::new(Tokens::new(), backend, Config::default()).unwrap();
        let mut shingles = Shingles::new();
        let doc_a = Arc::new(177);
        let doc_b = Arc::new(277);

        hd.shinglify("here is some useful text to sign and check", &mut shingles).unwrap();
        let signature = hd.sign(&shingles).unwrap();
        hd.insert(signature, doc_a.clone()).unwrap();

        hd.shinglify("then here is some other useful text to sign and maybe check", &mut shingles).unwrap();
        let signature = hd.sign(&shingles).unwrap();
        hd.insert(signature, doc_b.clone()).unwrap();

        hd.shinglify("is useful text to sign and check", &mut shingles).unwrap();
        let signature = hd.sign(&shingles).unwrap();
        let found_a = hd.lookup_best(signature).unwrap().unwrap();
        assert_eq!(found_a.document, doc_a.clone());

        hd.shinglify("is some other useful text to sign and", &mut shingles).unwrap();
        let signature = hd.sign(&shingles).unwrap();
        let found_b = hd.lookup_best(signature).unwrap().unwrap();
        assert_eq!(found_b.document, doc_b.clone());

        hd.shinglify("here is some useful text to sign and check", &mut shingles).unwrap();
        let signature = hd.sign(&shingles).unwrap();
        let mut found_all = hd.lookup_all(signature).unwrap();
        found_all.sort_by(|a, b| a.document.cmp(&b.document));
        match found_all.get(0) {
            Some(&LookupResult { similarity: sim, document: ref doc, }) if sim > 0.99 && doc == &doc_a => (),
            other => panic!("unexpected result 0: {:?}", other),
        }
        match found_all.get(1) {
            Some(&LookupResult { similarity: sim, document: ref doc, }) if sim >= 0.4 && doc == &doc_b => (),
            other => panic!("unexpected result 1: {:?}", other),
        }
    }

    #[test]
    fn insert_lookup_basic_in_memory() {
        backend_insert_lookup_basic(InMemory::new());
    }

    #[test]
    fn insert_lookup_basic_in_memory_worker() {
        backend_insert_lookup_basic(Worker::<_, ()>::run(InMemory::new()));
    }

    #[test]
    fn insert_lookup_basic_pile_rw() {
        backend_insert_lookup_basic(PileRw::new(
            "/tmp/hd_pile_rw_a",
            Default::default(),
            pile_compile::Params {
                min_tree_height: 1,
                max_block_size: 32,
                memory_limit_power: 13,
                parallel_config: ParallelConfig::SingleThread,
            }).unwrap());
    }

    fn gen_text() -> String {
        let mut rng = thread_rng();
        let total = 50 + (rng.gen::<usize>() % 50);
        (0 .. total).map(|_| format!("{} ", rng.gen::<u8>())).collect()
    }

    #[test]
    fn stream_stress() {
        let texts: Vec<_> = (0 .. 1000).map(|_| Arc::new(gen_text())).collect();
        {
            let _ = fs::remove_dir_all("/tmp/hd_stream_a");
            let backend = Stream::new("/tmp/hd_stream_a", Params {
                lookup_params: Default::default(),
                compile_params: pile_compile::Params {
                    min_tree_height: 1,
                    max_block_size: 32,
                    memory_limit_power: 13,
                    parallel_config: ParallelConfig::SingleThread,
                },
                windows_count: 8,
            }).unwrap();
            let mut hd = HashDupl::new(Tokens::new(), backend, Config::default()).unwrap();
            let mut shingles = Shingles::new();

            // check & fill
            for (i, text) in texts.iter().enumerate() {
                hd.shinglify(&***text, &mut shingles).unwrap();
                let signature = hd.sign(&shingles).unwrap();

                if let Some(LookupResult { similarity: sim, document: doc }) = hd.lookup_best(signature.clone()).unwrap() {
                    if sim >= 0.99 {
                        panic!("unexpected found result pass #1 with sim = {} at this point: {:?}", sim, doc);
                    }
                }

                hd.insert(signature.clone(), text.clone()).unwrap();
                if (i + 1) % 100 == 0 {
                    hd.backend_mut().rotate().unwrap();
                }
            }

            // check
            for (i, text) in texts.iter().enumerate() {
                hd.shinglify(&***text, &mut shingles).unwrap();
                let signature = hd.sign(&shingles).unwrap();

                match (i, hd.lookup_best(signature.clone()).unwrap()) {
                    (ref j, Some(LookupResult { similarity: ref sim, document: ref doc })) if j >= &200 && sim >= &0.99 && doc == text =>
                        (),
                    (ref j, Some(LookupResult { similarity: ref sim, .. })) if j < &200 && sim < &0.99 =>
                        (),
                    (ref j, None) if j < &200 =>
                        (),
                    other =>
                        panic!("unexpected lookup_best result match: {:?}", other),
                }
            }
        }
        {
            let backend = Stream::new("/tmp/hd_stream_a", Params {
                lookup_params: Default::default(),
                compile_params: pile_compile::Params {
                    min_tree_height: 1,
                    max_block_size: 32,
                    memory_limit_power: 13,
                    parallel_config: ParallelConfig::SingleThread,
                },
                windows_count: 8,
            }).unwrap();
            let mut hd = HashDupl::new(Tokens::new(), backend, Config::default()).unwrap();
            let mut shingles = Shingles::new();

            // check
            for (i, text) in texts.iter().enumerate() {
                hd.shinglify(&***text, &mut shingles).unwrap();
                let signature = hd.sign(&shingles).unwrap();

                match (i, hd.lookup_best(signature.clone()).unwrap()) {
                    (ref j, Some(LookupResult { similarity: ref sim, document: ref doc })) if j >= &200 && sim >= &0.99 && doc == text =>
                        (),
                    (ref j, Some(LookupResult { similarity: ref sim, .. })) if j < &200 && sim < &0.99 =>
                        (),
                    (ref j, None) if j < &200 =>
                        (),
                    other =>
                        panic!("unexpected lookup_best pass #2 result match: {:?}", other),
                }
            }
        }
    }
}
