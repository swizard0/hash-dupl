use std::hash::{Hash, Hasher};
use std::iter::Iterator;
use std::collections::HashSet;
use tokenizer;
use fnv::FnvHasher;
use super::super::Lexer;

pub struct Tokens {
    stop_words: Option<HashSet<u64>>,
}

impl Tokens {
    pub fn new() -> Tokens {
        Tokens {
            stop_words: None,
        }
    }

    pub fn set_stop_words<I, E>(&mut self, words: I) -> Result<(), E> where I: Iterator<Item = Result<String, E>> {
        let mut dict = HashSet::new();
        for maybe_word in words {
            let word = try!(maybe_word);
            let mut hasher = FnvHasher::default();
            word.hash(&mut hasher);
            dict.insert(hasher.finish());
        }
        self.stop_words = Some(dict);
        Ok(())
    }
}

pub struct TokensIter;

impl Iterator for TokensIter {
    type Item = Result<u64, ()>;

    fn next(&mut self) -> Option<Result<u64, ()>> {
        None
    }
}

impl Lexer for Tokens {
    type Error = ();
    type Iter = TokensIter;

    fn lexify(&mut self, text: &str) -> Result<TokensIter, ()> {
        Ok(TokensIter)
    }
}
