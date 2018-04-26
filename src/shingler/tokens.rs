use std::hash::{Hash, Hasher};
use std::iter::Iterator;
use std::collections::HashSet;
use tokenizer;
use tokenizer::{Token, Number};
use token_ngrams::{Ngrams, Attrs, AcceptEverything};
use fnv::FnvHasher;
use super::super::{Shingles, Shingler};

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
            let word = maybe_word?;
            let mut hasher = FnvHasher::default();
            word.hash(&mut hasher);
            dict.insert(hasher.finish());
        }
        self.stop_words = Some(dict);
        Ok(())
    }
}

struct TokenWithHash<'a> {
    token: Token,
    hash: u64,
    dict: Option<&'a HashSet<u64>>,
}

impl<'a> TokenWithHash<'a> {
    fn new(token: Token, dict: Option<&'a HashSet<u64>>) -> TokenWithHash<'a> {
        let mut hasher = FnvHasher::default();
        match token {
            Token::PlainWord(ref s) => s.hash(&mut hasher),
            Token::MixedWord(ref s) => s.hash(&mut hasher),
            Token::Number(Number::Integer(ref s)) => s.hash(&mut hasher),
            Token::Number(Number::Float(ref s)) => s.hash(&mut hasher),
            Token::Number(Number::BigInteger(ref s)) => s.hash(&mut hasher),
            Token::Punct(ref s) => s.hash(&mut hasher),
            Token::Whitespaces(..) | Token::Newline => (),
        }

        TokenWithHash {
            token: token,
            hash: hasher.finish(),
            dict: dict,
        }
    }
}

impl<'a> Attrs for TokenWithHash<'a> {
    fn require_flush(&self) -> bool {
        self.token.require_flush()
    }

    fn should_be_skipped(&self) -> bool {
        self.token.should_be_skipped() || self.dict.map(|d| d.contains(&self.hash)).unwrap_or(false)
    }
}

impl Shingler for Tokens {
    type Error = ();

    fn shinglify<'a>(&mut self, text: &str, shingle_length: usize, hashed_shingles: &'a mut Shingles) -> Result<&'a Shingles, ()> {
        let hashed_tokens = tokenizer::Tokens::<_, ()>::new(text.chars().flat_map(|c| c.to_lowercase().map(|lc| Ok(lc))))
            .map(|token| token.map(|tok| TokenWithHash::new(tok, self.stop_words.as_ref())));
        let ngrams_iter = Ngrams::new(hashed_tokens, shingle_length, AcceptEverything);
        let hashed_ngrams_iter = ngrams_iter
            .map(|g| g.unwrap().ngram)
            .map(|ngram| {
                let mut hasher = FnvHasher::default();
                for tok in ngram.iter() {
                    tok.hash.hash(&mut hasher)
                }
                hasher.finish()
            });
        hashed_shingles.0.clear();
        hashed_shingles.0.extend(hashed_ngrams_iter);
        hashed_shingles.0.sort();
        Ok(hashed_shingles)
    }
}

#[cfg(test)]
mod test {
    use super::Tokens;
    use super::super::super::{Shingles, Shingler};

    #[test]
    fn shinglify_some() {
        let mut t = Tokens::new();
        let mut shingles = Shingles::new();
        t.shinglify("Create pointer to the data and increase the reference counter.", 3, &mut shingles).unwrap();
        assert_eq!(shingles.0,
                   vec![219189533156472843, 752905498211977740, 906776722371435292, 1198374789896484484, 1627016984320289849,
                        2709404953724036216, 4754942937748297979, 6529366725122468182, 6773681406702382296, 7445513554970959646,
                        7861918477386412141, 8098393129744522104, 9711975790191380953, 10140415399392094134, 11079460637125266068,
                        12267850099031037718, 12368885195975451608, 12596819376607615678, 13728245501073889720, 13807849560265838446,
                        13807849560265838446, 14517612144232257057, 15267727650223836275, 15681233656820225738, 16164504645936422063,
                        16361755923273750197, 17098348046202689018, 17670374965621067880, 17889277151056874761, 18160843838573307490]);
    }

    #[test]
    fn shinglify_stop_words() {
        let mut t = Tokens::new();
        t.set_stop_words::<_, ()>(["to", "the", "and", "."].into_iter().map(|&s| Ok(s.to_owned()))).unwrap();
        let mut shingles = Shingles::new();
        t.shinglify("Create pointer to the data and increase the reference counter.", 3, &mut shingles).unwrap();
        assert_eq!(shingles.0,
                   vec![219189533156472843, 2709404953724036216, 4754942937748297979, 7861918477386412141, 8230815495134460693,
                        8969221563668974120, 12188844219384569804, 12309543090974928189, 12596819376607615678, 14124759991247487330,
                        14202797205389785579, 15267727650223836275, 15416987452205239818, 15681233656820225738, 18160843838573307490]);
    }
}
