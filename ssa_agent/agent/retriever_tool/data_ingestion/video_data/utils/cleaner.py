"""
Text cleaning to remove stopwords, filler words, and spell correct the text.
"""

import re
import string
import nltk
from nltk.corpus import stopwords, words
from nltk.tokenize import word_tokenize
from nltk.metrics.distance import jaccard_distance, edit_distance
from nltk.util import ngrams
from typing import List, Optional, Dict, Set
import logging
from functools import lru_cache
import pandas as pd

logger = logging.getLogger(__name__)

class SpellChecker:
    """Spell checker using NLTK word corpus with custom vocabulary support."""
    
    def __init__(self, custom_vocabulary: Optional[List[str]] = []):
        nltk.download('words', quiet=True)
        from nltk.corpus import words
        
        vocabulary = set(words.words() + custom_vocabulary)
        self.vocabulary_lower = [w.lower() for w in vocabulary]
        
        self.indexed_vocabulary = {}
        for word in self.vocabulary_lower:
            first_letter = word[0].lower()
            if first_letter not in self.indexed_vocabulary:
                self.indexed_vocabulary[first_letter] = []
            self.indexed_vocabulary[first_letter].append(word)
        
        logger.info(f"Initialized optimized spell checker with {len(vocabulary)} words")
    
    @lru_cache(maxsize=10000)
    def spell_correct_cached(self, word: str) -> str:
        return self._spell_correct_internal(word)
    
    def _spell_correct_internal(self, word: str, word_length_threshold: int = 6) -> str:
        """Internal spell correction logic."""
        if not word or len(word) < 2:
            return word
            
        word_lower = word.lower()        
        if word_lower in self.vocabulary_lower:
            return word
        if re.search(r'\d', word): # contains numbers, skip correction
            return word
            
        candidates = self.indexed_vocabulary.get(word_lower[0], [])
        if not candidates:
            return word
        
        if len(word) <= word_length_threshold:
            return self._jaccard_correct(word_lower, candidates)
        else:
            return self._edit_distance_correct(word_lower, candidates)
    
    def _jaccard_correct(self, word: str, candidates: List[str]) -> str:
        """Jaccard distance"""
        word_bigrams = set(ngrams(word, 2))
        
        best_distance = float('inf')
        best_word = word
        
        for candidate in candidates:
            if abs(len(candidate) - len(word)) > 3:
                continue
                
            candidate_bigrams = set(ngrams(candidate.lower(), 2))
            distance = jaccard_distance(word_bigrams, candidate_bigrams)
            
            if distance < best_distance:
                best_distance = distance
                best_word = candidate
                
            if distance == 0:
                break
        
        return best_word
    
    def _edit_distance_correct(self, word: str, candidates: List[str]) -> str:
        """Edit distance correction"""
        best_distance = float('inf')
        best_word = word
        
        for candidate in candidates:
            if abs(len(candidate) - len(word)) > 4: 
                continue
                
            distance = edit_distance(word, candidate.lower())
            
            if distance < best_distance:
                best_distance = distance
                best_word = candidate
                
            if distance <= 1: 
                break
        
        return best_word
    
    def spell_correct_text(self, text: str) -> str:
        if not text or not isinstance(text, str):
            return text
        
        words_list = text.strip().split()
        corrected_words = []
        
        for word in words_list:
            if not word or word == " ":
                continue
                
            clean_word = word.strip(string.punctuation)
            punct_suffix = word[len(clean_word):]
            
            if clean_word.lower() in self.vocabulary_lower or re.search(r'\d', clean_word):
                corrected_words.append(word)
                continue
            
            try:
                corrected = self.spell_correct_cached(clean_word)
                if word.isupper():
                    corrected = corrected.upper()
                elif word.istitle():
                    corrected = corrected.capitalize()
                
                corrected_words.append(corrected + punct_suffix)
            except Exception as e:
                logger.warning(f"Error correcting word '{word}': {e}")
                corrected_words.append(word)  # keep og
        
        return " ".join(corrected_words)

class TranscriptCleaner:    
    def __init__(self, remove_stopwords: bool = False, fix_spelling: bool = True):
        self.remove_stopwords = remove_stopwords
        self.fix_spelling = fix_spelling
        
        self.filler_patterns = self._compile_filler_patterns()
        
        if self.remove_stopwords:
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt', quiet=True)
            self.stop_words = set(stopwords.words('english'))
        
        if self.fix_spelling:
            # TODO: adding custom vocab multi words
            custom_vocab = [
                "databricks", "vscode", "DABS", "DLTs", "mlflow", "lakeflow",
                "jupyter", "notebook", "python", "sql", "spark", "delta",
                "kubernetes", "docker", "github", "gitlab", "cicd", "devops"
            ]
            self.spell_checker = SpellChecker(custom_vocabulary=custom_vocab)
    
    def _compile_filler_patterns(self) -> List[re.Pattern]:
        """Pre-compile regex patterns for filler word removal."""
        filler_words = [
            'um', 'uh', 'uhm', 'hmm', 'hm', 'ah', 'oh', 'eh', 'er', 'erm',
            'like', 'you know', 'i mean', 'sort of', 'kind of', 'basically',
            'actually', 'literally', 'totally', 'definitely', 'absolutely',
            'obviously', 'clearly', 'honestly', 'frankly', 'really',
            'so yeah', 'yeah so', 'and stuff', 'or whatever', 'and things',
            'you see', 'you understand', 'if you will', 'as it were'
        ]
        
        patterns = []
        for filler in sorted(filler_words, key=len, reverse=True):
            pattern = re.compile(r'\b' + re.escape(filler) + r'\b', re.IGNORECASE)
            patterns.append(pattern)
        
        return patterns
    
    def clean(self, text: str) -> str:
        """Entry point for cleaning"""
        if not text or not isinstance(text, str):
            return ""
        
        text = self._remove_filler_words(text)
        
        if self.fix_spelling:
            text = self.spell_checker.spell_correct_text(text)
        
        if self.remove_stopwords:
            text = self._remove_stopwords(text)
        
        text = self._normalize_text(text)
        
        return text.strip()
    
    def _remove_filler_words(self, text: str) -> str:
        for pattern in self.filler_patterns:
            text = pattern.sub(' ', text)
        
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def _remove_stopwords(self, text: str) -> str:
        if not self.stop_words:
            return text

        words_list = text.lower().split()
        filtered_words = [word for word in words_list if word not in self.stop_words and word not in string.punctuation and word.strip()]
        return ' '.join(filtered_words)
    
    def _normalize_text(self, text: str) -> str:
        normalizations = [
            (re.compile(r'[.]{2,}'), '.'),
            (re.compile(r'[!]{2,}'), '!'),
            (re.compile(r'[?]{2,}'), '?'),
            (re.compile(r'["""]'), '"'),
            (re.compile(r"[''']"), "'"),
            (re.compile(r'\s+([.!?])'), r'\1'),
            (re.compile(r'([.!?])\s*([A-Z])'), r'\1 \2'),
            (re.compile(r'\s+'), ' ')
        ]
        
        for pattern, replacement in normalizations:
            text = pattern.sub(replacement, text)
        
        return text
