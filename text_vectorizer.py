# text_vectorizer.py
# TF-IDF 文本向量化：分词、Snowball English 词干、ceil(max_df)、IDF

from __future__ import annotations

import copy
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import numpy as np
import snowballstemmer

from system_params import MAX_VOCAB_SIZE


def get_stop_words() -> Set[str]:
    """英文停用词与 domain 噪声词集合。"""
    return {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with", "would", "am", "been", "being",
        "did", "do", "does", "doing", "had", "has", "have", "having", "he", "her", "here",
        "hers", "herself", "him", "himself", "his", "how", "i", "me", "my", "myself",
        "our", "ours", "ourselves", "she", "should", "than", "that", "theirs", "them",
        "themselves", "there", "these", "they", "this", "those", "through", "too", "under",
        "until", "up", "very", "was", "we", "were", "what", "when", "where", "which", "while",
        "who", "whom", "why", "you", "your", "yours", "yourself", "yourselves",
        "will", "be", "the", "market", "price", "prediction", "event", "outcome",
        "contract", "share", "stock", "binary", "option", "trade", "trading",
        "buy", "sell", "yes", "no", "up", "down", "over", "under",
    }


def _split_words_boundary(text: str) -> List[str]:
    """按非标点字符切分，保留连字符；字母统一小写。"""
    text = text.lower()
    words: List[str] = []
    current: List[str] = []
    for ch in text:
        if ch.isalnum() or ch == "-":
            current.append(ch)
        else:
            if current:
                words.append("".join(current))
                current = []
    if current:
        words.append("".join(current))
    return [w for w in words if w]


@dataclass
class VectorizerConfig:
    use_stemming: bool = True
    filter_stop_words: bool = True
    min_word_length: int = 2
    max_df_ratio: float = 0.8
    min_df: int = 1
    normalize: bool = True
    custom_stop_words: Set[str] = field(default_factory=set)
    max_features: Optional[int] = MAX_VOCAB_SIZE


class TextVectorizer:
    """TF-IDF 风格拟合与查询向量构建。"""

    def __init__(self, config: VectorizerConfig):
        self.config = config
        self.stop_words = get_stop_words()
        for word in config.custom_stop_words:
            self.stop_words.add(word)

        self._stemmer = snowballstemmer.stemmer("english") if config.use_stemming else None

        self.vocabulary: Dict[str, int] = {}
        self.idf: List[float] = []
        self.n_docs = 0
        self.fitted = False

    def __deepcopy__(self, memo: dict) -> "TextVectorizer":
        """并行建索引时 `deepcopy(vectorizer)`：重建 Snowball stemmer（不可 pickle 时安全）。"""
        dup = object.__new__(TextVectorizer)
        memo[id(self)] = dup
        dup.config = copy.deepcopy(self.config, memo)
        dup.stop_words = get_stop_words()
        for word in dup.config.custom_stop_words:
            dup.stop_words.add(word)
        dup._stemmer = snowballstemmer.stemmer("english") if dup.config.use_stemming else None
        dup.vocabulary = copy.deepcopy(self.vocabulary, memo)
        dup.idf = list(self.idf)
        dup.n_docs = self.n_docs
        dup.fitted = self.fitted
        return dup

    def tokenize(self, text: str) -> List[str]:
        words = _split_words_boundary(text)
        result: List[str] = []
        for word in words:
            if "-" in word:
                for part in word.split("-"):
                    if not part:
                        continue
                    processed = self._process_token(part)
                    if processed:
                        result.append(processed)
            else:
                processed = self._process_token(word)
                if processed:
                    result.append(processed)
        return result

    def _process_token(self, token: str) -> Optional[str]:
        # 纯 ASCII 数字串
        if token and all("0" <= c <= "9" for c in token):
            if len(token) == 4 and token[0] in ("1", "2"):
                return f"YEAR_{token}"
            return None

        if len(token) < self.config.min_word_length:
            return None

        if self.config.filter_stop_words and token in self.stop_words:
            return None

        if self._stemmer is not None:
            return self._stemmer.stemWord(token)
        return token

    def fit(self, documents: List[str]) -> "TextVectorizer":
        if not documents:
            return self

        self.n_docs = len(documents)
        all_tokens = [self.tokenize(doc) for doc in documents]

        doc_freq: Dict[str, int] = defaultdict(int)
        for tokens in all_tokens:
            for token in set(tokens):
                doc_freq[token] += 1

        # 文档频率上限：ceil(max_df_ratio * n_docs)
        max_df = int(math.ceil(self.config.max_df_ratio * float(self.n_docs)))

        vocab_with_freq = [
            (token, df)
            for token, df in doc_freq.items()
            if df >= self.config.min_df and df <= max_df
        ]
        vocab_with_freq.sort(key=lambda x: x[1], reverse=True)

        if self.config.max_features is not None and len(vocab_with_freq) > self.config.max_features:
            vocab_with_freq = vocab_with_freq[: self.config.max_features]

        self.vocabulary = {word: i for i, (word, _) in enumerate(vocab_with_freq)}

        vocab_size = len(self.vocabulary)
        self.idf = [0.0] * vocab_size

        filtered_doc_freq = [0] * vocab_size
        for tokens in all_tokens:
            for token in set(tokens):
                idx = self.vocabulary.get(token)
                if idx is not None:
                    filtered_doc_freq[idx] += 1

        for idx, df in enumerate(filtered_doc_freq):
            if df > 0:
                self.idf[idx] = math.log((1.0 + self.n_docs) / (1.0 + df)) + 1.0
            else:
                self.idf[idx] = 1.0

        self.fitted = True
        return self

    def transform(self, text: str) -> Optional[np.ndarray]:
        if not self.fitted or not self.vocabulary:
            return None

        tokens = self.tokenize(text)
        vector = np.zeros(len(self.vocabulary), dtype=np.float64)

        for token in tokens:
            idx = self.vocabulary.get(token)
            if idx is not None:
                vector[idx] += 1.0

        if np.all(vector == 0):
            return None

        for idx, val in enumerate(vector):
            if val > 0:
                vector[idx] = val * self.idf[idx]

        if self.config.normalize:
            norm = float(np.linalg.norm(vector))
            if norm > 1e-12:
                vector = vector / norm

        return vector

    def vocab_size(self) -> int:
        return len(self.vocabulary)

    def is_fitted(self) -> bool:
        return self.fitted


def cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> float:
    dot = np.dot(v1, v2)
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(dot / (norm1 * norm2))
