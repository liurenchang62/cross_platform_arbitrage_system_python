# text_vectorizer.py
#! TF-IDF 文本向量化模块

import numpy as np
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass, field
import re
from collections import Counter, defaultdict
import math

from query_params import MAX_VOCAB_SIZE


# 停用词集合（常见无意义词语）
def get_stop_words() -> Set[str]:
    """获取停用词集合"""
    stop_words = {
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
    return stop_words


@dataclass
class VectorizerConfig:
    """文本向量化器配置"""
    use_stemming: bool = True
    filter_stop_words: bool = True
    min_word_length: int = 2
    max_df_ratio: float = 0.8
    min_df: int = 1
    normalize: bool = True
    custom_stop_words: Set[str] = field(default_factory=set)
    max_features: Optional[int] = MAX_VOCAB_SIZE


class TextVectorizer:
    """文本向量化器"""

    def __init__(self, config: VectorizerConfig):
        self.config = config
        self.stop_words = get_stop_words()
        # 添加自定义停用词
        for word in config.custom_stop_words:
            self.stop_words.add(word)

        self.vocabulary: Dict[str, int] = {}
        self.idf: List[float] = []
        self.n_docs = 0
        self.fitted = False

    def tokenize(self, text: str) -> List[str]:
        """分词"""
        text = text.lower()

        # 使用正则分割
        words = re.findall(r'[a-zA-Z0-9-]+', text)

        result = []
        for word in words:
            if '-' in word:
                # 处理带连字符的词
                for part in word.split('-'):
                    processed = self._process_token(part)
                    if processed:
                        result.append(processed)
            else:
                processed = self._process_token(word)
                if processed:
                    result.append(processed)

        return result

    def _process_token(self, token: str) -> Optional[str]:
        """处理单个token"""
        # 纯数字处理
        if token.isdigit():
            if len(token) == 4 and token.startswith(('1', '2')):
                return f"YEAR_{token}"
            return None

        # 长度检查
        if len(token) < self.config.min_word_length:
            return None

        # 停用词检查
        if self.config.filter_stop_words and token in self.stop_words:
            return None

        # 简易词干提取（仅去除常见后缀）
        if self.config.use_stemming:
            if token.endswith('ing'):
                token = token[:-3]
            elif token.endswith('ed'):
                token = token[:-2]
            elif token.endswith('s') and len(token) > 3:
                token = token[:-1]

        return token

    def fit(self, documents: List[str]) -> 'TextVectorizer':
        """拟合向量化器"""
        if not documents:
            return self

        self.n_docs = len(documents)

        # 对所有文档进行分词
        all_tokens = [self.tokenize(doc) for doc in documents]

        # 计算文档频率
        doc_freq = defaultdict(int)
        for tokens in all_tokens:
            unique_tokens = set(tokens)
            for token in unique_tokens:
                doc_freq[token] += 1

        # 过滤高频词和低频词
        max_df = int(self.config.max_df_ratio * self.n_docs)
        vocab_with_freq = [
            (token, df) for token, df in doc_freq.items()
            if df >= self.config.min_df and df <= max_df
        ]

        # 按文档频率降序排序
        vocab_with_freq.sort(key=lambda x: x[1], reverse=True)

        # 如果有上限，截取前 max_features 个
        if self.config.max_features and len(vocab_with_freq) > self.config.max_features:
            vocab_with_freq = vocab_with_freq[:self.config.max_features]

        # 构建词汇表
        self.vocabulary = {token: i for i, (token, _) in enumerate(vocab_with_freq)}

        # 计算IDF
        vocab_size = len(self.vocabulary)
        self.idf = [0.0] * vocab_size

        # 重新计算过滤后的文档频率
        filtered_doc_freq = [0] * vocab_size
        for tokens in all_tokens:
            unique_tokens = set(tokens)
            for token in unique_tokens:
                if token in self.vocabulary:
                    idx = self.vocabulary[token]
                    filtered_doc_freq[idx] += 1

        # 计算IDF值
        for idx, df in enumerate(filtered_doc_freq):
            if df > 0:
                self.idf[idx] = math.log((1.0 + self.n_docs) / (1.0 + df)) + 1.0
            else:
                self.idf[idx] = 1.0

        self.fitted = True
        return self

    def transform(self, text: str) -> Optional[np.ndarray]:
        """将文本转换为向量"""
        if not self.fitted or not self.vocabulary:
            return None

        tokens = self.tokenize(text)
        vector = np.zeros(len(self.vocabulary))

        # 计算词频
        for token in tokens:
            if token in self.vocabulary:
                idx = self.vocabulary[token]
                vector[idx] += 1.0

        if np.all(vector == 0):
            return None

        # 应用IDF
        for idx, val in enumerate(vector):
            if val > 0:
                vector[idx] = val * self.idf[idx]

        # L2归一化
        if self.config.normalize:
            norm = np.linalg.norm(vector)
            if norm > 1e-12:
                vector = vector / norm

        return vector

    def vocab_size(self) -> int:
        """获取词汇表大小"""
        return len(self.vocabulary)

    def is_fitted(self) -> bool:
        """检查是否已拟合"""
        return self.fitted


def cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> float:
    """计算余弦相似度"""
    dot = np.dot(v1, v2)
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)