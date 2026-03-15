# text_vectorizer.py
import re
import math
from collections import Counter, defaultdict
from typing import List, Dict, Set, Optional, Tuple
import numpy as np
from dataclasses import dataclass, field


# 停用词集合
def get_stop_words() -> Set[str]:
    """获取默认停用词列表"""
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
        # 预测市场常见无意义词
        "will", "be", "the", "market", "price", "prediction", "event", "outcome",
        "contract", "share", "stock", "binary", "option", "trade", "trading",
        "buy", "sell", "yes", "no", "up", "down", "over", "under"
    }


@dataclass
class VectorizerConfig:
    """向量化器配置"""
    use_stemming: bool = True
    filter_stop_words: bool = True
    min_word_length: int = 2
    max_df_ratio: float = 0.8
    min_df: int = 1
    normalize: bool = True
    custom_stop_words: Set[str] = field(default_factory=set)


class TextVectorizer:
    """文本向量化器"""

    def __init__(self, config: Optional[VectorizerConfig] = None):
        self.config = config or VectorizerConfig()
        self.stop_words = get_stop_words().union(self.config.custom_stop_words)
        self.vocabulary: Dict[str, int] = {}
        self.idf: List[float] = []
        self.n_docs: int = 0
        self.fitted: bool = False

    def tokenize(self, text: str) -> List[str]:
        """对文本进行分词和预处理"""
        text = text.lower()

        # 简单分词：按非字母数字字符分割，但保留连字符
        words = []
        # 使用正则分割，保留连字符连接的词
        for word in re.split(r'[^a-z0-9-]+', text):
            if word and word != '-':
                words.append(word)

        result = []
        for word in words:
            # 如果词包含连字符，拆分成多个词
            if '-' in word:
                for part in word.split('-'):
                    if part:
                        processed = self._process_token(part)
                        if processed:
                            result.append(processed)
            else:
                processed = self._process_token(word)
                if processed:
                    result.append(processed)

        return result

    def _process_token(self, token: str) -> Optional[str]:
        """处理单个词元（过滤、词干提取）"""
        # 检查是否全是数字
        if token.isdigit():
            # 保留年份（4位数字）和常见数字
            if len(token) == 4 and token[0] in ('1', '2'):
                return f"YEAR_{token}"
            return None

        # 检查长度
        if len(token) < self.config.min_word_length:
            return None

        # 检查停用词
        if self.config.filter_stop_words and token in self.stop_words:
            return None

        # 注意：Python中不做真实的词干提取，保持原样
        # 如果将来需要，可以引入 nltk.stem.PorterStemmer
        return token

    def fit(self, documents: List[str]) -> 'TextVectorizer':
        """拟合文档集，构建词汇表和 IDF"""
        if not documents:
            return self

        self.n_docs = len(documents)

        # 第一步：对所有文档分词
        all_tokens = [self.tokenize(doc) for doc in documents]

        # 第二步：统计词频和文档频率
        doc_freq = defaultdict(int)

        for tokens in all_tokens:
            # 文档内去重用于文档频率统计
            unique_tokens = set(tokens)
            for token in unique_tokens:
                doc_freq[token] += 1

        # 第三步：过滤词汇表
        max_df = int(self.config.max_df_ratio * self.n_docs)

        vocab = [
            word for word, df in doc_freq.items()
            if df >= self.config.min_df and df <= max_df
        ]

        # 按字母排序保持一致性
        vocab.sort()

        # 构建词汇表索引
        self.vocabulary = {word: i for i, word in enumerate(vocab)}

        # 第四步：计算 IDF
        vocab_size = len(self.vocabulary)
        self.idf = [0.0] * vocab_size

        # 重新统计文档频率（只保留词汇表中的词）
        filtered_doc_freq = [0] * vocab_size

        for tokens in all_tokens:
            unique_tokens = set(tokens)
            for token in unique_tokens:
                if token in self.vocabulary:
                    idx = self.vocabulary[token]
                    filtered_doc_freq[idx] += 1

        # 计算 IDF: idf = log((1 + n) / (1 + df)) + 1
        for idx, df in enumerate(filtered_doc_freq):
            if df > 0:
                self.idf[idx] = math.log((1.0 + self.n_docs) / (1.0 + df)) + 1.0
            else:
                self.idf[idx] = 1.0  # 默认值

        self.fitted = True
        return self

    def transform(self, text: str) -> Optional[np.ndarray]:
        """将单个文本转换为 TF-IDF 向量"""
        if not self.fitted or not self.vocabulary:
            return None

        tokens = self.tokenize(text)
        vector = np.zeros(len(self.vocabulary), dtype=np.float64)

        # 计算 TF
        for token in tokens:
            if token in self.vocabulary:
                idx = self.vocabulary[token]
                vector[idx] += 1.0

        # 如果文本中没有词汇表中的词，返回 None
        if np.all(vector == 0):
            return None

        # 计算 TF-IDF
        for idx, idf_val in enumerate(self.idf):
            if vector[idx] > 0.0:
                # TF 使用原始词频
                vector[idx] *= idf_val

        # L2 归一化
        if self.config.normalize:
            norm = np.linalg.norm(vector)
            if norm > 1e-12:
                vector = vector / norm

        return vector

    def transform_batch(self, texts: List[str]) -> List[Optional[np.ndarray]]:
        """批量转换文本为向量"""
        return [self.transform(text) for text in texts]

    def fit_transform(self, documents: List[str]) -> List[Optional[np.ndarray]]:
        """拟合并转换所有文档"""
        self.fit(documents)
        return self.transform_batch(documents)

    def vocab_size(self) -> int:
        """获取词汇表大小"""
        return len(self.vocabulary)

    def is_fitted(self) -> bool:
        """检查是否已拟合"""
        return self.fitted

    def get_vocabulary(self) -> Dict[str, int]:
        """获取词汇表（用于调试）"""
        return self.vocabulary.copy()


def cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> float:
    """计算两个向量的余弦相似度"""
    if v1.shape != v2.shape:
        return 0.0

    dot_product = np.dot(v1, v2)
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)

    if norm1 < 1e-12 or norm2 < 1e-12:
        return 0.0

    return dot_product / (norm1 * norm2)