# category_vectorizer.py
#! 类别独立的向量化器管理（与 Rust `category_vectorizer.rs` 对齐：并行 fit_all / with_fitted_vectorizer / insert_built_category）

from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Tuple, Any

from text_vectorizer import TextVectorizer, VectorizerConfig
from vector_index import VectorIndex, IndexItem


class CategoryVectorizer:
    """类别向量化器"""

    def __init__(self, category: str):
        self.category = category
        self.vectorizer = TextVectorizer(VectorizerConfig())
        self.index = VectorIndex(category)
        self.fitted = False

    @classmethod
    def with_fitted_vectorizer(cls, category: str, vectorizer: TextVectorizer) -> "CategoryVectorizer":
        """从已训练好的 `TextVectorizer` 构造（用于并行建索引，与 `fit` 后状态一致）。"""
        cv = cls(category)
        cv.vectorizer = copy.deepcopy(vectorizer)
        cv.fitted = True
        return cv

    def fit(self, titles: List[str]) -> None:
        """拟合向量化器"""
        if not titles:
            return
        self.vectorizer.fit(titles)
        self.fitted = True
        # 只输出词汇表大小，不输出每个类别

    def add_markets_batch(self, items: List[Tuple[str, str, Optional[Any]]]) -> None:
        """批量添加市场到索引"""
        if not self.fitted:
            return

        index_items = []
        total = len(items)

        for i, (market_id, title, data) in enumerate(items):
            # 每5000个输出一次进度
            if i % 5000 == 0 and i > 0:
                print(f"          构建索引: {i}/{total}")

            vector = self.vectorizer.transform(title)
            if vector is not None:
                index_items.append(IndexItem(
                    id=market_id,
                    vector=vector,
                    data=data
                ))

        if index_items:
            if total > 1000:
                print(f"          构建精确余弦索引 ({len(index_items)} 条向量)...")
            self.index.build(index_items)

    def find_similar(
        self,
        title: str,
        threshold: float,
        max_results: int,
    ) -> List[Tuple[IndexItem, float]]:
        """查找相似的市场"""
        if not self.fitted:
            return []

        query_vector = self.vectorizer.transform(title)
        if query_vector is not None:
            return self.index.find_similar_with_threshold(query_vector, threshold, max_results)
        return []


def _fit_one_category(args: Tuple[str, List[str]]) -> Tuple[str, CategoryVectorizer]:
    """供 `fit_all` 线程池调用（每线程独立 CategoryVectorizer）。"""
    category, titles = args
    cv = CategoryVectorizer(category)
    cv.fit(titles)
    return category, cv


class CategoryVectorizerManager:
    """类别向量化器管理器"""

    def __init__(self):
        self.vectorizers: Dict[str, CategoryVectorizer] = {}
        self.unclassified_vectorizer = CategoryVectorizer("unclassified")

    def get_or_create(self, category: str) -> Optional[CategoryVectorizer]:
        """获取或创建类别向量化器"""
        if category == "unclassified":
            return self.unclassified_vectorizer

        if category not in self.vectorizers:
            self.vectorizers[category] = CategoryVectorizer(category)
        return self.vectorizers[category]

    def get(self, category: str) -> Optional[CategoryVectorizer]:
        """获取类别向量化器"""
        if category == "unclassified":
            return self.unclassified_vectorizer
        return self.vectorizers.get(category)

    def insert_built_category(self, category: str, cv: CategoryVectorizer) -> None:
        """将并行构建好的类别向量化器写回（仅替换对应键）。"""
        if category == "unclassified":
            self.unclassified_vectorizer = cv
        else:
            self.vectorizers[category] = cv

    def fit_all(self, markets_by_category: Dict[str, List[str]]) -> None:
        """并行拟合所有类别（与 Rust `rayon` 语义对齐）。"""
        total = len(markets_by_category)
        if total == 0:
            return
        print(f"      并行拟合 {total} 个类别 (rayon)...")
        pairs = sorted(markets_by_category.items(), key=lambda x: x[0])
        max_workers = min(32, total)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fitted = list(ex.map(_fit_one_category, pairs))
        for category, cv in fitted:
            self.insert_built_category(category, cv)

    def get_all_categories(self) -> List[str]:
        """获取所有类别名称"""
        cats = list(self.vectorizers.keys())
        cats.append("unclassified")
        cats.sort()
        return cats

    def category_size(self, category: str) -> int:
        """获取类别索引大小"""
        vec = self.get(category)
        return vec.index.len() if vec else 0

    def total_size(self) -> int:
        """获取所有索引的总大小"""
        total = self.unclassified_vectorizer.index.len()
        for vec in self.vectorizers.values():
            total += vec.index.len()
        return total

    def clear(self) -> None:
        """清理所有向量化器"""
        self.vectorizers.clear()
        self.unclassified_vectorizer = CategoryVectorizer("unclassified")