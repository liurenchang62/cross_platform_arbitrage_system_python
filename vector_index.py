# vector_index.py
#! 向量索引模块：在 L2 归一化 TF-IDF 向量上用 **精确余弦相似度**（等价于点积）检索。
#!
#! 与历史上基于 KD-Tree + 欧氏球半径的实现相比：在同一向量与同一阈值下，候选集合由
#! `dot(q, v) >= threshold` 直接定义，无近似近邻；构建阶段堆叠为矩阵，与 Rust `vector_index.rs` 对齐。

from __future__ import annotations

import numpy as np
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# K-D Tree 时代遗留的默认维度提示（实际维度以首条向量为准）
DEFAULT_DIMENSION_HINT = 100


@dataclass
class IndexItem:
    """向量索引项（id / 可选元数据；向量与矩阵行一致）"""

    id: str
    vector: np.ndarray
    data: Optional[Any] = None


class VectorIndex:
    """向量索引：行堆叠矩阵 + 精确点积检索（与 Rust `VectorIndex` 一致）"""

    def __init__(self, category_name: str, dimension: int = DEFAULT_DIMENSION_HINT):
        self.category_name = category_name
        self.id_to_idx: Dict[str, int] = {}
        self.items: List[IndexItem] = []
        self.data_matrix: Optional[np.ndarray] = None
        self.dimension = dimension
        self.built = False

    def build(self, items: List[IndexItem]) -> None:
        """从向量列表构建索引（堆叠为矩阵，无 KD 构建开销）"""
        if not items:
            self.items.clear()
            self.id_to_idx.clear()
            self.data_matrix = None
            self.built = True
            return

        total = len(items)
        print(f"        构建索引: {total} 个项")
        start_time = datetime.now()

        self.dimension = int(len(items[0].vector))
        for i, item in enumerate(items):
            if len(item.vector) != self.dimension:
                raise ValueError(
                    f"类别 {self.category_name} 向量维度不一致: 首条 dim={self.dimension}, "
                    f"第 {i} 条 dim={len(item.vector)}"
                )

        self.items = items
        self.id_to_idx.clear()
        for idx, item in enumerate(self.items):
            self.id_to_idx[item.id] = idx

        print(f"          堆叠相似度矩阵 ({total} × {self.dimension})...")
        self.data_matrix = np.vstack([np.asarray(it.vector, dtype=np.float64) for it in self.items])
        self.built = True

        elapsed = datetime.now() - start_time
        print(f"        索引构建完成，耗时: {elapsed.total_seconds():.3f}s")

    def insert(self, item: IndexItem) -> None:
        """插入单个向量（仅追加列表；下次查询前需对整个类别重新 `build`）"""
        idx = len(self.items)
        self.id_to_idx[item.id] = idx
        self.items.append(item)
        self.built = False
        self.data_matrix = None

    def find_similar_with_threshold(
        self,
        query_vector: np.ndarray,
        threshold: float,
        max_results: int,
    ) -> List[Tuple[IndexItem, float]]:
        """精确打分：全体候选 `scores = X @ q`，过滤阈值后按相似度降序，最多 `max_results` 条。"""
        if not self.built or not self.items:
            return []

        if len(query_vector) != self.dimension:
            return []

        mat = self.data_matrix
        if mat is None:
            return []

        q = np.asarray(query_vector, dtype=np.float64).reshape(-1)
        scores = mat @ q

        hits: List[Tuple[int, float]] = [
            (i, float(s)) for i, s in enumerate(scores) if s >= threshold
        ]
        hits.sort(key=lambda x: x[1], reverse=True)
        if max_results > 0 and len(hits) > max_results:
            hits = hits[:max_results]

        return [(self.items[i], s) for i, s in hits]

    def len(self) -> int:
        return len(self.items)

    def is_empty(self) -> bool:
        return len(self.items) == 0

    def is_built(self) -> bool:
        return self.built

    def get_dimension(self) -> int:
        return self.dimension

    def clear(self) -> None:
        self.id_to_idx.clear()
        self.items.clear()
        self.data_matrix = None
        self.built = False


def _unit_vec2(x: float, y: float) -> np.ndarray:
    a = np.array([x, y], dtype=np.float64)
    n = float(np.linalg.norm(a))
    if n <= 0.0:
        return a
    return a / n


def _test_exact_top_matches_brute_force() -> None:
    """与 Rust `vector_index::tests::exact_top_matches_brute_force` 等价"""
    idx = VectorIndex("t", dimension=2)
    items = [
        IndexItem(id="a", vector=_unit_vec2(1.0, 0.0)),
        IndexItem(id="b", vector=_unit_vec2(1.0, 1.0)),
        IndexItem(id="c", vector=_unit_vec2(0.0, 1.0)),
    ]
    ids_vecs = [(it.id, it.vector.copy()) for it in items]

    idx.build(items)

    q = _unit_vec2(1.0, 0.1)
    threshold = 0.5
    max_results = 10

    got = idx.find_similar_with_threshold(q, threshold, max_results)

    brute: List[Tuple[str, float]] = []
    for bid, v in ids_vecs:
        s = float(np.dot(q, v))
        if s >= threshold:
            brute.append((bid, s))
    brute.sort(key=lambda x: x[1], reverse=True)

    assert len(got) == len(brute), (got, brute)
    for (g, gs), (bid, bs) in zip(got, brute):
        assert g.id == bid
        assert abs(gs - bs) < 1e-9, (gs, bs)
    print("vector_index self-test: exact_top_matches_brute_force OK")


if __name__ == "__main__":
    _test_exact_top_matches_brute_force()
