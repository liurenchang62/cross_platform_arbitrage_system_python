# vector_index.py
#! 向量索引模块，使用 K-D Tree 实现近似最近邻搜索

import numpy as np
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from sklearn.neighbors import KDTree
import datetime
import json
import time
# K-D Tree 维度（向量维度）
TREE_DIMENSION = 100


def _agent_debug_log(hypothesisId: str, location: str, message: str, data: Dict[str, Any]) -> None:
    #region agent log
    try:
        with open("debug-951685.log", "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "sessionId": "951685",
                        "runId": "pre-fix",
                        "hypothesisId": hypothesisId,
                        "location": location,
                        "message": message,
                        "data": data,
                        "timestamp": int(time.time() * 1000),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        pass
    #endregion agent log


@dataclass
class IndexItem:
    """向量索引项"""
    id: str
    vector: np.ndarray
    data: Optional[Any] = None


class VectorIndex:
    """向量索引，使用 K-D Tree 实现近似最近邻搜索"""

    def __init__(self, category_name: str, dimension: int = TREE_DIMENSION):
        self.category_name = category_name
        self.tree: Optional[KDTree] = None
        self.id_to_idx: Dict[str, int] = {}
        self.items: List[IndexItem] = []
        self.dimension = dimension
        self.built = False

    def build(self, items: List[IndexItem]) -> None:
        """从向量列表构建索引（优化版 - 真正批量构建）"""
        if not items:
            return

        total = len(items)
        print(f"        构建索引: {total} 个项")
        _agent_debug_log(
            "H1_datetime_import_or_shadow",
            "vector_index.py:build:before_now",
            "Inspect datetime binding before calling now()",
            {
                "datetime_repr": repr(datetime),
                "datetime_type": str(type(datetime)),
                "datetime_has_attr_now": bool(getattr(datetime, "now", None)),
                "datetime_has_attr_datetime": bool(getattr(datetime, "datetime", None)),
                "datetime_module_file": getattr(datetime, "__file__", None),
                "globals_has_datetime": "datetime" in globals(),
            },
        )
        start_time = datetime.datetime.now()
        _agent_debug_log(
            "H1_datetime_import_or_shadow",
            "vector_index.py:build:after_now",
            "Called datetime.now() successfully",
            {"start_time_type": str(type(start_time)), "start_time_repr": repr(start_time)},
        )

        # 确定维度
        self.dimension = len(items[0].vector)

        # 清空现有数据
        self.items = items
        self.id_to_idx.clear()

        # 构建 ID 映射
        for idx, item in enumerate(self.items):
            self.id_to_idx[item.id] = idx
            if idx % 5000 == 0 and idx > 0:
                print(f"          构建ID映射: {idx}/{total}")

        # 一次性构建 K-D Tree
        print(f"          构建K-D Tree...")
        vectors = np.array([item.vector for item in items])
        self.tree = KDTree(vectors)

        self.built = True
        elapsed = datetime.datetime.now() - start_time
        print(f"        索引构建完成，耗时: {elapsed.total_seconds() * 1000:.0f}ms")

    def insert(self, item: IndexItem) -> None:
        """插入单个向量到索引（现在只是添加到列表，不重建树）"""
        # 简单添加到列表，不重建树
        idx = len(self.items)
        self.id_to_idx[item.id] = idx
        self.items.append(item)

        # 标记为未构建，下次查询前需要重建
        self.built = False

    def find_similar_with_threshold(
        self,
        query_vector: np.ndarray,
        threshold: float,
        max_results: int,
    ) -> List[Tuple[IndexItem, float]]:
        """查找超过相似度阈值的所有向量"""
        if not self.built or not self.items:
            return []

        if len(query_vector) != self.dimension:
            return []

        # 对于归一化向量，余弦相似度阈值转换为欧氏距离阈值
        # similarity >= threshold => 1 - (dist²/2) >= threshold => dist² <= 2*(1-threshold)
        dist_sq_threshold = 2.0 * (1.0 - threshold)

        query = query_vector.reshape(1, -1)

        # 使用 radius 查询查找半径内的所有点
        indices = self.tree.query_radius(query, r=np.sqrt(dist_sq_threshold))[0]

        if len(indices) == 0:
            return []

        # 计算每个点的实际距离和相似度
        results = []
        for idx in indices:
            item = self.items[idx]
            # 重新计算精确距离
            dist = np.linalg.norm(query_vector - item.vector)
            similarity = 1.0 - (dist ** 2 / 2.0)
            similarity = max(0.0, min(1.0, similarity))

            if similarity >= threshold:
                results.append((item, similarity))

        # 按相似度降序排序
        results.sort(key=lambda x: x[1], reverse=True)

        # 限制结果数量
        if len(results) > max_results:
            results = results[:max_results]

        return results

    def len(self) -> int:
        """获取索引大小"""
        return len(self.items)

    def is_empty(self) -> bool:
        """检查索引是否为空"""
        return len(self.items) == 0

    def is_built(self) -> bool:
        """检查是否已构建"""
        return self.built

    def get_dimension(self) -> int:
        """获取向量维度"""
        return self.dimension

    def clear(self) -> None:
        """清理索引"""
        self.tree = None
        self.id_to_idx.clear()
        self.items.clear()
        self.built = False