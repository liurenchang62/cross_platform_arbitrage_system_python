# vector_index.py
import numpy as np
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from sklearn.neighbors import KDTree

# K-D Tree 维度（向量维度）
TREE_DIMENSION = 100


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
        """从向量列表构建索引"""
        if not items:
            return

        # 检查维度一致性
        first_dim = len(items[0].vector)
        if first_dim != self.dimension:
            # 重新创建树以适应实际维度
            self.dimension = first_dim

        self.items = items
        self.id_to_idx.clear()

        # 构建向量矩阵
        vectors = np.array([item.vector for item in items])

        # 构建 K-D Tree
        self.tree = KDTree(vectors)

        # 构建 ID 到索引的映射
        for idx, item in enumerate(self.items):
            self.id_to_idx[item.id] = idx

        self.built = True

    def insert(self, item: IndexItem) -> None:
        """插入单个向量到索引（注意：这会重建整个树）"""
        # 由于 KDTree 不支持增量插入，需要重建
        self.items.append(item)
        self.build(self.items)

    def find_similar(self, query_vector: np.ndarray, k: int) -> List[Tuple[IndexItem, float]]:
        """查找最相似的 k 个向量"""
        if not self.built or not self.items:
            return []

        # 检查维度
        if len(query_vector) != self.dimension:
            print(f"查询向量维度 {len(query_vector)} 不匹配索引维度 {self.dimension}")
            return []

        # 查询 K-D Tree
        query = query_vector.reshape(1, -1)
        distances, indices = self.tree.query(query, k=min(k, len(self.items)))

        results = []
        for i, idx in enumerate(indices[0]):
            item = self.items[idx]
            # 将欧氏距离转换为余弦相似度
            # 对于归一化向量，余弦相似度 = 1 - (dist² / 2)
            similarity = 1.0 - (distances[0][i] ** 2 / 2.0)
            similarity = max(0.0, min(1.0, similarity))  # 限制在 [0,1] 范围
            results.append((item, similarity))

        return results

    def find_similar_with_threshold(
            self,
            query_vector: np.ndarray,
            threshold: float,
            max_results: int
    ) -> List[Tuple[IndexItem, float]]:
        """查找超过相似度阈值的所有向量"""
        if not self.built or not self.items:
            return []

        # 检查维度
        if len(query_vector) != self.dimension:
            return []

        # 对于归一化向量，余弦相似度阈值转换为欧氏距离阈值
        # similarity >= threshold => 1 - (dist²/2) >= threshold => dist² <= 2*(1-threshold)
        dist_threshold = np.sqrt(2.0 * (1.0 - threshold))

        query = query_vector.reshape(1, -1)

        # 使用 radius 查询查找半径内的所有点
        indices = self.tree.query_radius(query, r=dist_threshold)[0]

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

    def get_by_id(self, id: str) -> Optional[IndexItem]:
        """通过 ID 获取向量项"""
        idx = self.id_to_idx.get(id)
        if idx is not None:
            return self.items[idx]
        return None

    def items_list(self) -> List[IndexItem]:
        """获取所有项"""
        return self.items.copy()

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

    def clear(self):
        """清理索引（重建）"""
        self.tree = None
        self.id_to_idx.clear()
        self.items.clear()
        self.built = False


def batch_find_similar(
        index: VectorIndex,
        query_vectors: List[np.ndarray],
        threshold: float,
        max_results_per_query: int
) -> List[List[Tuple[IndexItem, float]]]:
    """简化的批处理查找函数"""
    results = []
    for vec in query_vectors:
        results.append(index.find_similar_with_threshold(vec, threshold, max_results_per_query))
    return results