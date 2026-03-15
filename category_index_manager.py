# category_index_manager.py
import numpy as np
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field

from vector_index import VectorIndex, IndexItem
from category_mapper import CategoryMapper


class CategoryIndexManager:
    """类别索引管理器：按类别管理多个 VectorIndex"""

    def __init__(self, mapper: CategoryMapper):
        self.indices: Dict[str, VectorIndex] = {}
        self.mapper = mapper
        self.unclassified_index = VectorIndex("unclassified")

    def _get_or_create_index(self, category: str) -> VectorIndex:
        """获取或创建类别索引"""
        if category == "unclassified":
            return self.unclassified_index

        if category not in self.indices:
            self.indices[category] = VectorIndex(category)

        return self.indices[category]

    def add_event(self, event_id: str, vector: np.ndarray, categories: List[str], data: Optional[Any] = None) -> None:
        """添加事件到对应的类别索引"""
        item = IndexItem(
            id=event_id,
            vector=vector,
            data=data
        )

        if not categories:
            # 无类别：加入未分类索引
            self.unclassified_index.insert(item)
        else:
            # 有类别：加入每个类别对应的索引
            for category in categories:
                index = self._get_or_create_index(category)
                # 克隆 item 以避免共享引用
                item_copy = IndexItem(
                    id=item.id,
                    vector=item.vector.copy(),
                    data=item.data
                )
                index.insert(item_copy)

    def add_events_batch(self, items: List[Tuple[str, np.ndarray, List[str], Optional[Any]]]) -> None:
        """批量添加事件"""
        success_count = 0
        fail_count = 0

        for event_id, vector, categories, data in items:
            try:
                self.add_event(event_id, vector, categories, data)
                success_count += 1
            except Exception as e:
                fail_count += 1
                if fail_count <= 5:
                    print(f"   ⚠️ 添加事件失败: {e}")

        if fail_count > 0:
            print(f"   ⚠️ 批量添加完成: 成功 {success_count} 个, 失败 {fail_count} 个")

    def find_similar_in_categories(
            self,
            categories: List[str],
            query_vector: np.ndarray,
            threshold: float,
            max_results: int
    ) -> List[Tuple[IndexItem, float, str]]:
        """在多个类别中查找相似事件"""
        all_results = []

        for category in categories:
            if category == "unclassified":
                if self.unclassified_index.len() > 0:
                    results = self.unclassified_index.find_similar_with_threshold(
                        query_vector, threshold, max_results
                    )
                else:
                    results = []
            elif category in self.indices:
                index = self.indices[category]
                if index.len() > 0:
                    results = index.find_similar_with_threshold(
                        query_vector, threshold, max_results
                    )
                else:
                    results = []
            else:
                continue

            for item, score in results:
                all_results.append((item, score, category))

        # 按相似度排序
        all_results.sort(key=lambda x: x[1], reverse=True)
        all_results = all_results[:max_results]

        return all_results

    def get_all_categories(self) -> List[str]:
        """获取所有类别名称"""
        cats = list(self.indices.keys())
        cats.append("unclassified")
        cats.sort()
        return cats

    def category_size(self, category: str) -> int:
        """获取类别索引大小"""
        if category == "unclassified":
            return self.unclassified_index.len()
        elif category in self.indices:
            return self.indices[category].len()
        else:
            return 0

    def total_size(self) -> int:
        """获取所有索引的总大小"""
        total = self.unclassified_index.len()
        for index in self.indices.values():
            total += index.len()
        return total

    def clear(self):
        """清理所有索引"""
        self.indices.clear()
        self.unclassified_index.clear()