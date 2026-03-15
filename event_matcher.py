# event_matcher.py
import numpy as np
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json

from event import Event
from text_vectorizer import TextVectorizer, VectorizerConfig, cosine_similarity
from category_mapper import CategoryMapper
from category_index_manager import CategoryIndexManager, IndexItem
from unclassified_logger import UnclassifiedLogger


@dataclass
class MatchConfidence:
    """匹配结果置信度"""
    overall_score: float
    text_similarity: float
    date_match: bool
    category_match: bool

    def is_high_confidence(self) -> bool:
        """是否为高置信度匹配（>= 0.75）"""
        return self.overall_score >= 0.75

    def is_medium_confidence(self) -> bool:
        """是否为中等置信度匹配（0.50 - 0.75）"""
        return 0.50 <= self.overall_score < 0.75


@dataclass
class EventMatcherConfig:
    """事件匹配器配置"""
    similarity_threshold: float = 0.5
    vectorizer_config: VectorizerConfig = field(default_factory=VectorizerConfig)
    use_date_boost: bool = True
    use_category_boost: bool = True
    date_boost_factor: float = 0.05
    category_boost_factor: float = 0.03


class EventMatcher:
    """事件匹配器"""

    def __init__(self, config: EventMatcherConfig, category_mapper: CategoryMapper):
        self.config = config
        self.vectorizer = TextVectorizer(config.vectorizer_config)
        self.kalshi_index = CategoryIndexManager(category_mapper)
        self.polymarket_index = CategoryIndexManager(category_mapper)
        self.category_mapper = category_mapper
        self.unclassified_logger: Optional[UnclassifiedLogger] = None
        self.fitted = False

    def with_logger(self, logger: UnclassifiedLogger) -> 'EventMatcher':
        """设置未分类日志器"""
        self.unclassified_logger = logger
        return self

    def with_threshold(self, threshold: float) -> 'EventMatcher':
        """设置相似度阈值"""
        self.config.similarity_threshold = threshold
        return self

    def fit_vectorizer(self, events: List[Event]):
        """训练向量化器（统一词汇表）"""
        titles = [e.title for e in events]
        self.vectorizer.fit(titles)
        print(f"📚 向量化器训练完成，词汇表大小: {self.vectorizer.vocab_size()}")

    def build_kalshi_index(self, events: List[Event]) -> None:
        """从事件列表构建 Kalshi 索引"""
        if not events:
            return

        print(f"📊 构建 Kalshi 事件索引: 处理 {len(events)} 个事件")
        print(f"   📚 使用已有词汇表大小: {self.vectorizer.vocab_size()}")

        items_to_add = []

        for event in events:
            vector = self.vectorizer.transform(event.title)
            if vector is None:
                continue

            categories = self.category_mapper.classify(event.title)

            if not categories and self.unclassified_logger:
                self.unclassified_logger.log_unclassified(event)

            data = {
                "title": event.title,
                "platform": event.platform,
                "category": event.category,
                "resolution_date": event.resolution_date.isoformat() if event.resolution_date else None
            }

            items_to_add.append((
                f"{event.platform}:{event.event_id}",
                vector,
                categories,
                data
            ))

        print(f"   📊 生成 {len(items_to_add)} 个待添加项")
        self.kalshi_index.add_events_batch(items_to_add)
        self.fitted = True

        print(f"   ✅ Kalshi 索引构建完成，总事件数: {self.kalshi_index.total_size()}")

    def build_polymarket_index(self, events: List[Event]) -> None:
        """从事件列表构建 Polymarket 索引"""
        if not events:
            return

        print(f"\n📊 构建 Polymarket 事件索引: 处理 {len(events)} 个事件")
        print(f"   📚 使用已有词汇表大小: {self.vectorizer.vocab_size()}")

        items_to_add = []

        for event in events:
            vector = self.vectorizer.transform(event.title)
            if vector is None:
                continue

            categories = self.category_mapper.classify(event.title)

            if not categories and self.unclassified_logger:
                self.unclassified_logger.log_unclassified(event)

            data = {
                "title": event.title,
                "platform": event.platform,
                "category": event.category,
                "resolution_date": event.resolution_date.isoformat() if event.resolution_date else None
            }

            items_to_add.append((
                f"{event.platform}:{event.event_id}",
                vector,
                categories,
                data
            ))

        print(f"   📊 生成 {len(items_to_add)} 个待添加项")
        self.polymarket_index.add_events_batch(items_to_add)

        print(f"   ✅ Polymarket 索引构建完成，总事件数: {self.polymarket_index.total_size()}")

    def find_matches_bidirectional(
            self,
            pm_events: List[Event],
            kalshi_events: List[Event]
    ) -> List[Tuple[Event, Event, float]]:
        """双向查找匹配的事件对"""
        if not self.fitted:
            print("⚠️ 索引未构建")
            return []

        print("\n🔍 ====== 开始双向匹配 ======")

        print("\n📊 共同类别检查:")
        check_cats = [
            "politics_us", "politics_uk", "gaming", "crypto", "business",
            "entertainment_movies", "entertainment_music", "sports_basketball"
        ]

        for cat in check_cats:
            k_size = self.kalshi_index.category_size(cat)
            p_size = self.polymarket_index.category_size(cat)
            if k_size > 0 and p_size > 0:
                print(f"   ✅ {cat}: Kalshi {k_size} 个, Polymarket {p_size} 个")

        print("\n   📌 方向1: Polymarket → Kalshi")
        matches1 = self._find_matches_directional(pm_events, kalshi_events, self.kalshi_index)

        print("\n   📌 方向2: Kalshi → Polymarket")
        matches2 = self._find_matches_directional(kalshi_events, pm_events, self.polymarket_index)

        all_matches = []
        seen_pairs = set()

        # 处理方向1的匹配 (Polymarket → Kalshi)
        for e1, e2, score in matches1:
            pair_key = f"{e1.event_id}:{e2.event_id}"
            reverse_key = f"{e2.event_id}:{e1.event_id}"

            if pair_key not in seen_pairs and reverse_key not in seen_pairs:
                seen_pairs.add(pair_key)
                if e1.platform == "polymarket" and e2.platform == "kalshi":
                    all_matches.append((e1, e2, score))
                else:
                    all_matches.append((e2, e1, score))

        # 处理方向2的匹配 (Kalshi → Polymarket)
        for e1, e2, score in matches2:
            pair_key = f"{e2.event_id}:{e1.event_id}"
            reverse_key = f"{e1.event_id}:{e2.event_id}"

            if pair_key not in seen_pairs and reverse_key not in seen_pairs:
                seen_pairs.add(pair_key)
                all_matches.append((e2, e1, score))

        # 按相似度降序排序
        all_matches.sort(key=lambda x: x[2], reverse=True)

        print(f"\n📊 ====== 匹配完成 ======")
        print(f"   共找到 {len(all_matches)} 个匹配对")

        return all_matches

    def _find_matches_directional(
            self,
            query_events: List[Event],
            target_events: List[Event],
            target_index: CategoryIndexManager
    ) -> List[Tuple[Event, Event, float]]:
        """单向查找匹配的事件对"""
        all_matches = []
        total_queries = 0

        # 构建目标事件映射，方便查找
        target_map = {f"{e.platform}:{e.event_id}": e for e in target_events}

        for query_event in query_events:
            query_vector = self.vectorizer.transform(query_event.title)
            if query_vector is None:
                continue

            query_categories = self.category_mapper.classify(query_event.title)
            if not query_categories:
                continue

            total_queries += 1

            similar = target_index.find_similar_in_categories(
                query_categories,
                query_vector,
                self.config.similarity_threshold,
                5
            )

            for item, similarity, category in similar:
                target_event = target_map.get(item.id)
                if target_event is None:
                    continue

                confidence = self._calculate_confidence(
                    query_event,
                    target_event,
                    similarity
                )

                if confidence.overall_score >= self.config.similarity_threshold:
                    all_matches.append((
                        query_event,
                        target_event,
                        confidence.overall_score
                    ))

        if total_queries > 0:
            print(f"      📊 查询事件: {len(query_events)}, 有类别事件: {total_queries}, 匹配: {len(all_matches)}")

        return all_matches

    def _calculate_confidence(
            self,
            event1: Event,
            event2: Event,
            vector_similarity: float
    ) -> MatchConfidence:
        """计算最终置信度（向量相似度 + 辅助特征加成）"""
        final_score = vector_similarity

        # 日期匹配加成
        date_match = False
        if event1.resolution_date and event2.resolution_date:
            diff = abs((event1.resolution_date - event2.resolution_date).total_seconds())
            if diff <= 86400:  # 1天内
                match_quality = 1.0
                date_match = True
            elif diff <= 604800:  # 1周内
                match_quality = 0.5
                date_match = True
            else:
                match_quality = 0.0

            if self.config.use_date_boost and match_quality > 0.0:
                final_score += self.config.date_boost_factor * match_quality

        # 类别匹配加成
        category_match = False
        if event1.category and event2.category:
            if event1.category.lower() == event2.category.lower():
                match_quality = 1.0
                category_match = True
            else:
                match_quality = 0.0

            if self.config.use_category_boost and match_quality > 0.0:
                final_score += self.config.category_boost_factor * match_quality

        # 确保分数不超过 1.0
        final_score = min(final_score, 1.0)

        return MatchConfidence(
            overall_score=final_score,
            text_similarity=vector_similarity,
            date_match=date_match,
            category_match=category_match
        )

    def vectorizer(self) -> TextVectorizer:
        """获取向量化器（用于调试）"""
        return self.vectorizer

    def kalshi_index_size(self) -> int:
        """获取 Kalshi 索引大小"""
        return self.kalshi_index.total_size()

    def polymarket_index_size(self) -> int:
        """获取 Polymarket 索引大小"""
        return self.polymarket_index.total_size()

    def is_fitted(self) -> bool:
        """检查是否已拟合"""
        return self.fitted

    def calculate_similarity(self, event1: Event, event2: Event) -> float:
        """计算两个事件的相似度（直接计算，不使用索引）"""
        if not self.fitted:
            return 0.0

        v1 = self.vectorizer.transform(event1.title)
        v2 = self.vectorizer.transform(event2.title)

        if v1 is None or v2 is None:
            return 0.0

        return cosine_similarity(v1, v2)

    def calculate_similarity_with_confidence(
            self,
            event1: Event,
            event2: Event
    ) -> MatchConfidence:
        """计算两个事件的相似度（带置信度详情）"""
        if not self.fitted:
            return MatchConfidence(
                overall_score=0.0,
                text_similarity=0.0,
                date_match=False,
                category_match=False
            )

        v1 = self.vectorizer.transform(event1.title)
        v2 = self.vectorizer.transform(event2.title)

        if v1 is None or v2 is None:
            return MatchConfidence(
                overall_score=0.0,
                text_similarity=0.0,
                date_match=False,
                category_match=False
            )

        similarity = cosine_similarity(v1, v2)
        return self._calculate_confidence(event1, event2, similarity)


def extract_year_from_title(title: str) -> Optional[int]:
    """提取事件标题中的年份（如果有）"""
    import re
    match = re.search(r'\b(20\d{2})\b', title)
    if match:
        return int(match.group(1))
    return None


def normalize_text_for_debug(text: str) -> str:
    """标准化事件标题用于调试"""
    return ' '.join(
        word for word in text.lower().split()
        if word.isalnum()
    )[:100]