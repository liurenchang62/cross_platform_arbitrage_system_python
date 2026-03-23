# market_matcher.py
#! 市场匹配器：TF-IDF 文本向量 + 类内精确余弦（堆叠矩阵点积）检索，与 Rust 一致

import numpy as np
from typing import List, Dict, Optional, Tuple, Set, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import json

from market import Market
from category_mapper import CategoryMapper
from unclassified_logger import UnclassifiedLogger
from query_params import SIMILARITY_THRESHOLD, SIMILARITY_TOP_K
from category_vectorizer import CategoryVectorizerManager
from text_vectorizer import VectorizerConfig
from validation import ValidationPipeline, MatchInfo


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
class MarketMatcherConfig:
    """市场匹配器配置"""
    similarity_threshold: float = SIMILARITY_THRESHOLD
    vectorizer_config: VectorizerConfig = field(default_factory=VectorizerConfig)
    use_date_boost: bool = True
    use_category_boost: bool = True
    date_boost_factor: float = 0.05
    category_boost_factor: float = 0.03


class MarketMatcher:
    """市场匹配器"""

    def __init__(self, config: MarketMatcherConfig, category_mapper: CategoryMapper):
        self.config = config
        self.category_mapper = category_mapper
        self.unclassified_logger: Optional[UnclassifiedLogger] = None
        self.kalshi_vectorizers = CategoryVectorizerManager()
        self.polymarket_vectorizers = CategoryVectorizerManager()
        self.fitted = False
        self.market_cache: Dict[str, Market] = {}
        self.validation_pipeline = ValidationPipeline()

    def with_logger(self, logger: UnclassifiedLogger) -> 'MarketMatcher':
        """设置未分类日志器"""
        self.unclassified_logger = logger
        return self

    def fit_vectorizer(self, kalshi_markets: List[Market], polymarket_markets: List[Market]) -> None:
        """按类别训练向量化器"""
        print("📚 按类别训练向量化器...")

        kalshi_by_category: Dict[str, List[str]] = {}
        for market in kalshi_markets:
            categories = self.category_mapper.classify(market.title)
            for cat in categories:
                if cat not in kalshi_by_category:
                    kalshi_by_category[cat] = []
                kalshi_by_category[cat].append(market.title)

        polymarket_by_category: Dict[str, List[str]] = {}
        for market in polymarket_markets:
            categories = self.category_mapper.classify(market.title)
            for cat in categories:
                if cat not in polymarket_by_category:
                    polymarket_by_category[cat] = []
                polymarket_by_category[cat].append(market.title)

        print(f"   📊 训练 Kalshi 类别向量化器...")
        self.kalshi_vectorizers.fit_all(kalshi_by_category)

        print(f"   📊 训练 Polymarket 类别向量化器...")
        self.polymarket_vectorizers.fit_all(polymarket_by_category)

        self.fitted = True

    def build_kalshi_index(self, markets: List[Market]) -> None:
        """构建 Kalshi 市场索引"""
        if not markets:
            return

        print("📊 构建 Kalshi 市场索引...")

        by_category: Dict[str, List[Tuple[str, str, Optional[Any]]]] = {}
        cache = {}

        for market in markets:
            market_id = f"{market.platform}:{market.market_id}"
            cache[market_id] = market

            categories = self.category_mapper.classify(market.title)
            data = {
                "title": market.title,
                "platform": market.platform,
            }

            if not categories:
                if self.unclassified_logger:
                    try:
                        self.unclassified_logger.log_unclassified(market)
                    except Exception as e:
                        print(f"   ⚠️ 记录未分类市场失败: {e}")
                if "unclassified" not in by_category:
                    by_category["unclassified"] = []
                by_category["unclassified"].append((market_id, market.title, data))
            else:
                for cat in categories:
                    if cat not in by_category:
                        by_category[cat] = []
                    by_category[cat].append((market_id, market.title, data))

        self.market_cache.update(cache)

        for category, items in by_category.items():
            vectorizer = self.kalshi_vectorizers.get_or_create(category)
            if vectorizer:
                vectorizer.add_markets_batch(items)

        print(f"   ✅ Kalshi 索引构建完成，总市场数: {self.kalshi_vectorizers.total_size()}")

    def build_polymarket_index(self, markets: List[Market]) -> None:
        """构建 Polymarket 市场索引"""
        if not markets:
            return

        print("📊 构建 Polymarket 市场索引...")

        by_category: Dict[str, List[Tuple[str, str, Optional[Any]]]] = {}
        cache = {}

        for market in markets:
            market_id = f"{market.platform}:{market.market_id}"
            cache[market_id] = market

            categories = self.category_mapper.classify(market.title)
            data = {
                "title": market.title,
                "platform": market.platform,
            }

            if not categories:
                if self.unclassified_logger:
                    try:
                        self.unclassified_logger.log_unclassified(market)
                    except Exception as e:
                        print(f"   ⚠️ 记录未分类市场失败: {e}")
                if "unclassified" not in by_category:
                    by_category["unclassified"] = []
                by_category["unclassified"].append((market_id, market.title, data))
            else:
                for cat in categories:
                    if cat not in by_category:
                        by_category[cat] = []
                    by_category[cat].append((market_id, market.title, data))

        self.market_cache.update(cache)

        for category, items in by_category.items():
            vectorizer = self.polymarket_vectorizers.get_or_create(category)
            if vectorizer:
                vectorizer.add_markets_batch(items)

        print(f"   ✅ Polymarket 索引构建完成，总市场数: {self.polymarket_vectorizers.total_size()}")

    async def find_matches_bidirectional(
        self,
        pm_markets: List[Market],
        kalshi_markets: List[Market],
    ) -> List[Tuple[Market, Market, float, str, str, bool]]:
        """双向查找匹配的市场对"""
        if not self.fitted:
            print("⚠️ 索引未构建")
            return []

        self.validation_pipeline.reset_filtered_count()

        print("\n🔍 ====== 开始双向匹配 ======")

        # 克隆需要的数据用于并行任务
        kalshi_vec = self.kalshi_vectorizers
        polymarket_vec = self.polymarket_vectorizers

        category_mapper1 = self.category_mapper
        category_mapper2 = self.category_mapper

        config1 = self.config
        config2 = self.config

        market_cache1 = self.market_cache
        market_cache2 = self.market_cache

        pm_markets_list = pm_markets.copy()
        kalshi_markets_list = kalshi_markets.copy()

        start_time = datetime.now()

        print("\n📌 并行执行两个方向...")

        # 真正的并行执行
        task1 = asyncio.create_task(
            self._find_matches_directional_internal(
                pm_markets_list,
                kalshi_vec,
                category_mapper1,
                config1,
                market_cache1,
                "PM→Kalshi",
            )
        )

        task2 = asyncio.create_task(
            self._find_matches_directional_internal(
                kalshi_markets_list,
                polymarket_vec,
                category_mapper2,
                config2,
                market_cache2,
                "Kalshi→PM",
            )
        )

        results = await asyncio.gather(task1, task2)
        matches1, pipeline1 = results[0]
        matches2, pipeline2 = results[1]

        initial_count = len(matches1) + len(matches2)

        all_matches = []
        seen_pairs = set()

        # 处理方向1的匹配 (PM→Kalshi)
        for m1, m2, score, pm_side, ks_side, needs_inversion in matches1:
            pair_key = f"{m1.market_id}:{m2.market_id}"
            reverse_key = f"{m2.market_id}:{m1.market_id}"

            if pair_key not in seen_pairs and reverse_key not in seen_pairs:
                seen_pairs.add(pair_key)
                if m1.platform == "polymarket" and m2.platform == "kalshi":
                    all_matches.append((m1, m2, score, pm_side, ks_side, needs_inversion))
                else:
                    # 如果方向反了，交换并保留方向信息
                    all_matches.append((m2, m1, score, pm_side, ks_side, needs_inversion))

        # 处理方向2的匹配 (Kalshi→PM)
        for m1, m2, score, pm_side, ks_side, needs_inversion in matches2:
            pair_key = f"{m2.market_id}:{m1.market_id}"
            reverse_key = f"{m1.market_id}:{m2.market_id}"

            if pair_key not in seen_pairs and reverse_key not in seen_pairs:
                seen_pairs.add(pair_key)
                # 方向2中，m1是Kalshi，m2是PM，需要交换并保留方向信息
                all_matches.append((m2, m1, score, pm_side, ks_side, needs_inversion))

        all_matches.sort(key=lambda x: x[2], reverse=True)

        final_count = len(all_matches)
        filtered_count = initial_count - final_count

        self.validation_pipeline.filtered_count = filtered_count

        # 合并留存样本
        for cat, samples in pipeline1.retained_samples.items():
            self.validation_pipeline.retained_samples[cat] = samples
        for cat, samples in pipeline2.retained_samples.items():
            self.validation_pipeline.retained_samples[cat] = samples

        elapsed = datetime.now() - start_time
        print(f"\n📊 ====== 匹配统计 ======")
        print(f"   并行匹配耗时: {elapsed.total_seconds() * 1000:.0f}ms")
        print(f"   初筛匹配对: {initial_count} 个")
        print(f"   二筛过滤: {filtered_count} 个")
        print(f"   二筛后待跟踪: {final_count} 个")

        self.validation_pipeline.print_retained_samples()

        return all_matches

    async def _find_matches_directional_internal(
        self,
        query_markets: List[Market],
        target_vectorizers: CategoryVectorizerManager,
        category_mapper: CategoryMapper,
        config: MarketMatcherConfig,
        market_cache: Dict[str, Market],
        direction_label: str,
    ) -> Tuple[List[Tuple[Market, Market, float, str, str, bool]], ValidationPipeline]:
        """单向查找匹配的市场对"""
        all_matches = []
        total = len(query_markets)
        pipeline = ValidationPipeline()

        print(f"      🔍 匹配 {total} 个市场 [{direction_label}]...")
        start_time = datetime.now()
        is_kalshi_pm = direction_label == "Kalshi→PM"

        for idx, query_market in enumerate(query_markets):
            if idx > 0 and idx % 1000 == 0:
                elapsed = datetime.now() - start_time
                avg_time = elapsed.total_seconds() * 1000 / idx
                remaining = (total - idx) * avg_time / 1000
                print(f"        进度: {idx}/{total} 个市场 [{direction_label}], 已用 {elapsed.total_seconds():.1f}s, 预计剩余 {remaining:.1f}s")

            query_full_id = f"{query_market.platform}:{query_market.market_id}"
            seen_qt: Set[Tuple[str, str]] = set()

            query_categories = category_mapper.classify(query_market.title)

            for category in query_categories:
                vectorizer = target_vectorizers.get(category)
                if not vectorizer:
                    continue

                similar = vectorizer.find_similar(
                    query_market.title,
                    config.similarity_threshold,
                    SIMILARITY_TOP_K,
                )

                for item, similarity in similar:
                    if (query_full_id, item.id) in seen_qt:
                        continue
                    seen_qt.add((query_full_id, item.id))

                    target_market = market_cache.get(item.id)
                    if not target_market:
                        continue

                    pm_title = target_market.title if is_kalshi_pm else query_market.title
                    kalshi_title = query_market.title if is_kalshi_pm else target_market.title

                    match_info = pipeline.validate(
                        pm_title,
                        kalshi_title,
                        similarity,
                        category,
                    )

                    if match_info:
                        confidence = self._calculate_confidence(
                            query_market,
                            target_market,
                            similarity,
                            config,
                        )

                        if confidence.overall_score >= config.similarity_threshold:
                            all_matches.append((
                                query_market,
                                target_market,
                                confidence.overall_score,
                                match_info.pm_side,
                                match_info.kalshi_side,
                                match_info.needs_inversion,
                            ))

        elapsed = datetime.now() - start_time
        print(f"        匹配完成 [{direction_label}], 耗时: {elapsed.total_seconds():.1f}s, 找到 {len(all_matches)} 个匹配")

        return all_matches, pipeline

    def _calculate_confidence(
        self,
        market1: Market,
        market2: Market,
        vector_similarity: float,
        config: MarketMatcherConfig,
    ) -> MatchConfidence:
        """计算置信度"""
        final_score = vector_similarity

        date_match = False
        if market1.resolution_date and market2.resolution_date:
            diff = abs((market1.resolution_date - market2.resolution_date).total_seconds())
            if diff <= 86400:  # 1天内
                match_quality = 1.0
                date_match = True
            else:
                match_quality = 0.0

            if config.use_date_boost:
                final_score += config.date_boost_factor * match_quality

        category_match = False
        if market1.category and market2.category:
            if market1.category.lower() == market2.category.lower():
                match_quality = 1.0
                category_match = True
            else:
                match_quality = 0.0

            if config.use_category_boost:
                final_score += config.category_boost_factor * match_quality

        final_score = min(final_score, 1.0)

        return MatchConfidence(
            overall_score=final_score,
            text_similarity=vector_similarity,
            date_match=date_match,
            category_match=category_match,
        )

    def kalshi_index_size(self) -> int:
        """获取 Kalshi 索引大小"""
        return self.kalshi_vectorizers.total_size()

    def polymarket_index_size(self) -> int:
        """获取 Polymarket 索引大小"""
        return self.polymarket_vectorizers.total_size()