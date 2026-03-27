# market_matcher.py
# 市场匹配器：按类别堆叠 TF-IDF（L2 归一化）向量矩阵，用 P·Kᵀ / K·Pᵀ 做初筛与 Top-K，再统一二筛。

import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

from market import Market
from category_mapper import CategoryMapper
from category_vectorizer import CategoryVectorizer, CategoryVectorizerManager
from text_vectorizer import TextVectorizer, VectorizerConfig
from unclassified_logger import UnclassifiedLogger, log_unclassified_market
from system_params import MATCH_MATMUL_CHUNK_ROWS, SIMILARITY_THRESHOLD, SIMILARITY_TOP_K
from validation import ValidationPipeline
from vector_index import IndexItem


def _parallel_build_category_worker(
    task: Tuple[str, List[Tuple[str, str, Optional[Any]]], TextVectorizer],
) -> Tuple[str, CategoryVectorizer]:
    """供 `parallel_build_category_indices` 线程池使用的 worker。"""
    category, items, vz = task
    cv = CategoryVectorizer.with_fitted_vectorizer(category, vz)
    cv.add_markets_batch(items)
    return category, cv


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
        self.kalshi_market_cache: Dict[str, Market] = {}
        self.polymarket_market_cache: Dict[str, Market] = {}
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

    @staticmethod
    def parallel_build_category_indices(
        manager: CategoryVectorizerManager,
        by_category: Dict[str, List[Tuple[str, str, Optional[Any]]]],
    ) -> None:
        """按类别并行建索引；与逐类串行 `add_markets_batch` 等价。"""
        n_cat = len(by_category)
        if n_cat == 0:
            return
        print(f"      并行构建 {n_cat} 个类别索引...")
        tasks: List[Tuple[str, List[Tuple[str, str, Optional[Any]]], TextVectorizer]] = []
        for category in sorted(by_category.keys()):
            items = by_category[category]
            cv = manager.get(category)
            if cv is None or not cv.fitted:
                continue
            tasks.append((category, items, copy.deepcopy(cv.vectorizer)))
        if not tasks:
            return
        max_workers = min(32, len(tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            built = list(ex.map(_parallel_build_category_worker, tasks))
        for cat, cv in built:
            manager.insert_built_category(cat, cv)

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
                    log_unclassified_market(self.unclassified_logger, market)
                if "unclassified" not in by_category:
                    by_category["unclassified"] = []
                by_category["unclassified"].append((market_id, market.title, data))
            else:
                for cat in categories:
                    if cat not in by_category:
                        by_category[cat] = []
                    by_category[cat].append((market_id, market.title, data))

        self.kalshi_market_cache = cache
        self.parallel_build_category_indices(self.kalshi_vectorizers, by_category)

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
                    log_unclassified_market(self.unclassified_logger, market)
                if "unclassified" not in by_category:
                    by_category["unclassified"] = []
                by_category["unclassified"].append((market_id, market.title, data))
            else:
                for cat in categories:
                    if cat not in by_category:
                        by_category[cat] = []
                    by_category[cat].append((market_id, market.title, data))

        self.polymarket_market_cache = cache
        self.parallel_build_category_indices(self.polymarket_vectorizers, by_category)

        print(f"   ✅ Polymarket 索引构建完成，总市场数: {self.polymarket_vectorizers.total_size()}")

    @staticmethod
    def _top_k_similarities_for_row(
        row: np.ndarray, threshold: float, max_results: int
    ) -> List[Tuple[int, float]]:
        if max_results == 0:
            return []
        hits = [(j, float(s)) for j, s in enumerate(row) if s >= threshold]
        hits.sort(key=lambda x: x[1], reverse=True)
        return hits[:max_results]

    @classmethod
    def _sweep_pm_to_ks_candidates_ordered(
        cls,
        p_mat: np.ndarray,
        k_mat: np.ndarray,
        pm_items: List[IndexItem],
        ks_items: List[IndexItem],
        chunk: int,
        threshold: float,
        top_k: int,
    ) -> List[Tuple[str, str, float]]:
        n_p = int(p_mat.shape[0])
        if n_p == 0:
            return []
        k_t = np.ascontiguousarray(k_mat.T)
        chunk = max(1, int(chunk))
        starts = list(range(0, n_p, chunk))

        def work(r0: int) -> Tuple[int, List[Tuple[str, str, float]]]:
            r1 = min(r0 + chunk, n_p)
            p_sl = p_mat[r0:r1, :]
            scores = p_sl @ k_t
            out: List[Tuple[str, str, float]] = []
            for local in range(scores.shape[0]):
                row_idx = r0 + local
                pm_id = pm_items[row_idx].id
                hits = cls._top_k_similarities_for_row(scores[local], threshold, top_k)
                for j, sim in hits:
                    ks_id = ks_items[j].id
                    out.append((pm_id, ks_id, sim))
            return r0, out

        max_workers = min(32, max(1, len(starts)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            parts = list(ex.map(work, starts))
        parts.sort(key=lambda x: x[0])
        flat: List[Tuple[str, str, float]] = []
        for _, v in parts:
            flat.extend(v)
        return flat

    @classmethod
    def _sweep_ks_to_pm_candidates_ordered(
        cls,
        p_mat: np.ndarray,
        k_mat: np.ndarray,
        pm_items: List[IndexItem],
        ks_items: List[IndexItem],
        chunk: int,
        threshold: float,
        top_k: int,
    ) -> List[Tuple[str, str, float]]:
        n_k = int(k_mat.shape[0])
        if n_k == 0:
            return []
        p_t = np.ascontiguousarray(p_mat.T)
        chunk = max(1, int(chunk))
        starts = list(range(0, n_k, chunk))

        def work(r0: int) -> Tuple[int, List[Tuple[str, str, float]]]:
            r1 = min(r0 + chunk, n_k)
            k_sl = k_mat[r0:r1, :]
            scores = k_sl @ p_t
            out: List[Tuple[str, str, float]] = []
            for local in range(scores.shape[0]):
                row_idx = r0 + local
                ks_id = ks_items[row_idx].id
                hits = cls._top_k_similarities_for_row(scores[local], threshold, top_k)
                for j, sim in hits:
                    pm_id = pm_items[j].id
                    out.append((pm_id, ks_id, sim))
            return r0, out

        max_workers = min(32, max(1, len(starts)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            parts = list(ex.map(work, starts))
        parts.sort(key=lambda x: x[0])
        flat: List[Tuple[str, str, float]] = []
        for _, v in parts:
            flat.extend(v)
        return flat

    @staticmethod
    def _fold_candidates_into_best(
        best: Dict[Tuple[str, str], Tuple[float, str]],
        pm: str,
        ks: str,
        sim: float,
        cat: str,
    ) -> None:
        key = (pm, ks)
        if key in best:
            if sim > best[key][0]:
                best[key] = (sim, cat)
        else:
            best[key] = (sim, cat)

    def _try_push_pair_candidate(
        self,
        pm_id: str,
        ks_id: str,
        vector_sim: float,
        category: str,
        polymarket_cache: Dict[str, Market],
        kalshi_cache: Dict[str, Market],
        seen_accepted: Set[Tuple[str, str]],
        validation_pipeline: ValidationPipeline,
        out: List[Tuple[Market, Market, float, str, str, bool]],
    ) -> None:
        key = (pm_id, ks_id)
        if key in seen_accepted:
            return
        pm = polymarket_cache.get(pm_id)
        ks = kalshi_cache.get(ks_id)
        if pm is None or ks is None:
            return
        match_info = validation_pipeline.validate(
            pm.title, ks.title, vector_sim, category
        )
        if match_info:
            confidence = self._calculate_confidence(
                pm, ks, vector_sim, self.config
            )
            if confidence.overall_score >= self.config.similarity_threshold:
                seen_accepted.add(key)
                out.append(
                    (
                        pm,
                        ks,
                        confidence.overall_score,
                        match_info.pm_side,
                        match_info.kalshi_side,
                        match_info.needs_inversion,
                    )
                )

    def _find_matches_batched_sync(
        self,
    ) -> Tuple[List[Tuple[Market, Market, float, str, str, bool]], ValidationPipeline]:
        start_time = datetime.now()
        validation_pipeline = ValidationPipeline()
        chunk = max(1, MATCH_MATMUL_CHUNK_ROWS)
        best_by_pair: Dict[Tuple[str, str], Tuple[float, str]] = {}
        cumulative_raw_hits = 0
        categories_used = 0

        polymarket_vec = self.polymarket_vectorizers
        kalshi_vec = self.kalshi_vectorizers
        polymarket_cache = self.polymarket_market_cache
        kalshi_cache = self.kalshi_market_cache
        cfg = self.config

        for cat in polymarket_vec.get_all_categories():
            pm_cv = polymarket_vec.get(cat)
            ks_cv = kalshi_vec.get(cat)
            if pm_cv is None or ks_cv is None:
                continue
            p_mat = pm_cv.index.data_matrix
            k_mat = ks_cv.index.data_matrix
            if p_mat is None or k_mat is None:
                continue
            if p_mat.shape[0] == 0 or k_mat.shape[0] == 0:
                continue
            if p_mat.shape[1] != k_mat.shape[1]:
                print(
                    f"   ⚠️  类别 {cat!r} Poly 与 Kalshi 向量维不一致 ({p_mat.shape[1]} vs {k_mat.shape[1]})，跳过"
                )
                continue
            categories_used += 1
            pm_items = pm_cv.index.items
            ks_items = ks_cv.index.items
            n_p, n_k = int(p_mat.shape[0]), int(k_mat.shape[0])
            cat_started = datetime.now()

            cand_pk = self._sweep_pm_to_ks_candidates_ordered(
                np.asarray(p_mat, dtype=np.float64),
                np.asarray(k_mat, dtype=np.float64),
                pm_items,
                ks_items,
                chunk,
                cfg.similarity_threshold,
                SIMILARITY_TOP_K,
            )
            cand_kp = self._sweep_ks_to_pm_candidates_ordered(
                np.asarray(p_mat, dtype=np.float64),
                np.asarray(k_mat, dtype=np.float64),
                pm_items,
                ks_items,
                chunk,
                cfg.similarity_threshold,
                SIMILARITY_TOP_K,
            )
            cat_raw = len(cand_pk) + len(cand_kp)
            cumulative_raw_hits += cat_raw
            for pm_id, ks_id, sim in cand_pk:
                self._fold_candidates_into_best(best_by_pair, pm_id, ks_id, sim, cat)
            for pm_id, ks_id, sim in cand_kp:
                self._fold_candidates_into_best(best_by_pair, pm_id, ks_id, sim, cat)
            print(
                f"   ✓ #{categories_used} 「{cat}」Poly×Kalshi {n_p}×{n_k} · 本类初筛 {cat_raw} 条 · 累计初筛 {cumulative_raw_hits} 条 · {(datetime.now() - cat_started).total_seconds():.3f}s"
            )

        n_unique_pairs = len(best_by_pair)
        merged: List[Tuple[str, str, float, str]] = [
            (pm, ks, sim, c) for (pm, ks), (sim, c) in best_by_pair.items()
        ]
        merged.sort(key=lambda x: (x[0], x[1]))
        seen_accepted: Set[Tuple[str, str]] = set()
        all_raw: List[Tuple[Market, Market, float, str, str, bool]] = []

        for pm_id, ks_id, sim, cat in merged:
            self._try_push_pair_candidate(
                pm_id,
                ks_id,
                sim,
                cat,
                polymarket_cache,
                kalshi_cache,
                seen_accepted,
                validation_pipeline,
                all_raw,
            )

        elapsed = (datetime.now() - start_time).total_seconds()
        print(
            f"   ✅ 大类 {categories_used} 个 · {elapsed:.3f}s · 初筛原始命中 {cumulative_raw_hits} 条 · 去重 {n_unique_pairs} 对 · 统一二筛保留 {len(all_raw)} 条"
        )

        return all_raw, validation_pipeline

    async def find_matches_bidirectional(
        self,
        pm_markets: List[Market],
        kalshi_markets: List[Market],
    ) -> List[Tuple[Market, Market, float, str, str, bool]]:
        """按大类矩阵初筛（P·Kᵀ / K·Pᵀ）后统一二筛；`pm_markets` / `kalshi_markets` 仅与旧接口兼容，实际使用已建索引。"""
        del pm_markets, kalshi_markets
        if not self.fitted:
            print("⚠️ 索引未构建")
            return []

        self.validation_pipeline.reset_filtered_count()

        print("\n🔍 双向匹配：按大类 P·Kᵀ / K·Pᵀ 初筛（类内并行）→ 全类统一二筛")

        start_time = datetime.now()

        all_raw, pipeline = await asyncio.to_thread(self._find_matches_batched_sync)
        self.validation_pipeline = pipeline

        initial_count = len(all_raw)
        all_raw.sort(key=lambda x: x[2], reverse=True)

        deduped: List[Tuple[Market, Market, float, str, str, bool]] = []
        seen_m: Set[Tuple[str, str]] = set()
        for t in all_raw:
            key = (t[0].market_id, t[1].market_id)
            if key not in seen_m:
                seen_m.add(key)
                deduped.append(t)

        final_count = len(deduped)
        merge_deduped = initial_count - final_count
        self.validation_pipeline.filtered_count = merge_deduped

        elapsed = (datetime.now() - start_time).total_seconds()
        print(
            f"\n📊 匹配：{elapsed:.3f}s · 二筛后列表 {initial_count} 条 · 按 market_id 去重后 {final_count} · 列表合并压掉 {merge_deduped}"
        )

        self.validation_pipeline.print_retained_samples()

        return deduped

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