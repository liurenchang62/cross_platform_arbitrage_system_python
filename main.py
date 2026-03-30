# main.py — Polymarket ↔ Kalshi 跨平台监控与套利验证入口
from __future__ import annotations

import json
import asyncio
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp

from market import Market
from market_filter import filter_markets_by_resolution_horizon
from clients import PolymarketClient, KalshiClient
from category_mapper import CategoryMapper
from unclassified_logger import UnclassifiedLogger
from market_matcher import MarketMatcher, MarketMatcherConfig
from arbitrage_detector import (
    ArbitrageDetector,
    ArbitrageOpportunity,
    PairLadderBuildFail,
    build_pair_orderbook_ladders,
    build_pair_orderbook_ladders_result,
    orderbook_best_ask_price,
    pm_buy_no_uses_yes_token_complement,
    polymarket_clob_token_id_for_buy,
)
from monitor_logger import MonitorLogger
from tracking import MonitorState, TrackedArbitrage, flip_binary_side, oriented_track_id
from kalshi_demo import KalshiDemoConfig, KalshiDemoConfigError, place_demo_buy_ioc
from arb_cycle_diag_log import append_arb_cycle_diagnostic_row
from log_format import utc_datetime_to_rfc3339
from system_params import (
    DEMO_REFERENCE_BUDGET_USD,
    FULL_FETCH_INTERVAL,
    KALSHI_DEMO_API_KEY_ID_ENV,
    KALSHI_DEMO_BASE_URL,
    KALSHI_DEMO_MODE_ENABLED,
    KALSHI_DEMO_PRIVATE_KEY_PATH_ENV,
    LOCAL_TOTAL_USD,
    MAX_RETRIES,
    RETRY_INITIAL_DELAY_MS,
    SIMILARITY_THRESHOLD,
    arb_track_concurrency,
    arb_tracking_diagnostics_enabled,
    paper_caps_demo,
    paper_caps_local,
)
from paper_trading import PaperEngine, validate_opportunity_from_ladders
import cycle_statistics

OpportunityRow = Tuple[ArbitrageOpportunity, str, str, Optional[datetime], Optional[datetime]]

_MATCH_ROW_T = Tuple[Market, Market, float, str, str, bool]


@dataclass
class TrackingArbDiag:
    attempts: int = 0
    no_pm_clob_token: int = 0
    pm_book_missing: int = 0
    ks_book_missing: int = 0
    ladders_failed: int = 0
    ladder_pm_invalid_side: int = 0
    ladder_pm_missing_bids: int = 0
    ladder_pm_missing_asks: int = 0
    ladder_pm_malformed: int = 0
    ladder_pm_liq_empty_side: int = 0
    ladder_pm_liq_all_filtered: int = 0
    ladder_ks_no_body: int = 0
    ladder_ks_invalid_side: int = 0
    ladder_ks_malformed: int = 0
    ladder_ks_liq_empty_side: int = 0
    ladder_ks_liq_all_filtered: int = 0
    rejected_strict: int = 0
    missing_best_ask: int = 0
    sum_ask_ge_1: int = 0
    sum_ask_lt_1: int = 0
    loose_pass_strict_fail: int = 0
    loose_still_fail: int = 0
    accepted: int = 0

    def primary_attribution(self) -> str:
        """本周期未通过时的主因标签；有通过时区分全过 / 部分过。"""
        if self.attempts == 0:
            return "no_attempts"
        if self.accepted >= self.attempts:
            return "all_pass"
        if self.accepted > 0:
            return "partial_pass"
        coarse = [
            ("no_pm_clob_token", self.no_pm_clob_token),
            ("pm_book_missing", self.pm_book_missing),
            ("ks_book_missing", self.ks_book_missing),
            ("ladders_failed", self.ladders_failed),
            ("rejected_strict", self.rejected_strict),
        ]
        coarse.sort(key=lambda x: -x[1])
        if coarse[0][1] == 0:
            return "fail_unknown"
        return f"fail_{coarse[0][0]}"

    def attribution_top_json(self) -> str:
        """按次数降序取前若干项非零计数，便于定位归因。"""
        pairs: list[tuple[str, int]] = [
            ("no_pm_clob_token", self.no_pm_clob_token),
            ("pm_book_missing", self.pm_book_missing),
            ("ks_book_missing", self.ks_book_missing),
            ("ladders_failed", self.ladders_failed),
            ("ladder_pm_invalid_side", self.ladder_pm_invalid_side),
            ("ladder_pm_missing_bids", self.ladder_pm_missing_bids),
            ("ladder_pm_missing_asks", self.ladder_pm_missing_asks),
            ("ladder_pm_malformed", self.ladder_pm_malformed),
            ("ladder_pm_liq_empty_side", self.ladder_pm_liq_empty_side),
            ("ladder_pm_liq_all_filtered", self.ladder_pm_liq_all_filtered),
            ("ladder_ks_no_body", self.ladder_ks_no_body),
            ("ladder_ks_invalid_side", self.ladder_ks_invalid_side),
            ("ladder_ks_malformed", self.ladder_ks_malformed),
            ("ladder_ks_liq_empty_side", self.ladder_ks_liq_empty_side),
            ("ladder_ks_liq_all_filtered", self.ladder_ks_liq_all_filtered),
            ("rejected_strict", self.rejected_strict),
            ("missing_best_ask", self.missing_best_ask),
            ("sum_ask_ge_1", self.sum_ask_ge_1),
            ("sum_ask_lt_1", self.sum_ask_lt_1),
            ("loose_pass_strict_fail", self.loose_pass_strict_fail),
            ("loose_still_fail", self.loose_still_fail),
        ]
        nonzero = [(k, v) for k, v in pairs if v > 0]
        nonzero.sort(key=lambda x: -x[1])
        top = dict(nonzero[:10])
        top["attempts"] = self.attempts
        top["accepted"] = self.accepted
        return json.dumps(top, ensure_ascii=False)

    def as_csv_row(
        self,
        time_s: str,
        cycle_id: int,
        cycle_phase: str,
        pool_size: int,
        arb_conc: int,
    ) -> list[str]:
        return [
            time_s,
            str(cycle_id),
            cycle_phase,
            str(pool_size),
            str(arb_conc),
            self.primary_attribution(),
            self.attribution_top_json(),
            str(self.attempts),
            str(self.accepted),
            str(self.no_pm_clob_token),
            str(self.pm_book_missing),
            str(self.ks_book_missing),
            str(self.ladders_failed),
            str(self.ladder_pm_invalid_side),
            str(self.ladder_pm_missing_bids),
            str(self.ladder_pm_missing_asks),
            str(self.ladder_pm_malformed),
            str(self.ladder_pm_liq_empty_side),
            str(self.ladder_pm_liq_all_filtered),
            str(self.ladder_ks_no_body),
            str(self.ladder_ks_invalid_side),
            str(self.ladder_ks_malformed),
            str(self.ladder_ks_liq_empty_side),
            str(self.ladder_ks_liq_all_filtered),
            str(self.rejected_strict),
            str(self.missing_best_ask),
            str(self.sum_ask_ge_1),
            str(self.sum_ask_lt_1),
            str(self.loose_pass_strict_fail),
            str(self.loose_still_fail),
        ]

    def record_ladder_fail(self, r: PairLadderBuildFail) -> None:
        self.ladders_failed += 1
        if r == PairLadderBuildFail.PM_INVALID_BUY_SIDE:
            self.ladder_pm_invalid_side += 1
        elif r == PairLadderBuildFail.PM_MISSING_BIDS_ARRAY:
            self.ladder_pm_missing_bids += 1
        elif r == PairLadderBuildFail.PM_MISSING_ASKS_ARRAY:
            self.ladder_pm_missing_asks += 1
        elif r == PairLadderBuildFail.PM_MALFORMED_QUOTE:
            self.ladder_pm_malformed += 1
        elif r == PairLadderBuildFail.PM_NO_ASK_LIQUIDITY_EMPTY_SIDE:
            self.ladder_pm_liq_empty_side += 1
        elif r == PairLadderBuildFail.PM_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED:
            self.ladder_pm_liq_all_filtered += 1
        elif r == PairLadderBuildFail.KS_MISSING_ORDERBOOK_BODY:
            self.ladder_ks_no_body += 1
        elif r == PairLadderBuildFail.KS_INVALID_BUY_SIDE:
            self.ladder_ks_invalid_side += 1
        elif r == PairLadderBuildFail.KS_MALFORMED_QUOTE:
            self.ladder_ks_malformed += 1
        elif r == PairLadderBuildFail.KS_NO_ASK_LIQUIDITY_EMPTY_SIDE:
            self.ladder_ks_liq_empty_side += 1
        elif r == PairLadderBuildFail.KS_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED:
            self.ladder_ks_liq_all_filtered += 1

    def merge_from(self, o: TrackingArbDiag) -> None:
        self.attempts += o.attempts
        self.no_pm_clob_token += o.no_pm_clob_token
        self.pm_book_missing += o.pm_book_missing
        self.ks_book_missing += o.ks_book_missing
        self.ladders_failed += o.ladders_failed
        self.ladder_pm_invalid_side += o.ladder_pm_invalid_side
        self.ladder_pm_missing_bids += o.ladder_pm_missing_bids
        self.ladder_pm_missing_asks += o.ladder_pm_missing_asks
        self.ladder_pm_malformed += o.ladder_pm_malformed
        self.ladder_pm_liq_empty_side += o.ladder_pm_liq_empty_side
        self.ladder_pm_liq_all_filtered += o.ladder_pm_liq_all_filtered
        self.ladder_ks_no_body += o.ladder_ks_no_body
        self.ladder_ks_invalid_side += o.ladder_ks_invalid_side
        self.ladder_ks_malformed += o.ladder_ks_malformed
        self.ladder_ks_liq_empty_side += o.ladder_ks_liq_empty_side
        self.ladder_ks_liq_all_filtered += o.ladder_ks_liq_all_filtered
        self.rejected_strict += o.rejected_strict
        self.missing_best_ask += o.missing_best_ask
        self.sum_ask_ge_1 += o.sum_ask_ge_1
        self.sum_ask_lt_1 += o.sum_ask_lt_1
        self.loose_pass_strict_fail += o.loose_pass_strict_fail
        self.loose_still_fail += o.loose_still_fail
        self.accepted += o.accepted

    def print_summary(self) -> None:
        print(
            f"\n   🔎 ARB_TRACK_DIAG | 本周期尝试 {self.attempts} 次 | 通过严格检测 {self.accepted}"
        )
        print(
            f"      前置失败：无 PM token {self.no_pm_clob_token} · PM 簿缺 {self.pm_book_missing} · "
            f"KS 簿缺 {self.ks_book_missing} · 阶梯解析失败 {self.ladders_failed}"
        )
        if self.ladders_failed > 0:
            print(
                "      阶梯失败细分：PM 非Y/N {} · 缺bids {} · 缺asks {} · 档坏 {} · 空侧无行 {} · 有行全过滤 {} | "
                "KS 无orderbook {} · 非Y/N {} · 档坏 {} · 空侧无行 {} · 有行全过滤 {}".format(
                    self.ladder_pm_invalid_side,
                    self.ladder_pm_missing_bids,
                    self.ladder_pm_missing_asks,
                    self.ladder_pm_malformed,
                    self.ladder_pm_liq_empty_side,
                    self.ladder_pm_liq_all_filtered,
                    self.ladder_ks_no_body,
                    self.ladder_ks_invalid_side,
                    self.ladder_ks_malformed,
                    self.ladder_ks_liq_empty_side,
                    self.ladder_ks_liq_all_filtered,
                )
            )
            print(
                "      （流动性）空侧=该腿 asks/bids（或KS对应数组）0行；"
                "全过滤=有行但均不满足 0<价<1 且 size>0（含反推价越界、非有限、量≤0）"
            )
        print(
            f"      严格未过 {self.rejected_strict}：无最优Ask {self.missing_best_ask} · "
            f"两卖价和≥1 → {self.sum_ask_ge_1} · 和<1 → {self.sum_ask_lt_1}"
        )
        print(
            f"      和<1 子集：放宽 min_profit 可过 {self.loose_pass_strict_fail} · "
            f"放宽仍否 {self.loose_still_fail}（多因深度/n 缩放）"
        )
        print(
            "      （解读）和≥1 多为定价无结构套利；和<1 且放宽可过＝门槛/费用吃光；放宽仍否＝簿深度或计算路径问题"
        )

    def print_compact(self) -> None:
        print(
            "   📎 追踪判定摘要: 尝试 {} | 通过 {} | 无PM token {} | PM簿缺 {} KS簿缺 {} | 阶梯失败 {} | "
            "严格拒 {} (其中和≥1: {})".format(
                self.attempts,
                self.accepted,
                self.no_pm_clob_token,
                self.pm_book_missing,
                self.ks_book_missing,
                self.ladders_failed,
                self.rejected_strict,
                self.sum_ask_ge_1,
            )
        )


def _load_dotenv() -> None:
    """从本仓库根目录加载 `.env`（与 `main.py` 同目录），不依赖进程当前工作目录。"""
    try:
        from dotenv import load_dotenv

        env_path = Path(__file__).resolve().parent / ".env"
        load_dotenv(env_path)
    except ImportError:
        pass


def print_kalshi_demo_mode_missing_credentials() -> None:
    print(
        f"❌ system_params.KALSHI_DEMO_MODE_ENABLED=True，但未配置 "
        f"{KALSHI_DEMO_API_KEY_ID_ENV} 与 {KALSHI_DEMO_PRIVATE_KEY_PATH_ENV}"
    )
    print("   请在运行进程的环境中同时设置上述变量（可使用项目根目录 `.env`，启动时会自动加载）。")


def expand_dual_orientations(
    matches: List[Tuple[Market, Market, float, str, str, bool]],
) -> List[Tuple[Market, Market, float, str, str, bool]]:
    """同一 PM–Kalshi 匹配在验证器给出的腿向上，再增加「两侧同时取反」的另一腿，去重后用于追踪与验证。"""
    out: List[Tuple[Market, Market, float, str, str, bool]] = []
    seen: set[Tuple[str, str, str, str]] = set()

    for pm, ks, sim, pms, kss, inv in matches:

        def try_push(ps: str, ks_: str) -> None:
            key = (pm.market_id, ks.market_id, ps, ks_)
            if key not in seen:
                seen.add(key)
                out.append((deepcopy(pm), deepcopy(ks), sim, ps, ks_, inv))

        try_push(pms, kss)
        fa = flip_binary_side(pms)
        fb = flip_binary_side(kss)
        if fa is not None and fb is not None:
            try_push(fa, fb)

    return out


def format_resolution_expiry(label: str, dt: Optional[datetime]) -> str:
    if dt is None:
        return f"{label} 到期: 未知"
    now = datetime.now(timezone.utc)
    t = dt
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    else:
        t = t.astimezone(timezone.utc)
    days = (t - now).days
    if days > 0:
        day_hint = f"距今 {days} 天"
    elif days < 0:
        day_hint = f"已过期 {-days} 天"
    else:
        day_hint = "今天到期"
    wall = t.strftime("%Y-%m-%d %H:%M")
    return f"{label} 到期: {wall} UTC ({day_hint})"


async def refresh_pm_market_from_gamma(
    polymarket: PolymarketClient, target: Market
) -> None:
    try:
        fresh_pm = await polymarket.fetch_market_snapshot_by_id(target.market_id)
    except Exception:
        return
    target.outcome_prices = fresh_pm.outcome_prices or target.outcome_prices
    target.best_ask = fresh_pm.best_ask if fresh_pm.best_ask is not None else target.best_ask
    target.best_bid = fresh_pm.best_bid if fresh_pm.best_bid is not None else target.best_bid
    target.last_trade_price = (
        fresh_pm.last_trade_price
        if fresh_pm.last_trade_price is not None
        else target.last_trade_price
    )
    target.volume_24h = fresh_pm.volume_24h
    if fresh_pm.token_ids:
        target.token_ids = fresh_pm.token_ids
    if target.resolution_date is None:
        target.resolution_date = fresh_pm.resolution_date


async def validate_arbitrage_core(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    arb_detector: ArbitrageDetector,
    pm_market: Market,
    kalshi_market: Market,
    pm_side: str,
    kalshi_side: str,
    needs_inversion: bool,
    capital_usdt: float,
    pair_cap_usdt: float,
    arb_diag: Optional[TrackingArbDiag],
) -> Optional[Tuple[ArbitrageOpportunity, Optional[datetime], Optional[datetime]]]:
    if arb_diag is not None:
        arb_diag.attempts += 1
    if not pm_market.token_ids:
        if arb_diag is not None:
            arb_diag.no_pm_clob_token += 1
        return None
    pm_buy_no_via = pm_buy_no_uses_yes_token_complement(pm_market, pm_side)
    tid_opt = polymarket_clob_token_id_for_buy(pm_market, pm_side)
    if tid_opt is None:
        if arb_diag is not None:
            arb_diag.no_pm_clob_token += 1
        return None
    tid = tid_opt
    pm_book, ks_book = await asyncio.gather(
        polymarket.get_order_book(tid),
        kalshi.get_order_book(kalshi_market.market_id),
    )
    if not pm_book:
        if arb_diag is not None:
            arb_diag.pm_book_missing += 1
        return None
    if not ks_book:
        if arb_diag is not None:
            arb_diag.ks_book_missing += 1
        return None

    ladders_r = build_pair_orderbook_ladders_result(
        pm_book, ks_book, pm_side, kalshi_side, pm_buy_no_via
    )
    if isinstance(ladders_r, PairLadderBuildFail):
        if arb_diag is not None:
            arb_diag.record_ladder_fail(ladders_r)
        return None
    ladders = ladders_r

    opp = validate_opportunity_from_ladders(
        arb_detector,
        ladders,
        pm_side,
        kalshi_side,
        needs_inversion,
        capital_usdt,
        pair_cap_usdt,
    )
    if opp is None:
        if arb_diag is not None:
            arb_diag.rejected_strict += 1
            p = orderbook_best_ask_price(ladders.pm_asks)
            k = orderbook_best_ask_price(ladders.ks_asks)
            if p is not None and k is not None:
                if p + k >= 1.0:
                    arb_diag.sum_ask_ge_1 += 1
                else:
                    arb_diag.sum_ask_lt_1 += 1
                    loose = ArbitrageDetector(-1e100)
                    if (
                        validate_opportunity_from_ladders(
                            loose,
                            ladders,
                            pm_side,
                            kalshi_side,
                            needs_inversion,
                            capital_usdt,
                            pair_cap_usdt,
                        )
                        is not None
                    ):
                        arb_diag.loose_pass_strict_fail += 1
                    else:
                        arb_diag.loose_still_fail += 1
            else:
                arb_diag.missing_best_ask += 1
        return None

    if arb_diag is not None:
        arb_diag.accepted += 1

    pm_expiry = pm_market.resolution_date
    if pm_expiry is None:
        pm_expiry = await polymarket.fetch_resolution_by_market_id(pm_market.market_id)
    ks_expiry = kalshi_market.resolution_date
    if ks_expiry is None:
        ks_expiry = await kalshi.fetch_resolution_by_ticker(kalshi_market.market_id)

    return (opp, pm_expiry, ks_expiry)


async def validate_arbitrage_pair_post_success(
    pm_market: Market,
    kalshi_market: Market,
    similarity: float,
    pm_side: str,
    kalshi_side: str,
    needs_inversion: bool,
    capital_usdt: float,
    per_leg_cap_usd: float,
    cycle_id: int,
    cycle_phase: str,
    logger: MonitorLogger,
    pair_label: str,
    paper: Optional[PaperEngine],
    demo_http: Optional[aiohttp.ClientSession],
    kalshi_demo_cfg: Optional[KalshiDemoConfig],
    demo_trading_active: List[bool],
    opp: ArbitrageOpportunity,
    pm_expiry: Optional[datetime],
    ks_expiry: Optional[datetime],
) -> None:
    tid_opt = polymarket_clob_token_id_for_buy(pm_market, pm_side)
    if tid_opt is None:
        return
    tid = tid_opt
    pm_buy_no_via = pm_buy_no_uses_yes_token_complement(pm_market, pm_side)

    inv = " (Y/N颠倒)" if needs_inversion else ""
    pm_slip = (
        (opp.pm_avg_slipped - opp.pm_optimal) / opp.pm_optimal * 100.0
    ) if opp.pm_optimal > 0.0 else 0.0
    ks_slip = (
        (opp.kalshi_avg_slipped - opp.kalshi_optimal) / opp.kalshi_optimal * 100.0
    ) if opp.kalshi_optimal > 0.0 else 0.0

    print(f"\n  📌 验证通过 (相似度: {similarity:.3f}){inv}")
    print(f"     PM:      {pm_market.title}")
    print(f"     Kalshi:  {kalshi_market.title}")
    print(f"     📅 {format_resolution_expiry('PM', pm_expiry)}")
    print(f"     📅 {format_resolution_expiry('Kalshi', ks_expiry)}")
    print("     ─────────────────────────────────────────────────────────")
    print(f"     📗 PM 订单簿(买{pm_side}) Top5:")
    for j, (p, s) in enumerate(opp.orderbook_pm_top5[:5]):
        print(f"         #{j + 1}. 价 {p:.4f} 量 {s:.1f}")
    if not opp.orderbook_pm_top5:
        print("         (无订单簿)")
    print(f"     📗 Kalshi 订单簿(买{kalshi_side}) Top5:")
    for j, (p, s) in enumerate(opp.orderbook_kalshi_top5[:5]):
        print(f"         #{j + 1}. 价 {p:.4f} 量 {s:.1f}")
    if not opp.orderbook_kalshi_top5:
        print("         (无订单簿)")
    print("     ─────────────────────────────────────────────────────────")
    print(f"     📊 策略: Polymarket 买 {pm_side}  +  Kalshi 买 {kalshi_side}")
    print("     ─────────────────────────────────────────────────────────")
    print(f"     📊 对冲份数 n: {opp.contracts:.4f}")
    print(
        f"     💵 最优 Ask:     PM {opp.pm_optimal:.4f}  |  Kalshi {opp.kalshi_optimal:.4f}"
    )
    print(
        f"     📉 滑点后均价:   PM {opp.pm_avg_slipped:.4f} ({pm_slip:+.2f}%)  |  Kalshi {opp.kalshi_avg_slipped:.4f} ({ks_slip:+.2f}%)"
    )
    print("     ─────────────────────────────────────────────────────────")
    print(f"     💰 投入 ${capital_usdt:.2f}（每腿探针上限）利润拆解:")
    print(f"        毛利润(兑付): ${opp.contracts:.2f}")
    print(f"        - 成本:        ${opp.capital_used:.2f}")
    print(f"        - 手续费:      ${opp.fees_amount:.2f}")
    print(f"        - Gas费:       ${opp.gas_amount:.2f}")
    print(f"        = 净利润:      ${opp.net_profit_100:.2f}")
    print(f"        ROI:           {opp.roi_100_percent:.1f}%")
    print("     ─────────────────────────────────────────────────────────")

    try:
        logger.log_arbitrage_opportunity(
            cycle_id,
            cycle_phase,
            opp,
            pm_market.market_id,
            kalshi_market.market_id,
            pm_market.title,
            kalshi_market.title,
            similarity,
            pm_side,
            kalshi_side,
            needs_inversion,
            pm_expiry,
            ks_expiry,
        )
    except Exception as e:
        print(f"         ⚠️ 写入监控 CSV 失败: {e}")

    if paper is not None:
        ks_exec_notes = "ks_exec=local"
        if (
            demo_trading_active[0]
            and kalshi_demo_cfg is not None
            and demo_http is not None
        ):
            placed: Optional[Tuple[str, str]] = None
            last_err: Optional[str] = None
            for attempt in range(MAX_RETRIES):
                client_order_id = str(uuid.uuid4())
                try:
                    order_id = await place_demo_buy_ioc(
                        demo_http,
                        kalshi_demo_cfg,
                        kalshi_market.market_id,
                        kalshi_side,
                        opp,
                        client_order_id,
                        per_leg_cap_usd,
                    )
                    placed = (order_id, client_order_id)
                    break
                except Exception as e:
                    last_err = str(e)
                    print(
                        f"   ⚠️ [Kalshi Demo] 下单失败 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}"
                    )
                    if attempt + 1 < MAX_RETRIES:
                        wait_ms = RETRY_INITIAL_DELAY_MS * (attempt + 1)
                        await asyncio.sleep(wait_ms / 1000.0)
            if placed is not None:
                oid, cid = placed
                ks_exec_notes = (
                    f"ks_exec=demo kalshi_order_id={oid} kalshi_client_order_id={cid}"
                )
                print(
                    f"   📗 [Kalshi Demo] 已提交 IOC 限价买单 order_id={oid} ticker={kalshi_market.market_id}"
                )
            else:
                print(
                    f"   ❌ [Kalshi Demo] 重试耗尽，停止 Demo 下单；纸面切换为纯本地新会话。最后错误: {last_err!r}"
                )
                demo_trading_active[0] = False
                paper.reset_to_pure_local_after_demo_failure(
                    f"kalshi_demo_order_failed_after_retries {last_err or ''}"
                )
        try:
            paper.try_open(
                pair_label,
                opp,
                pm_side,
                kalshi_side,
                cycle_id,
                pm_market.market_id,
                kalshi_market.market_id,
                tid,
                datetime.now(timezone.utc),
                ks_exec_notes,
                pm_buy_no_via,
            )
        except Exception as e:
            print(f"   ⚠️ [Paper] try_open 失败: {e}")


async def validate_arbitrage_pair(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    arb_detector: ArbitrageDetector,
    pm_market: Market,
    kalshi_market: Market,
    similarity: float,
    pm_side: str,
    kalshi_side: str,
    needs_inversion: bool,
    capital_usdt: float,
    pair_cap_usdt: float,
    per_leg_cap_usd: float,
    cycle_id: int,
    cycle_phase: str,
    logger: MonitorLogger,
    pair_label: str,
    paper: Optional[PaperEngine],
    demo_http: Optional[aiohttp.ClientSession],
    kalshi_demo_cfg: Optional[KalshiDemoConfig],
    demo_trading_active: List[bool],
    arb_diag: Optional[TrackingArbDiag] = None,
) -> Optional[Tuple[ArbitrageOpportunity, Optional[datetime], Optional[datetime]]]:
    core = await validate_arbitrage_core(
        polymarket,
        kalshi,
        arb_detector,
        pm_market,
        kalshi_market,
        pm_side,
        kalshi_side,
        needs_inversion,
        capital_usdt,
        pair_cap_usdt,
        arb_diag,
    )
    if core is None:
        return None
    opp, pm_exp, ks_exp = core
    await validate_arbitrage_pair_post_success(
        pm_market,
        kalshi_market,
        similarity,
        pm_side,
        kalshi_side,
        needs_inversion,
        capital_usdt,
        per_leg_cap_usd,
        cycle_id,
        cycle_phase,
        logger,
        pair_label,
        paper,
        demo_http,
        kalshi_demo_cfg,
        demo_trading_active,
        opp,
        pm_exp,
        ks_exp,
    )
    return (opp, pm_exp, ks_exp)


async def paper_cycle_start_close_checks(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    paper: Optional[PaperEngine],
    cycle_id: int,
) -> None:
    if paper is None:
        return
    positions = paper.snapshot_open_positions()
    if not positions:
        return
    check_time = datetime.now(timezone.utc)
    for pos in positions:
        pm_book = await polymarket.get_order_book(pos.pm_token_id)
        if pm_book is None:
            paper.log_no_close_book_error(
                pos, cycle_id, check_time, "pm_orderbook_fetch_failed"
            )
            continue
        ks_book = await kalshi.get_order_book(pos.kalshi_market_id)
        if ks_book is None:
            paper.log_no_close_book_error(
                pos, cycle_id, check_time, "kalshi_orderbook_fetch_failed"
            )
            continue
        ladders = build_pair_orderbook_ladders(
            pm_book,
            ks_book,
            pos.pm_side,
            pos.kalshi_side,
            pos.pm_buy_no_via_yes_book_bids,
        )
        if ladders is None:
            paper.log_no_close_book_error(
                pos, cycle_id, check_time, "orderbook_parse_failed"
            )
            continue
        paper.check_early_close_at_cycle(
            pos.pair_label, ladders, cycle_id, check_time
        )


def format_top10_opportunities(
    opportunities: List[OpportunityRow], mode_caption: str
) -> str:
    lines: List[str] = []
    lines.append("")
    if not opportunities:
        lines.append("🏆 本周期利润 Top 10: 无套利机会")
        return "\n".join(lines) + "\n"

    sorted_rows = sorted(opportunities, key=lambda r: r[0].net_profit_100, reverse=True)
    lines.append(f"🏆 本周期利润 Top 10（{mode_caption}，含滑点/手续费/Gas）")
    lines.append(
        "──────────────────────────────────────────────────────────────────────"
    )

    for i, (opp, pm_title, kalshi_title, pm_dt, ks_dt) in enumerate(sorted_rows[:10]):
        pm_slip = (
            (opp.pm_avg_slipped - opp.pm_optimal) / opp.pm_optimal * 100.0
        ) if opp.pm_optimal > 0.0 else 0.0
        ks_slip = (
            (opp.kalshi_avg_slipped - opp.kalshi_optimal) / opp.kalshi_optimal * 100.0
        ) if opp.kalshi_optimal > 0.0 else 0.0
        inv = " (Y/N颠倒)" if "颠倒" in opp.strategy else ""
        lines.append("")
        lines.append(
            f"   ┌─ #{i + 1} 净利润 ${opp.net_profit_100:.2f}  ROI {opp.roi_100_percent:.1f}% ─────────────────────────────────"
        )
        lines.append(f"   │  PM:      {pm_title}")
        lines.append(f"   │  Kalshi:  {kalshi_title}")
        lines.append(f"   │  📅 {format_resolution_expiry('PM', pm_dt)}")
        lines.append(f"   │  📅 {format_resolution_expiry('Kalshi', ks_dt)}")
        lines.append("   │  ─────────────────────────────────────────────────────────────")
        lines.append(
            f"   │  📊 策略: Polymarket 买 {opp.polymarket_action[1]}  +  Kalshi 买 {opp.kalshi_action[1]}{inv}"
        )
        lines.append(f"   │  📊 对冲份数 n: {opp.contracts:.4f}")
        lines.append(
            f"   │  💵 最优Ask: PM {opp.pm_optimal:.4f}  Kalshi {opp.kalshi_optimal:.4f}  →  滑点后: PM {opp.pm_avg_slipped:.4f}  Kalshi {opp.kalshi_avg_slipped:.4f}"
        )
        lines.append(f"   │  📉 滑点%: PM {pm_slip:+.2f}%  |  Kalshi {ks_slip:+.2f}%")
        lines.append(
            f"   │  💰 成本${opp.capital_used:.2f}  手续费${opp.fees_amount:.2f}  Gas${opp.gas_amount:.2f}  →  净利${opp.net_profit_100:.2f}"
        )
        lines.append(f"   │  ROI: {opp.roi_100_percent:.1f}%")
        lines.append("   └────────────────────────────────────────────────────────────────")

    lines.append("")
    return "\n".join(lines)


def _write_arb_cycle_diagnostic_csv(
    diag: TrackingArbDiag,
    cycle_id: int,
    cycle_phase: str,
    pool_size: int,
) -> None:
    try:
        append_arb_cycle_diagnostic_row(
            diag.as_csv_row(
                utc_datetime_to_rfc3339(datetime.now(timezone.utc)),
                cycle_id,
                cycle_phase,
                pool_size,
                arb_track_concurrency(),
            )
        )
    except Exception as e:
        print(f"   ⚠️ 写入套利周期诊断 CSV 失败: {e}")


async def run_full_match_work(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    matcher: MarketMatcher,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    polymarket_markets: List[Market],
    kalshi_markets: List[Market],
    trade_amount: float,
    pair_cap_usdt: float,
    per_leg_cap_usd: float,
    paper: Optional[PaperEngine],
    top10_mode_caption: str,
    demo_http: Optional[aiohttp.ClientSession],
    kalshi_demo_cfg: Optional[KalshiDemoConfig],
    demo_trading_active: List[bool],
    rebuild_index: bool,
) -> Tuple[int, int, str, str]:
    if rebuild_index:
        print("\n   🔄 重建索引...")
        matcher.fit_vectorizer(kalshi_markets, polymarket_markets)
        matcher.build_kalshi_index(kalshi_markets)
        matcher.build_polymarket_index(polymarket_markets)

    print("   🔍 匹配市场...")
    raw_matches = await matcher.find_matches_bidirectional(
        polymarket_markets, kalshi_markets
    )
    base_pairs = len(raw_matches)
    matches = expand_dual_orientations(raw_matches)
    print(
        f"      ✅ 基础匹配 {base_pairs} 对，展开双向腿后 {len(matches)} 条追踪配置"
    )

    all_matches: List[Tuple[Market, Market, float, str, str, bool]] = []
    for pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion in matches:
        all_matches.append(
            (pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion)
        )

    verified_count = 0
    opportunities: List[OpportunityRow] = []
    conc = arb_track_concurrency()
    full_cycle_id = monitor_state.current_cycle
    diag_total = TrackingArbDiag()

    if conc <= 1:
        for pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion in matches:
            pair_label = oriented_track_id(
                pm_market.market_id,
                kalshi_market.market_id,
                pm_side,
                kalshi_side,
            )
            validated = await validate_arbitrage_pair(
                polymarket,
                kalshi,
                arb_detector,
                pm_market,
                kalshi_market,
                similarity,
                pm_side,
                kalshi_side,
                needs_inversion,
                trade_amount,
                pair_cap_usdt,
                per_leg_cap_usd,
                full_cycle_id,
                "full_match",
                logger,
                pair_label,
                paper,
                demo_http,
                kalshi_demo_cfg,
                demo_trading_active,
                diag_total,
            )
            if validated:
                verified, pm_exp, ks_exp = validated
                verified_count += 1
                cycle_statistics.record_opportunity(verified)
                opportunities.append(
                    (verified, pm_market.title, kalshi_market.title, pm_exp, ks_exp)
                )
    else:
        print(
            f"      … 全量套利验证并发 {conc}（{len(matches)} 条配置，与追踪共用 ARB_TRACK_CONCURRENCY）"
        )
        sem = asyncio.Semaphore(conc)

        async def _full_one(
            row: _MATCH_ROW_T,
        ) -> Tuple[
            Optional[Tuple[ArbitrageOpportunity, Optional[datetime], Optional[datetime], str, _MATCH_ROW_T]],
            TrackingArbDiag,
        ]:
            pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion = row
            pair_label = oriented_track_id(
                pm_market.market_id,
                kalshi_market.market_id,
                pm_side,
                kalshi_side,
            )
            frag = TrackingArbDiag()
            async with sem:
                core = await validate_arbitrage_core(
                    polymarket,
                    kalshi,
                    arb_detector,
                    pm_market,
                    kalshi_market,
                    pm_side,
                    kalshi_side,
                    needs_inversion,
                    trade_amount,
                    pair_cap_usdt,
                    frag,
                )
            if core is None:
                return (None, frag)
            opp, pm_exp, ks_exp = core
            return ((opp, pm_exp, ks_exp, pair_label, row), frag)

        chunk = await asyncio.gather(*[_full_one(row) for row in matches])
        for item, frag in chunk:
            diag_total.merge_from(frag)
            if item is None:
                continue
            opp, pm_exp, ks_exp, pair_label, row = item
            pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion = row
            await validate_arbitrage_pair_post_success(
                pm_market,
                kalshi_market,
                similarity,
                pm_side,
                kalshi_side,
                needs_inversion,
                trade_amount,
                per_leg_cap_usd,
                full_cycle_id,
                "full_match",
                logger,
                pair_label,
                paper,
                demo_http,
                kalshi_demo_cfg,
                demo_trading_active,
                opp,
                pm_exp,
                ks_exp,
            )
            verified_count += 1
            cycle_statistics.record_opportunity(opp)
            opportunities.append(
                (opp, pm_market.title, kalshi_market.title, pm_exp, ks_exp)
            )

    _write_arb_cycle_diagnostic_csv(
        diag_total, full_cycle_id, "full_match", len(matches)
    )
    diag_total.print_compact()

    top10_block = format_top10_opportunities(opportunities, top10_mode_caption)
    print(top10_block, end="")
    full_cycle_block = cycle_statistics.on_full_cycle_completed(opportunities)

    monitor_state.update_tracked_pairs(all_matches)

    return len(matches), verified_count, top10_block, full_cycle_block


async def run_full_match_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    matcher: MarketMatcher,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    trade_amount: float,
    pair_cap_usdt: float,
    per_leg_cap_usd: float,
    paper: Optional[PaperEngine],
    top10_mode_caption: str,
    demo_http: Optional[aiohttp.ClientSession],
    kalshi_demo_cfg: Optional[KalshiDemoConfig],
    demo_trading_active: List[bool],
) -> Tuple[int, int, str, str]:
    print("   📡 执行全量匹配（拉取最新市场并重建索引）...")

    polymarket_raw = await polymarket.fetch_all_markets()
    kalshi_raw = await kalshi.fetch_all_markets()
    now = datetime.now(timezone.utc)
    polymarket_markets = filter_markets_by_resolution_horizon(polymarket_raw, now)
    kalshi_markets = filter_markets_by_resolution_horizon(kalshi_raw, now)

    print(
        f"      Polymarket: {len(polymarket_markets)} 个市场 (21d 窗口内), Kalshi: {len(kalshi_markets)} 个市场 (21d 窗口内)"
    )

    return await run_full_match_work(
        polymarket,
        kalshi,
        matcher,
        arb_detector,
        logger,
        monitor_state,
        polymarket_markets,
        kalshi_markets,
        trade_amount,
        pair_cap_usdt,
        per_leg_cap_usd,
        paper,
        top10_mode_caption,
        demo_http,
        kalshi_demo_cfg,
        demo_trading_active,
        True,
    )


async def run_tracking_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    trade_amount: float,
    pair_cap_usdt: float,
    per_leg_cap_usd: float,
    paper: Optional[PaperEngine],
    top10_mode_caption: str,
    demo_http: Optional[aiohttp.ClientSession],
    kalshi_demo_cfg: Optional[KalshiDemoConfig],
    demo_trading_active: List[bool],
) -> Tuple[int, str]:
    print("   📡 执行价格追踪...")
    print(f"      追踪 {len(monitor_state.tracked_pairs)} 个匹配对")

    await polymarket.clear_price_cache()
    await kalshi.clear_price_cache()

    opportunity_count = 0
    opportunities: List[OpportunityRow] = []
    diag_verbose = arb_tracking_diagnostics_enabled()
    conc = arb_track_concurrency()
    n_active = sum(1 for p in monitor_state.tracked_pairs if p.active)
    tick_start = datetime.now()

    diag_total = TrackingArbDiag()

    if conc <= 1 or n_active == 0:
        done_active = 0
        diag_slot = TrackingArbDiag()
        for pair in monitor_state.tracked_pairs:
            if not pair.active:
                continue
            done_active += 1
            await refresh_pm_market_from_gamma(polymarket, pair.pm_market)
            if done_active % 25 == 0 or done_active == n_active:
                elapsed = (datetime.now() - tick_start).total_seconds()
                print(
                    f"      … 追踪校验进度 {done_active}/{n_active}，已用 {elapsed:.1f}s"
                )

            validated = await validate_arbitrage_pair(
                polymarket,
                kalshi,
                arb_detector,
                pair.pm_market,
                pair.kalshi_market,
                pair.similarity,
                pair.pm_side,
                pair.kalshi_side,
                pair.needs_inversion,
                trade_amount,
                pair_cap_usdt,
                per_leg_cap_usd,
                monitor_state.current_cycle,
                "price_track",
                logger,
                pair.pair_id,
                paper,
                demo_http,
                kalshi_demo_cfg,
                demo_trading_active,
                diag_slot,
            )
            if validated:
                verified, pm_exp, ks_exp = validated
                opportunity_count += 1
                cycle_statistics.record_opportunity(verified)
                pair.last_check = datetime.now(timezone.utc)
                if verified.net_profit_100 > pair.best_profit:
                    pair.best_profit = verified.net_profit_100
                opportunities.append(
                    (verified, pair.pm_market.title, pair.kalshi_market.title, pm_exp, ks_exp)
                )
        diag_total = diag_slot
    else:
        print(
            f"      … 使用并发 {conc} 校验 {n_active} 个活跃对（Gamma 仍每对一次，PM/KS 簿并行拉取）"
        )
        jobs: List[Tuple[int, TrackedArbitrage]] = [
            (i, deepcopy(p))
            for i, p in enumerate(monitor_state.tracked_pairs)
            if p.active
        ]
        sem = asyncio.Semaphore(conc)

        async def work(
            idx: int, pair: TrackedArbitrage
        ) -> Tuple[
            int,
            TrackedArbitrage,
            Optional[Tuple[ArbitrageOpportunity, Optional[datetime], Optional[datetime]]],
            TrackingArbDiag,
        ]:
            await refresh_pm_market_from_gamma(polymarket, pair.pm_market)
            frag = TrackingArbDiag()
            async with sem:
                core = await validate_arbitrage_core(
                    polymarket,
                    kalshi,
                    arb_detector,
                    pair.pm_market,
                    pair.kalshi_market,
                    pair.pm_side,
                    pair.kalshi_side,
                    pair.needs_inversion,
                    trade_amount,
                    pair_cap_usdt,
                    frag,
                )
            return idx, pair, core, frag

        results = await asyncio.gather(*[work(i, p) for i, p in jobs])
        for idx, pair_snap, core_res, frag in results:
            diag_total.merge_from(frag)
            tr = monitor_state.tracked_pairs[idx]
            tr.pm_market = pair_snap.pm_market
            if core_res is None:
                continue
            opp, pm_exp, ks_exp = core_res
            await validate_arbitrage_pair_post_success(
                tr.pm_market,
                tr.kalshi_market,
                tr.similarity,
                tr.pm_side,
                tr.kalshi_side,
                tr.needs_inversion,
                trade_amount,
                per_leg_cap_usd,
                monitor_state.current_cycle,
                "price_track",
                logger,
                tr.pair_id,
                paper,
                demo_http,
                kalshi_demo_cfg,
                demo_trading_active,
                opp,
                pm_exp,
                ks_exp,
            )
            opportunity_count += 1
            cycle_statistics.record_opportunity(opp)
            tr.last_check = datetime.now(timezone.utc)
            if opp.net_profit_100 > tr.best_profit:
                tr.best_profit = opp.net_profit_100
            opportunities.append(
                (opp, tr.pm_market.title, tr.kalshi_market.title, pm_exp, ks_exp)
            )
        print(
            "      … 并发追踪校验结束，总用时 "
            f"{(datetime.now() - tick_start).total_seconds():.1f}s"
        )

    if diag_verbose:
        diag_total.print_summary()
    else:
        diag_total.print_compact()

    _write_arb_cycle_diagnostic_csv(
        diag_total, monitor_state.current_cycle, "price_track", n_active
    )

    top10_block = format_top10_opportunities(opportunities, top10_mode_caption)
    print(top10_block, end="")

    return opportunity_count, top10_block


class CycleStats:
    def __init__(self, new_matches: int, arbitrage_opportunities: int):
        self.new_matches = new_matches
        self.arbitrage_opportunities = arbitrage_opportunities


async def run_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    matcher: MarketMatcher,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    paper: Optional[PaperEngine],
    demo_http: Optional[aiohttp.ClientSession],
    kalshi_demo_cfg: Optional[KalshiDemoConfig],
    demo_trading_active: List[bool],
) -> CycleStats:
    start_time = datetime.now()
    print(
        f"🔄 开始新周期 #{monitor_state.current_cycle} - {start_time.strftime('%H:%M:%S')}"
    )

    monitor_state.prune_tracked_beyond_resolution_horizon(datetime.now(timezone.utc))

    if paper is not None:
        paper.tick_cooldowns()

    await paper_cycle_start_close_checks(
        polymarket, kalshi, paper, monitor_state.current_cycle
    )

    use_demo_caps = demo_trading_active[0] and kalshi_demo_cfg is not None
    per_leg_cap, pair_cap = (
        paper_caps_demo() if use_demo_caps else paper_caps_local()
    )
    trade_amount = per_leg_cap
    per_leg_cap_usd = per_leg_cap
    top10_caption = (
        f"demo · 标尺 ${int(DEMO_REFERENCE_BUDGET_USD)} · 每腿 ${per_leg_cap:.2f} · 每对 ${pair_cap:.2f}"
        if use_demo_caps
        else f"local · 纸面 ${int(LOCAL_TOTAL_USD)} · 每腿 ${per_leg_cap:.2f} · 每对 ${pair_cap:.2f}"
    )
    is_full_match_cycle = monitor_state.should_full_match()

    if is_full_match_cycle and monitor_state.current_cycle > 0:
        cycle_statistics.reset_big_period_accumulator()

    if is_full_match_cycle:
        m, v, _top10, _full = await run_full_match_cycle(
            polymarket,
            kalshi,
            matcher,
            arb_detector,
            logger,
            monitor_state,
            trade_amount,
            pair_cap,
            per_leg_cap_usd,
            paper,
            top10_caption,
            demo_http,
            kalshi_demo_cfg,
            demo_trading_active,
        )
        new_matches, opportunities = m, v
    else:
        c, _top10 = await run_tracking_cycle(
            polymarket,
            kalshi,
            arb_detector,
            logger,
            monitor_state,
            trade_amount,
            pair_cap,
            per_leg_cap_usd,
            paper,
            top10_caption,
            demo_http,
            kalshi_demo_cfg,
            demo_trading_active,
        )
        new_matches, opportunities = 0, c

    elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    print(f"   ⏱️ 周期完成, 耗时: {elapsed_ms}ms")

    return CycleStats(new_matches, opportunities)


async def fetch_initial_markets(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
) -> Tuple[List[Market], List[Market]]:
    print("   📡 获取 Polymarket 市场...")
    polymarket_raw = await polymarket.fetch_all_markets()
    print("   📡 获取 Kalshi 市场...")
    kalshi_raw = await kalshi.fetch_all_markets()

    now = datetime.now(timezone.utc)
    polymarket_markets = filter_markets_by_resolution_horizon(polymarket_raw, now)
    kalshi_markets = filter_markets_by_resolution_horizon(kalshi_raw, now)

    print(
        f"      ✅ Polymarket: {len(polymarket_markets)} 个 (21d 窗口), Kalshi: {len(kalshi_markets)} 个 (21d 窗口)"
    )

    return kalshi_markets, polymarket_markets


async def main_async() -> None:
    _load_dotenv()

    kalshi_demo_cfg: Optional[KalshiDemoConfig] = None
    demo_trading_active: List[bool]

    if KALSHI_DEMO_MODE_ENABLED:
        try:
            kalshi_demo_cfg = KalshiDemoConfig.try_from_env()
        except KalshiDemoConfigError as e:
            print(f"❌ Kalshi Demo 配置错误: {e}")
            return
        if kalshi_demo_cfg is None:
            print_kalshi_demo_mode_missing_credentials()
            return
        demo_trading_active = [True]
    else:
        demo_trading_active = [False]

    print("🚀 启动跨平台套利监控系统")
    print("📊 监控平台: Polymarket ↔ Kalshi")

    logger = MonitorLogger("logs")
    unclassified_logger = UnclassifiedLogger("logs/unclassified")

    print("📚 加载类别配置...")
    category_mapper = CategoryMapper.from_file("config/categories.toml")

    polymarket = PolymarketClient()
    if kalshi_demo_cfg is not None:
        print("   📗 Kalshi Demo 已开启（system_params）：市场/订单簿使用 Demo API")
        kalshi = KalshiClient(KALSHI_DEMO_BASE_URL)
        pl, pp = paper_caps_demo()
        print("   📗 Kalshi Demo 凭证已加载：IOC 限价单走官方 Demo API")
        print(
            f"   📗 标尺 [demo]：策略总盘子 ${int(DEMO_REFERENCE_BUDGET_USD)} | "
            f"每腿探针 ${pl:.2f} | 每对名义上限 ${pp:.2f}"
        )
    else:
        print(
            "   📒 Kalshi Demo 已关闭（system_params.KALSHI_DEMO_MODE_ENABLED=False），使用生产 Trade API"
        )
        kalshi = KalshiClient()
        pl, pp = paper_caps_local()
        print(
            f"   📒 标尺 [local]：纸面总资金 ${int(LOCAL_TOTAL_USD)} | "
            f"每腿探针 ${pl:.2f} | 每对名义上限 ${pp:.2f}"
        )

    matcher_config = MarketMatcherConfig(
        similarity_threshold=SIMILARITY_THRESHOLD,
        use_date_boost=True,
        use_category_boost=True,
        date_boost_factor=0.05,
        category_boost_factor=0.03,
    )

    matcher = MarketMatcher(matcher_config, category_mapper).with_logger(unclassified_logger)
    arb_detector = ArbitrageDetector(0.02)
    if arb_tracking_diagnostics_enabled():
        print(
            "   🔎 已启用 ARB_TRACK_DIAG：价格追踪周期末打印套利判定分层统计"
        )
    print(
        f"   🚀 价格追踪并发 ARB_TRACK_CONCURRENCY={arb_track_concurrency()}（环境变量可改，1=完全串行）"
    )
    monitor_state = MonitorState(FULL_FETCH_INTERVAL, 10000)
    paper_engine = PaperEngine.try_new()

    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as demo_http:
        print("📡 首次获取市场并构建索引...")

        try:
            kalshi_markets, polymarket_markets = await fetch_initial_markets(
                polymarket, kalshi
            )
        except Exception as e:
            print(f"❌ 首次获取市场失败: {e}")
            if paper_engine is not None:
                paper_engine.write_session_end("init_fetch_failed")
            return

        matcher.fit_vectorizer(kalshi_markets, polymarket_markets)

        print("🌲 构建 Kalshi 市场索引...")
        matcher.build_kalshi_index(kalshi_markets)

        print("🌲 构建 Polymarket 市场索引...")
        matcher.build_polymarket_index(polymarket_markets)

        print(
            "\n🔍 初始化 · 全量匹配（周期 #0，沿用本次拉取与索引，不重复请求、不重训）..."
        )
        use_demo_caps = demo_trading_active[0] and kalshi_demo_cfg is not None
        per_leg_cap0, pair_cap0 = (
            paper_caps_demo() if use_demo_caps else paper_caps_local()
        )
        top10_caption_init = (
            f"demo · 标尺 ${int(DEMO_REFERENCE_BUDGET_USD)} · 每腿 ${per_leg_cap0:.2f} · 每对 ${pair_cap0:.2f}"
            if use_demo_caps
            else f"local · 纸面 ${int(LOCAL_TOTAL_USD)} · 每腿 ${per_leg_cap0:.2f} · 每对 ${pair_cap0:.2f}"
        )
        await run_full_match_work(
            polymarket,
            kalshi,
            matcher,
            arb_detector,
            logger,
            monitor_state,
            polymarket_markets,
            kalshi_markets,
            per_leg_cap0,
            pair_cap0,
            per_leg_cap0,
            paper_engine,
            top10_caption_init,
            demo_http,
            kalshi_demo_cfg,
            demo_trading_active,
            False,
        )

        print("\n✅ 初始化完成（全量匹配已作为周期 #0 完成）")
        print(f"   📊 Kalshi 市场数: {len(kalshi_markets)}")
        print(f"   📊 Polymarket 市场数: {len(polymarket_markets)}")
        print(f"   📊 Kalshi 索引大小: {matcher.kalshi_index_size()}")
        print(f"   📊 Polymarket 索引大小: {matcher.polymarket_index_size()}")
        print(
            f"   📌 监控从周期 #1 起为价格追踪；下一全量重建约在周期 #{FULL_FETCH_INTERVAL}"
        )
        monitor_state.next_cycle()

        print("\n🔄 开始监控循环 (间隔: 30秒)...\n")

        try:
            while True:
                try:
                    stats = await run_cycle(
                        polymarket,
                        kalshi,
                        matcher,
                        arb_detector,
                        logger,
                        monitor_state,
                        paper_engine,
                        demo_http,
                        kalshi_demo_cfg,
                        demo_trading_active,
                    )
                    print(
                        f"📊 周期统计: 新匹配 {stats.new_matches} 对, 套利 {stats.arbitrage_opportunities} 个, 追踪 {len(monitor_state.tracked_pairs)} 对"
                    )
                except Exception as e:
                    print(f"❌ 周期错误: {e}")

                monitor_state.next_cycle()
                print("⏳ 等待下一周期...\n")
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            print("\n🛑 监控已停止")
        finally:
            if paper_engine is not None:
                paper_engine.write_session_end("shutdown")


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main_async())
    except KeyboardInterrupt:
        print("\n🛑 用户中断")
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


if __name__ == "__main__":
    main()
