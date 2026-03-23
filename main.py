# main.py — 与 Rust `main.rs` 行为/输出对齐
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from market import Market
from clients import PolymarketClient, KalshiClient
from category_mapper import CategoryMapper
from unclassified_logger import UnclassifiedLogger
from market_matcher import MarketMatcher, MarketMatcherConfig
from arbitrage_detector import (
    ArbitrageDetector,
    ArbitrageOpportunity,
    orderbook_best_ask_price,
    parse_kalshi_orderbook,
    parse_polymarket_orderbook,
)
from monitor_logger import MonitorLogger
from tracking import MonitorState
from query_params import FULL_FETCH_INTERVAL, SIMILARITY_THRESHOLD
import cycle_statistics
import cycle_report

OpportunityRow = Tuple[ArbitrageOpportunity, str, str, Optional[datetime], Optional[datetime]]


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
    _logger: MonitorLogger,
) -> Optional[Tuple[ArbitrageOpportunity, Optional[datetime], Optional[datetime]]]:
    pm_orderbook_vec: List[Tuple[float, float]] = []
    if pm_market.token_ids:
        tid = pm_market.token_ids[0]
        ob_data = await polymarket.get_order_book(tid)
        if ob_data:
            parsed = parse_polymarket_orderbook(ob_data, pm_side)
            if parsed:
                pm_orderbook_vec = parsed
    if not pm_orderbook_vec:
        return None

    kalshi_orderbook_vec: List[Tuple[float, float]] = []
    ob_ks = await kalshi.get_order_book(kalshi_market.market_id)
    if ob_ks:
        parsed_ks = parse_kalshi_orderbook(ob_ks, kalshi_side)
        if parsed_ks:
            kalshi_orderbook_vec = parsed_ks
    if not kalshi_orderbook_vec:
        return None

    pm_opt = orderbook_best_ask_price(pm_orderbook_vec)
    ks_opt = orderbook_best_ask_price(kalshi_orderbook_vec)
    if pm_opt is None or ks_opt is None:
        return None

    opp = arb_detector.calculate_arbitrage_100usdt(
        pm_opt,
        ks_opt,
        pm_orderbook_vec,
        kalshi_orderbook_vec,
        pm_side,
        kalshi_side,
        needs_inversion,
        capital_usdt,
    )
    if opp is None:
        return None

    inv = " (Y/N颠倒)" if needs_inversion else ""
    pm_slip = ((opp.pm_avg_slipped - opp.pm_optimal) / opp.pm_optimal * 100.0) if opp.pm_optimal > 0.0 else 0.0
    ks_slip = (
        (opp.kalshi_avg_slipped - opp.kalshi_optimal) / opp.kalshi_optimal * 100.0
    ) if opp.kalshi_optimal > 0.0 else 0.0

    pm_expiry = pm_market.resolution_date
    if pm_expiry is None:
        pm_expiry = await polymarket.fetch_resolution_by_market_id(pm_market.market_id)

    ks_expiry = kalshi_market.resolution_date
    if ks_expiry is None:
        ks_expiry = await kalshi.fetch_resolution_by_ticker(kalshi_market.market_id)

    print(f"\n  📌 验证通过 (相似度: {similarity:.3f}){inv}")
    print(f"     PM:      {pm_market.title}")
    print(f"     Kalshi:  {kalshi_market.title}")
    print(f"     📅 {format_resolution_expiry('PM', pm_expiry)}")
    print(f"     📅 {format_resolution_expiry('Kalshi', ks_expiry)}")
    print("     ─────────────────────────────────────────────────────────")
    print(f"     📊 策略: Polymarket 买 {pm_side}  +  Kalshi 买 {kalshi_side}")
    print("     ─────────────────────────────────────────────────────────")
    print(f"     📊 对冲份数 n: {opp.contracts:.4f}")
    print(f"     💵 最优 Ask:     PM {opp.pm_optimal:.4f}  |  Kalshi {opp.kalshi_optimal:.4f}")
    print(
        f"     📉 滑点后均价:   PM {opp.pm_avg_slipped:.4f} ({pm_slip:+.2f}%)  |  Kalshi {opp.kalshi_avg_slipped:.4f} ({ks_slip:+.2f}%)"
    )
    print("     ─────────────────────────────────────────────────────────")
    print(f"     💰 投入 {int(capital_usdt)} USDT 利润拆解:")
    print(f"        毛利润(兑付): ${opp.contracts:.2f}")
    print(f"        - 成本:        ${opp.capital_used:.2f}")
    print(f"        - 手续费:      ${opp.fees_amount:.2f}")
    print(f"        - Gas费:       ${opp.gas_amount:.2f}")
    print(f"        = 净利润:      ${opp.net_profit_100:.2f}")
    print(f"        ROI:           {opp.roi_100_percent:.1f}%")
    print("     ─────────────────────────────────────────────────────────")

    return (opp, pm_expiry, ks_expiry)


def format_top10_opportunities(opportunities: List[OpportunityRow]) -> str:
    lines: List[str] = []
    lines.append("")
    if not opportunities:
        lines.append("🏆 本周期利润 Top 10: 无套利机会")
        return "\n".join(lines) + "\n"

    sorted_rows = sorted(opportunities, key=lambda r: r[0].net_profit_100, reverse=True)
    lines.append("╔══════════════════════════════════════════════════════════════════════╗")
    lines.append("║  🏆 本周期利润 Top 10（100 USDT 本金，含滑点/手续费/Gas）                 ║")
    lines.append("╚══════════════════════════════════════════════════════════════════════╝")

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
        lines.append("   │  ─────────────────────────────────────────────────────────────")
        lines.append(f"   │  📗 PM 订单簿(买{opp.polymarket_action[1]}) Top5:")
        if opp.orderbook_pm_top5:
            for j, (p, s) in enumerate(opp.orderbook_pm_top5[:5]):
                lines.append(f"   │      #{j + 1}. 价 {p:.4f} 量 {s:.1f}")
        else:
            lines.append("   │      (无订单簿)")
        lines.append(f"   │  📗 Kalshi 订单簿(买{opp.kalshi_action[1]}) Top5:")
        if opp.orderbook_kalshi_top5:
            for j, (p, s) in enumerate(opp.orderbook_kalshi_top5[:5]):
                lines.append(f"   │      #{j + 1}. 价 {p:.4f} 量 {s:.1f}")
        else:
            lines.append("   │      (无订单簿)")
        lines.append("   └────────────────────────────────────────────────────────────────")

    lines.append("")
    return "\n".join(lines)


async def run_full_match_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    matcher: MarketMatcher,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    trade_amount: float,
) -> Tuple[int, int, str, str]:
    print("   📡 执行全量匹配...")

    polymarket_markets = await polymarket.fetch_all_markets()
    kalshi_markets = await kalshi.fetch_all_markets()

    print(f"      Polymarket: {len(polymarket_markets)} 个市场, Kalshi: {len(kalshi_markets)} 个市场")

    print("\n   🔄 重建索引...")
    matcher.fit_vectorizer(kalshi_markets, polymarket_markets)
    matcher.build_kalshi_index(kalshi_markets)
    matcher.build_polymarket_index(polymarket_markets)

    print("   🔍 匹配市场...")
    matches = await matcher.find_matches_bidirectional(polymarket_markets, kalshi_markets)
    print(f"      ✅ 找到 {len(matches)} 个匹配对")

    all_matches: List[Tuple[Market, Market, float, str, str, bool]] = []
    for pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion in matches:
        all_matches.append(
            (pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion)
        )

    verified_count = 0
    opportunities: List[OpportunityRow] = []

    for pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion in matches:
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
            logger,
        )
        if validated:
            verified, pm_exp, ks_exp = validated
            verified_count += 1
            cycle_statistics.record_opportunity(verified)
            opportunities.append(
                (verified, pm_market.title, kalshi_market.title, pm_exp, ks_exp)
            )
            try:
                logger.log_opportunity(verified)
            except Exception as e:
                print(f"         ⚠️ 记录日志失败: {e}")

    top10_block = format_top10_opportunities(opportunities)
    print(top10_block, end="")
    full_cycle_block = cycle_statistics.on_full_cycle_completed(opportunities)

    monitor_state.update_tracked_pairs(all_matches)

    return len(matches), verified_count, top10_block, full_cycle_block


async def run_tracking_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    trade_amount: float,
) -> Tuple[int, str]:
    print("   📡 执行价格追踪...")
    print(f"      追踪 {len(monitor_state.tracked_pairs)} 个匹配对")

    await polymarket.clear_price_cache()
    await kalshi.clear_price_cache()

    opportunity_count = 0
    opportunities: List[OpportunityRow] = []

    for pair in monitor_state.tracked_pairs:
        if not pair.active:
            continue

        try:
            fresh_pm = await polymarket.fetch_market_snapshot_by_id(pair.pm_market.market_id)
            pair.pm_market.outcome_prices = fresh_pm.outcome_prices or pair.pm_market.outcome_prices
            pair.pm_market.best_ask = fresh_pm.best_ask if fresh_pm.best_ask is not None else pair.pm_market.best_ask
            pair.pm_market.best_bid = fresh_pm.best_bid if fresh_pm.best_bid is not None else pair.pm_market.best_bid
            pair.pm_market.last_trade_price = (
                fresh_pm.last_trade_price
                if fresh_pm.last_trade_price is not None
                else pair.pm_market.last_trade_price
            )
            pair.pm_market.volume_24h = fresh_pm.volume_24h
            if fresh_pm.token_ids:
                pair.pm_market.token_ids = fresh_pm.token_ids
            if pair.pm_market.resolution_date is None:
                pair.pm_market.resolution_date = fresh_pm.resolution_date
        except Exception:
            pass

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
            logger,
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
            try:
                logger.log_opportunity(verified)
            except Exception as e:
                print(f"         ⚠️ 记录日志失败: {e}")

    top10_block = format_top10_opportunities(opportunities)
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
) -> CycleStats:
    start_time = datetime.now()
    print(
        f"🔄 开始新周期 #{monitor_state.current_cycle} - {start_time.strftime('%H:%M:%S')}"
    )

    trade_amount = 100.0
    is_full_match_cycle = monitor_state.should_full_match()

    big_period_preamble = ""
    if is_full_match_cycle and monitor_state.current_cycle > 0:
        big_period_preamble = cycle_statistics.flush_big_period_report_at_boundary(
            monitor_state.current_cycle,
            monitor_state.full_match_interval,
        )
        print(big_period_preamble, end="")

    if is_full_match_cycle:
        m, v, top10, full = await run_full_match_cycle(
            polymarket,
            kalshi,
            matcher,
            arb_detector,
            logger,
            monitor_state,
            trade_amount,
        )
        new_matches, opportunities, report_body, full_roi_block = m, v, top10, full
    else:
        c, top10 = await run_tracking_cycle(
            polymarket,
            kalshi,
            arb_detector,
            logger,
            monitor_state,
            trade_amount,
        )
        new_matches, opportunities, report_body, full_roi_block = 0, c, top10, None

    elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    print(f"   ⏱️ 周期完成, 耗时: {elapsed_ms}ms")

    footer = (
        f"   ⏱️ 周期完成, 耗时: {elapsed_ms}ms\n"
        f"📊 周期统计: 新匹配 {new_matches} 对, 套利 {opportunities} 个, 追踪 {len(monitor_state.tracked_pairs)} 对\n"
    )
    if full_roi_block:
        report_body = report_body + full_roi_block
    report_body = report_body + footer

    if big_period_preamble:
        report_body = big_period_preamble + report_body

    header = (
        f"======== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 周期 #{monitor_state.current_cycle} | "
        f"{'全量匹配' if is_full_match_cycle else '价格追踪'} ========"
    )
    try:
        cycle_report.append_cycle_report(header, report_body)
    except Exception as e:
        print(f"⚠️ 写入周期报告文件失败: {e}")

    return CycleStats(new_matches, opportunities)


async def fetch_initial_markets(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
) -> Tuple[List[Market], List[Market]]:
    print("   📡 获取 Polymarket 市场...")
    polymarket_markets = await polymarket.fetch_all_markets()
    print(f"      ✅ 获取到 {len(polymarket_markets)} 个市场")

    print("   📡 获取 Kalshi 市场...")
    kalshi_markets = await kalshi.fetch_all_markets()
    print(f"      ✅ 获取到 {len(kalshi_markets)} 个市场")

    return kalshi_markets, polymarket_markets


async def main_async() -> None:
    print("🚀 启动跨平台套利监控系统")
    print("📊 监控平台: Polymarket ↔ Kalshi")

    logger = MonitorLogger("logs")
    unclassified_logger = UnclassifiedLogger("logs/unclassified")

    print("📚 加载类别配置...")
    category_mapper = CategoryMapper.from_file("config/categories.toml")

    polymarket = PolymarketClient()
    kalshi = KalshiClient()

    matcher_config = MarketMatcherConfig(
        similarity_threshold=SIMILARITY_THRESHOLD,
        use_date_boost=True,
        use_category_boost=True,
        date_boost_factor=0.05,
        category_boost_factor=0.03,
    )

    matcher = MarketMatcher(matcher_config, category_mapper).with_logger(unclassified_logger)
    arb_detector = ArbitrageDetector(0.02)
    monitor_state = MonitorState(FULL_FETCH_INTERVAL, 10000)

    print("📡 首次获取市场并构建索引...")

    try:
        kalshi_markets, polymarket_markets = await fetch_initial_markets(polymarket, kalshi)
    except Exception as e:
        print(f"❌ 首次获取市场失败: {e}")
        return

    matcher.fit_vectorizer(kalshi_markets, polymarket_markets)

    print("🌲 构建 Kalshi 市场索引...")
    matcher.build_kalshi_index(kalshi_markets)

    print("🌲 构建 Polymarket 市场索引...")
    matcher.build_polymarket_index(polymarket_markets)

    print("\n✅ 初始化完成")
    print(f"   📊 Kalshi 市场数: {len(kalshi_markets)}")
    print(f"   📊 Polymarket 市场数: {len(polymarket_markets)}")
    print(f"   📊 Kalshi 索引大小: {matcher.kalshi_index_size()}")
    print(f"   📊 Polymarket 索引大小: {matcher.polymarket_index_size()}")
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
