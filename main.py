# main.py
import asyncio
import os
import signal
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from market import Market
from clients import PolymarketClient, KalshiClient
from category_mapper import CategoryMapper
from unclassified_logger import UnclassifiedLogger
from market_matcher import MarketMatcher, MarketMatcherConfig
from arbitrage_detector import ArbitrageDetector, ArbitrageOpportunity
from monitor_logger import MonitorLogger
from tracking import MonitorState
from query_params import FULL_FETCH_INTERVAL, SIMILARITY_THRESHOLD
from arbitrage_detector import (
    calculate_slippage_with_fixed_usdt,
    parse_polymarket_orderbook,
    parse_kalshi_orderbook
)


class CycleStats:
    """周期统计"""

    def __init__(self, new_matches: int, arbitrage_opportunities: int):
        self.new_matches = new_matches
        self.arbitrage_opportunities = arbitrage_opportunities


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
    trade_amount: float,
    _logger: MonitorLogger,
) -> Optional[ArbitrageOpportunity]:
    """验证单个套利对（带滑点检查）"""
    # 获取价格
    try:
        pm_prices = await polymarket.fetch_prices(pm_market)
    except:
        return None

    kalshi_prices = await kalshi.get_market_prices(kalshi_market.market_id)
    if kalshi_prices is None:
        return None

    # 获取 Polymarket 订单簿
    pm_orderbook = None
    if pm_market.token_ids:
        token_id = pm_market.token_ids[0]
        ob_data = await polymarket.get_order_book(token_id)
        if ob_data:
            pm_orderbook = parse_polymarket_orderbook(ob_data, pm_side)

    # 获取 Kalshi 订单簿
    kalshi_orderbook = None
    ob_data = await kalshi.get_order_book(kalshi_market.market_id)
    if ob_data:
        kalshi_orderbook = parse_kalshi_orderbook(ob_data, kalshi_side)

    # 计算 Polymarket 滑点
    pm_optimal = pm_prices.yes_ask if pm_side == "YES" else pm_prices.no_ask
    if pm_optimal is None:
        pm_optimal = pm_prices.yes if pm_side == "YES" else pm_prices.no

    if pm_orderbook:
        pm_info = calculate_slippage_with_fixed_usdt(pm_orderbook, trade_amount)
        pm_avg = pm_info.avg_price
        pm_slip = pm_info.slippage_percent
    else:
        pm_avg = pm_optimal
        pm_slip = 0.0

    # 计算 Kalshi 滑点
    kalshi_optimal = kalshi_prices.yes_ask if kalshi_side == "YES" else kalshi_prices.no_ask
    if kalshi_optimal is None:
        kalshi_optimal = kalshi_prices.yes if kalshi_side == "YES" else kalshi_prices.no

    if kalshi_orderbook:
        kalshi_info = calculate_slippage_with_fixed_usdt(kalshi_orderbook, trade_amount)
        kalshi_avg = kalshi_info.avg_price
        kalshi_slip = kalshi_info.slippage_percent
    else:
        kalshi_avg = kalshi_optimal
        kalshi_slip = 0.0

    # 计算最终利润
    verified = arb_detector.calculate_arbitrage_with_direction(
        pm_prices,
        kalshi_prices,
        pm_side,
        kalshi_side,
        needs_inversion,
    )

    if verified is None:
        return None

    # 输出验证结果（包含滑点信息）
    inversion_note = " (Y/N颠倒)" if needs_inversion else ""

    print(f"\n  📌 验证通过 (相似度: {similarity:.3f}){inversion_note}")
    print(f"     PM: {pm_market.title}")
    print(f"     Kalshi: {kalshi_market.title}")
    print()
    print(f"     📊 策略方向:")
    print(f"        Polymarket {pm_side}: 买 {pm_side}")
    print(f"        Kalshi {kalshi_side}: 买 {kalshi_side}")
    print()
    print(f"     📊 滑点分析:")
    print(f"        Polymarket {pm_side}: 最优价 {pm_optimal:.3f} → 考虑滑点平均价 {pm_avg:.3f} ({pm_slip:+.2f}%)")
    print(f"        Kalshi {kalshi_side}: 最优价 {kalshi_optimal:.3f} → 考虑滑点平均价 {kalshi_avg:.3f} ({kalshi_slip:+.2f}%)")
    print()
    print(f"     💰 利润计算:")
    print(f"        理想利润: ${verified.net_profit + verified.fees + verified.gas_fee:.3f}")
    print(f"        - 滑点影响: ${(pm_avg - pm_optimal) + (kalshi_avg - kalshi_optimal):.3f}")
    print(f"        - 手续费: ${verified.fees:.3f}")
    print(f"        - Gas费: ${verified.gas_fee:.3f}")
    print(f"        = 最终净利润: ${verified.final_profit:.3f}")
    print(f"        ROI: {verified.final_roi_percent:.1f}%")
    print(f"     ------------------------------------")

    return verified


async def run_full_match_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    matcher: MarketMatcher,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    trade_amount: float,
) -> Tuple[int, int]:
    """执行全量匹配周期"""
    print(f"   📡 执行全量匹配...")

    # 获取全量市场
    polymarket_markets = await polymarket.fetch_all_markets()
    kalshi_markets = await kalshi.fetch_all_markets()

    print(f"      Polymarket: {len(polymarket_markets)} 个市场, Kalshi: {len(kalshi_markets)} 个市场")

    print(f"\n   🔄 重建索引...")
    matcher.build_kalshi_index(kalshi_markets)
    matcher.build_polymarket_index(polymarket_markets)

    print(f"   🔍 匹配市场...")
    matches = await matcher.find_matches_bidirectional(polymarket_markets, kalshi_markets)
    print(f"      ✅ 找到 {len(matches)} 个匹配对")

    # 所有匹配对都加入追踪列表（带方向信息）
    all_matches = []
    for (pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion) in matches:
        all_matches.append((
            pm_market,
            kalshi_market,
            similarity,
            pm_side,
            kalshi_side,
            needs_inversion
        ))

    # 验证每个匹配对，统计有套利机会的
    verified_count = 0

    for (pm_market, kalshi_market, similarity, pm_side, kalshi_side, needs_inversion) in matches:
        verified = await validate_arbitrage_pair(
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
            logger
        )
        if verified:
            verified_count += 1
            try:
                logger.log_opportunity(verified)
            except Exception as e:
                print(f"         ⚠️ 记录日志失败: {e}")

    # 更新追踪列表（所有匹配对，带方向信息）
    monitor_state.update_tracked_pairs(all_matches)

    return len(matches), verified_count


async def run_tracking_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
    trade_amount: float,
) -> int:
    """执行价格追踪周期"""
    print(f"   📡 执行价格追踪...")
    print(f"      追踪 {len(monitor_state.tracked_pairs)} 个匹配对")

    opportunity_count = 0

    for pair in monitor_state.tracked_pairs:
        if not pair.active:
            continue

        verified = await validate_arbitrage_pair(
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
            logger
        )
        if verified:
            opportunity_count += 1
            pair.last_check = datetime.now()
            if verified.final_profit > pair.best_profit:
                pair.best_profit = verified.final_profit

            try:
                logger.log_opportunity(verified)
            except Exception as e:
                print(f"         ⚠️ 记录日志失败: {e}")

    return opportunity_count


async def run_cycle(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
    matcher: MarketMatcher,
    arb_detector: ArbitrageDetector,
    logger: MonitorLogger,
    monitor_state: MonitorState,
) -> CycleStats:
    """运行单个监控周期"""
    start_time = datetime.now()
    print(f"🔄 开始新周期 #{monitor_state.current_cycle} - {start_time.strftime('%H:%M:%S')}")

    trade_amount = 100.0

    if monitor_state.should_full_match():
        # 全量匹配周期
        new_matches, opportunities = await run_full_match_cycle(
            polymarket, kalshi, matcher, arb_detector, logger,
            monitor_state, trade_amount
        )
    else:
        # 价格追踪周期
        opportunities = await run_tracking_cycle(
            polymarket, kalshi, arb_detector, logger,
            monitor_state, trade_amount
        )
        new_matches = 0

    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    print(f"   ⏱️ 周期完成, 耗时: {int(elapsed)}ms")

    return CycleStats(new_matches, opportunities)


async def fetch_initial_markets(
    polymarket: PolymarketClient,
    kalshi: KalshiClient,
) -> Tuple[List[Market], List[Market]]:
    """首次获取市场"""
    print(f"   📡 获取 Polymarket 市场...")
    polymarket_markets = await polymarket.fetch_all_markets()
    print(f"      ✅ 获取到 {len(polymarket_markets)} 个市场")

    print(f"   📡 获取 Kalshi 市场...")
    kalshi_markets = await kalshi.fetch_all_markets()
    print(f"      ✅ 获取到 {len(kalshi_markets)} 个市场")

    return kalshi_markets, polymarket_markets


async def main_async():
    """异步主函数"""
    print("🚀 启动跨平台套利监控系统")
    print("📊 监控平台: Polymarket ↔ Kalshi")

    # 初始化日志
    logger = MonitorLogger("logs")

    # 初始化未分类日志器
    unclassified_logger = UnclassifiedLogger("logs/unclassified")

    # 初始化类别映射器
    print("📚 加载类别配置...")
    category_mapper = CategoryMapper.from_file("config/categories.toml")

    # 初始化客户端
    polymarket = PolymarketClient()
    kalshi = KalshiClient()

    # 初始化匹配器
    matcher_config = MarketMatcherConfig(
        similarity_threshold=SIMILARITY_THRESHOLD,
        use_date_boost=True,
        use_category_boost=True,
        date_boost_factor=0.05,
        category_boost_factor=0.03,
    )

    matcher = MarketMatcher(matcher_config, category_mapper)
    matcher = matcher.with_logger(unclassified_logger)

    # 初始化套利检测器
    arb_detector = ArbitrageDetector(0.02)

    # 初始化监控状态
    monitor_state = MonitorState(FULL_FETCH_INTERVAL, 10000)

    print("📡 首次获取市场并构建索引...")

    # 首次获取全量市场
    try:
        kalshi_markets, polymarket_markets = await fetch_initial_markets(polymarket, kalshi)
    except Exception as e:
        print(f"❌ 首次获取市场失败: {e}")
        return

    # 按类别训练向量化器
    matcher.fit_vectorizer(kalshi_markets, polymarket_markets)

    # 构建双索引
    print("🌲 构建 Kalshi 市场索引...")
    matcher.build_kalshi_index(kalshi_markets)

    print("🌲 构建 Polymarket 市场索引...")
    matcher.build_polymarket_index(polymarket_markets)

    print(f"\n✅ 初始化完成")
    print(f"   📊 Kalshi 市场数: {len(kalshi_markets)}")
    print(f"   📊 Polymarket 市场数: {len(polymarket_markets)}")
    print(f"   📊 Kalshi 索引大小: {matcher.kalshi_index_size()}")
    print(f"   📊 Polymarket 索引大小: {matcher.polymarket_index_size()}")
    print(f"\n🔄 开始监控循环 (间隔: 30秒)...\n")

    # 主循环
    try:
        while True:
            stats = await run_cycle(
                polymarket,
                kalshi,
                matcher,
                arb_detector,
                logger,
                monitor_state,
            )

            print(f"📊 周期统计: 新匹配 {stats.new_matches} 对, 套利 {stats.arbitrage_opportunities} 个, 追踪 {len(monitor_state.tracked_pairs)} 对")

            monitor_state.next_cycle()
            print("⏳ 等待下一周期...\n")
            await asyncio.sleep(30)
    except asyncio.CancelledError:
        print("\n🛑 监控已停止")


def main():
    """主函数入口"""
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