# main.py
import asyncio
import os
import signal
from datetime import datetime, timedelta
from typing import Optional, Tuple

from event import Event
from clients import PolymarketClient, KalshiClient
from category_mapper import CategoryMapper
from unclassified_logger import UnclassifiedLogger
from event_matcher import EventMatcher, EventMatcherConfig
from arbitrage_detector import ArbitrageDetector, calculate_slippage_with_fixed_usdt
from arbitrage_detector import parse_polymarket_orderbook, parse_kalshi_orderbook
from monitor_logger import MonitorLogger


class CycleStats:
    """周期统计"""

    def __init__(self, matches: int, opportunities: int):
        self.matches = matches
        self.opportunities = opportunities


async def fetch_initial_events(
        polymarket: PolymarketClient,
        kalshi: KalshiClient,
) -> Tuple[list, list]:
    """首次获取事件"""
    print("   📡 获取 Polymarket 事件...")
    pm_events = await polymarket.fetch_events()
    print(f"      ✅ 获取到 {len(pm_events)} 个事件")

    print("   📡 获取 Kalshi 事件...")
    kalshi_events = await kalshi.fetch_events()
    print(f"      ✅ 获取到 {len(kalshi_events)} 个事件")

    return kalshi_events, pm_events


async def run_cycle(
        polymarket: PolymarketClient,
        kalshi: KalshiClient,
        matcher: EventMatcher,
        arb_detector: ArbitrageDetector,
        logger: MonitorLogger,
        cycle_count: int,
) -> CycleStats:
    """运行单个监控周期"""
    start_time = datetime.now()
    print(f"🔄 开始新周期 #{cycle_count} - {start_time.strftime('%H:%M:%S')}")

    # 获取最新事件
    print("   📡 获取最新事件...")
    pm_events = await polymarket.fetch_events()
    kalshi_events = await kalshi.fetch_events()

    print(f"      Polymarket: {len(pm_events)} 个, Kalshi: {len(kalshi_events)} 个")

    # 匹配事件
    print("   🔍 匹配事件...")
    matches = matcher.find_matches_bidirectional(pm_events, kalshi_events)
    print(f"      ✅ 找到 {len(matches)} 个匹配对")

    print(f"\n📊 ====== 套利机会深度验证 ======")

    opportunity_count = 0
    verified_count = 0
    trade_amount = 100.0  # 固定交易金额 100 USDT

    for pm_event, kalshi_event, similarity in matches:
        # 获取价格
        try:
            pm_prices = await polymarket.fetch_prices(pm_event)
        except:
            continue

        kalshi_prices = await kalshi.get_market_prices(kalshi_event.event_id)
        if kalshi_prices is None:
            continue

        # 先用最优价检查潜在机会
        opportunity = arb_detector.check_arbitrage_optimal(pm_prices, kalshi_prices)
        if not opportunity:
            continue

        opportunity_count += 1

        # 确定策略对应的买卖方向
        if "Buy Yes on Kalshi" in opportunity.strategy:
            pm_side = "NO"  # 买 Polymarket NO
            kalshi_side = "YES"  # 买 Kalshi YES
        else:
            pm_side = "YES"  # 买 Polymarket YES
            kalshi_side = "NO"  # 买 Kalshi NO

        print(f"\n  📌 潜在机会 #{opportunity_count} (相似度: {similarity:.2f})")
        print(f"     PM: {pm_event.title}")
        print(f"     Kalshi: {kalshi_event.title}")
        print(f"     策略: {opportunity.strategy}")
        print()
        print(f"     📊 最优价格:")

        pm_optimal = pm_prices.yes_ask if pm_side == "YES" else pm_prices.no_ask
        kalshi_optimal = kalshi_prices.yes_ask if kalshi_side == "YES" else kalshi_prices.no_ask

        print(f"        Polymarket {pm_side}: {pm_optimal:.3f}")
        print(f"        Kalshi {kalshi_side}: {kalshi_optimal:.3f}")
        print(
            f"     💰 理想利润: ${opportunity.net_profit:.3f} | 理想成本: ${opportunity.total_cost:.3f} | ROI: {opportunity.roi_percent:.1f}%")
        print()
        print(f"     🔍 验证深度 (投入 {trade_amount} USDT)...")

        # 获取 Polymarket 订单簿
        pm_orderbook = None
        if pm_event.token_ids:
            token_id = pm_event.token_ids[0]
            ob_data = await polymarket.get_order_book(token_id)
            if ob_data:
                pm_orderbook = parse_polymarket_orderbook(ob_data, pm_side)

        # 获取 Kalshi 订单簿
        kalshi_orderbook = None
        ob_data = await kalshi.get_order_book(kalshi_event.event_id)
        if ob_data:
            kalshi_orderbook = parse_kalshi_orderbook(ob_data, kalshi_side)

        # 计算 Polymarket 滑点
        if pm_orderbook:
            pm_info = calculate_slippage_with_fixed_usdt(pm_orderbook, trade_amount)
            pm_avg = pm_info.avg_price
            pm_slip = pm_info.slippage_percent
        else:
            pm_avg = pm_optimal
            pm_slip = 0.0

        # 计算 Kalshi 滑点
        if kalshi_orderbook:
            kalshi_info = calculate_slippage_with_fixed_usdt(kalshi_orderbook, trade_amount)
            kalshi_avg = kalshi_info.avg_price
            kalshi_slip = kalshi_info.slippage_percent
        else:
            kalshi_avg = kalshi_optimal
            kalshi_slip = 0.0

        print()
        print(f"     📊 滑点分析:")
        print(f"        Polymarket {pm_side}: 最优价 {pm_optimal:.3f} → 考虑滑点平均价 {pm_avg:.3f} ({pm_slip:+.2f}%)")
        print(
            f"        Kalshi {kalshi_side}: 最优价 {kalshi_optimal:.3f} → 考虑滑点平均价 {kalshi_avg:.3f} ({kalshi_slip:+.2f}%)")

        # 用考虑了滑点的价格重新计算套利机会
        pm_adjusted = pm_prices
        kalshi_adjusted = kalshi_prices

        if pm_side == "YES":
            pm_adjusted.yes = pm_avg
        else:
            pm_adjusted.no = pm_avg

        if kalshi_side == "YES":
            kalshi_adjusted.yes = kalshi_avg
        else:
            kalshi_adjusted.no = kalshi_avg

        verified = arb_detector.check_arbitrage_optimal(pm_adjusted, kalshi_adjusted)

        if verified:
            verified_count += 1
            print()
            print(f"     ✅ 考虑滑点后仍然有机会!")
            print(
                f"        💰 实际利润: ${verified.net_profit:.3f} | 实际成本: ${verified.total_cost:.3f} | ROI: {verified.roi_percent:.1f}%")

            # 记录套利机会
            try:
                logger.log_opportunity(verified)
            except Exception as e:
                print(f"         ⚠️ 记录日志失败: {e}")
        else:
            print()
            print(f"     ❌ 考虑滑点后无套利机会")

        print(f"     ────────────────────────────────────")

    print(f"\n📊 ====== 周期统计 ======")
    print(f"   潜在机会: {opportunity_count} 个")
    print(f"   验证通过: {verified_count} 个")
    print(f"   验证失败: {opportunity_count - verified_count} 个")

    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    print(f"   ⏱️ 周期完成, 耗时: {int(elapsed)}ms")

    return CycleStats(len(matches), verified_count)


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
    if os.path.exists("config/categories.toml"):
        category_mapper = CategoryMapper.from_file("config/categories.toml")
    else:
        print("⚠️ 配置文件不存在，使用默认空映射器")
        category_mapper = CategoryMapper.default()

    # 初始化客户端 - 使用 async with 确保正确关闭
    async with PolymarketClient() as polymarket, KalshiClient() as kalshi:
        # 初始化匹配器
        matcher_config = EventMatcherConfig(
            similarity_threshold=0.5,
            use_date_boost=True,
            use_category_boost=True,
            date_boost_factor=0.05,
            category_boost_factor=0.03
        )

        matcher = EventMatcher(matcher_config, category_mapper)
        matcher = matcher.with_logger(unclassified_logger)

        # 初始化套利检测器
        arb_detector = ArbitrageDetector(0.02)

        print("📡 首次获取事件并构建双索引...")

        # 首次获取事件
        try:
            kalshi_events, pm_events = await fetch_initial_events(polymarket, kalshi)
        except Exception as e:
            print(f"❌ 首次获取事件失败: {e}")
            return

        # 先用所有事件训练统一的向量化器
        print("📚 训练统一向量化器...")
        all_events = kalshi_events + pm_events
        matcher.fit_vectorizer(all_events)

        # 构建双索引
        print("🌲 构建 Kalshi 事件索引...")
        matcher.build_kalshi_index(kalshi_events)

        print("🌲 构建 Polymarket 事件索引...")
        matcher.build_polymarket_index(pm_events)

        print(f"\n✅ 初始化完成")
        print(f"   📊 Kalshi 事件数: {len(kalshi_events)}")
        print(f"   📊 Polymarket 事件数: {len(pm_events)}")
        print(f"   📚 词汇表大小: {matcher.vectorizer.vocab_size()}")
        print(f"   📊 Kalshi 索引大小: {matcher.kalshi_index_size()}")
        print(f"   📊 Polymarket 索引大小: {matcher.polymarket_index_size()}")
        print(f"\n🔄 开始监控循环 (间隔: 30秒)...\n")

        # 主循环
        cycle_count = 0
        try:
            while True:
                cycle_count += 1

                stats = await run_cycle(
                    polymarket,
                    kalshi,
                    matcher,
                    arb_detector,
                    logger,
                    cycle_count
                )

                print(f"📊 周期统计: 匹配 {stats.matches} 对, 套利 {stats.opportunities} 个")
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