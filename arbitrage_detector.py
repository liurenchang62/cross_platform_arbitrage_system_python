# arbitrage_detector.py
# 固定本金订单簿探针、精确 n 份成本、两腿 Gas、双边 ask/bid 阶梯 walk。
import math
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple, Union

from market import Market, MarketPrices

# Gas 费配置（固定值，单位 USDT）— 每笔交易 $0.02，两腿共 $0.04
GAS_FEE_PER_TX: float = 0.02
# 兼容旧名：两腿总 Gas（单笔 ×2）
GAS_FEE: float = GAS_FEE_PER_TX * 2.0


@dataclass
class ArbitrageOpportunity:
    """单边深度 walk 后的套利机会数值摘要。"""
    strategy: str
    kalshi_action: Tuple[str, str, float]
    polymarket_action: Tuple[str, str, float]
    total_cost: float
    gross_profit: float
    fees: float
    net_profit: float
    roi_percent: float
    gas_fee: float
    final_profit: float
    final_roi_percent: float
    # 100 USDT 本金模式 / 订单簿展示
    pm_optimal: float = 0.0
    kalshi_optimal: float = 0.0
    pm_avg_slipped: float = 0.0
    kalshi_avg_slipped: float = 0.0
    contracts: float = 0.0
    capital_used: float = 0.0
    fees_amount: float = 0.0
    gas_amount: float = 0.0
    net_profit_100: float = 0.0
    roi_100_percent: float = 0.0
    orderbook_pm_top5: List[Tuple[float, float]] = field(default_factory=list)
    orderbook_kalshi_top5: List[Tuple[float, float]] = field(default_factory=list)


@dataclass
class Fees:
    polymarket: float = 0.01
    kalshi: float = 0.01


@dataclass
class SlippageInfo:
    avg_price: float
    slippage_percent: float
    filled: bool
    filled_amount: float
    filled_contracts: float


class ArbitrageDetector:
    def __init__(self, min_profit_threshold: float = 0.02):
        self.min_profit_threshold = min_profit_threshold
        self.fees = Fees()

    def with_fees(self, polymarket_fee: float, kalshi_fee: float) -> "ArbitrageDetector":
        self.fees.polymarket = polymarket_fee
        self.fees.kalshi = kalshi_fee
        return self

    def calculate_arbitrage_100usdt(
        self,
        pm_optimal: float,
        kalshi_optimal: float,
        pm_orderbook: Optional[List[Tuple[float, float]]],
        kalshi_orderbook: Optional[List[Tuple[float, float]]],
        pm_side: str,
        kalshi_side: str,
        needs_inversion: bool,
        capital_usdt: float,
        pair_cap_usdt: float = 0.0,
    ) -> Optional[ArbitrageOpportunity]:
        """按固定单腿本金 cap 与每对名义上限计算可对冲规模与净利。"""
        total_cost_opt = pm_optimal + kalshi_optimal
        if total_cost_opt >= 1.0 or total_cost_opt <= 0.0:
            return None
        if capital_usdt <= 0.0:
            return None

        if pm_orderbook is not None:
            if not pm_orderbook:
                return None
            contracts_pm = calculate_slippage_with_fixed_usdt(pm_orderbook, capital_usdt).filled_contracts
        elif pm_optimal > 0.0:
            contracts_pm = capital_usdt / pm_optimal
        else:
            return None

        if kalshi_orderbook is not None:
            if not kalshi_orderbook:
                return None
            contracts_ks = calculate_slippage_with_fixed_usdt(kalshi_orderbook, capital_usdt).filled_contracts
        elif kalshi_optimal > 0.0:
            contracts_ks = capital_usdt / kalshi_optimal
        else:
            return None

        if contracts_pm > 0.0 and contracts_ks > 0.0:
            n = min(contracts_pm, contracts_ks)
        else:
            return None

        if pm_orderbook is not None:
            cpm = cost_for_exact_contracts(pm_orderbook, n)
            if cpm is None:
                return None
            c_pm, pm_avg = cpm
        else:
            c_pm = n * pm_optimal
            pm_avg = pm_optimal

        if kalshi_orderbook is not None:
            cks = cost_for_exact_contracts(kalshi_orderbook, n)
            if cks is None:
                return None
            c_ks, kalshi_avg = cks
        else:
            c_ks = n * kalshi_optimal
            kalshi_avg = kalshi_optimal

        capital_used = c_pm + c_ks
        if pair_cap_usdt > 0.0 and capital_used > pair_cap_usdt:
            scale = pair_cap_usdt / capital_used
            n *= scale
            if n <= 0.0 or not math.isfinite(n):
                return None
            if pm_orderbook is not None:
                cpm = cost_for_exact_contracts(pm_orderbook, n)
                if cpm is None:
                    return None
                c_pm, pm_avg = cpm
            else:
                c_pm = n * pm_optimal
                pm_avg = pm_optimal
            if kalshi_orderbook is not None:
                cks = cost_for_exact_contracts(kalshi_orderbook, n)
                if cks is None:
                    return None
                c_ks, kalshi_avg = cks
            else:
                c_ks = n * kalshi_optimal
                kalshi_avg = kalshi_optimal
            capital_used = c_pm + c_ks

        gross = n * 1.0
        fees_amount = c_pm * self.fees.polymarket + c_ks * self.fees.kalshi
        gas_amount = GAS_FEE_PER_TX * 2.0
        net_profit_100 = gross - capital_used - fees_amount - gas_amount

        if net_profit_100 <= self.min_profit_threshold:
            return None
        roi_100 = (net_profit_100 / capital_used) * 100.0 if capital_used > 0.0 else 0.0

        inversion_note = " [Y/N颠倒]" if needs_inversion else ""
        strategy = f"Buy {pm_side} on Polymarket + Buy {kalshi_side} on Kalshi{inversion_note}"

        orderbook_pm_top5 = list(pm_orderbook[:5]) if pm_orderbook else []
        orderbook_kalshi_top5 = list(kalshi_orderbook[:5]) if kalshi_orderbook else []

        return ArbitrageOpportunity(
            strategy=strategy,
            kalshi_action=("BUY", kalshi_side, kalshi_optimal),
            polymarket_action=("BUY", pm_side, pm_optimal),
            total_cost=total_cost_opt,
            gross_profit=1.0 - total_cost_opt,
            fees=self.fees.polymarket + self.fees.kalshi,
            net_profit=(1.0 - total_cost_opt) - (self.fees.polymarket + self.fees.kalshi),
            roi_percent=roi_100,
            gas_fee=gas_amount,
            final_profit=net_profit_100,
            final_roi_percent=roi_100,
            pm_optimal=pm_optimal,
            kalshi_optimal=kalshi_optimal,
            pm_avg_slipped=pm_avg,
            kalshi_avg_slipped=kalshi_avg,
            contracts=n,
            capital_used=capital_used,
            fees_amount=fees_amount,
            gas_amount=gas_amount,
            net_profit_100=net_profit_100,
            roi_100_percent=roi_100,
            orderbook_pm_top5=orderbook_pm_top5,
            orderbook_kalshi_top5=orderbook_kalshi_top5,
        )

    def calculate_arbitrage_with_direction(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
        pm_side: str,
        kalshi_side: str,
        needs_inversion: bool,
    ) -> Optional[ArbitrageOpportunity]:
        if pm_side == "YES":
            pm_action = "BUY"
            pm_price = pm_prices.yes_ask if pm_prices.yes_ask is not None else pm_prices.yes
        else:
            pm_action = "BUY"
            pm_price = pm_prices.no_ask if pm_prices.no_ask is not None else pm_prices.no

        if kalshi_side == "YES":
            kalshi_action = "BUY"
            kalshi_price = kalshi_prices.yes_ask if kalshi_prices.yes_ask is not None else kalshi_prices.yes
        else:
            kalshi_action = "BUY"
            kalshi_price = kalshi_prices.no_ask if kalshi_prices.no_ask is not None else kalshi_prices.no

        total_cost = pm_price + kalshi_price
        profit = 1.0 - total_cost
        total_fees = self.fees.polymarket + self.fees.kalshi

        if profit <= total_fees + self.min_profit_threshold:
            return None

        net_profit = profit - total_fees
        gas_total = GAS_FEE_PER_TX * 2.0
        final_profit = net_profit - gas_total

        if final_profit <= self.min_profit_threshold:
            return None

        roi = (final_profit / total_cost) * 100.0 if total_cost > 0 else 0.0

        inversion_note = " [Y/N颠倒]" if needs_inversion else ""
        strategy = f"Buy {pm_side} on Polymarket + Buy {kalshi_side} on Kalshi{inversion_note}"

        return ArbitrageOpportunity(
            strategy=strategy,
            kalshi_action=(kalshi_action, kalshi_side, kalshi_price),
            polymarket_action=(pm_action, pm_side, pm_price),
            total_cost=total_cost,
            gross_profit=profit,
            fees=total_fees,
            net_profit=net_profit,
            roi_percent=roi,
            gas_fee=gas_total,
            final_profit=final_profit,
            final_roi_percent=roi,
            pm_optimal=pm_price,
            kalshi_optimal=kalshi_price,
            pm_avg_slipped=pm_price,
            kalshi_avg_slipped=kalshi_price,
        )

    def calculate_final_profit(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
        pm_slippage: float,
        kalshi_slippage: float,
    ) -> Optional[ArbitrageOpportunity]:
        opportunity = self.check_arbitrage_optimal(pm_prices, kalshi_prices)
        if opportunity is None:
            return None

        if "Buy Yes on Polymarket" in opportunity.strategy:
            pm_slipped = pm_prices.yes * (1.0 + pm_slippage / 100.0)
        else:
            pm_slipped = pm_prices.no * (1.0 + pm_slippage / 100.0)

        if "Buy Yes on Kalshi" in opportunity.strategy:
            kalshi_slipped = kalshi_prices.yes * (1.0 + kalshi_slippage / 100.0)
        else:
            kalshi_slipped = kalshi_prices.no * (1.0 + kalshi_slippage / 100.0)

        slipped_cost = pm_slipped + kalshi_slipped
        slipped_profit = 1.0 - slipped_cost

        if slipped_profit <= 0.0:
            return None

        total_fees = self.fees.polymarket + self.fees.kalshi
        net_profit = slipped_profit - total_fees
        gas_total = GAS_FEE_PER_TX * 2.0
        final_profit = net_profit - gas_total

        if final_profit <= self.min_profit_threshold:
            return None

        roi = (final_profit / slipped_cost) * 100.0 if slipped_cost > 0 else 0.0

        return ArbitrageOpportunity(
            strategy=opportunity.strategy,
            kalshi_action=opportunity.kalshi_action,
            polymarket_action=opportunity.polymarket_action,
            total_cost=slipped_cost,
            gross_profit=slipped_profit,
            fees=total_fees,
            net_profit=net_profit,
            roi_percent=(net_profit / slipped_cost) * 100.0 if slipped_cost > 0 else 0.0,
            gas_fee=gas_total,
            final_profit=final_profit,
            final_roi_percent=roi,
            pm_optimal=opportunity.pm_optimal,
            kalshi_optimal=opportunity.kalshi_optimal,
            pm_avg_slipped=pm_slipped,
            kalshi_avg_slipped=kalshi_slipped,
        )

    def check_arbitrage_optimal(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
    ) -> Optional[ArbitrageOpportunity]:
        if kalshi_prices.yes == 0.0 and kalshi_prices.no == 0.0:
            return None
        if not pm_prices.validate() or not kalshi_prices.validate():
            return None

        if pm_prices.yes_ask is None or pm_prices.no_ask is None:
            return None
        if kalshi_prices.yes_ask is None or kalshi_prices.no_ask is None:
            return None

        pm_yes_ask = pm_prices.yes_ask
        pm_no_ask = pm_prices.no_ask
        kalshi_yes_ask = kalshi_prices.yes_ask
        kalshi_no_ask = kalshi_prices.no_ask

        cost_strategy_1 = kalshi_yes_ask + pm_no_ask
        profit_strategy_1 = 1.0 - cost_strategy_1

        cost_strategy_2 = kalshi_no_ask + pm_yes_ask
        profit_strategy_2 = 1.0 - cost_strategy_2

        total_fees = self.fees.polymarket + self.fees.kalshi

        if profit_strategy_1 > total_fees + self.min_profit_threshold:
            net_profit = profit_strategy_1 - total_fees
            roi = (net_profit / cost_strategy_1) * 100.0 if cost_strategy_1 > 0 else 0.0
            return ArbitrageOpportunity(
                strategy="Buy Yes on Kalshi + Buy No on Polymarket",
                kalshi_action=("BUY", "YES", kalshi_yes_ask),
                polymarket_action=("BUY", "NO", pm_no_ask),
                total_cost=cost_strategy_1,
                gross_profit=profit_strategy_1,
                fees=total_fees,
                net_profit=net_profit,
                roi_percent=roi,
                gas_fee=0.0,
                final_profit=0.0,
                final_roi_percent=0.0,
                pm_optimal=pm_no_ask,
                kalshi_optimal=kalshi_yes_ask,
                pm_avg_slipped=pm_no_ask,
                kalshi_avg_slipped=kalshi_yes_ask,
            )

        if profit_strategy_2 > total_fees + self.min_profit_threshold:
            net_profit = profit_strategy_2 - total_fees
            roi = (net_profit / cost_strategy_2) * 100.0 if cost_strategy_2 > 0 else 0.0
            return ArbitrageOpportunity(
                strategy="Buy No on Kalshi + Buy Yes on Polymarket",
                kalshi_action=("BUY", "NO", kalshi_no_ask),
                polymarket_action=("BUY", "YES", pm_yes_ask),
                total_cost=cost_strategy_2,
                gross_profit=profit_strategy_2,
                fees=total_fees,
                net_profit=net_profit,
                roi_percent=roi,
                gas_fee=0.0,
                final_profit=0.0,
                final_roi_percent=0.0,
                pm_optimal=pm_yes_ask,
                kalshi_optimal=kalshi_no_ask,
                pm_avg_slipped=pm_yes_ask,
                kalshi_avg_slipped=kalshi_no_ask,
            )

        return None

    check_arbitrage = check_arbitrage_optimal


def cost_for_exact_contracts(asks: List[Tuple[float, float]], n: float) -> Optional[Tuple[float, float]]:
    """从卖盘（价格升序）吃进恰好 n 份合约；流动性不足则返回 None。"""
    if n <= 0.0 or not math.isfinite(n):
        return None
    eps = 1e-9
    remaining = n
    total_cost = 0.0
    for price, size in asks:
        if remaining <= eps:
            break
        if size <= 0.0 or price <= 0.0:
            continue
        take = min(remaining, size)
        total_cost += take * price
        remaining -= take
    if remaining > 1e-6:
        return None
    return (total_cost, total_cost / n)


def calculate_slippage_with_fixed_usdt(
    asks: List[Tuple[float, float]],
    usdt_amount: float,
) -> SlippageInfo:
    remaining_usdt = usdt_amount
    total_contracts = 0.0
    total_cost = 0.0
    best_price = asks[0][0] if asks else 0.0

    for price, size in asks:
        level_value = price * size
        if remaining_usdt >= level_value:
            total_contracts += size
            total_cost += level_value
            remaining_usdt -= level_value
        else:
            buy_size = remaining_usdt / price
            total_contracts += buy_size
            total_cost += remaining_usdt
            remaining_usdt = 0.0
            break

    filled_amount = usdt_amount - remaining_usdt
    filled = remaining_usdt == 0.0

    if total_contracts == 0.0:
        return SlippageInfo(0.0, 0.0, filled, filled_amount, 0.0)

    avg_price = total_cost / total_contracts
    slippage_percent = (avg_price - best_price) / best_price * 100.0 if best_price > 0 else 0.0

    return SlippageInfo(avg_price, slippage_percent, filled, filled_amount, total_contracts)


def orderbook_best_ask_price(levels: List[Tuple[float, float]]) -> Optional[float]:
    if not levels:
        return None
    return levels[0][0]


def _json_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return 0.0


def _json_num_field(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return x if math.isfinite(x) else None
    try:
        x = float(str(v).strip())
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def ladder_level_ok(price: float, size: float) -> bool:
    return (
        math.isfinite(price)
        and math.isfinite(size)
        and price > 0.0
        and price < 1.0
        and size > 0.0
    )


class PairLadderBuildFail(Enum):
    PM_INVALID_BUY_SIDE = auto()
    PM_MISSING_BIDS_ARRAY = auto()
    PM_MISSING_ASKS_ARRAY = auto()
    PM_MALFORMED_QUOTE = auto()
    PM_NO_ASK_LIQUIDITY_EMPTY_SIDE = auto()
    PM_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED = auto()
    KS_MISSING_ORDERBOOK_BODY = auto()
    KS_INVALID_BUY_SIDE = auto()
    KS_MALFORMED_QUOTE = auto()
    KS_NO_ASK_LIQUIDITY_EMPTY_SIDE = auto()
    KS_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED = auto()


def polymarket_clob_token_id_for_buy(pm: Market, buy_side: str) -> Optional[str]:
    if buy_side == "YES":
        return pm.token_ids[0] if pm.token_ids else None
    if buy_side == "NO":
        if len(pm.token_ids) >= 2:
            return pm.token_ids[1]
        return pm.token_ids[0] if pm.token_ids else None
    return pm.token_ids[0] if pm.token_ids else None


def pm_buy_no_uses_yes_token_complement(pm: Market, buy_side: str) -> bool:
    return buy_side == "NO" and len(pm.token_ids) < 2


def try_parse_polymarket_buy_asks(
    data: dict,
    buy_side: str,
    buy_no_via_yes_token_bids: bool,
) -> Union[List[Tuple[float, float]], PairLadderBuildFail]:
    if buy_side not in ("YES", "NO"):
        return PairLadderBuildFail.PM_INVALID_BUY_SIDE
    if buy_side == "NO" and buy_no_via_yes_token_bids:
        bids = data.get("bids")
        if not isinstance(bids, list):
            return PairLadderBuildFail.PM_MISSING_BIDS_ARRAY
        n_rows = len(bids)
        result: List[Tuple[float, float]] = []
        for bid in bids:
            if not isinstance(bid, dict):
                return PairLadderBuildFail.PM_MALFORMED_QUOTE
            pp = _json_num_field(bid.get("price"))
            sz = _json_num_field(bid.get("size"))
            if pp is None or sz is None:
                return PairLadderBuildFail.PM_MALFORMED_QUOTE
            ask_price = 1.0 - pp
            if ladder_level_ok(ask_price, sz):
                result.append((ask_price, sz))
        result.sort(key=lambda x: x[0])
        if not result:
            return (
                PairLadderBuildFail.PM_NO_ASK_LIQUIDITY_EMPTY_SIDE
                if n_rows == 0
                else PairLadderBuildFail.PM_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED
            )
        return result
    asks = data.get("asks")
    if not isinstance(asks, list):
        return PairLadderBuildFail.PM_MISSING_ASKS_ARRAY
    n_rows = len(asks)
    result = []
    for ask in asks:
        if not isinstance(ask, dict):
            return PairLadderBuildFail.PM_MALFORMED_QUOTE
        price = _json_num_field(ask.get("price"))
        size = _json_num_field(ask.get("size"))
        if price is None or size is None:
            return PairLadderBuildFail.PM_MALFORMED_QUOTE
        if ladder_level_ok(price, size):
            result.append((price, size))
    result.sort(key=lambda x: x[0])
    if not result:
        return (
            PairLadderBuildFail.PM_NO_ASK_LIQUIDITY_EMPTY_SIDE
            if n_rows == 0
            else PairLadderBuildFail.PM_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED
        )
    return result


def parse_polymarket_orderbook(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    r = try_parse_polymarket_buy_asks(data, side, side == "NO")
    return r if isinstance(r, list) else None


def _kalshi_orderbook_body(data: dict) -> Optional[dict]:
    ob = data.get("orderbook_fp")
    if isinstance(ob, dict):
        return ob
    ob2 = data.get("orderbook")
    return ob2 if isinstance(ob2, dict) else None


def try_parse_kalshi_orderbook(
    data: dict, side: str
) -> Union[List[Tuple[float, float]], PairLadderBuildFail]:
    orderbook = _kalshi_orderbook_body(data)
    if orderbook is None:
        return PairLadderBuildFail.KS_MISSING_ORDERBOOK_BODY
    if side == "YES":
        no_bids = orderbook.get("no_dollars", [])
        if not isinstance(no_bids, list):
            no_bids = []
        n_rows = len(no_bids)
        result: List[Tuple[float, float]] = []
        for entry in no_bids:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                return PairLadderBuildFail.KS_MALFORMED_QUOTE
            bid_price = _json_num_field(entry[0])
            size = _json_num_field(entry[1])
            if bid_price is None or size is None:
                return PairLadderBuildFail.KS_MALFORMED_QUOTE
            ask_price = 1.0 - bid_price
            if ladder_level_ok(ask_price, size):
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        if not result:
            return (
                PairLadderBuildFail.KS_NO_ASK_LIQUIDITY_EMPTY_SIDE
                if n_rows == 0
                else PairLadderBuildFail.KS_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED
            )
        return result
    if side == "NO":
        yes_bids = orderbook.get("yes_dollars", [])
        if not isinstance(yes_bids, list):
            yes_bids = []
        n_rows = len(yes_bids)
        result = []
        for entry in yes_bids:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                return PairLadderBuildFail.KS_MALFORMED_QUOTE
            bid_price = _json_num_field(entry[0])
            size = _json_num_field(entry[1])
            if bid_price is None or size is None:
                return PairLadderBuildFail.KS_MALFORMED_QUOTE
            ask_price = 1.0 - bid_price
            if ladder_level_ok(ask_price, size):
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        if not result:
            return (
                PairLadderBuildFail.KS_NO_ASK_LIQUIDITY_EMPTY_SIDE
                if n_rows == 0
                else PairLadderBuildFail.KS_NO_ASK_LIQUIDITY_ALL_ROWS_FILTERED
            )
        return result
    return PairLadderBuildFail.KS_INVALID_BUY_SIDE


def parse_kalshi_orderbook(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    r = try_parse_kalshi_orderbook(data, side)
    return r if isinstance(r, list) else None


# ==================== 卖出侧（bid 阶梯）用于模拟平仓 ====================


@dataclass
class PairOrderbookLadders:
    pm_asks: List[Tuple[float, float]]
    ks_asks: List[Tuple[float, float]]
    pm_bids_desc: List[Tuple[float, float]]
    ks_bids_desc: List[Tuple[float, float]]


def parse_polymarket_bids_desc(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    _ = side
    bids = data.get("bids", [])
    if not isinstance(bids, list):
        return None
    result: List[Tuple[float, float]] = []
    for bid in bids:
        if not isinstance(bid, dict):
            continue
        price = _json_num_field(bid.get("price"))
        size = _json_num_field(bid.get("size"))
        if price is not None and size is not None and ladder_level_ok(price, size):
            result.append((price, size))
    if not result:
        return None
    result.sort(key=lambda x: x[0], reverse=True)
    return result


def parse_kalshi_bids_desc(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    orderbook = _kalshi_orderbook_body(data)
    if orderbook is None:
        return None
    if side == "YES":
        arr = orderbook.get("yes_dollars", [])
    elif side == "NO":
        arr = orderbook.get("no_dollars", [])
    else:
        return None
    if not isinstance(arr, list):
        return None
    result: List[Tuple[float, float]] = []
    for entry in arr:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        bid_price = _json_num_field(entry[0])
        size = _json_num_field(entry[1])
        if bid_price is not None and size is not None and ladder_level_ok(bid_price, size):
            result.append((bid_price, size))
    if not result:
        return None
    result.sort(key=lambda x: x[0], reverse=True)
    return result


def build_pair_orderbook_ladders_result(
    pm_book: Dict[str, Any],
    ks_book: Dict[str, Any],
    pm_side: str,
    ks_side: str,
    pm_buy_no_via_yes_book_bids: bool,
) -> Union[PairOrderbookLadders, PairLadderBuildFail]:
    pm_r = try_parse_polymarket_buy_asks(pm_book, pm_side, pm_buy_no_via_yes_book_bids)
    if isinstance(pm_r, PairLadderBuildFail):
        return pm_r
    ks_r = try_parse_kalshi_orderbook(ks_book, ks_side)
    if isinstance(ks_r, PairLadderBuildFail):
        return ks_r
    pm_bids = parse_polymarket_bids_desc(pm_book, pm_side) or []
    ks_bids = parse_kalshi_bids_desc(ks_book, ks_side) or []
    return PairOrderbookLadders(
        pm_asks=pm_r,
        ks_asks=ks_r,
        pm_bids_desc=pm_bids,
        ks_bids_desc=ks_bids,
    )


def build_pair_orderbook_ladders(
    pm_book: Dict[str, Any],
    ks_book: Dict[str, Any],
    pm_side: str,
    ks_side: str,
    pm_buy_no_via_yes_book_bids: bool,
) -> Optional[PairOrderbookLadders]:
    r = build_pair_orderbook_ladders_result(
        pm_book, ks_book, pm_side, ks_side, pm_buy_no_via_yes_book_bids
    )
    return r if isinstance(r, PairOrderbookLadders) else None


def proceeds_for_exact_contracts_sell(
    bids_desc: List[Tuple[float, float]], n: float
) -> Optional[Tuple[float, float]]:
    if n <= 0.0 or not math.isfinite(n):
        return None
    eps = 1e-9
    remaining = n
    total_proceeds = 0.0
    for price, size in bids_desc:
        if remaining <= eps:
            break
        if size <= 0.0 or price <= 0.0:
            continue
        take = min(remaining, size)
        total_proceeds += take * price
        remaining -= take
    if remaining > 1e-6:
        return None
    return (total_proceeds, total_proceeds / n)
