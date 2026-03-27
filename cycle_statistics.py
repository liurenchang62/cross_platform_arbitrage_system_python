# cycle_statistics.py — 周期 Top10 / 全轮回撤等控制台统计
# 大周期边界仅 `reset_big_period_accumulator()`，不输出「上一大周期总绩效」。
from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from arbitrage_detector import ArbitrageOpportunity

OpportunityTuple = Tuple[
    "ArbitrageOpportunity",
    str,
    str,
    Optional[datetime],
    Optional[datetime],
]


@dataclass
class _CumulativeStats:
    arb_hits: int = 0
    sum_capital: float = 0.0
    sum_gas: float = 0.0
    sum_fees: float = 0.0
    sum_gross_payout: float = 0.0
    sum_net_profit: float = 0.0
    full_match_cycles_completed: int = 0


@dataclass
class _BigPeriodStats:
    arb_hits: int = 0
    sum_capital: float = 0.0
    sum_gas: float = 0.0
    sum_fees: float = 0.0
    sum_gross_payout: float = 0.0
    sum_net_profit: float = 0.0


_GLOBAL = _CumulativeStats()
_BIG_PERIOD = _BigPeriodStats()
_LOCK = threading.Lock()


def record_opportunity(opp: "ArbitrageOpportunity") -> None:
    with _LOCK:
        g = _GLOBAL
        g.arb_hits += 1
        g.sum_capital += opp.capital_used
        g.sum_gas += opp.gas_amount
        g.sum_fees += opp.fees_amount
        g.sum_gross_payout += opp.contracts
        g.sum_net_profit += opp.net_profit_100

        bp = _BIG_PERIOD
        bp.arb_hits += 1
        bp.sum_capital += opp.capital_used
        bp.sum_gas += opp.gas_amount
        bp.sum_fees += opp.fees_amount
        bp.sum_gross_payout += opp.contracts
        bp.sum_net_profit += opp.net_profit_100


def reset_big_period_accumulator() -> None:
    """在下一全量匹配周期开始前调用：仅清零大周期累加器，不打印、不写报表。"""
    global _BIG_PERIOD
    with _LOCK:
        _BIG_PERIOD = _BigPeriodStats()


def on_full_cycle_completed(rows: List[OpportunityTuple]) -> str:
    with _LOCK:
        _GLOBAL.full_match_cycles_completed += 1
    s = format_full_cycle_roi_top10_only(rows)
    print(s, end="")
    return s


def format_full_cycle_roi_top10_only(rows: List[OpportunityTuple]) -> str:
    with _LOCK:
        n = _GLOBAL.full_match_cycles_completed

    lines: List[str] = []
    lines.append("")
    lines.append("╔══════════════════════════════════════════════════════════════════════╗")
    lines.append(
        f"║  📈 全量匹配周期 #{n} · 利润率 Top 10（按 ROI%，100 USDT 腿资金口径）      ║"
    )
    lines.append("╚══════════════════════════════════════════════════════════════════════╝")

    if not rows:
        lines.append("   （本全量周期无验证通过的套利）")
    else:
        sorted_rows = sorted(rows, key=lambda r: r[0].roi_100_percent, reverse=True)
        for i, (opp, pm_title, ks_title, _, _) in enumerate(sorted_rows[:10]):
            lines.append("")
            lines.append(
                f"   #{i + 1:>2}  ROI {opp.roi_100_percent:>7.2f}%  净利 ${opp.net_profit_100:<10.2f}  | PM: {_truncate_title(pm_title, 72)} …"
            )
            lines.append(f"        Kalshi: {_truncate_title(ks_title, 76)}")
    lines.append("")
    return "\n".join(lines)


def _truncate_title(s: str, max_chars: int) -> str:
    """按 Unicode 码位计数截断标题。"""
    chars = list(s)
    if len(chars) <= max_chars:
        return s
    return "".join(chars[: max_chars - 3]) + "..."
