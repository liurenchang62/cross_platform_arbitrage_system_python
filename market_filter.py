# market_filter.py — 与 Rust `market_filter.rs` 对齐
"""按解析日筛选市场：剔除「有明确解析时间且晚于 horizon」的远期市场；无解析日期的保留。"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from market import Market
from query_params import RESOLUTION_HORIZON_DAYS


def filter_markets_by_resolution_horizon(
    markets: List[Market],
    now: datetime,
) -> List[Market]:
    """若市场有 `resolution_date` 且该时间 **严格晚于** `now + horizon_days`，则剔除。"""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    horizon = timedelta(days=RESOLUTION_HORIZON_DAYS)
    cutoff = now + horizon

    out: List[Market] = []
    for m in markets:
        rd = m.resolution_date
        if rd is None:
            out.append(m)
            continue
        if rd.tzinfo is None:
            rd = rd.replace(tzinfo=timezone.utc)
        else:
            rd = rd.astimezone(timezone.utc)
        if rd <= cutoff:
            out.append(m)
    return out


def tracked_pair_exceeds_horizon(pm: Market, ks: Market, now: datetime) -> bool:
    """任一侧有解析日且该日晚于 cutoff，则该追踪对应剔除。"""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    horizon = timedelta(days=RESOLUTION_HORIZON_DAYS)
    cutoff = now + horizon

    def _far(dt: Optional[datetime]) -> bool:
        if dt is None:
            return False
        t = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        t = t.astimezone(timezone.utc)
        return t > cutoff

    return _far(pm.resolution_date) or _far(ks.resolution_date)
