# log_format.py
# 时间与 CSV 相关格式，与 chrono `DateTime::to_rfc3339`（秒精度、UTC 用 Z）及本地时间行对齐。
from __future__ import annotations

from datetime import datetime, timezone


def utc_datetime_to_rfc3339(dt: datetime) -> str:
    """UTC 时刻格式化为 RFC3339，与 Rust `chrono::DateTime<Utc>::to_rfc3339()` 默认秒精度一致（末尾 Z）。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def local_datetime_line(dt_utc: datetime) -> str:
    """与 `Utc::now().with_timezone(&Local).format(\"%Y-%m-%d %H:%M:%S\")` 一致：本地墙钟、无秒小数。"""
    return dt_utc.astimezone().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
