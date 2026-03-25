# 交互式 CLI：与参考 `backtest` crate 流程一致
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

import questionary

from system_params import PAPER_TRADES_CSV, PAPER_TRADES_CSV_ENV

from .engine import (
    analyze_session,
    collect_session_anchor_dates,
    load_csv,
    sessions_started_on_date,
)


def pick_date(dates: List[date]) -> Optional[date]:
    if not dates:
        print("没有可解析的「会话启动日」（UTC）。请确认文件中包含会话开始记录。")
        return None

    years = sorted({d.year for d in dates})
    y_str = questionary.select(
        "选择 UTC 年份（会话启动日）",
        choices=[str(y) for y in years],
    ).ask()
    if y_str is None:
        return None
    year = int(y_str)

    months = sorted({d.month for d in dates if d.year == year})
    m_str = questionary.select(
        "选择 UTC 月份",
        choices=[f"{m:02d}" for m in months],
    ).ask()
    if m_str is None:
        return None
    month = int(m_str)

    days = sorted({d.day for d in dates if d.year == year and d.month == month})
    d_str = questionary.select(
        "选择 UTC 日",
        choices=[f"{dd:02d}" for dd in days],
    ).ask()
    if d_str is None:
        return None
    day = int(d_str)

    return date(year, month, day)


def main() -> int:
    path_str = os.environ.get(PAPER_TRADES_CSV_ENV, PAPER_TRADES_CSV)
    path = Path(path_str)
    print(f"读取 {path}")
    if not path.is_file():
        print(f"文件不存在: {path}", file=sys.stderr)
        return 1

    rows = load_csv(path)
    dates = collect_session_anchor_dates(rows)
    target = pick_date(dates)
    if target is None:
        return 0

    print(f"\n选中 UTC 日（会话启动日）：{target}")

    sessions = sessions_started_on_date(rows, target)
    if not sessions:
        print("该日无会话启动记录。")
        return 0

    labels = [
        f"编号 {sid} · {sum(1 for r in rows if r.session_id == sid)} 条记录"
        for sid in sessions
    ]
    choice = questionary.select("选择会话", choices=labels).ask()
    if choice is None:
        return 0
    idx = labels.index(choice)
    session_id = sessions[idx]

    analyze_session(rows, session_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
