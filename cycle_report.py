# cycle_report.py — 与 Rust `cycle_report.rs` 对齐
import os
from typing import Optional

REPORT_PATH = "logs/cycle_report.txt"


def append_cycle_report(header_line: str, body: str) -> None:
    """追加一整段周期报告（含分隔头与正文，格式与终端一致）。"""
    path = REPORT_PATH
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(header_line + "\n")
        f.write(body)
        if body and not body.endswith("\n"):
            f.write("\n")
        f.write("\n")
