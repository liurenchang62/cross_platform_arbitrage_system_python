# kalshi_demo.py
# Kalshi Demo：RSA-PSS-SHA256 签名与 POST /portfolio/orders 限价 IOC 下单（与 Rust `kalshi_demo.rs` 对齐）。
from __future__ import annotations

import base64
import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from arbitrage_detector import ArbitrageOpportunity
from system_params import (
    CASH_UTILIZATION_MAX,
    KALSHI_DEMO_API_KEY_ID_ENV,
    KALSHI_DEMO_BASE_URL,
    KALSHI_DEMO_PRIVATE_KEY_PATH_ENV,
)

KALSHI_SIGN_PATH_ORDERS = "/trade-api/v2/portfolio/orders"
KALSHI_SIGN_PATH_BALANCE = "/trade-api/v2/portfolio/balance"


class KalshiDemoError(Exception):
    pass


class KalshiDemoConfigError(KalshiDemoError):
    pass


def _millis_timestamp() -> str:
    return str(int(time.time() * 1000))


def _sign_request(private_key: Any, timestamp_ms: str, method: str, path: str) -> str:
    msg = f"{timestamp_ms}{method}{path}".encode("utf-8")
    sig = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("ascii")


@dataclass
class KalshiDemoConfig:
    api_key_id: str
    private_key: Any
    base_url: str

    @classmethod
    def try_from_env(cls) -> Optional[KalshiDemoConfig]:
        key_id = str(os.environ.get(KALSHI_DEMO_API_KEY_ID_ENV, ""))
        path = str(os.environ.get(KALSHI_DEMO_PRIVATE_KEY_PATH_ENV, ""))
        if not key_id.strip() or not path.strip():
            return None
        key_id = key_id.strip()
        path = path.strip()
        try:
            pem = Path(path).read_text(encoding="utf-8")
        except OSError as e:
            raise KalshiDemoConfigError(
                f"读取私钥文件失败 {path}: {e}"
            ) from e
        if pem.startswith("\ufeff"):
            pem = pem[1:]
        try:
            private_key = serialization.load_pem_private_key(
                pem.strip().encode("utf-8"),
                password=None,
            )
        except ValueError as e:
            raise KalshiDemoConfigError(
                "解析 PEM 私钥失败（需有效 RSA PEM，常见为 PKCS#1 `BEGIN RSA PRIVATE KEY` 或 PKCS#8）"
            ) from e
        base = KALSHI_DEMO_BASE_URL.rstrip("/")
        return cls(api_key_id=key_id, private_key=private_key, base_url=base)


async def get_demo_balance_cents(
    http: aiohttp.ClientSession, cfg: KalshiDemoConfig
) -> int:
    url = f"{cfg.base_url}/portfolio/balance"
    ts = _millis_timestamp()
    sig = _sign_request(cfg.private_key, ts, "GET", KALSHI_SIGN_PATH_BALANCE)
    headers = {
        "KALSHI-ACCESS-KEY": cfg.api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": sig,
    }
    async with http.get(url, headers=headers) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise KalshiDemoError(
                f"Kalshi Demo 余额查询 HTTP {resp.status}: {text.strip()}"
            )
    try:
        v = json.loads(text)
    except json.JSONDecodeError as e:
        raise KalshiDemoError(f"balance JSON: {text}") from e
    bal = v.get("balance")
    if bal is None:
        raise KalshiDemoError(f"响应缺少 balance（美分）: {text}")
    if isinstance(bal, int):
        return bal
    if isinstance(bal, float):
        return int(bal)
    try:
        return int(bal)
    except (TypeError, ValueError) as e:
        raise KalshiDemoError(f"balance 非整数: {bal!r}") from e


async def place_demo_buy_ioc(
    http: aiohttp.ClientSession,
    cfg: KalshiDemoConfig,
    ticker: str,
    kalshi_side_upper: str,
    opp: ArbitrageOpportunity,
    client_order_id: str,
    per_leg_cap_usd: float,
) -> str:
    count_fp = float(math.floor(opp.contracts))
    count_fp = max(1.0, count_fp)
    count = int(count_fp)
    if count < 1:
        raise KalshiDemoError("合约份数过小，跳过 Demo 下单")

    side = kalshi_side_upper.lower()
    if side not in ("yes", "no"):
        raise KalshiDemoError(
            f"Kalshi 腿方向应为 YES/NO，当前: {kalshi_side_upper}"
        )

    raw_cents = int(math.ceil(opp.kalshi_avg_slipped * 100.0))
    price_cents = max(1, min(99, raw_cents))

    if per_leg_cap_usd > 0.0 and price_cents > 0:
        leg_cents = int(max(1.0, math.floor(per_leg_cap_usd * 100.0)))
        max_by_leg_cap = max(0, leg_cents // price_cents)
    else:
        max_by_leg_cap = 10**18

    if max_by_leg_cap >= 0 and count > max_by_leg_cap:
        print(
            f"   📘 [Kalshi Demo] 每腿上限 ${per_leg_cap_usd:.2f}，份数由 {count} 缩至 {max_by_leg_cap}（限价 {price_cents}¢）"
        )
        count = max_by_leg_cap
    if count < 1:
        raise KalshiDemoError(
            f"Kalshi Demo 每腿上限 ${per_leg_cap_usd:.2f} 不足以在当前限价 {price_cents}¢ 下买 1 份"
        )

    try:
        balance = await get_demo_balance_cents(http, cfg)
    except KalshiDemoError as e:
        raise KalshiDemoError(
            f"Kalshi Demo 读取余额失败（须先查询余额再下单）: {e}"
        ) from e

    if balance <= 0:
        raise KalshiDemoError(
            f"Kalshi Demo 可用余额为 0（{balance} 分）；请在 https://demo.kalshi.com 账户中确认虚拟资金"
        )

    spendable = int(math.floor(float(balance) * CASH_UTILIZATION_MAX))
    reserve = max(1, price_cents)
    max_by_price = spendable // reserve
    max_by_unit = spendable // 100
    max_affordable = max(0, min(max_by_price, max_by_unit))
    if max_affordable < 1:
        raise KalshiDemoError(
            f"Kalshi Demo 余额不足以买 1 份：可用约 {balance / 100.0:.2f} USD（balance={balance}¢），限价 {price_cents}¢/份；请充值 Demo 或等纸面用更小 n"
        )
    if count > max_affordable:
        print(
            f"   📘 [Kalshi Demo] 可用余额约 {balance / 100.0:.2f} USD（按 {CASH_UTILIZATION_MAX * 100.0:.0f}% 计），Kalshi 下单份数由 {count} 缩至 {max_affordable}（限价 {price_cents}¢）"
        )
        count = max_affordable

    body: dict = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "count": count,
        "type": "limit",
        "time_in_force": "immediate_or_cancel",
        "client_order_id": client_order_id,
    }
    if side == "yes":
        body["yes_price"] = price_cents
    else:
        body["no_price"] = price_cents

    url = f"{cfg.base_url}/portfolio/orders"
    ts = _millis_timestamp()
    sig = _sign_request(cfg.private_key, ts, "POST", KALSHI_SIGN_PATH_ORDERS)
    headers = {
        "KALSHI-ACCESS-KEY": cfg.api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "Content-Type": "application/json",
    }
    async with http.post(url, headers=headers, json=body) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise KalshiDemoError(
                f"Kalshi Demo 下单 HTTP {resp.status}: {text.strip()}"
            )
    try:
        v = json.loads(text)
    except json.JSONDecodeError as e:
        raise KalshiDemoError(f"解析 JSON: {text}") from e
    order = v.get("order") or {}
    order_id = str(order.get("order_id") or "")
    if not order_id:
        raise KalshiDemoError(f"响应无 order_id: {text}")
    return order_id
