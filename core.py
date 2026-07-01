#!/usr/bin/env python3
"""
Shared market data — Binance Futures + Polymarket BTC 5m UP/DOWN
"""

import asyncio
import json
import sqlite3
import time
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import aiohttp
import websockets

# ═══════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════
BINANCE_WS = (
    "wss://fstream.binance.com/stream?streams="
    "btcusdt@depth20@100ms/"
    "btcusdt@aggTrade"
)
BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"

# ═══════════════════════════════════════════════
# POLYMARKET
# ═══════════════════════════════════════════════
POLY_GAMMA = "https://gamma-api.polymarket.com"

WALL_MIN_BTC = 15.0
WALL_FAKE_SEC = 8


# ═══════════════════════════════════════════════
# РИНОК — ПОШУК ПОТОЧНОГО СЛОТУ
# ═══════════════════════════════════════════════
def current_slot() -> int:
    """Unix timestamp кінця поточного 5-хвилинного слоту."""
    now = int(time.time())
    return ((now // 300) + 1) * 300


def slot_slug(ts: int) -> str:
    return f"btc-updown-5m-{ts}"


# ═══════════════════════════════════════════════
# MARKET STATE
# ═══════════════════════════════════════════════
class MarketState:
    def __init__(self):
        self.btc_price: float = 0.0
        self.cvd: float = 0.0
        self.cvd_reset_val: float = 0.0
        self.oi: float = 0.0
        self.oi_prev: float = 0.0

        # Polymarket
        self.up_price: float = 0.0    # ціна UP (те що юзер купує)
        self.down_price: float = 0.0
        self.up_token_id: str = ""
        self.down_token_id: str = ""
        self.market_title: str = "шукаємо..."
        self.market_end_ts: int = 0   # Unix коли закінчується контракт
        self.current_slug: str = ""

        # Стакан
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.wall_tracker: Dict[float, Tuple[float, float]] = {}

        self.status: str = "запуск..."

    def cvd_delta(self) -> float:
        return self.cvd - self.cvd_reset_val

    def oi_trend(self) -> str:
        if self.oi == 0 or self.oi_prev == 0:
            return "—"
        diff = self.oi - self.oi_prev
        if diff > 100:
            return f"[green]↑ +{diff:.0f} BTC (тренд підтверджено)[/]"
        if diff < -100:
            return f"[red]↓ {diff:.0f} BTC (пастка?)[/]"
        return f"[dim]→ {diff:+.0f} BTC (флет)[/]"

    def seconds_left(self) -> int:
        if self.market_end_ts == 0:
            return 0
        return max(0, self.market_end_ts - int(time.time()))

    def bid_walls(self) -> List[Tuple[float, float]]:
        return sorted(
            [(p, s) for p, s in self.bids.items() if s >= WALL_MIN_BTC],
            key=lambda x: x[0], reverse=True
        )

    def ask_walls(self) -> List[Tuple[float, float]]:
        return sorted(
            [(p, s) for p, s in self.asks.items() if s >= WALL_MIN_BTC],
            key=lambda x: x[0]
        )

    def check_wall(self, price: float, size: float) -> Tuple[bool, str]:
        now = time.time()
        if price not in self.wall_tracker:
            self.wall_tracker[price] = (size, now)
            return True, "нова"
        prev_size, first_seen = self.wall_tracker[price]
        age = now - first_seen
        if size < prev_size * 0.3 and age < WALL_FAKE_SEC:
            self.wall_tracker.pop(price, None)
            return False, f"ФЕЙК ({age:.1f}с)"
        self.wall_tracker[price] = (size, first_seen)
        return True, f"реальна {age:.0f}с/{size:.1f}BTC"


# ═══════════════════════════════════════════════
# BINANCE WEBSOCKET
# ═══════════════════════════════════════════════
async def run_binance(state: MarketState):
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20) as ws:
                state.status = "Binance ✓"
                async for raw in ws:
                    msg    = json.loads(raw)
                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})

                    if "depth" in stream:
                        for b in data.get("b", []):
                            p, s = float(b[0]), float(b[1])
                            if s == 0:
                                state.bids.pop(p, None)
                            else:
                                state.bids[p] = s
                        for a in data.get("a", []):
                            p, s = float(a[0]), float(a[1])
                            if s == 0:
                                state.asks.pop(p, None)
                            else:
                                state.asks[p] = s
                        if state.bids and state.asks:
                            state.btc_price = (max(state.bids) + min(state.asks)) / 2

                    elif "aggTrade" in stream:
                        qty = float(data.get("q", 0))
                        if data.get("m"):
                            state.cvd -= qty
                        else:
                            state.cvd += qty

        except Exception as e:
            state.status = f"Binance err: {e}"
            await asyncio.sleep(3)


# ═══════════════════════════════════════════════
# OPEN INTEREST (кожні 30с)
# ═══════════════════════════════════════════════
async def run_oi(state: MarketState, session: aiohttp.ClientSession):
    while True:
        try:
            async with session.get(BINANCE_OI_URL) as r:
                if r.status == 200:
                    d = await r.json()
                    new_oi = float(d.get("openInterest", 0))
                    if new_oi > 0:
                        state.oi_prev = state.oi if state.oi > 0 else new_oi
                        state.oi = new_oi
        except Exception:
            pass
        await asyncio.sleep(30)


# ═══════════════════════════════════════════════
# CVD RESET (кожні 5 хв)
# ═══════════════════════════════════════════════
async def run_cvd_reset(state: MarketState):
    while True:
        await asyncio.sleep(300)
        state.cvd_reset_val = state.cvd


# ═══════════════════════════════════════════════
# POLYMARKET POLLER
# Автоматично знаходить поточний 5-хв контракт
# і оновлює ціни кожні 3 секунди
# ═══════════════════════════════════════════════
async def run_polymarket(state: MarketState, session: aiohttp.ClientSession):
    while True:
        try:
            slot = current_slot()
            slug = slot_slug(slot)

            # Якщо ринок змінився — завантажуємо новий
            if slug != state.current_slug:
                url = f"{POLY_GAMMA}/events?slug={slug}"
                async with session.get(url) as r:
                    if r.status == 200:
                        events = await r.json()
                        if events:
                            ev = events[0]
                            markets = ev.get("markets", [])
                            if markets:
                                m = markets[0]
                                outcomes = json.loads(m.get("outcomes", "[]"))
                                prices   = json.loads(m.get("outcomePrices", "[]"))
                                tokens   = json.loads(m.get("clobTokenIds", "[]"))

                                # outcomes[0] = "Up", outcomes[1] = "Down"
                                up_idx = 0 if outcomes and outcomes[0].lower() == "up" else 1

                                state.up_price    = float(prices[up_idx]) if prices else 0.0
                                state.down_price  = float(prices[1 - up_idx]) if prices else 0.0
                                state.up_token_id   = tokens[up_idx] if tokens else ""
                                state.down_token_id = tokens[1 - up_idx] if tokens else ""
                                state.market_title  = ev.get("title", slug)
                                state.market_end_ts = slot
                                state.current_slug  = slug
                        else:
                            pass  # ринок ще не створений

            else:
                # Оновлюємо тільки ціни для поточного ринку
                url = f"{POLY_GAMMA}/events?slug={slug}"
                async with session.get(url) as r:
                    if r.status == 200:
                        events = await r.json()
                        if events:
                            m = events[0].get("markets", [{}])[0]
                            outcomes = json.loads(m.get("outcomes", "[]"))
                            prices   = json.loads(m.get("outcomePrices", "[]"))
                            if prices and outcomes:
                                up_idx = 0 if outcomes[0].lower() == "up" else 1
                                state.up_price   = float(prices[up_idx])
                                state.down_price = float(prices[1 - up_idx])

        except Exception as e:
            pass

        await asyncio.sleep(3)


# ═══════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════
def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_time    TEXT,
            exit_time     TEXT,
            direction     TEXT,
            entry_price   REAL,
            exit_price    REAL,
            stake         REAL,
            pnl           REAL,
            outcome       TEXT,
            signal_reason TEXT,
            btc_at_entry  REAL,
            cvd_at_entry  REAL,
            oi_at_entry   REAL,
            wall_signal   TEXT,
            market_slug   TEXT
        )
    """)
    conn.commit()
    return conn


def save_trade(db: sqlite3.Connection, t_open: dict, exit_price: float, outcome: str) -> float:
    pnl = round((exit_price - t_open["entry"]) * (t_open["stake"] / t_open["entry"]), 3)
    db.execute("""
        INSERT INTO trades
        (entry_time, exit_time, direction, entry_price, exit_price, stake, pnl,
         outcome, signal_reason, btc_at_entry, cvd_at_entry, oi_at_entry, wall_signal, market_slug)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        t_open["entry_time"].isoformat(),
        datetime.now().isoformat(),
        t_open.get("direction", "UP"),
        t_open["entry"], exit_price,
        t_open["stake"], pnl, outcome,
        t_open["reason"],
        t_open["btc"], t_open["cvd"], t_open["oi"],
        t_open.get("wall", ""),
        t_open.get("slug", ""),
    ))
    db.commit()
    return pnl
