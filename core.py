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
# POLYMARKET API CREDENTIALS
# ═══════════════════════════════════════════════
POLY_API_KEY        = "98579c51-db9f-b12a-6fa7-9635b39c9088"
POLY_API_SECRET     = "MxmT2utzi7afGyc-bODjHV0xqqYAPDX6onuVyJxUNKU="
POLY_API_PASSPHRASE = "0b45c8d7e8120c28307e0e6a258b54c52e7d8f009c67038028bcc63d711db185"
POLY_PRIVATE_KEY    = "0x9e438d5eaf4f230ebdddc2344b46748739cabe3856b6f20e20e50150f2c1d713"

# ═══════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════
BINANCE_WS        = "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms"
BINANCE_OI_URL    = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
BINANCE_AGG_URL   = "https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT&limit=200"

# ═══════════════════════════════════════════════
# POLYMARKET
# ═══════════════════════════════════════════════
POLY_GAMMA   = "https://gamma-api.polymarket.com"
POLY_WS      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

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
# BINANCE WEBSOCKET — стакан (depth)
# ═══════════════════════════════════════════════
async def run_binance(state: MarketState):
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20) as ws:
                state.status = "Binance ✓"
                async for raw in ws:
                    msg  = json.loads(raw)
                    data = msg.get("data", {})
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
        except Exception as e:
            state.status = f"Binance err: {e}"
            await asyncio.sleep(3)


# ═══════════════════════════════════════════════
# CVD через REST (aggTrades polling кожні 2с)
# ═══════════════════════════════════════════════
async def run_cvd(state: MarketState, session: aiohttp.ClientSession):
    last_ts = int(time.time() * 1000)
    while True:
        try:
            url = f"{BINANCE_AGG_URL}&startTime={last_ts}"
            async with session.get(url) as r:
                if r.status == 200:
                    trades = await r.json()
                    for t in trades:
                        qty = float(t.get("q", 0))
                        if t.get("m"):      # buyer is maker = taker sell
                            state.cvd -= qty
                        else:               # taker buy
                            state.cvd += qty
                        ts = t.get("T", last_ts)
                        if ts > last_ts:
                            last_ts = ts + 1
        except Exception:
            pass
        await asyncio.sleep(2)


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
# POLYMARKET — REST (знаходить контракт і токени)
# ═══════════════════════════════════════════════
async def run_polymarket(state: MarketState, session: aiohttp.ClientSession):
    """Кожні 10с перевіряє чи змінився контракт і оновлює token_id."""
    while True:
        try:
            slot = current_slot()
            slug = slot_slug(slot)

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
                                tokens   = json.loads(m.get("clobTokenIds", "[]"))
                                up_idx   = 0 if outcomes and outcomes[0].lower() == "up" else 1

                                state.up_token_id   = tokens[up_idx] if tokens else ""
                                state.down_token_id = tokens[1 - up_idx] if tokens else ""
                                state.market_title  = ev.get("title", slug)
                                state.market_end_ts = slot
                                state.current_slug  = slug
        except Exception:
            pass
        await asyncio.sleep(10)


# ═══════════════════════════════════════════════
# POLYMARKET REST — поллінг стакану кожні 2с (основна ціна)
# ═══════════════════════════════════════════════
POLY_CLOB = "https://clob.polymarket.com"

async def run_polymarket_book(state: MarketState, session: aiohttp.ClientSession):
    """Опитує REST /book кожні 2с — завжди актуальна ціна."""
    while True:
        try:
            if state.up_token_id:
                url = f"{POLY_CLOB}/book?token_id={state.up_token_id}"
                async with session.get(url) as r:
                    if r.status == 200:
                        book = await r.json()
                        bids = book.get("bids", [])
                        asks = book.get("asks", [])
                        if bids and asks:
                            # bids ascending → last = best bid
                            # asks descending → last = best ask
                            b = float(bids[-1]["price"])
                            a = float(asks[-1]["price"])
                            state.up_price   = round((b + a) / 2, 3)
                            state.down_price = round(1 - state.up_price, 3)
        except Exception:
            pass
        await asyncio.sleep(2)


# ═══════════════════════════════════════════════
# POLYMARKET WEBSOCKET — живі ціни в реальному часі
# ═══════════════════════════════════════════════
async def run_polymarket_ws(state: MarketState):
    """WebSocket — живі ціни через best_bid_ask події."""
    while True:
        try:
            if not state.up_token_id:
                await asyncio.sleep(2)
                continue

            async with websockets.connect(POLY_WS, ping_interval=20, ping_timeout=20) as ws:
                sub = json.dumps({
                    "type": "market",
                    "assets_ids": [state.up_token_id, state.down_token_id],
                    "custom_feature_enabled": True,
                })
                await ws.send(sub)

                up_bid = up_ask = dn_bid = dn_ask = None

                while True:
                    # При зміні контракту — перепідписуємось
                    if state.current_slug != slot_slug(current_slot()):
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    data = json.loads(raw)
                    msgs = data if isinstance(data, list) else [data]

                    for msg in msgs:
                        if not isinstance(msg, dict):
                            continue

                        etype = msg.get("event_type")
                        aid   = msg.get("asset_id", "")

                        if etype == "best_bid_ask":
                            try:
                                b = float(msg["best_bid"]) if msg.get("best_bid") is not None else None
                                a = float(msg["best_ask"]) if msg.get("best_ask") is not None else None
                            except Exception:
                                continue
                            if aid == state.up_token_id:
                                if b is not None: up_bid = b
                                if a is not None: up_ask = a
                            elif aid == state.down_token_id:
                                if b is not None: dn_bid = b
                                if a is not None: dn_ask = a

                        elif etype == "book" or etype is None:
                            # Initial snapshot or periodic book update
                            # bids sorted ascending → last = best bid
                            # asks sorted descending → last = best ask
                            bids_raw = msg.get("bids", [])
                            asks_raw = msg.get("asks", [])
                            if bids_raw and asks_raw:
                                try:
                                    b = float(bids_raw[-1]["price"])
                                    a = float(asks_raw[-1]["price"])
                                    if aid == state.up_token_id:
                                        up_bid, up_ask = b, a
                                    elif aid == state.down_token_id:
                                        dn_bid, dn_ask = b, a
                                except Exception:
                                    pass

                    # Рахуємо midpoint як середнє bid/ask
                    if up_ask is not None and up_bid is not None:
                        state.up_price   = round((up_bid + up_ask) / 2, 3)
                        state.down_price = round(1 - state.up_price, 3)
                    elif up_ask is not None:
                        state.up_price   = round(up_ask, 3)
                        state.down_price = round(1 - up_ask, 3)

        except Exception:
            await asyncio.sleep(2)


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
