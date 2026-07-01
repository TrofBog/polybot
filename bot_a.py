#!/usr/bin/env python3
"""
БОТ A — Тільки лімітки
Купуємо UP коли ціна просідає на LIMIT_OFFSET нижче ринку.
Запуск: python bot_a.py
"""

import asyncio
from datetime import datetime
from typing import Optional

import aiohttp
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.columns import Columns
from rich import box

from core import (
    MarketState, init_db, save_trade,
    run_binance, run_cvd, run_oi, run_cvd_reset, run_polymarket, run_polymarket_ws,
)

INITIAL_BALANCE  = 42.0
STAKE_PCT        = 0.08   # 8% від поточного балансу
LIMIT_OFFSET     = 0.03   # купуємо на 3¢ нижче поточної ціни
TAKE_PROFIT      = 0.87
MIN_UP_PRICE     = 0.75   # не входимо якщо ціна нижче 0.75 (ринок невпевнений)
ENTRY_SEC_MIN    = 90     # входимо не раніше ніж за 90с до кінця
ENTRY_SEC_MAX    = 150    # і не пізніше ніж за 90с (вікно: 90-150с)
DB_PATH          = "trades_a.db"

console = Console()


# ═══════════════════════════════════════════════
# РАХУНОК
# ═══════════════════════════════════════════════
class Account:
    def __init__(self):
        self.balance   = INITIAL_BALANCE
        self.open: Optional[dict] = None
        self.wins      = 0
        self.losses    = 0
        self.total_pnl = 0.0
        self.log: list = []

    def enter(self, entry: float, state: MarketState, reason: str) -> bool:
        if self.open or self.balance < 1.0:
            return False
        stake = round(self.balance * STAKE_PCT, 2)
        self.balance -= stake
        self.open = {
            "entry":      entry,
            "stake":      stake,
            "entry_time": datetime.now(),
            "direction":  "UP",
            "reason":     reason,
            "btc":        state.btc_price,
            "cvd":        state.cvd,
            "oi":         state.oi,
            "slug":       state.current_slug,
        }
        return True

    def close(self, exit_price: float, outcome: str, db) -> float:
        if not self.open:
            return 0.0
        pnl = save_trade(db, self.open, exit_price, outcome)
        self.balance   += self.open["stake"] + pnl
        self.total_pnl += pnl
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        self.log.append({
            "time":    datetime.now().strftime("%H:%M:%S"),
            "entry":   self.open["entry"],
            "exit":    exit_price,
            "pnl":     pnl,
            "outcome": outcome,
        })
        self.open = None
        return pnl


# ═══════════════════════════════════════════════
# СТРАТЕГІЯ A
# ═══════════════════════════════════════════════
def get_signal(state: MarketState, acc: Account) -> Optional[float]:
    """
    Входимо тільки коли:
    - залишилось 90-150 секунд до кінця контракту
    - ціна UP вже висока (>= 0.75) — ринок впевнений
    - ставимо ліміт на 3¢ нижче поточної ціни
    """
    if acc.open or state.up_price <= 0:
        return None

    secs = state.seconds_left()

    # Тільки у вікні 90-150 секунд до кінця
    if not (ENTRY_SEC_MIN <= secs <= ENTRY_SEC_MAX):
        return None

    # Ціна має бути висока — ринок впевнений що UP
    if state.up_price < MIN_UP_PRICE:
        return None

    limit = round(state.up_price - LIMIT_OFFSET, 3)
    return limit


# ═══════════════════════════════════════════════
# DISPLAY
# ═══════════════════════════════════════════════
def build_display(state: MarketState, acc: Account) -> Panel:
    cvd_d = state.cvd_delta()
    cvd_c = "green" if cvd_d > 0 else "red"

    mt = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    mt.add_row("BTC ф'ючерс",  f"[bold]${state.btc_price:,.1f}[/]")
    mt.add_row("UP  (Poly)",   f"[bold cyan]{state.up_price:.3f}[/]")
    mt.add_row("DOWN (Poly)",  f"[dim]{state.down_price:.3f}[/]")
    mt.add_row("CVD Δ (5хв)", f"[{cvd_c}]{cvd_d:+.1f} BTC[/]")
    mt.add_row("OI",           state.oi_trend())
    bw = state.bid_walls()
    aw = state.ask_walls()
    mt.add_row("BID стіна", f"${bw[0][0]:.0f}×{bw[0][1]:.1f}BTC" if bw else "—")
    mt.add_row("ASK стіна", f"${aw[0][0]:.0f}×{aw[0][1]:.1f}BTC" if aw else "—")
    mt.add_row("Контракт",  state.market_title[:40] if state.market_title else "—")
    mt.add_row("Залишилось", f"[yellow]{state.seconds_left()}с[/]")

    at = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    pnl_c = "green" if acc.total_pnl >= 0 else "red"
    wr = f"{acc.wins/(acc.wins+acc.losses)*100:.0f}%" if (acc.wins+acc.losses) > 0 else "—"
    at.add_row("Баланс",   f"[bold]${acc.balance:.2f}[/]")
    at.add_row("PnL",      f"[{pnl_c}]{acc.total_pnl:+.3f}[/]")
    at.add_row("Win/Loss", f"{acc.wins}/{acc.losses} ({wr})")

    if acc.open:
        age = (datetime.now() - acc.open["entry_time"]).seconds
        at.add_row("Угода",   f"[yellow]UP @ {acc.open['entry']:.3f}[/]")
        at.add_row("Вік",     f"{age}с")
        at.add_row("TP",      f"вихід на {TAKE_PROFIT}")
    else:
        lim = round(state.up_price - LIMIT_OFFSET, 3) if state.up_price > 0 else 0
        at.add_row("Стан",   "[dim]очікую...[/]")
        at.add_row("Ліміт",  f"[dim]куплю @ {lim:.3f}[/]")

    lt = Table(box=box.SIMPLE)
    lt.add_column("Час",   width=8)
    lt.add_column("Вхід",  justify="right", width=6)
    lt.add_column("Вихід", justify="right", width=6)
    lt.add_column("PnL",   justify="right", width=8)
    for t in reversed(acc.log[-8:]):
        c = "green" if t["pnl"] > 0 else "red"
        lt.add_row(t["time"], f"{t['entry']:.3f}", f"{t['exit']:.3f}",
                   f"[{c}]{t['pnl']:+.3f}[/]")

    cols = Columns([
        Panel(mt, title="[yellow]РИНОК[/]",         width=36),
        Panel(at, title="[blue]БОТ A — ЛІМІТ[/]",  width=30),
        Panel(lt, title="[yellow]УГОДИ[/]",          width=32),
    ])
    return Panel(cols,
        title="[bold blue]БОТ A: Тільки лімітки[/] — [dim]Ctrl+C зупинити[/]",
        border_style="blue")


# ═══════════════════════════════════════════════
# TRADING LOOP
# ═══════════════════════════════════════════════
async def trading_loop(state: MarketState, acc: Account, db):
    last_slug = ""
    while True:
        await asyncio.sleep(1)

        if state.btc_price == 0 or state.up_price == 0:
            continue

        # Новий контракт — скидаємо відкриту угоду якщо є
        if state.current_slug != last_slug and last_slug != "":
            if acc.open:
                outcome = "win" if state.up_price >= 0.5 else "loss"
                acc.close(state.up_price, f"expiry_{outcome}", db)
        last_slug = state.current_slug

        if not acc.open:
            entry = get_signal(state, acc)
            if entry:
                reason = f"ліміт UP={state.up_price:.3f}→{entry:.3f}"
                acc.enter(entry, state, reason)
        else:
            # Take profit
            if state.up_price >= TAKE_PROFIT:
                acc.close(TAKE_PROFIT, "take_profit", db)
            # Контракт закінчується за 5 секунд
            elif state.seconds_left() <= 5:
                outcome = "win" if state.up_price >= 0.5 else "loss"
                acc.close(state.up_price, f"expiry_{outcome}", db)


# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════
async def main():
    console.print("[bold blue]БОТ A запускається...[/]")
    db    = init_db(DB_PATH)
    state = MarketState()
    acc   = Account()

    async with aiohttp.ClientSession() as session:
        with Live(build_display(state, acc), refresh_per_second=2, console=console) as live:

            async def refresh():
                while True:
                    live.update(build_display(state, acc))
                    await asyncio.sleep(0.5)

            await asyncio.gather(
                run_binance(state),
                run_cvd(state, session),
                run_oi(state, session),
                run_cvd_reset(state),
                run_polymarket(state, session),
                run_polymarket_ws(state),
                trading_loop(state, acc, db),
                refresh(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]БОТ A зупинено. Результати в trades_a.db[/]")
