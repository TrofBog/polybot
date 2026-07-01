#!/usr/bin/env python3
"""
БОТ B — Лімітки + CVD фільтр + аналіз стін (реальна/фейкова)
Запуск: python bot_b.py
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
STAKE_PCT        = 0.08
LIMIT_OFFSET     = 0.03
TAKE_PROFIT      = 0.87
CVD_MIN_DELTA    = 50.0
MIN_UP_PRICE     = 0.75   # не входимо якщо ціна нижче 0.75
ENTRY_SEC_MIN    = 90     # вікно входу: 90-150с до кінця
ENTRY_SEC_MAX    = 150
DB_PATH          = "trades_b.db"

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
        self.skipped   = 0   # скільки разів фільтр заблокував вхід

    def enter(self, entry: float, state: MarketState, reason: str, wall: str = "") -> bool:
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
            "wall":       wall,
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
        self.balance   += STAKE + pnl
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
            "wall":    self.open.get("wall", ""),
        })
        self.open = None
        return pnl


# ═══════════════════════════════════════════════
# СТРАТЕГІЯ B
# ═══════════════════════════════════════════════
def get_signal(state: MarketState, acc: Account):
    """
    Повертає (entry_price, wall_note) або None.
    Фільтри: CVD не ведмежий + стіна реальна (якщо є).
    """
    if acc.open or state.up_price <= 0:
        return None

    limit = round(state.up_price - LIMIT_OFFSET, 3)
    if not (0.35 <= limit <= 0.72):
        return None

    secs = state.seconds_left()
    if not (ENTRY_SEC_MIN <= secs <= ENTRY_SEC_MAX):
        return None

    if state.up_price < MIN_UP_PRICE:
        return None

    cvd_d = state.cvd_delta()

    # Блокуємо якщо CVD явно ведмежий
    if cvd_d < -CVD_MIN_DELTA:
        acc.skipped += 1
        return None

    # Перевірка стін
    wall_note = "стін немає"
    bw = state.bid_walls()
    if bw:
        top_p, top_s = bw[0]
        real, reason = state.check_wall(top_p, top_s)
        if not real:
            acc.skipped += 1
            return None
        wall_note = f"${top_p:.0f}×{top_s:.1f}BTC ({reason})"

    return limit, wall_note


# ═══════════════════════════════════════════════
# DISPLAY
# ═══════════════════════════════════════════════
def build_display(state: MarketState, acc: Account) -> Panel:
    cvd_d = state.cvd_delta()
    cvd_c = "green" if cvd_d > CVD_MIN_DELTA else ("red" if cvd_d < -CVD_MIN_DELTA else "yellow")
    cvd_label = "бичачий ✓" if cvd_d > CVD_MIN_DELTA else ("ведмежий ✗" if cvd_d < -CVD_MIN_DELTA else "нейтральний")

    bw = state.bid_walls()
    aw = state.ask_walls()

    # Статус стіни
    wall_status = "—"
    if bw:
        top_p, top_s = bw[0]
        # Підглядаємо без записування
        if top_p in state.wall_tracker:
            prev_s, _ = state.wall_tracker[top_p]
            if top_s < prev_s * 0.3:
                wall_status = f"[red]ФЕЙК ${top_p:.0f}×{top_s:.1f}BTC[/]"
            else:
                wall_status = f"[green]РЕАЛЬНА ${top_p:.0f}×{top_s:.1f}BTC[/]"
        else:
            wall_status = f"[dim]нова ${top_p:.0f}×{top_s:.1f}BTC[/]"

    mt = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    mt.add_row("BTC ф'ючерс",  f"[bold]${state.btc_price:,.1f}[/]")
    mt.add_row("UP  (Poly)",   f"[bold cyan]{state.up_price:.3f}[/]")
    mt.add_row("DOWN (Poly)",  f"[dim]{state.down_price:.3f}[/]")
    mt.add_row("CVD Δ (5хв)", f"[{cvd_c}]{cvd_d:+.1f} BTC — {cvd_label}[/]")
    mt.add_row("OI",           state.oi_trend())
    mt.add_row("BID стіна",   wall_status)
    mt.add_row("ASK стіна",   f"${aw[0][0]:.0f}×{aw[0][1]:.1f}BTC" if aw else "—")
    mt.add_row("Контракт",    state.market_title[:38] if state.market_title else "—")
    mt.add_row("Залишилось",  f"[yellow]{state.seconds_left()}с[/]")

    at = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    pnl_c = "green" if acc.total_pnl >= 0 else "red"
    wr = f"{acc.wins/(acc.wins+acc.losses)*100:.0f}%" if (acc.wins+acc.losses) > 0 else "—"
    at.add_row("Баланс",    f"[bold]${acc.balance:.2f}[/]")
    at.add_row("PnL",       f"[{pnl_c}]{acc.total_pnl:+.3f}[/]")
    at.add_row("Win/Loss",  f"{acc.wins}/{acc.losses} ({wr})")
    at.add_row("Пропущено", f"[dim]{acc.skipped} (фільтр)[/]")

    if acc.open:
        age = (datetime.now() - acc.open["entry_time"]).seconds
        at.add_row("Угода",  f"[yellow]UP @ {acc.open['entry']:.3f}[/]")
        at.add_row("Вік",    f"{age}с")
        at.add_row("Стіна",  f"[dim]{acc.open.get('wall','—')[:25]}[/]")
        at.add_row("TP",     f"вихід на {TAKE_PROFIT}")
    else:
        lim = round(state.up_price - LIMIT_OFFSET, 3) if state.up_price > 0 else 0
        at.add_row("Стан",  "[dim]очікую сигнал...[/]")
        at.add_row("Ліміт", f"[dim]куплю @ {lim:.3f}[/]")
        at.add_row("CVD ok?", "[green]так[/]" if cvd_d >= -CVD_MIN_DELTA else "[red]ні — блок[/]")

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
        Panel(mt, title="[yellow]РИНОК + СИГНАЛИ[/]",          width=40),
        Panel(at, title="[magenta]БОТ B — ЛІМІТ+CVD+СТІНИ[/]", width=32),
        Panel(lt, title="[yellow]УГОДИ[/]",                      width=30),
    ])
    return Panel(cols,
        title="[bold magenta]БОТ B: Лімітки + CVD + Стіни[/] — [dim]Ctrl+C зупинити[/]",
        border_style="magenta")


# ═══════════════════════════════════════════════
# TRADING LOOP
# ═══════════════════════════════════════════════
async def trading_loop(state: MarketState, acc: Account, db):
    last_slug = ""
    while True:
        await asyncio.sleep(1)

        if state.btc_price == 0 or state.up_price == 0:
            continue

        if state.current_slug != last_slug and last_slug != "":
            if acc.open:
                outcome = "win" if state.up_price >= 0.5 else "loss"
                acc.close(state.up_price, f"expiry_{outcome}", db)
        last_slug = state.current_slug

        if not acc.open:
            result = get_signal(state, acc)
            if result:
                entry, wall_note = result
                cvd_d = state.cvd_delta()
                reason = f"ліміт+CVD{cvd_d:+.0f}"
                acc.enter(entry, state, reason, wall_note)
        else:
            if state.up_price >= TAKE_PROFIT:
                acc.close(TAKE_PROFIT, "take_profit", db)
            elif state.seconds_left() <= 5:
                outcome = "win" if state.up_price >= 0.5 else "loss"
                acc.close(state.up_price, f"expiry_{outcome}", db)


# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════
async def main():
    console.print("[bold magenta]БОТ B запускається...[/]")
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
        console.print("\n[yellow]БОТ B зупинено. Результати в trades_b.db[/]")
