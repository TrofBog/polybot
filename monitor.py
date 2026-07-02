#!/usr/bin/env python3
"""
Живий монітор цін Polymarket BTC 5m — оновлення кожні 0.5с
Запуск: python monitor.py
"""
import asyncio
import json
import sys
import time

import requests
import websockets

POLY_GAMMA = "https://gamma-api.polymarket.com"
POLY_WS    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def get_tokens():
    now  = int(time.time())
    slot = (now // 300) * 300
    slug = f"btc-updown-5m-{slot}"
    ev   = requests.get(f"{POLY_GAMMA}/events?slug={slug}").json()[0]
    m    = ev["markets"][0]
    outcomes = json.loads(m["outcomes"])
    tokens   = json.loads(m["clobTokenIds"])
    up_idx   = 0 if outcomes[0].lower() == "up" else 1
    return slug, tokens[up_idx], tokens[1 - up_idx]


async def main():
    slug, up_id, dn_id = get_tokens()
    up_bids: dict = {}
    up_asks: dict = {}
    last_print = 0.0

    print(f"Контракт: https://polymarket.com/event/{slug}")
    print("Ctrl+C — зупинити\n")

    async with websockets.connect(POLY_WS, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "type": "market",
            "assets_ids": [up_id, dn_id],
            "custom_feature_enabled": True,
        }))

        while True:
            now  = int(time.time())
            slot = (now // 300) * 300
            secs = slot + 300 - now

            if secs <= 0:
                slug, up_id, dn_id = get_tokens()
                up_bids, up_asks = {}, {}
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": [up_id, dn_id],
                    "custom_feature_enabled": True,
                }))
                print(f"\nНовий: https://polymarket.com/event/{slug}")

            try:
                raw  = await asyncio.wait_for(ws.recv(), timeout=0.1)
                msgs = json.loads(raw)
                if not isinstance(msgs, list):
                    msgs = [msgs]

                for msg in msgs:
                    if not isinstance(msg, dict):
                        continue
                    etype = msg.get("event_type")
                    aid   = msg.get("asset_id", "")

                    if etype in ("book", None) and aid == up_id:
                        up_bids = {float(x["price"]): float(x["size"]) for x in msg.get("bids", []) if float(x["size"]) > 0}
                        up_asks = {float(x["price"]): float(x["size"]) for x in msg.get("asks", []) if float(x["size"]) > 0}

                    elif etype == "price_change":
                        for ch in msg.get("price_changes", []):
                            if ch.get("asset_id") != up_id:
                                continue
                            p = float(ch["price"])
                            s = float(ch.get("size", 0))
                            if ch["side"] == "BUY":
                                if s > 0: up_bids[p] = s
                                else:     up_bids.pop(p, None)
                            else:
                                if s > 0: up_asks[p] = s
                                else:     up_asks.pop(p, None)

                    elif etype == "best_bid_ask" and aid == up_id:
                        b = msg.get("best_bid")
                        a = msg.get("best_ask")
                        if b: up_bids[float(b)] = up_bids.get(float(b), 1)
                        if a: up_asks[float(a)] = up_asks.get(float(a), 1)

            except asyncio.TimeoutError:
                pass

            if up_bids and up_asks and time.time() - last_print >= 0.5:
                mid   = (max(up_bids) + min(up_asks)) / 2
                up_c  = round(mid * 100)
                dn_c  = 100 - up_c
                mins  = secs // 60
                sec_r = secs % 60
                arrow = "↑ UP  " if up_c > 50 else ("↓ DOWN" if up_c < 50 else "→     ")
                sys.stdout.write(
                    f"\r{arrow}  UP: {up_c:>2}¢  DOWN: {dn_c:>2}¢  |  {mins}:{sec_r:02d} до кінця   "
                )
                sys.stdout.flush()
                last_print = time.time()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗупинено.")
