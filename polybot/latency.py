"""Latency check for choosing a server: python latency.py

Run it on each candidate machine (your Mac, a VPS in each region you consider) and compare. The bot's edge is
reacting to Binance before Polymarket's book moves, so what matters is how fresh Binance data arrives here plus how
fast an order gets from here to Polymarket. Prints medians; nothing is sent besides public read-only requests.
"""
import asyncio
import json
import statistics
import time

import requests
import websockets

from feeds import BINANCE_WS, CLOB_WS, CTX
from market import MarketFeed

http = requests.Session()
REST = {
    "Polymarket CLOB  /time": "https://clob.polymarket.com/time",
    "Polymarket Gamma /markets": "https://gamma-api.polymarket.com/markets?limit=1",
    "Binance futures  /time": "https://fapi.binance.com/fapi/v1/time",
}


def rest_rtt(url, n=12):
    http.get(url, timeout=10)                     # warm the connection (the bot keeps one open)
    out = []
    for _ in range(n):
        t = time.perf_counter()
        http.get(url, timeout=10)
        out.append((time.perf_counter() - t) * 1000)
    return statistics.median(out)


def binance_clock_offset_ms():
    """Binance server time - local time, corrected by half the round trip."""
    best = None
    for _ in range(8):
        t0 = time.time()
        srv = http.get("https://fapi.binance.com/fapi/v1/time", timeout=10).json()["serverTime"]
        t1 = time.time()
        rtt = t1 - t0
        if best is None or rtt < best[0]:
            best = (rtt, srv - (t0 + t1) / 2 * 1000)
    return best[1]


async def binance_age_ms(offset, n=300):
    """How old Binance futures best-bid/ask updates are when they reach this machine (exchange event time 'E')."""
    ages = []
    async with websockets.connect(BINANCE_WS[0], ssl=CTX, open_timeout=10) as ws:
        while len(ages) < n:
            d = json.loads(await ws.recv())
            ages.append(time.time() * 1000 + offset - d["E"])
    return statistics.median(ages)


async def polymarket_ws(seconds=20):
    """Polymarket order-book socket: connect time and update rate for the current BTC 5m market."""
    m = MarketFeed(http).current(time.time())[0]
    t = time.perf_counter()
    async with websockets.connect(CLOB_WS, ssl=CTX, open_timeout=10, max_size=None) as ws:
        await ws.send(json.dumps({"assets_ids": [m.up_token, m.down_token], "type": "market"}))
        await ws.recv()
        first = (time.perf_counter() - t) * 1000
        n, end = 0, time.time() + seconds
        while time.time() < end:
            try:
                await asyncio.wait_for(ws.recv(), end - time.time())
                n += 1
            except asyncio.TimeoutError:
                break
    return first, n / seconds


def main():
    print("REST round trip (median of 12, warm connection) — an order costs about one of these:")
    for name, url in REST.items():
        print(f"  {name:28s} {rest_rtt(url):7.1f} ms")
    off = binance_clock_offset_ms()
    age = asyncio.run(binance_age_ms(off))
    print(f"Binance futures stream: updates arrive {age:.1f} ms after the exchange event (clock offset {off:+.0f} ms)")
    first, rate = asyncio.run(polymarket_ws())
    print(f"Polymarket book socket: subscribed + first book in {first:.0f} ms, {rate:.0f} updates/s")
    print("Rough reaction time = Binance age + Polymarket CLOB round trip. Lower is better; compare across servers.")


if __name__ == "__main__":
    main()
