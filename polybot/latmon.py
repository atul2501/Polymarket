"""Latency monitor in its own process: python latmon.py  ->  runs/latency.json every second

Measures how old Binance BTC/USDT updates are on arrival (exchange event time E, clock-corrected like NTP from the
fastest recent round trip) and the round trips an order would pay (Polymarket CLOB, Polymarket Gamma, Binance REST).
It runs alone because inside a busy process (the dashboard parses ~1,000 Polymarket book events a second) Binance
messages queue up and the "age" measures that backlog instead of the network: 1.6 s there vs 82 ms here.
"""
import collections
import json
import os
import threading
import time
from pathlib import Path

import requests

from feeds import BinanceFeed

OUT = Path(os.getenv("LATMON_OUT", Path(__file__).parent / "runs" / "latency.json"))
http = requests.Session()
bfeed = BinanceFeed()
lat = collections.deque(maxlen=720)                # (t, CLOB ms, Gamma ms, Binance REST ms), every 5 s = 1 hour
clock = {"offset": 0.0, "samples": collections.deque(maxlen=24)}   # Binance server - local, ms; (rtt, offset)


def rtt(url):
    t = time.perf_counter()
    r = http.get(url, timeout=5)
    return (time.perf_counter() - t) * 1000, r


def probe():
    while True:
        try:
            a, _ = rtt("https://clob.polymarket.com/time")
            b, _ = rtt("https://gamma-api.polymarket.com/markets?limit=1")
            t0 = time.time()
            c, r = rtt("https://fapi.binance.com/fapi/v1/time")
            clock["samples"].append((c, r.json()["serverTime"] - (t0 * 1000 + c / 2)))
            clock["offset"] = min(clock["samples"])[1]   # trust the fastest round trip of the last 2 min
            lat.append((time.time(), round(a, 1), round(b, 1), round(c, 1)))
        except Exception:                            # ponytail: a failed probe leaves a gap in the latency line
            pass
        time.sleep(5)


def pctl(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def snapshot(now):
    with bfeed.lock:
        pts = [p for p in bfeed.pts if p[0] > now - 120]
        lags = [l + clock["offset"] for t, l in bfeed.lags if t > now - 60]
    step = max(1, len(pts) // 240)
    return {
        "t": now, "offset": clock["offset"],
        "binance": {"price": pts[-1][1] if pts else None, "age": now - pts[-1][0] if pts else None,
                    "rate": sum(1 for p in pts if p[0] > now - 5) / 5,
                    "series": [[round(t, 2), v] for t, v in pts[::step]],
                    "lag_p50": pctl(lags, .5), "lag_p95": pctl(lags, .95)},
        "history": [list(x) for x in lat.copy()],      # copy: probe() appends from another thread
    }


STREAM = os.getenv("LATMON_STREAM", "1") != "0"   # 0: round trips only, no Binance stream (~0 CPU, small servers)

if __name__ == "__main__":
    if STREAM:
        bfeed.start()
    threading.Thread(target=probe, daemon=True).start()
    tmp = OUT.with_suffix(".tmp")
    while True:
        tmp.write_text(json.dumps(snapshot(time.time())))
        tmp.replace(OUT)                             # atomic: the dashboard never reads half a file
        time.sleep(1)
