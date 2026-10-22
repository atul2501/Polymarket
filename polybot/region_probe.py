"""Where should the bot run? Run this on each candidate server and compare the last line.

    python3 region_probe.py            # 180 s test, prints a report and one RESULT json line
    python3 region_probe.py 600        # longer test (better p95)

Standalone (needs only: requests websockets certifi). Measures what the strategy pays for:
  * geoblock     - polymarket.com/api/geoblock from THIS machine; blocked => region unusable
  * binance_age  - how old Binance futures BTC bookTicker updates are on arrival (event time E -> now)
  * book_age     - how old Polymarket BTC 5m order-book events are on arrival (event timestamp -> now)
  * clob_rtt     - round trip of an HTTPS request to the Polymarket CLOB on a warm connection (an order pays this)
  * reaction     - binance_age + clob_rtt / 2 : Binance moves -> our order lands at Polymarket. Lower wins.
Clock: ages need a correct clock; the Binance server-time offset is measured and applied (AWS clocks are good).
"""
import asyncio
import json
import ssl
import statistics
import sys
import threading
import time

import certifi
import requests
import websockets

SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 180
CTX = ssl.create_default_context(cafile=certifi.where())
BINANCE_WS = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
BOOK_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
http = requests.Session()
res = {"binance": [], "book": [], "clob": [], "binance_rest": [], "reconnects": 0, "book_msgs": 0, "bn_msgs": 0}
offset = {"ms": 0.0}   # Binance server time - local time


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else None


def clock_offset():
    best = None
    for _ in range(8):
        t0 = time.time()
        r = http.get("https://fapi.binance.com/fapi/v1/time", timeout=5).json()
        rtt = (time.time() - t0) * 1000
        off = r["serverTime"] - (t0 * 1000 + rtt / 2)
        if best is None or rtt < best[0]:
            best = (rtt, off)
    return best[1]


def btc_tokens():
    start = int(time.time()) // 300 * 300
    for s in (start, start + 300):
        m = http.get("https://gamma-api.polymarket.com/markets", params={"slug": f"btc-updown-5m-{s}"}, timeout=10).json()
        if m:
            return json.loads(m[0]["clobTokenIds"]), s + 300
    raise RuntimeError("no BTC 5m market found")


async def binance(stop):
    while time.time() < stop:
        try:
            async with websockets.connect(BINANCE_WS, ssl=CTX, open_timeout=10) as ws:
                while time.time() < stop:
                    d = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    res["bn_msgs"] += 1
                    res["binance"].append(time.time() * 1000 + offset["ms"] - d["E"])
        except Exception:
            res["reconnects"] += 1
            await asyncio.sleep(1)


async def book(stop):
    while time.time() < stop:
        try:
            tokens, ends = btc_tokens()
            async with websockets.connect(BOOK_WS, ssl=CTX, open_timeout=10, max_size=None) as ws:
                await ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))
                while time.time() < min(stop, ends):          # next round -> resubscribe to its tokens
                    msg = await asyncio.wait_for(ws.recv(), 15)
                    if msg in ("PONG", ""):
                        continue
                    for ev in (lambda x: x if isinstance(x, list) else [x])(json.loads(msg)):
                        if ev.get("timestamp"):
                            res["book_msgs"] += 1
                            res["book"].append(time.time() * 1000 + offset["ms"] - float(ev["timestamp"]))
        except Exception:
            res["reconnects"] += 1
            await asyncio.sleep(1)


def rtts(stop):
    while time.time() < stop:
        for key, url in (("clob", "https://clob.polymarket.com/time"), ("binance_rest", "https://fapi.binance.com/fapi/v1/time")):
            try:
                t0 = time.perf_counter()
                http.get(url, timeout=5)
                res[key].append((time.perf_counter() - t0) * 1000)
            except requests.RequestException:
                pass
        time.sleep(1)


def main():
    host = requests.get("https://checkip.amazonaws.com", timeout=5).text.strip()
    geo = http.get("https://polymarket.com/api/geoblock", timeout=10).json()
    offset["ms"] = clock_offset()
    print(f"ip {host} | geoblock {geo} | clock offset vs Binance {offset['ms']:+.1f} ms | testing {SECS} s ...", flush=True)
    stop = time.time() + SECS
    threading.Thread(target=rtts, args=(stop,), daemon=True).start()

    async def both():
        await asyncio.gather(binance(stop), book(stop))
    asyncio.run(both())

    s = {k: {"p50": pct(res[k], .5), "p95": pct(res[k], .95), "p99": pct(res[k], .99), "n": len(res[k])}
         for k in ("binance", "book", "clob", "binance_rest")}
    out = {"ip": host, "country": geo.get("country"), "region": geo.get("region"), "blocked": geo.get("blocked"),
           "binance_age_ms": s["binance"], "book_age_ms": s["book"], "clob_rtt_ms": s["clob"],
           "binance_rest_rtt_ms": s["binance_rest"], "reconnects": res["reconnects"],
           "binance_msgs_s": round(res["bn_msgs"] / SECS, 1), "book_msgs_s": round(res["book_msgs"] / SECS, 1)}
    if s["binance"]["p50"] is not None and s["clob"]["p50"] is not None:
        out["reaction_p50_ms"] = round(s["binance"]["p50"] + s["clob"]["p50"] / 2, 1)
        out["reaction_p95_ms"] = round(s["binance"]["p95"] + s["clob"]["p95"] / 2, 1)
    print(f"\n{'':14}{'p50':>9}{'p95':>9}{'p99':>9}{'samples':>9}")
    for k, label in (("binance", "binance age"), ("book", "book age"), ("clob", "clob rtt"), ("binance_rest", "binance rtt")):
        print(f"{label:14}{s[k]['p50'] or '-':>9}{s[k]['p95'] or '-':>9}{s[k]['p99'] or '-':>9}{s[k]['n']:>9}")
    print(f"\nreaction p50 {out.get('reaction_p50_ms')} ms | p95 {out.get('reaction_p95_ms')} ms | reconnects "
          f"{res['reconnects']} | {'BLOCKED - do not trade from here' if geo.get('blocked') else 'not geoblocked'}")
    print("RESULT " + json.dumps(out))


if __name__ == "__main__":
    assert pct([1, 2, 3, 4], .5) == 3 and pct([], .5) is None   # tiny self-check of the percentile helper
    main()
