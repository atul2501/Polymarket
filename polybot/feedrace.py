"""Which BTC feed shows a move first at THIS server, and how long before Polymarket's book moves? Read-only, no keys.

    python feedrace.py record [hours]     every feed's best bid/ask + Polymarket BTC 5m UP book -> runs/feedrace.db
    python feedrace.py report [db]        per feed: delay, lead vs Binance futures on the same moves, head start
                                          before Polymarket, and the "first of all feeds" signal

Today's signal is Binance futures (Tokyo): ~135-220 ms old when it reaches Stockholm, and Polymarket takes ~340 ms to
match our order, so the book often reprices before we arrive. A feed that shows the same move sooner here is a head
start. Being close isn't enough (a London venue can still follow Binance), so the report compares feeds on the same
moves.
"""
import asyncio
import collections
import json
import sqlite3
import statistics
import sys
import threading
import time
from bisect import bisect_left, bisect_right
from datetime import datetime
from pathlib import Path

import requests
import websockets

from feeds import CTX, BookFeed
from market import MarketFeed

DB = Path(__file__).parent / "runs" / "feedrace.db"
ROWS = collections.deque()       # (table, row) from every feed; flushed to sqlite once a second
F = float


def top(levels):
    """Best price of [[price, size], ...] (first level), or None."""
    return F(levels[0][0]) if levels else None


def iso_ms(s):
    """Coinbase "2026-10-25T05:51:42.123456789Z" -> epoch ms (fromisoformat takes at most 6 fraction digits)."""
    head, _, frac = s.rstrip("Z").partition(".")
    return datetime.fromisoformat(f"{head}.{(frac or '0')[:6]}+00:00").timestamp() * 1000


# name -> (url, subscribe messages, parse(msg dict, last (bid, ask)) -> (bid, ask, exchange ms or None) or None)
FEEDS = {
    "binance-fut": ("wss://fstream.binance.com/ws/btcusdt@bookTicker", [],
                    lambda d, _: (F(d["b"]), F(d["a"]), d["E"]) if "b" in d else None),
    "binance-spot": ("wss://stream.binance.com:9443/ws/btcusdt@bookTicker", [],
                     lambda d, _: (F(d["b"]), F(d["a"]), None) if "b" in d else None),
    "bybit": ("wss://stream.bybit.com/v5/public/linear", [{"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}],
              lambda d, last: (top([x for x in d["data"]["b"] if F(x[1])]) or last[0],
                               top([x for x in d["data"]["a"] if F(x[1])]) or last[1], d["ts"]) if "data" in d else None),
    "okx": ("wss://ws.okx.com:8443/ws/v5/public",
            [{"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}]}],
            lambda d, _: (top(d["data"][0]["bids"]), top(d["data"][0]["asks"]), int(d["data"][0]["ts"]))
            if d.get("data") else None),
    "coinbase": ("wss://advanced-trade-ws.coinbase.com",
                 [{"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"},
                  {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "heartbeats"}],
                 lambda d, _: (F(d["events"][-1]["tickers"][0]["best_bid"]), F(d["events"][-1]["tickers"][0]["best_ask"]),
                               iso_ms(d["timestamp"])) if d.get("channel") == "ticker" and d.get("events") else None),
    "kraken": ("wss://ws.kraken.com/v2",
               [{"method": "subscribe", "params": {"channel": "ticker", "symbol": ["BTC/USD"], "event_trigger": "bbo"}}],
               lambda d, _: (F(d["data"][0]["bid"]), F(d["data"][0]["ask"]), None)
               if d.get("channel") == "ticker" and d.get("data") else None),
    "bitstamp": ("wss://ws.bitstamp.net", [{"event": "bts:subscribe", "data": {"channel": "order_book_btcusd"}}],
                 lambda d, _: (top(d["data"]["bids"]), top(d["data"]["asks"]), int(d["data"]["microtimestamp"]) / 1000)
                 if d.get("event") == "data" else None),
    "deribit": ("wss://www.deribit.com/ws/api/v2",
                [{"jsonrpc": "2.0", "id": 1, "method": "public/subscribe", "params": {"channels": ["quote.BTC-PERPETUAL"]}}],
                lambda d, _: (F(d["params"]["data"]["best_bid_price"]), F(d["params"]["data"]["best_ask_price"]),
                              d["params"]["data"]["timestamp"]) if d.get("method") == "subscription" else None),
    "hyperliquid": ("wss://api.hyperliquid.xyz/ws", [{"method": "subscribe", "subscription": {"type": "bbo", "coin": "BTC"}}],
                    lambda d, _: (F(d["data"]["bbo"][0]["px"]), F(d["data"]["bbo"][1]["px"]), d["data"]["time"])
                    if d.get("channel") == "bbo" and all(d["data"]["bbo"]) else None),
}


async def feed(name):
    url, subs, parse = FEEDS[name]
    last = (None, None)
    while True:
        try:
            async with websockets.connect(url, ssl=CTX, open_timeout=10, ping_interval=15, max_size=None,
                                          max_queue=None) as ws:
                ROWS.append(("events", (time.time(), name, "connect")))
                for s in subs:
                    await ws.send(json.dumps(s))
                while True:
                    raw = await asyncio.wait_for(ws.recv(), 30)
                    now = time.time()
                    if raw in ("pong", "ping"):
                        continue
                    try:
                        got = parse(json.loads(raw), last)
                    except (KeyError, IndexError, TypeError, ValueError):
                        continue                                   # acks, heartbeats, other message kinds
                    if got and got[0] and got[1] and got[:2] != last:
                        last = got[:2]
                        ROWS.append(("quotes", (now, name, (got[0] + got[1]) / 2, got[2])))
        except Exception as e:  # noqa: BLE001 - one dead feed must never stop the others
            ROWS.append(("events", (time.time(), name, repr(e)[:120])))
            await asyncio.sleep(2)


def polymarket():
    """Every change of the current BTC 5m UP best bid/ask, polled from the book socket every 5 ms (own thread)."""
    ws = BookFeed().start()
    mf, last, seen = MarketFeed(requests.Session(), ws), None, time.time()
    while True:
        try:
            now = time.time()
            ms = mf.current(now)
            mf.upcoming(now)
            side = {ms[0].up_token: "Up", ms[0].down_token: "Down"}
            for t, asset, px, sz, taker in ws.trades_since(seen):      # every trade print, ms arrival time: wallet
                if asset in side:                                     # fills are matched to these (its API stamps
                    ROWS.append(("prints", (t, ms[0].slug, side[asset], px, sz, taker)))   # run ~2 s late)
                seen = max(seen, t)
            b = mf.books(ms)[ms[0].up_token]
            cur = (ms[0].slug, b.bid, b.ask)
            if cur != last and b.bid and b.ask:
                last = cur
                ROWS.append(("poly", (now, *cur)))
        except Exception as e:  # noqa: BLE001
            ROWS.append(("events", (time.time(), "polymarket", repr(e)[:120])))
            time.sleep(1)
        time.sleep(0.005)


async def flush(db, end):
    while time.time() < end:
        await asyncio.sleep(1)
        by = collections.defaultdict(list)
        while ROWS:
            t, r = ROWS.popleft()
            by[t].append(r)
        for t, rs in by.items():
            db.executemany(f"INSERT INTO {t} VALUES ({','.join('?' * len(rs[0]))})", rs)
        db.commit()


async def record(hours):
    DB.parent.mkdir(exist_ok=True)
    db = sqlite3.connect(DB)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("CREATE TABLE IF NOT EXISTS quotes(t REAL, feed TEXT, mid REAL, ex REAL);"
                     "CREATE TABLE IF NOT EXISTS poly(t REAL, slug TEXT, bid REAL, ask REAL);"
                     "CREATE TABLE IF NOT EXISTS events(t REAL, feed TEXT, what TEXT);"
                     "CREATE TABLE IF NOT EXISTS prints(t REAL, slug TEXT, outcome TEXT, price REAL, size REAL, side TEXT);")
    threading.Thread(target=polymarket, daemon=True).start()
    tasks = [asyncio.create_task(feed(n)) for n in FEEDS]
    await flush(db, time.time() + hours * 3600)
    for t in tasks:
        t.cancel()


# ---------------------------------------------------------------- report

def series(db, name):
    rows = db.execute("SELECT t, mid FROM quotes WHERE feed=? ORDER BY t", (name,)).fetchall()
    return [r[0] for r in rows], [r[1] for r in rows]


def value_at(ts, vs, t, max_age=30.0):
    # rows are written only when bid/ask CHANGE, so the last row stays the price until the next one (the live bot keeps
    # every update and uses max_age 1 s; 1 s here missed every move that started from a quiet second)
    i = bisect_right(ts, t) - 1
    return vs[i] if i >= 0 and t - ts[i] <= max_age else None


def signals(ts, vs, bps=2.0, window=1.0, gap=2.0):
    """(time, +1/-1) of each first crossing of >= bps over the last window seconds; then quiet for `gap` s."""
    out, quiet = [], 0.0
    for i, t in enumerate(ts):
        if t < quiet:
            continue
        a = value_at(ts, vs, t - window)
        if a and abs(vs[i] / a - 1) * 1e4 >= bps:
            out.append((t, 1 if vs[i] > a else -1))
            quiet = t + gap
    return out


def crossing(ts, vs, t_ref, d, need_bps, lo, hi):
    """First time in [t_ref+lo, t_ref+hi] the feed moved need_bps in direction d from its level at t_ref - 1 s."""
    base = value_at(ts, vs, t_ref - 1)
    if not base:
        return None
    for i in range(bisect_left(ts, t_ref + lo), bisect_right(ts, t_ref + hi)):
        if d * (vs[i] / base - 1) * 1e4 >= need_bps:
            return ts[i]
    return None


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * q))] if xs else None


def report(path):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    t0, t1 = db.execute("SELECT min(t), max(t) FROM quotes").fetchone()
    print(f"{(t1 - t0) / 3600:.1f} h recorded | {time.strftime('%d %b %H:%M', time.gmtime(t0))}-"
          f"{time.strftime('%H:%M', time.gmtime(t1))} UTC\n")
    S = {n: series(db, n) for n in FEEDS}
    S = {n: s for n, s in S.items() if len(s[0]) > 100}
    pts = db.execute("SELECT t, slug, bid, ask FROM poly ORDER BY t").fetchall()
    pt, pm, pslug = [r[0] for r in pts], [(r[2] + r[3]) / 2 for r in pts], [r[1] for r in pts]

    def poly_head(t, d):
        """Seconds from t until the UP mid moves >= 1 c in direction d (same round, within 3 s), or None."""
        i = bisect_right(pt, t) - 1
        if i < 0 or t - pt[i] > 5:
            return None
        start = int(pslug[i].rsplit("-", 1)[1])
        if not 30 <= start + 300 - t <= 290:                      # the bot's own window: not at a round's edges
            return None
        for j in range(i + 1, bisect_right(pt, t + 3)):
            if pslug[j] != pslug[i]:
                return None
            if d * (pm[j] - pm[i]) >= 0.01 - 1e-9:
                return pt[j] - t
        return None

    print(f"{'feed':13} {'updates/s':>9} {'age p50':>8} {'age p95':>8} {'reconn':>6} | {'signals':>7} {'Poly follows':>12} "
          f"{'head start p50':>14}")
    for n, (ts, vs) in S.items():
        ages = [(t - e / 1000) * 1000 for t, e in db.execute("SELECT t, ex FROM quotes WHERE feed=? AND ex IS NOT NULL", (n,))]
        rec = db.execute("SELECT count(*) FROM events WHERE feed=? AND what='connect'", (n,)).fetchone()[0] - 1
        sig = signals(ts, vs)
        heads = [poly_head(t, d) for t, d in sig]
        hit = [h for h in heads if h is not None]
        print(f"{n:13} {len(ts) / (t1 - t0):9.1f} {pct(ages, .5) or 0:8.0f} {pct(ages, .95) or 0:8.0f} {rec:6d} | "
              f"{len(sig):7d} {len(hit) / len(heads) * 100 if heads else 0:11.0f}% "
              f"{(pct(hit, .5) or 0) * 1000:12.0f} ms")

    if "binance-fut" not in S:
        return
    bt, bv = S["binance-fut"]
    ev = signals(bt, bv)
    half = pct([t for t, _ in ev], .5) if ev else 0
    print(f"\nSame moves: {len(ev)} Binance-futures signals (>= 2 bp in 1 s). Lead = how many ms EARLIER the feed showed "
          f"the same move here (both reached 1.5 bp from their own level 1 s before). Positive = faster than Binance.")
    print(f"{'feed':13} {'matched':>7} {'lead p50':>9} {'lead p25':>9} {'lead p75':>9} {'1st half':>9} {'2nd half':>9}")
    first = collections.defaultdict(float)                     # per event: best lead over all feeds
    for n, (ts, vs) in S.items():
        leads = []
        for t, d in ev:
            # both feeds measured at the same 1.5 bp from their own level, so Binance futures vs itself is exactly 0
            c, cb = crossing(ts, vs, t, d, 1.5, -0.6, 0.6), crossing(bt, bv, t, d, 1.5, -0.6, 0.6)
            if c is not None and cb is not None:
                leads.append((t, (cb - c) * 1000))
                first[t] = max(first[t], (cb - c) * 1000)
        a = [l for t, l in leads if t < half]
        b = [l for t, l in leads if t >= half]
        print(f"{n:13} {len(leads):7d} {pct([l for _, l in leads], .5) or 0:9.0f} {pct([l for _, l in leads], .25) or 0:9.0f} "
              f"{pct([l for _, l in leads], .75) or 0:9.0f} {pct(a, .5) or 0:9.0f} {pct(b, .5) or 0:9.0f}")
    if first:
        print(f"\nFirst of all feeds: the earliest feed on each move is {pct(list(first.values()), .5):.0f} ms ahead of "
              f"Binance futures (median), p25 {pct(list(first.values()), .25):.0f} ms.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    if cmd == "record":
        asyncio.run(record(float(sys.argv[2]) if len(sys.argv) > 2 else 6))
    else:
        report(sys.argv[2] if len(sys.argv) > 2 else DB)
