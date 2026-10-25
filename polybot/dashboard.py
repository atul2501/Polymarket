"""Local dashboard for the paper bots: python dashboard.py  ->  http://localhost:8787

Reads the bots' SQLite ledgers and logs read-only, plus the copied wallet's public fills, and serves one page that
refreshes every second. Binds to 127.0.0.1 only. The stop button sends SIGTERM to the bot processes (paper bots only).

DASH_BOTS   "name=ledger.db,..." (files under runs/)
DASH_SINCE  test start, "YYYY-MM-DD HH:MM" or "HH:MM" today (local time); every number on the page counts from here
DASH_HOURS  test length, for the progress bar (default 24)
DASH_MARK   log line that marks the test start in each bot log (signal/reconnect counts start after the last one)

The dashboard runs its own Chainlink and Polymarket book feeds for the live "data in" panel; Binance and latency come
from latmon.py (run it alongside: python latmon.py), a separate process so this one's load can't skew them.
"""
import base64
import collections
import hmac
import json
import os
import signal
import sqlite3
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

from feeds import BinanceFeed, BookFeed, ChainlinkFeed
from market import MarketFeed

RUNS = Path(__file__).parent / "runs"
BOTS = [b.split("=") for b in os.getenv("DASH_BOTS", "T-0.3=tk-d03.db,T-0.1=tk-d01.db,T-3bp=tk-3bp.db").split(",")]
WALLET = "0xc53375ff94e96100f2b30a4b5775db35218d69a9"
PORT = int(os.getenv("DASH_PORT", 8787))
HOST = os.getenv("DASH_HOST", "127.0.0.1")       # 0.0.0.0 to open it beyond the machine (then DASH_PASS is required)
AUTH = (os.getenv("DASH_USER", "admin"), os.getenv("DASH_PASS", ""))   # HTTP Basic login; off when DASH_PASS is empty
_since = os.getenv("DASH_SINCE", time.strftime("%H:%M"))
SINCE = int(time.mktime(time.strptime(_since if len(_since) > 5 else time.strftime("%Y-%m-%d ") + _since, "%Y-%m-%d %H:%M")))
HOURS = float(os.getenv("DASH_HOURS", 24))
MARK = os.getenv("DASH_MARK", "--- restart for 24 h test")
GAP_SEC = 2.0          # bots tick every 0.05-0.1 s; a hole longer than this counts as a gap in the recording
http = requests.Session()
cache = {}


def cached(key, ttl, fn):
    t, v = cache.get(key, (0, None))
    if time.time() - t > ttl:
        try:
            v = fn()
        except Exception as e:  # ponytail: show the last good value; a failed fetch is retried next ttl
            v = v if v is not None else {"error": repr(e)}
        cache[key] = (time.time(), v)
    return v


def tail(path, size=65536):
    """Last lines of a log without reading the whole file (they grow to many MB)."""
    if not path.exists():
        return []
    with open(path, "rb") as f:
        f.seek(max(0, f.seek(0, 2) - size))
        return f.read().decode(errors="ignore").splitlines()[1:]


def utc(ts):
    return time.mktime(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone


class Tracker:
    """Per bot, incremental: recording coverage and gaps since the test start, plus counts from the log."""

    def __init__(self, path):
        self.path, self.log = path, path.with_suffix(".log")
        # None until the bot's first row after SINCE: a bot started later is measured from its own start (25 Oct: D-HY2
        # began at 10:26 and showed "1.9% coverage, 11488 s hole" for the hours before it existed)
        self.t_last = self.b_last = self.start = None
        self.rid = {"ticks": 0, "binance": 0}           # last rowid read: rows go in time order, rowid is indexed
        self.ticks = self.bn = self.gaps = self.bn_gaps = 0
        self.max_gap = self.bn_max_gap = 0.0
        self.miss = self.bn_miss = 0.0                 # seconds not covered: interval time beyond 1 s
        self.off = None
        self.c = collections.Counter()

    def _scan(self, con, table, last, gap=GAP_SEC, ok=1.0):
        """ok: an interval up to this long counts as covered. The signal feed (Deribit) can be silent for seconds when BTC
        is quiet (no quote change = nothing to trade), so for it only silences over 5 s count as missing."""
        n = gaps = 0
        worst = miss = 0.0
        for rid, t in con.execute(f"SELECT rowid, t FROM {table} WHERE rowid > ? ORDER BY rowid", (self.rid[table],)):
            self.rid[table] = rid                      # 'WHERE t > ?' scanned the whole 100 MB table: 2.7 s per bot per refresh
            if t <= SINCE or (last is not None and t <= last):
                continue
            if last is None:                           # first row of the test for this bot: its start, not a gap
                self.start = t if self.start is None else min(self.start, t)
                last, n = t, n + 1
                continue
            d = t - last
            if d > gap:
                gaps += 1
            worst = max(worst, d)
            miss += max(0.0, d - ok)
            last = t
            n += 1
        return n, gaps, worst, miss, last

    def update(self, con):
        n, g, w, ms, self.t_last = self._scan(con, "ticks", self.t_last)
        self.ticks += n; self.gaps += g; self.max_gap = max(self.max_gap, w); self.miss += ms
        n, g, w, ms, self.b_last = self._scan(con, "binance", self.b_last, gap=5.0, ok=5.0)
        self.bn += n; self.bn_gaps += g; self.bn_max_gap = max(self.bn_max_gap, w); self.bn_miss += ms
        if not self.log.exists():
            return
        with open(self.log, "rb") as f:
            if self.off is None:                      # first pass: count from the last test-start marker
                data = f.read()
                i = data.rfind(MARK.encode())
                self.off = len(data)
                self._count(data[i:] if i >= 0 else b"")
            else:
                f.seek(self.off)
                data = f.read()
                self.off += len(data)
                self._count(data)

    def _count(self, data):
        for line in data.decode(errors="ignore").splitlines():
            for key in ("SIGNAL", " BUY ", " SELL ", "SETTLED", "reconnecting"):
                if key in line:
                    self.c[key.strip()] += 1

    def view(self, now):
        t0 = self.start or now
        el = max(1.0, min(now, SINCE + HOURS * 3600) - t0)
        tail_gap = now - (self.t_last or now)         # an ongoing hole counts too
        gaps = self.gaps + (tail_gap > GAP_SEC)
        miss = self.miss + max(0.0, tail_gap - 1.0)
        return {"ticks": self.ticks, "tick_rate": self.ticks / el, "gaps": gaps,
                "coverage": max(0.0, 1 - miss / el) * 100, "bn_coverage": max(0.0, 1 - self.bn_miss / el) * 100,
                "max_gap": max(self.max_gap, tail_gap if tail_gap > GAP_SEC else 0),
                "bn": self.bn, "bn_gaps": self.bn_gaps, "bn_max_gap": self.bn_max_gap,
                "signals": self.c["SIGNAL"], "buys": self.c["BUY"], "sells": self.c["SELL"],
                "settled_logs": self.c["SETTLED"], "reconnects": self.c["reconnecting"], "elapsed": el}


trackers = {}


def log_age(path):
    try:
        return time.time() - path.with_suffix(".log").stat().st_mtime
    except OSError:
        return 1e9


def bot_state(name, db_name):
    path = RUNS / db_name
    if not path.exists():
        return {"name": name, "missing": True}
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    now = time.time()
    tr = trackers.setdefault(name, Tracker(path))
    tr.update(con)
    # this test only: rounds that ended after the start
    rows = con.execute("SELECT ts, side, price, shares, pnl, status, cost, window_end FROM trades "
                       "WHERE window_end > ? ORDER BY id", (SINCE,)).fetchall()
    closed = [r for r in rows if r[5] != "open"]
    cum = peak = dd = 0.0
    wins, losses = [], []
    for r in closed:
        cum += r[4]
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
        (wins if r[4] > 0 else losses).append(r[4])
    series, a, c = [], 0.0, 0.0                      # one point per settled round, like the wallet's line
    for e, p, k in con.execute("SELECT window_end, sum(pnl), sum(cost) FROM trades WHERE status!='open' AND "
                               "window_end > ? GROUP BY window_end ORDER BY window_end", (SINCE,)):
        a += p
        c += k
        series.append([e, round(a, 2), round(c, 2)])
    tick = con.execute("SELECT t, slug, t_left, up_bid, up_ask, dn_bid, dn_ask FROM ticks ORDER BY rowid DESC LIMIT 1").fetchone()
    lines = tail(path.with_suffix(".log"))
    cost = sum(r[6] for r in closed)
    n = len(closed)
    return {
        # a bot that records books is alive while they're fresh; a live bot (TK_RECORD=0) logs a status line every 300 s
        "running": now - tick[0] < 10 if tick else log_age(path) < 330,
        "name": name, "pnl": round(cum, 2), "cost": round(cost, 2),
        "ret": cum / cost * 100 if cost else None, "trades": n, "open_n": len(rows) - n,
        "win_rate": len(wins) / n if n else None, "avg_win": sum(wins) / len(wins) if wins else None,
        "avg_loss": sum(losses) / len(losses) if losses else None, "max_dd": round(dd, 2), "series": series,
        "by_round": {e: [round(p, 2), round(k, 2)] for e, p, k in con.execute(   # every settled round: [P/L, $ spent]
                     "SELECT window_end, sum(pnl), sum(cost) FROM trades WHERE status!='open' AND window_end > ?"
                     " GROUP BY window_end", (SINCE,))},
        # end: the round's end, so the page can tell this round's positions from ended ones waiting for settlement
        "open": [{"side": r[1], "price": r[2], "shares": r[3], "cost": r[6], "end": r[7]} for r in rows if r[5] == "open"],
        "feed": [{"bot": name, "t": utc(r[0]), "side": r[1], "price": r[2], "shares": r[3], "status": r[5],
                  "pnl": r[4]} for r in rows[-30:]],
        "tick": dict(zip(["t", "slug", "t_left", "up_bid", "up_ask", "dn_bid", "dn_ask"], tick)) if tick else None,
        "warnings": [l[:200] for l in lines if " W " in l][-4:], "integrity": tr.view(now),
        "rounds_traded": con.execute("SELECT count(DISTINCT window_end) FROM trades WHERE window_end > ?", (SINCE,)).fetchone()[0],
    }


# ---- the dashboard's own live feeds (data-in panel)
cfeed, book = ChainlinkFeed(["btc/usd"]), BookFeed()
SIGNAL = os.getenv("DASH_SIGNAL", "binance")     # "deribit": the trigger tile shows Deribit (the live bot's signal)
sfeed = BinanceFeed()
sfeed.SOURCE = SIGNAL


def signal_state(now):
    """The live bot's own signal feed (Deribit), straight from its socket in this process."""
    with sfeed.lock:
        pts = [p for p in sfeed.pts if p[0] > now - 120]
        lag = sfeed.lags[-1][1] / 1000 if sfeed.lags else None
    if not pts:
        return None
    step = max(1, len(pts) // 240)
    return {"price": pts[-1][1], "age": lag, "rate": sum(1 for t, _ in pts if t > now - 5) / 5,
            "series": [[round(t, 2), v] for t, v in pts[::step]]}
mfeed = MarketFeed(http, book)
upmid = collections.deque(maxlen=240)            # (t, UP mid) sampled every 0.5 s
feedmeta = {"market": None, "msgs": [], "cl": []}


def sampler():
    """Keep the book feed on the current market and sample its UP mid; track message rates."""
    while True:
        now = time.time()
        try:
            m = mfeed.current(now)
            if m:
                m = m[0]
                if feedmeta["market"] is None or feedmeta["market"].slug != m.slug:
                    feedmeta["market"] = m
                    book.watch([m.up_token, m.down_token])
                b = book.book(m.up_token, max_age=5)
                if b and b.bid is not None and b.ask is not None:
                    upmid.append((now, (b.bid + b.ask) / 2, b.bid, b.ask))
        except Exception:                          # ponytail: a missed sample just leaves a hole in the sparkline
            pass
        feedmeta["msgs"] = (feedmeta["msgs"] + [(now, book.msgs)])[-20:]
        time.sleep(0.5)


def bot_binance(now):
    """Binance mids the first bot recorded itself (~10/s), for when latmon.py isn't running (e.g. a small server)."""
    path = RUNS / BOTS[0][1]
    if not path.exists():
        return None
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    last = con.execute("SELECT max(rowid) FROM binance").fetchone()[0] or 0
    pts = con.execute("SELECT t, mid FROM binance WHERE rowid > ? AND t > ? ORDER BY rowid", (last - 1500, now - 120)).fetchall()
    if not pts:
        return None
    step = max(1, len(pts) // 240)
    return {"price": pts[-1][1], "age": now - pts[-1][0], "rate": sum(1 for t, _ in pts if t > now - 5) / 5,
            "series": [[round(t, 2), v] for t, v in pts[::step]]}


def feeds_state(now):
    lm = (latmon() or {}).get("binance") or {}
    mb = ((signal_state(now) if SIGNAL != "binance" else None) or (lm if lm.get("price") is not None else None)
          or bot_binance(now) or {"price": None, "age": None, "rate": 0, "series": []})
    with cfeed.lock:
        cl = sorted((t, v) for t, v in cfeed.points.get("btc/usd", {}).items() if t > now - 120)
    msgs = feedmeta["msgs"]
    rate_p = (msgs[-1][1] - msgs[0][1]) / max(0.5, msgs[-1][0] - msgs[0][0]) if len(msgs) > 1 else 0
    m = feedmeta["market"]
    tape = []
    if m:
        side = {m.up_token: "Up", m.down_token: "Down"}
        with book.lock:
            tape = [[t, side[a], px, sz, ts] for t, a, px, sz, ts in book.prints[-200:] if a in side][-30:][::-1]
    u = upmid[-1] if upmid else None
    return {
        "binance": {k: mb.get(k) for k in ("price", "age", "rate", "series")}, "signal": SIGNAL,
        "chainlink": {"price": cl[-1][1] if cl else None, "age": now - cfeed.last_msg if cfeed.last_msg else None,
                      "rate": sum(1 for t, _ in cl if t > now - 10) / 10, "series": cl},
        "polymarket": {"mid": u[1] if u else None, "bid": u[2] if u else None, "ask": u[3] if u else None,
                       "age": now - book.updated.get(m.up_token, 0) if m else None, "rate": rate_p,
                       "slug": m.slug if m else None, "beat": beat.get(int(m.slug.split("-")[-1])) if m else None,
                       "series": [[round(t, 2), v] for t, v, *_ in upmid]},
        "tape": tape,
    }


def wallet_round(slug):
    m = cached(("mk", slug), 3600, lambda: http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug},
                                                    timeout=10).json() or http.get("https://gamma-api.polymarket.com/markets",
                                                    params={"slug": slug, "closed": "true"}, timeout=10).json())
    if not m or isinstance(m, dict):
        return None
    out = {"Up": [0.0, 0.0], "Down": [0.0, 0.0], "fills": 0, "last": None, "list": [], "taker_sh": 0.0}
    # "_": Cloudflare caches data-api answers 300 s; the empty answer from a round's first seconds stuck (25 Oct)
    for off in range(0, 10000, 500):
        page = http.get("https://data-api.polymarket.com/trades", params={"user": WALLET, "market": m[0]["conditionId"],
                        "limit": 500, "offset": off, "takerOnly": "false", "_": int(time.time())}, timeout=15).json()
        for x in page:
            out[x["outcome"]][0] += x["size"]
            out[x["outcome"]][1] += x["size"] * x["price"]
            out["fills"] += 1
            out["last"] = max(out["last"] or 0, x["timestamp"])
            out["list"].append([x["timestamp"], x["outcome"], round(x["size"], 2), x["price"]])
        if len(page) < 500:
            break
    taker = http.get("https://data-api.polymarket.com/trades", params={"user": WALLET, "market": m[0]["conditionId"],
                     "limit": 500, "takerOnly": "true", "_": int(time.time())}, timeout=15).json()
    out["taker_sh"] = sum(x["size"] for x in taker)   # first 500 taker fills is plenty for a share
    out["list"] = sorted(out["list"], reverse=True)[:30]
    return out


settled = {}   # round start -> the wallet's settled round, filled by wallet_history() in the background
live = {}      # current round start -> the wallet's fills so far, refreshed by wallet_live() every 3 s
beat = {}      # round start -> Polymarket's own price to beat (Chainlink open), fetched once per round


def price_to_beat(start):
    iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
    r = http.get("https://polymarket.com/api/crypto/crypto-price", timeout=10, params={
        "symbol": "BTC", "eventStartTime": iso(start), "variant": "fiveminute", "endDate": iso(start + 300)}).json()
    return r.get("openPrice")


def wallet_live():
    """Poll the wallet's current round in the background so /api/state never waits on Polymarket."""
    while True:
        cur = int(time.time()) // 300 * 300
        try:
            if beat.get(cur) is None:
                beat[cur] = price_to_beat(cur)
        except Exception:                      # ponytail: card shows "—" until the next 3 s retry
            pass
        try:
            live[cur] = wallet_round(f"btc-updown-5m-{cur}")
            for k in [k for k in live if k < cur]:
                del live[k]
        except Exception:                      # ponytail: keep the last good snapshot, retry in 3 s
            pass
        time.sleep(3)


def wallet_history():
    """Keep every settled round of the test: the chart needs all of them and each costs a few API calls."""
    first = SINCE - SINCE % 300 + (300 if SINCE % 300 else 0)   # first round that starts inside the test
    while True:
        now = int(time.time())
        for s in range(first, now - now % 300, 300):
            if s in settled:
                continue
            try:
                slug = f"btc-updown-5m-{s}"
                m = http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug, "closed": "true"},
                             timeout=10).json()
                if not m or m[0].get("umaResolutionStatus") != "resolved":
                    break                      # later rounds aren't resolved either
                p = [float(x) for x in json.loads(m[0]["outcomePrices"])]
                w = json.loads(m[0]["outcomes"])[p.index(max(p))]
                f = wallet_round(slug)
                settled[s] = {"start": s, "winner": w, "up": f["Up"], "down": f["Down"], "fills": f["fills"],
                              "taker_sh": f["taker_sh"], "cost": f["Up"][1] + f["Down"][1], "list": f["list"][:15],
                              "pnl": (f[w][0] - f["Up"][1] - f["Down"][1]) if f["fills"] else 0.0}
            except Exception:                  # ponytail: retry the round on the next pass
                break
        time.sleep(10)


def wallet_state():
    now = int(time.time())
    cur = now - now % 300
    rounds = [settled[s] for s in sorted(settled)]
    series, cum, cost = [], 0.0, 0.0
    for r in rounds:
        cum += r["pnl"]
        cost += r["cost"]
        series.append([r["start"] + 300, round(cum, 2), round(cost, 2)])
    lv = live.get(cur)
    active = [r for r in rounds if r["fills"]]
    pairs = [r["up"][1] / r["up"][0] + r["down"][1] / r["down"][0] for r in active if r["up"][0] and r["down"][0]]
    shares = sum(r["up"][0] + r["down"][0] for r in active)
    recent = ([[*x, "live"] for x in lv["list"]] if isinstance(lv, dict) and lv.get("list") else []) + \
             [[*x, r["winner"]] for r in reversed(rounds[-2:]) for x in r["list"]]
    return {"live": {k: v for k, v in lv.items() if k != "list"} if isinstance(lv, dict) else None,
            "live_start": cur, "rounds": [{k: v for k, v in r.items() if k != "list"} for r in rounds[::-1]],
            "series": series, "pnl": round(cum, 2), "cost": round(cost, 2), "ret": cum / cost * 100 if cost else None,
            "n": len(rounds), "active": len(active), "won": sum(r["pnl"] > 0 for r in active),
            "avg_pair": sum(pairs) / len(pairs) if pairs else None, "shares": shares,
            "taker_pct": sum(r["taker_sh"] for r in active) / shares * 100 if shares else None,
            "fills": sum(r["fills"] for r in active), "recent": sorted(recent, key=lambda x: -x[0])[:25]}


def pctl(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


LATFILE = RUNS / "latency.json"


def latmon():
    """Latest snapshot from latmon.py (a separate process, so this one's load can't inflate the numbers)."""
    try:
        d = json.loads(LATFILE.read_text())
        return d if time.time() - d["t"] < 10 else None
    except (OSError, ValueError, KeyError):
        return None


def latency_state(now):
    m = latmon()
    with book.lock:
        pl = [l for t, l in book.lags if t > now - 60]
    with cfeed.lock:
        pts = cfeed.points.get("btc/usd", {})
        cl_t = max(pts) if pts else None
    hist = [x for x in (m["history"] if m else []) if x[0] > now - 3600]
    recent = [x for x in hist if x[0] > now - 300]
    clob = pctl([x[1] for x in recent], .5)
    b, b95 = (m["binance"]["lag_p50"], m["binance"]["lag_p95"]) if m else (None, None)
    if b is None and (lv := live_status(now)) and lv.get("binance_lag"):   # latmon without its stream: the bot's own
        b, b95 = lv["binance_lag"]
    return {
        "monitor": m is not None, "signal": (m or {}).get("signal", "binance"),
        "clob": {"now": hist[-1][1] if hist else None, "p50": clob, "p95": pctl([x[1] for x in recent], .95)},
        "gamma": {"now": hist[-1][2] if hist else None, "p50": pctl([x[2] for x in recent], .5)},
        "binance_rest": {"now": hist[-1][3] if hist else None, "p50": pctl([x[3] for x in recent], .5)},
        "binance_stream": {"p50": b, "p95": b95},
        "book_stream": {"p50": pctl(pl, .5), "p95": pctl(pl, .95)},
        "chainlink_age": (now - cl_t) * 1000 if cl_t else None,
        "offset": m["offset"] if m else None,
        "reaction": b + clob if b is not None and clob is not None else None,
        "history": hist,
    }


def live_status(now):
    """The live bot's mode + USDC balance (runs/live.json, written each minute), or None if no live bot runs."""
    try:
        s = json.loads((RUNS / "live.json").read_text())
    except (OSError, ValueError):
        return None
    return s if now - s.get("t", 0) < 180 else None


def state():
    bots = [bot_state(n, f) for n, f in BOTS]
    feed = sorted((x for b in bots if not b.get("missing") for x in b.pop("feed")), key=lambda x: -x["t"])[:40]
    now = time.time()
    return {"now": now, "test": {"start": SINCE, "end": SINCE + HOURS * 3600}, "bots": bots, "feed": feed,
            "feeds": feeds_state(now), "gap_sec": GAP_SEC,
            "wallet": wallet_state(), "latency": (L := latency_state(now)), "api_ms": L["clob"]["now"],
            "live": live_status(now)}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def authed(self):
        """True if no password is set, or the request carries the right Basic login; else asks the browser for it."""
        if not AUTH[1]:
            return True
        want = "Basic " + base64.b64encode(f"{AUTH[0]}:{AUTH[1]}".encode()).decode()
        if hmac.compare_digest(self.headers.get("Authorization", ""), want):
            return True
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="polybot", charset="UTF-8"')
        self.send_header("Content-Length", "0")
        self.end_headers()
        return False

    def do_GET(self):
        if not self.authed():
            return
        if self.path.startswith("/api/state"):
            return self.send(200, json.dumps(state()).encode(), "application/json")
        self.send(200, PAGE_FILE.read_bytes(), "text/html; charset=utf-8")   # re-read: edits show on reload

    def do_POST(self):
        if not self.authed():
            return
        if self.path == "/api/stop":
            (RUNS / "STOP").touch()                  # live bots stop opening trades (delete the file to resume)
            pids = subprocess.run(["pgrep", "-f", "MacOS/Python (taker|mm).py$"], capture_output=True, text=True).stdout.split()
            for p in pids:
                os.kill(int(p), signal.SIGTERM)
            return self.send(200, json.dumps({"stopped": len(pids)}).encode(), "application/json")
        self.send(404, b"", "text/plain")


PAGE_FILE = Path(__file__).parent / "dashboard.html"

if __name__ == "__main__":
    threading.Thread(target=wallet_history, daemon=True).start()
    threading.Thread(target=wallet_live, daemon=True).start()
    cfeed.start(); book.start()
    if SIGNAL != "binance":
        sfeed.start()
    threading.Thread(target=sampler, daemon=True).start()
    print(f"dashboard on http://localhost:{PORT} | test from {time.strftime('%Y-%m-%d %H:%M', time.localtime(SINCE))}")
    if HOST not in ("127.0.0.1", "localhost") and not AUTH[1]:
        raise SystemExit("DASH_HOST opens the dashboard beyond this machine: set DASH_PASS (and DASH_USER) first")
    ThreadingHTTPServer((HOST, PORT), H).serve_forever()
