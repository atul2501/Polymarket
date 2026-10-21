"""Local dashboard for the paper bots: python dashboard.py  ->  http://localhost:8787

Reads the bots' SQLite ledgers and logs read-only, plus the copied wallet's public fills, and serves one page that
refreshes every second. Binds to 127.0.0.1 only. The stop button sends SIGTERM to the bot processes (paper bots only).

DASH_BOTS   "name=ledger.db,..." (files under runs/)
DASH_SINCE  test start, "YYYY-MM-DD HH:MM" or "HH:MM" today (local time); every number on the page counts from here
DASH_HOURS  test length, for the progress bar (default 24)
"""
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

RUNS = Path(__file__).parent / "runs"
BOTS = [b.split("=") for b in os.getenv("DASH_BOTS", "T-0.3=tk-d03.db,T-0.1=tk-d01.db,T-3bp=tk-3bp.db").split(",")]
WALLET = "0xc53375ff94e96100f2b30a4b5775db35218d69a9"
PORT = int(os.getenv("DASH_PORT", 8787))
_since = os.getenv("DASH_SINCE", time.strftime("%H:%M"))
SINCE = int(time.mktime(time.strptime(_since if len(_since) > 5 else time.strftime("%Y-%m-%d ") + _since, "%Y-%m-%d %H:%M")))
HOURS = float(os.getenv("DASH_HOURS", 24))
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


def bot_state(name, db_name):
    path = RUNS / db_name
    if not path.exists():
        return {"name": name, "missing": True}
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    now = time.time()
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
        "name": name, "running": tick is not None and now - tick[0] < 10, "pnl": round(cum, 2), "cost": round(cost, 2),
        "ret": cum / cost * 100 if cost else None, "trades": n, "open_n": len(rows) - n,
        "win_rate": len(wins) / n if n else None, "avg_win": sum(wins) / len(wins) if wins else None,
        "avg_loss": sum(losses) / len(losses) if losses else None, "max_dd": round(dd, 2), "series": series,
        "by_round": {e: round(p, 2) for e, p in con.execute("SELECT window_end, sum(pnl) FROM trades WHERE status!='open'"
                     " AND window_end > ? GROUP BY window_end ORDER BY window_end DESC LIMIT 12", (SINCE,))},
        "open": [{"side": r[1], "price": r[2], "shares": r[3], "cost": r[6]} for r in rows if r[5] == "open"],
        "feed": [{"bot": name, "t": utc(r[0]), "side": r[1], "price": r[2], "shares": r[3], "status": r[5],
                  "pnl": r[4]} for r in rows[-30:]],
        "tick": dict(zip(["t", "slug", "t_left", "up_bid", "up_ask", "dn_bid", "dn_ask"], tick)) if tick else None,
        "warnings": [l[:200] for l in lines if " W " in l][-4:],
    }


def wallet_round(slug):
    m = cached(("mk", slug), 3600, lambda: http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug},
                                                    timeout=10).json() or http.get("https://gamma-api.polymarket.com/markets",
                                                    params={"slug": slug, "closed": "true"}, timeout=10).json())
    if not m or isinstance(m, dict):
        return None
    out = {"Up": [0.0, 0.0], "Down": [0.0, 0.0], "fills": 0, "last": None, "list": [], "taker_sh": 0.0}
    for off in range(0, 10000, 500):
        page = http.get("https://data-api.polymarket.com/trades", params={"user": WALLET, "market": m[0]["conditionId"],
                        "limit": 500, "offset": off, "takerOnly": "false"}, timeout=15).json()
        for x in page:
            out[x["outcome"]][0] += x["size"]
            out[x["outcome"]][1] += x["size"] * x["price"]
            out["fills"] += 1
            out["last"] = max(out["last"] or 0, x["timestamp"])
            out["list"].append([x["timestamp"], x["outcome"], round(x["size"], 2), x["price"]])
        if len(page) < 500:
            break
    taker = http.get("https://data-api.polymarket.com/trades", params={"user": WALLET, "market": m[0]["conditionId"],
                     "limit": 500, "takerOnly": "true"}, timeout=15).json()
    out["taker_sh"] = sum(x["size"] for x in taker)   # first 500 taker fills is plenty for a share
    out["list"] = sorted(out["list"], reverse=True)[:30]
    return out


settled = {}   # round start -> the wallet's settled round, filled by wallet_history() in the background
live = {}      # current round start -> the wallet's fills so far, refreshed by wallet_live() every 3 s


def wallet_live():
    """Poll the wallet's current round in the background so /api/state never waits on Polymarket."""
    while True:
        cur = int(time.time()) // 300 * 300
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
            "live_start": cur, "rounds": [{k: v for k, v in r.items() if k != "list"} for r in rounds[-8:][::-1]],
            "series": series, "pnl": round(cum, 2), "cost": round(cost, 2), "ret": cum / cost * 100 if cost else None,
            "n": len(rounds), "active": len(active), "won": sum(r["pnl"] > 0 for r in active),
            "avg_pair": sum(pairs) / len(pairs) if pairs else None, "shares": shares,
            "taker_pct": sum(r["taker_sh"] for r in active) / shares * 100 if shares else None,
            "fills": sum(r["fills"] for r in active), "recent": sorted(recent, key=lambda x: -x[0])[:25]}


def api_latency():
    t = time.time()
    http.get("https://clob.polymarket.com/time", timeout=5)
    return round((time.time() - t) * 1000)


def state():
    bots = [bot_state(n, f) for n, f in BOTS]
    feed = sorted((x for b in bots if not b.get("missing") for x in b.pop("feed")), key=lambda x: -x["t"])[:40]
    return {"now": time.time(), "test": {"start": SINCE, "end": SINCE + HOURS * 3600}, "bots": bots, "feed": feed,
            "wallet": wallet_state(), "api_ms": cached("lat", 30, api_latency)}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/state"):
            return self.send(200, json.dumps(state()).encode(), "application/json")
        self.send(200, PAGE_FILE.read_bytes(), "text/html; charset=utf-8")   # re-read: edits show on reload

    def do_POST(self):
        if self.path == "/api/stop":
            pids = subprocess.run(["pgrep", "-f", "MacOS/Python (taker|mm).py$"], capture_output=True, text=True).stdout.split()
            for p in pids:
                os.kill(int(p), signal.SIGTERM)
            return self.send(200, json.dumps({"stopped": len(pids)}).encode(), "application/json")
        self.send(404, b"", "text/plain")


PAGE_FILE = Path(__file__).parent / "dashboard.html"

if __name__ == "__main__":
    threading.Thread(target=wallet_history, daemon=True).start()
    threading.Thread(target=wallet_live, daemon=True).start()
    print(f"dashboard on http://localhost:{PORT} | test from {time.strftime('%Y-%m-%d %H:%M', time.localtime(SINCE))}")
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
