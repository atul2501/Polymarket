"""Local dashboard for the paper bots: python dashboard.py  ->  http://localhost:8787

Reads the bots' SQLite ledgers and logs read-only, plus the copied wallet's public fills, and serves one page that
refreshes every 2 s. Binds to 127.0.0.1 only. The stop button sends SIGTERM to the bot processes (paper bots only).
Bots: DASH_BOTS="name=ledger.db,name=ledger.db" (paths under runs/), default = the three fast-taker bots.
"""
import json
import os
import signal
import sqlite3
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

RUNS = Path(__file__).parent / "runs"
BOTS = [b.split("=") for b in os.getenv("DASH_BOTS", "T-0.3=tk-d03.db,T-0.1=tk-d01.db,T-3bp=tk-3bp.db").split(",")]
WALLET = "0xc53375ff94e96100f2b30a4b5775db35218d69a9"
PORT = int(os.getenv("DASH_PORT", 8787))
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


def bot_state(name, db_name):
    path = RUNS / db_name
    if not path.exists():
        return {"name": name, "missing": True}
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    rows = con.execute("SELECT ts, side, price, shares, pnl, status, slug FROM trades WHERE status!='open' ORDER BY id").fetchall()
    now = time.time()
    parse = lambda ts: time.mktime(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone
    pts, cum, peak, dd, wins, losses = [], 0.0, 0.0, 0.0, [], []
    day = 0.0
    for ts, side, price, sh, pnl, status, slug in rows:
        cum += pnl
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
        t = parse(ts)
        pts.append([t, round(cum, 2)])
        (wins if pnl > 0 else losses).append(pnl)
        if now - t < 86400:
            day += pnl
    hours = {}
    for ts, _, _, _, pnl, _, _ in rows:
        h = time.strftime("%H:00", time.localtime(parse(ts)))
        hours.setdefault(h, [0, 0.0])
        hours[h][0] += 1
        hours[h][1] += pnl
    by_round = dict(con.execute("SELECT window_end, round(sum(pnl), 2) FROM trades WHERE status!='open' "
                                "GROUP BY window_end ORDER BY window_end DESC LIMIT 12").fetchall())
    open_pos = con.execute("SELECT side, price, shares, cost FROM trades WHERE status='open'").fetchall()
    tick = con.execute("SELECT t, slug, t_left, up_bid, up_ask, dn_bid, dn_ask FROM ticks ORDER BY rowid DESC LIMIT 1").fetchone()
    log = path.with_suffix(".log")
    lines = log.read_text(errors="ignore").splitlines()[-400:] if log.exists() else []
    age = now - log.stat().st_mtime if log.exists() else None
    warns = [l for l in lines if " W " in l][-5:]
    n = len(rows)
    return {
        "name": name, "running": tick is not None and now - tick[0] < 10, "log_age": age, "trades": n, "pnl": round(cum, 2),
        "pnl_24h": round(day, 2), "win_rate": len(wins) / n if n else None,
        "avg_win": sum(wins) / len(wins) if wins else None, "avg_loss": sum(losses) / len(losses) if losses else None,
        "max_dd": round(dd, 2), "series": pts[-2000:], "hours": hours, "by_round": by_round,
        "recent": [{"t": time.strftime("%H:%M:%S", time.localtime(parse(r[0]))), "side": r[1], "price": r[2],
                    "shares": r[3], "pnl": r[4]} for r in rows[-12:]][::-1],
        "open": [{"side": s, "price": p, "shares": sh, "cost": c} for s, p, sh, c in open_pos],
        "tick": dict(zip(["t", "slug", "t_left", "up_bid", "up_ask", "dn_bid", "dn_ask"], tick)) if tick else None,
        "warnings": warns, "last_line": lines[-1][:160] if lines else "",
    }


def wallet_round(slug):
    m = cached(("mk", slug), 3600, lambda: http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug},
                                                    timeout=10).json() or http.get("https://gamma-api.polymarket.com/markets",
                                                    params={"slug": slug, "closed": "true"}, timeout=10).json())
    if not m or isinstance(m, dict):
        return None
    out = {"Up": [0.0, 0.0], "Down": [0.0, 0.0], "fills": 0, "last": None}
    for off in range(0, 10000, 500):
        page = http.get("https://data-api.polymarket.com/trades", params={"user": WALLET, "market": m[0]["conditionId"],
                        "limit": 500, "offset": off, "takerOnly": "false"}, timeout=15).json()
        for x in page:
            out[x["outcome"]][0] += x["size"]
            out[x["outcome"]][1] += x["size"] * x["price"]
            out["fills"] += 1
            out["last"] = max(out["last"] or 0, x["timestamp"])
        if len(page) < 500:
            break
    return out


def wallet_state():
    now = int(time.time())
    cur = now - now % 300
    rounds = []
    for s in range(cur - 300 * 6, cur, 300):
        slug = f"btc-updown-5m-{s}"

        def done(slug=slug):
            m = http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug, "closed": "true"}, timeout=10).json()
            if not m or m[0].get("umaResolutionStatus") != "resolved":
                return None
            p = [float(x) for x in json.loads(m[0]["outcomePrices"])]
            w = json.loads(m[0]["outcomes"])[p.index(max(p))]
            f = wallet_round(slug)
            return {"start": s, "winner": w, "up": f["Up"], "down": f["Down"],
                    "pnl": (f[w][0] - f["Up"][1] - f["Down"][1]) if f["fills"] else 0.0}
        r = cached(("wr", s), 30, done)
        if r and r.get("winner"):
            cache[("wr", s)] = (time.time() + 10 ** 9, r)   # settled: never refetch
            rounds.append(r)
    live = cached(("wl", cur), 10, lambda: wallet_round(f"btc-updown-5m-{cur}"))
    return {"live": live, "live_start": cur, "rounds": rounds[::-1]}


def api_latency():
    t = time.time()
    http.get("https://clob.polymarket.com/time", timeout=5)
    return round((time.time() - t) * 1000)


def state():
    return {"now": time.time(), "bots": [bot_state(n, f) for n, f in BOTS],
            "wallet": cached("wallet", 10, wallet_state), "api_ms": cached("lat", 30, api_latency)}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/state"):
            return self.send(200, json.dumps(state()).encode(), "application/json")
        self.send(200, PAGE.encode(), "text/html; charset=utf-8")

    def do_POST(self):
        if self.path == "/api/stop":
            pids = subprocess.run(["pgrep", "-f", "MacOS/Python (taker|mm).py$"], capture_output=True, text=True).stdout.split()
            for p in pids:
                os.kill(int(p), signal.SIGTERM)
            return self.send(200, json.dumps({"stopped": len(pids)}).encode(), "application/json")
        self.send(404, b"", "text/plain")


PAGE = (Path(__file__).parent / "dashboard.html").read_text()

if __name__ == "__main__":
    print(f"dashboard on http://localhost:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
