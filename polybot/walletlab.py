"""How does the copied wallet trade? Its fills joined with what THIS server saw (runs/feedrace.db). Read-only.

    python walletlab.py [--since 2026-10-25T06:05] [--refresh]

Prints: WHEN it trades (seconds into the round), HOW (taker share, clip sizes, price vs the book, pair cost, sells),
WHY (which feed and window its buying follows, and whether its inventory drives it) and WHERE the money comes from
(pairs vs one-sided extras). Wallet fills carry whole-second timestamps, so "why" works at 1 s resolution.
Fills are cached in runs/wallet_fills.json (--refresh re-downloads every round).
"""
import argparse
import collections
import json
import sqlite3
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import config
from engine import taker_fee
from feedrace import DB, FEEDS, series, value_at
from market import MarketFeed

WALLET = "0xc53375ff94e96100f2b30a4b5775db35218d69a9"
DATA = "https://data-api.polymarket.com/trades"
CACHE = Path(__file__).parent / "runs" / "wallet_fills.json"


def round_fills(http, cond, taker_only):
    out = []
    for off in range(0, 10000, 500):
        page = http.get(DATA, params={"user": WALLET, "market": cond, "limit": 500, "offset": off,
                                      "takerOnly": str(taker_only).lower(), "_": int(time.time())}, timeout=20).json()
        out += page
        if len(page) < 500:
            break
    return out


def load(starts, refresh):
    """{round start: {"winner": "Up"/"Down", "fills": [(t, outcome, side, price, size, taker)]}} for finished rounds."""
    cache = json.loads(CACHE.read_text()) if CACHE.exists() and not refresh else {}
    http, mf = requests.Session(), MarketFeed(requests.Session())
    for s in starts:
        if str(s) in cache or s + 300 > time.time() - 120:
            continue
        slug = f"btc-updown-5m-{s}"
        w = mf.outcome(slug)
        m = http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug, "closed": "true"}, timeout=10).json()
        if not w or not m:
            continue
        cond = m[0]["conditionId"]
        taker = collections.Counter((f["timestamp"], f["outcome"], f["price"], f["size"]) for f in round_fills(http, cond, True))
        fills = []
        for f in round_fills(http, cond, False):
            k = (f["timestamp"], f["outcome"], f["price"], f["size"])
            fills.append((f["timestamp"], f["outcome"], f["side"], f["price"], f["size"], taker[k] > 0))
            taker[k] -= 1
        cache[str(s)] = {"winner": w, "fills": sorted(fills)}
        print(f"  fetched {slug}: {len(fills)} fills", flush=True)
    CACHE.write_text(json.dumps(cache))
    return {int(k): v for k, v in cache.items() if int(k) in starts}


def corr(xs, ys):
    if len(xs) < 10:
        return 0.0
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sx = sum((x - mx) ** 2 for x in xs) ** 0.5
    sy = sum((y - my) ** 2 for y in ys) ** 0.5
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (sx * sy) if sx and sy else 0.0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="")
    p.add_argument("--refresh", action="store_true")
    a = p.parse_args()
    db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    t0, t1 = db.execute("SELECT min(t), max(t) FROM quotes").fetchone()
    if a.since:
        t0 = max(t0, datetime.fromisoformat(a.since).replace(tzinfo=timezone.utc).timestamp())
    starts = list(range(int(t0) // 300 * 300 + 300, int(t1) // 300 * 300, 300))   # whole rounds inside the recording
    R = load(starts, a.refresh)
    fills = [(t, o, sd, p, s, tk, st) for st, r in R.items() for t, o, sd, p, s, tk in r["fills"]]
    print(f"\n{len(R)} rounds {datetime.fromtimestamp(min(R), timezone.utc):%H:%M}-"
          f"{datetime.fromtimestamp(max(R) + 300, timezone.utc):%H:%M} UTC, {len(fills)} wallet fills\n")

    # ---- WHEN
    print("WHEN  (share of its buying, by seconds into the 5-minute round)")
    by = collections.Counter()
    for t, o, sd, p, s, tk, st in fills:
        by[min(9, int((t - st) // 30))] += s
    tot = sum(by.values())
    print("  " + "  ".join(f"{i * 30:3d}-{i * 30 + 30:<3d}s {by[i] / tot * 100:4.0f}%" for i in range(10)))
    active = [len(r["fills"]) for r in R.values()]
    print(f"  fills per round: median {statistics.median(active):.0f}, rounds with no fills {sum(1 for x in active if not x)}")

    # ---- HOW
    sh = sum(f[4] for f in fills)
    sells = [f for f in fills if f[2] == "SELL"]
    clips = collections.Counter(round(f[4]) for f in fills if f[5])
    print("\nHOW")
    print(f"  taker {sum(f[4] for f in fills if f[5]) / sh * 100:.0f}% of shares, sells {len(sells)} fills, "
          f"common taker clips {', '.join(f'{c} sh x{n}' for c, n in clips.most_common(5))}")
    pts = db.execute("SELECT t, bid, ask FROM poly ORDER BY t").fetchall()
    pt = [r[0] for r in pts]
    rel = collections.Counter()
    for t, o, sd, p, s, tk, st in fills:
        i = __import__("bisect").bisect_right(pt, t + 0.999) - 1   # book at the end of that second
        if i < 0 or not tk:
            continue
        b, ak = pts[i][1], pts[i][2]
        ask = ak if o == "Up" else round(1 - b, 2)
        rel[max(-3, min(3, round((p - ask) * 100)))] += s
    tr = sum(rel.values()) or 1
    print("  taker price vs the ask we saw that second (cents): " +
          "  ".join(f"{k:+d}c {rel[k] / tr * 100:.0f}%" for k in sorted(rel)))
    band = collections.Counter(min(9, int(f[3] * 10)) for f in fills if f[5])
    print("  taker buys by price: " + "  ".join(f"{i / 10:.1f}-{i / 10 + .1:.1f} {band[i]}" for i in range(10)))

    # ---- WHERE the money comes from
    print("\nWHERE the money comes from (per round: pairs = min(UP, DOWN) shares; extras = the one-sided rest)")
    P = E = cost = 0.0
    ew = en = 0
    for st, r in R.items():
        u = [(p, s) for t, o, sd, p, s, tk in r["fills"] if o == "Up" and sd == "BUY"]
        d = [(p, s) for t, o, sd, p, s, tk in r["fills"] if o == "Down" and sd == "BUY"]
        us, ds = sum(s for _, s in u), sum(s for _, s in d)
        if not us and not ds:
            continue
        ua = sum(p * s for p, s in u) / us if us else 0
        da = sum(p * s for p, s in d) / ds if ds else 0
        pairs = min(us, ds)
        P += pairs * (1 - ua - da) if pairs else 0
        side, n, avg = ("Up", us - pairs, ua) if us > ds else ("Down", ds - pairs, da)
        won = r["winner"] == side
        E += n * ((1 if won else 0) - avg)
        ew += won and n > 0
        en += n > 0
        cost += sum(p * s for p, s in u + d)
    fees = sum(taker_fee(f[3], f[4], config.DEFAULT_FEE) for f in fills if f[5] and f[2] == "BUY")
    print(f"  pairs ${P:+.2f} | extras ${E:+.2f} (extras on the winner {ew}/{en} rounds) | total ${P + E:+.2f} on "
          f"${cost:.0f} = {(P + E) / cost * 100:+.2f}% before fees, {(P + E - fees) / cost * 100:+.2f}% after "
          f"${fees:.0f} taker fees (maker fills pay none)")

    # ---- WHY: which feed/window its net taker buying follows
    print("\nWHY  correlation of its net taker buying (UP - DOWN shares per second) with each feed's move")
    net = collections.Counter()
    for t, o, sd, p, s, tk, st in fills:
        if tk and sd == "BUY":
            net[t] += s if o == "Up" else -s
    secs = [s for s in range(int(min(R)), int(max(R)) + 300) if s >= t0 and s <= t1]
    ys = [net[s] for s in secs]
    best = []
    for f in FEEDS:
        ts, vs = series(db, f)
        if len(ts) < 100:
            continue
        for w in (0.3, 1, 3, 5, 10):
            for lag in (0, 1, 2):
                xs = []
                for s in secs:
                    e = s + 1 - lag                                   # move up to the end of the wallet's second
                    a0, a1 = value_at(ts, vs, e - w), value_at(ts, vs, e)
                    xs.append((a1 / a0 - 1) * 1e4 if a0 and a1 else 0.0)
                best.append((corr(xs, ys), f, w, lag))
    best.sort(reverse=True)
    for c, f, w, lag in best[:8]:
        print(f"  {f:13} move over {w:>4}s, {lag}s earlier: corr {c:+.3f}")
    print("  (strongest explains its timing best; compare Deribit vs Binance rows)")

    # ---- TRIGGER: how big a move makes it buy, using the best feed/window above
    c0, f, w, lag = best[0]
    ts, vs = series(db, f)
    print(f"\nTRIGGER  {f} move over {w}s ({lag}s earlier) -> does it buy that side in that second?")
    rows = collections.defaultdict(lambda: [0, 0, 0.0, 0.0])   # bucket -> [seconds, seconds it bought with, shares with, against]
    for s in secs:
        e = s + 1 - lag
        a0, a1 = value_at(ts, vs, e - w), value_at(ts, vs, e)
        if not (a0 and a1):
            continue
        m = (a1 / a0 - 1) * 1e4
        b = next(x for x in (0.25, 0.5, 1, 1.5, 2, 3, 99) if abs(m) < x)
        d = 1 if m >= 0 else -1
        r = rows[b]
        r[0] += 1
        r[1] += d * net[s] > 0
        r[2] += max(0, d * net[s])
        r[3] += max(0, -d * net[s])
    print("  |move| bp   seconds  bought that side   shares with / against the move")
    lo = 0
    for b in sorted(rows):
        n, k, sw, sa = rows[b]
        print(f"  {lo:4}-{b:<5} {n:8d} {k / n * 100:10.0f}%   {sw / n:8.1f} / {sa / n:.1f} per second")
        lo = b

    # ---- INVENTORY: does it buy the side it is short of (completing pairs)?
    xs2, ys2 = [], []
    by_sec = collections.defaultdict(list)
    for t, o, sd, p, sz, tk, st in fills:
        if sd == "BUY":
            by_sec[t].append((o, sz, st))
    hold = {}
    for s in secs:
        st = s - s % 300
        u, dn = hold.get(st, (0.0, 0.0))
        if net[s]:
            xs2.append(dn - u)                                        # > 0: short of UP
            ys2.append(net[s])
        for o, sz, _ in by_sec.get(s, []):
            u, dn = (u + sz, dn) if o == "Up" else (u, dn + sz)
        hold[st] = (u, dn)
    print(f"\nINVENTORY  corr(net buying toward UP, DOWN-minus-UP shares held) = {corr(xs2, ys2):+.3f} over {len(xs2)} "
          f"seconds with buying (positive = it buys the side it is short of)")


if __name__ == "__main__":
    main()
