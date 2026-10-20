"""Analyse a public Polymarket wallet's trades in 5m crypto Up/Down markets.

Run: python wallet.py <address> [max_trades=3000] [ledger.db ...]
Prints a per-round breakdown (shares and average price per side, pair cost, inventory, winner, PnL) and, when ledgers
with recorded ticks are given, how many fills were maker (at/below best bid) vs taker (at the ask).
"""
import bisect
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

DATA = "https://data-api.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
ET = ZoneInfo("America/New_York")


def fetch_trades(http, address, limit):
    out = []
    for off in range(0, limit, 500):
        page = http.get(f"{DATA}/trades", params={"user": address, "limit": 500, "offset": off}, timeout=30).json()
        if not page:
            break
        out += page
    return out


def winner(http, slug):
    rows = http.get(f"{GAMMA}/markets", params={"slug": slug, "closed": "true"}, timeout=10).json()
    if not rows or rows[0].get("umaResolutionStatus") != "resolved":
        return None
    prices = [float(p) for p in json.loads(rows[0]["outcomePrices"])]
    return json.loads(rows[0]["outcomes"])[prices.index(max(prices))]


def rounds(http, trades):
    """One dict per round: shares/cost per side, pair cost, inventory skew, timing, winner, pnl."""
    by = defaultdict(list)
    for t in trades:
        by[t["slug"]].append(t)
    out = []
    for slug in sorted(by):
        xs, start = by[slug], int(slug.rsplit("-", 1)[1])
        side = {o: [t for t in xs if t["outcome"] == o] for o in ("Up", "Down")}
        sh = {o: sum(t["size"] * (1 if t["side"] == "BUY" else -1) for t in side[o]) for o in side}
        cost = {o: sum(t["size"] * t["price"] * (1 if t["side"] == "BUY" else -1) for t in side[o]) for o in side}
        bought = {o: sum(t["size"] for t in side[o] if t["side"] == "BUY") for o in side}
        avg = {o: (sum(t["size"] * t["price"] for t in side[o] if t["side"] == "BUY") / bought[o]) if bought[o] else 0
               for o in side}
        w = winner(http, slug)
        out.append(dict(
            slug=slug, start=start, fills=len(xs), sells=sum(t["side"] == "SELL" for t in xs),
            up_sh=sh["Up"], dn_sh=sh["Down"], up_avg=avg["Up"], dn_avg=avg["Down"], pair=avg["Up"] + avg["Down"],
            cost=cost["Up"] + cost["Down"], skew=sh["Up"] - sh["Down"],
            first=max(start + 300 - t["timestamp"] for t in xs), last=min(start + 300 - t["timestamp"] for t in xs),
            winner=w, pnl=(sh[w] - cost["Up"] - cost["Down"]) if w else None))
    return out


def maker_taker(trades, ledgers):
    """Classify each fill against the book we recorded at the same second (within 2 s)."""
    books = defaultdict(list)
    for db in ledgers:
        for t, slug, ub, ua, dbid, da in sqlite3.connect(db).execute(
                "SELECT t, slug, up_bid, up_ask, dn_bid, dn_ask FROM ticks ORDER BY t"):
            books[slug].append((t, ub, ua, dbid, da))
    kinds, vs_mid = Counter(), []
    for x in trades:
        rows = books.get(x["slug"])
        if not rows:
            continue
        i = bisect.bisect_right([r[0] for r in rows], x["timestamp"]) - 1
        if i < 0 or x["timestamp"] - rows[i][0] > 2:
            continue
        _, ub, ua, dbid, da = rows[i]
        bid, ask = (ub, ua) if x["outcome"] == "Up" else (dbid, da)
        if bid is None or ask is None:
            continue
        p = x["price"]
        kinds["maker (at/below bid)" if p <= bid + 1e-9 else "taker (at/above ask)" if p >= ask - 1e-9 else "inside spread"] += 1
        vs_mid.append(p - (bid + ask) / 2)
    return kinds, (sum(vs_mid) / len(vs_mid) if vs_mid else None)


def main():
    address, limit = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 3000
    http = requests.Session()
    trades = fetch_trades(http, address, limit)
    if not trades:
        print("no trades")
        return
    span = [datetime.fromtimestamp(t["timestamp"]) for t in trades]
    print(f"{address}: {len(trades)} fills, {min(span):%Y-%m-%d %H:%M} -> {max(span):%H:%M} local")
    print("sides", dict(Counter(t["side"] for t in trades)), "| markets", dict(Counter(t["slug"].rsplit("-", 1)[0] for t in trades)))
    print("common fill sizes", Counter(round(t["size"]) for t in trades).most_common(5))
    rs = rounds(http, trades)
    print(f"\n{'round ET':8} {'fills':>5} {'UP sh':>7} {'UP avg':>6} {'DN sh':>7} {'DN avg':>6} {'pair':>6} {'cost $':>8} "
          f"{'skew':>6} {'secs left':>10} {'winner':>6} {'pnl $':>8}")
    for r in rs:
        pnl = "" if r["pnl"] is None else f"{r['pnl']:+8.2f}"
        print(f"{datetime.fromtimestamp(r['start'], ET):%I:%M%p} {r['fills']:5} {r['up_sh']:7.0f} {r['up_avg']:6.3f} "
              f"{r['dn_sh']:7.0f} {r['dn_avg']:6.3f} {r['pair']:6.3f} {r['cost']:8.2f} {r['skew']:6.0f} "
              f"{r['first']:4.0f}->{r['last']:<4.0f} {r['winner'] or '-':>6} {pnl}")
    done = [r for r in rs if r["pnl"] is not None]
    if done:
        print(f"\nresolved rounds {len(done)} | total pnl ${sum(r['pnl'] for r in done):+.2f} | "
              f"avg pair cost {sum(r['pair'] for r in done) / len(done):.3f} | avg $ per round {sum(r['cost'] for r in done) / len(done):.0f}")
    if len(sys.argv) > 3:
        kinds, vs_mid = maker_taker(trades, sys.argv[3:])
        n = sum(kinds.values())
        if n:
            print(f"\nfills matched to recorded books: {n}")
            for k, v in kinds.most_common():
                print(f"  {k:22} {v:5}  {v / n:.0%}")
            print(f"  avg fill vs mid: {vs_mid:+.4f}")


if __name__ == "__main__":
    main()
