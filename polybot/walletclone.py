"""How close are the clone bots to the copied wallet? Read-only; run on the server (needs runs/feedrace.db).

    python walletclone.py fetch [--since EPOCH]     wallet fills of every finished round, matched to our ms trade
                                                    prints -> runs/clone_fills.json (data-api stamps run ~2 s late)
    python walletclone.py compare runs/wclone.db [more ledgers]   per bot vs the wallet on the same rounds:
                                                    P/L, %/$, per-round correlation, share of the wallet's taker buys
                                                    the bot also decided (same side, within 1 s)
    python walletclone.py triggers                  which feed moved toward the side just before its taker buys

Found 26 Oct (29 rounds, 3,093 fills): it buys both sides every round, ask 0.10-0.90, from ~3 s in until 30 s left,
30-share clips at the ask, after Deribit moved >= ~0.2 bp toward the side within 0.5 s (53% of its buys vs 25% of random
moments). A replay clone with that rule decided 85-88% of its buys; it earns +2.2 %/$ with instant fills, 0 at 500 ms.
"""
import argparse
import bisect
import json
import random
import sqlite3
import statistics as st
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests

import config
import walletlab
from engine import taker_fee
from market import MarketFeed

RUNS = Path(__file__).parent / "runs"
FILLS = RUNS / "clone_fills.json"
FR = RUNS / "feedrace.db"


def finished_rounds(since, until):
    return list(range(int(since) // 300 * 300 + 300, int(until) // 300 * 300 - 300, 300))


def fetch(since):
    db = sqlite3.connect(f"file:{FR}?mode=ro", uri=True)
    prints = defaultdict(list)
    for t, slug, o, px, sz in db.execute("SELECT t, slug, outcome, price, size FROM prints WHERE t > ?", (since - 60,)):
        prints[(slug, o)].append((t, px, sz))
    end = db.execute("SELECT max(t) FROM prints").fetchone()[0]
    old = {f["slug"] for f in json.loads(FILLS.read_text())} if FILLS.exists() else set()
    fills = json.loads(FILLS.read_text()) if FILLS.exists() else []
    http, mf = requests.Session(), MarketFeed(requests.Session())
    for s in finished_rounds(since, end):
        slug = f"btc-updown-5m-{s}"
        if slug in old:
            continue
        w = mf.outcome(slug)
        m = http.get("https://gamma-api.polymarket.com/markets", params={"slug": slug, "closed": "true"}, timeout=10).json()
        if not w or not m:
            continue
        cond = m[0]["conditionId"]
        tk = Counter((f["timestamp"], f["outcome"], f["price"], f["size"]) for f in walletlab.round_fills(http, cond, True))
        used = set()
        for f in sorted(walletlab.round_fills(http, cond, False), key=lambda f: f["timestamp"]):
            k = (f["timestamp"], f["outcome"], f["price"], f["size"])
            taker = tk[k] > 0
            tk[k] -= 1
            px, sz, ts = float(f["price"]), float(f["size"]), f["timestamp"]
            cand = [(abs(p[0] - ts - 0.5), i, p[0]) for i, p in enumerate(prints[(slug, f["outcome"])])
                    if i not in used and abs(p[1] - px) < 1e-6 and abs(p[2] - sz) < 0.011 and ts - 4 <= p[0] <= ts + 1.5]
            t = None
            if cand:
                _, i, t = min(cand)
                used.add(i)
            fills.append(dict(slug=slug, start=s, winner=w, t_api=ts, t=t, outcome=f["outcome"], side=f["side"],
                              price=px, size=sz, taker=taker))
        time.sleep(0.25)
    FILLS.write_text(json.dumps(fills))
    n = sum(1 for f in fills if f["t"])
    print(f"{len({f['slug'] for f in fills})} rounds, {len(fills)} fills, {n} matched to a ms print ({100 * n / max(1, len(fills)):.0f}%)")


def wallet_rounds(fills):
    """slug -> (money in, P/L after taker fees on its taker fills)"""
    r = defaultdict(lambda: [0.0, 0.0])
    for f in fills:
        if f["side"] != "BUY":
            continue
        fee = taker_fee(f["price"], f["size"], config.DEFAULT_FEE) if f["taker"] else 0.0
        r[f["slug"]][0] += f["price"] * f["size"]
        r[f["slug"]][1] += (f["size"] if f["outcome"] == f["winner"] else 0) - f["price"] * f["size"] - fee
    return r


def compare(ledgers):
    fills = json.loads(FILLS.read_text())
    W = wallet_rounds(fills)
    wb = defaultdict(list)
    for f in fills:
        if f["t"] and f["taker"]:
            wb[(f["slug"], f["outcome"])].append(f["t"])
    for v in wb.values():
        v.sort()
    for path in ledgers:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        B = defaultdict(lambda: [0.0, 0.0])
        for slug, cost, pnl in db.execute("SELECT slug, cost, pnl FROM trades WHERE status IN ('won', 'lost')"):
            B[slug][0] += cost
            B[slug][1] += pnl
        dec = defaultdict(list)                                   # the bot's decisions: every order it sent
        for t, slug, side in db.execute("SELECT t, slug, side FROM signals WHERE outcome IN ('bought', 'nofill')"):
            dec[(slug, side)].append(t)
        rs = sorted(s for s in B if s in W)
        if not rs:
            print(f"{Path(path).name}: no finished rounds shared with the wallet yet")
            continue
        ours = [B[s][1] / B[s][0] for s in rs]
        theirs = [W[s][1] / W[s][0] for s in rs]
        mx, my = st.mean(ours), st.mean(theirs)
        den = (sum((a - mx) ** 2 for a in ours) * sum((b - my) ** 2 for b in theirs)) ** 0.5
        corr = sum((a - mx) * (b - my) for a, b in zip(ours, theirs)) / den if den else 0.0
        hit = n = 0
        for k, ts in wb.items():
            if k[0] not in B:
                continue
            mine = sorted(dec.get(k, []))
            for t in ts:
                n += 1
                i = bisect.bisect_left(mine, t - 1)
                hit += i < len(mine) and mine[i] <= t + 1
        bc, bp = sum(B[s][0] for s in rs), sum(B[s][1] for s in rs)
        wc, wp = sum(W[s][0] for s in rs), sum(W[s][1] for s in rs)
        h = len(rs) // 2
        print(f"{Path(path).stem:12} {len(rs):3d} rounds | bot ${bp:+8.2f} {100 * bp / bc:+5.1f}%/$ (halves "
              f"{sum(B[s][1] for s in rs[:h]):+.2f} / {sum(B[s][1] for s in rs[h:]):+.2f}) | wallet ${wp:+9.2f} "
              f"{100 * wp / wc:+5.1f}%/$ | per-round corr {corr:+.2f}, same sign {100 * sum((a > 0) == (b > 0) for a, b in zip(ours, theirs)) / len(rs):.0f}%"
              f" | decided {100 * hit / max(1, n):.0f}% of its taker buys")


def triggers():
    db = sqlite3.connect(f"file:{FR}?mode=ro", uri=True)
    F = [f for f in json.loads(FILLS.read_text()) if f["t"] and f["taker"]]
    since = min(f["start"] for f in F) - 60
    Q = {}
    for fd, in db.execute("SELECT DISTINCT feed FROM quotes WHERE t > ?", (since,)):
        rows = db.execute("SELECT t, mid FROM quotes WHERE feed = ? AND t > ? ORDER BY t", (fd, since)).fetchall()
        Q[fd] = ([r[0] for r in rows], [r[1] for r in rows])

    def mv(fd, t, w):
        ts, vs = Q[fd]
        i, j = bisect.bisect_right(ts, t - w) - 1, bisect.bisect_right(ts, t) - 1
        return (vs[j] / vs[i] - 1) * 1e4 if i >= 0 and j >= 0 and t - ts[j] < 5 else None

    rnd = random.Random(7)
    ev = [(f["t"] - 0.01, f["outcome"]) for f in F]
    base = [(f["start"] + rnd.uniform(3, 270), rnd.choice(("Up", "Down"))) for f in F]
    res = []
    for fd in Q:
        for w in (0.25, 0.5, 1.0, 2.0):
            def share(rows):
                xs = [m if o == "Up" else -m for t, o in rows if (m := mv(fd, t, w)) is not None]
                return (sum(x >= 0.2 for x in xs) - sum(x <= -0.2 for x in xs)) / len(xs) if len(xs) > 50 else None
            a, b = share(ev), share(base)
            if a is not None and b is not None:
                res.append((a - b, fd, w, a, b))
    print(f"{len(ev)} wallet taker buys: net share of moments the feed moved toward its side (>= 0.2 bp), vs random")
    for d, fd, w, a, b in sorted(res, reverse=True)[:10]:
        print(f"  {fd:13} over {w:4.2f} s: wallet {100 * a:+5.1f} pts, random {100 * b:+5.1f} pts -> {100 * d:+5.1f}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=("fetch", "compare", "triggers"))
    p.add_argument("ledgers", nargs="*")
    p.add_argument("--since", type=float, default=0)
    a = p.parse_args()
    if a.cmd == "fetch":
        fetch(a.since or time.time() - 6 * 3600)
    elif a.cmd == "compare":
        compare(a.ledgers)
    else:
        triggers()


if __name__ == "__main__":
    main()
