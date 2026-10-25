"""Replay a taker model on what THIS server saw, event by event (runs/feedrace.db), with the measured order timing.

    python racesim.py [--feeds binance-fut] [--bps 2] [--sec 1] [--hold 12] [--pad 0.04] [--match 0.34]
                      [--since 2026-10-25T06:05] [--list] [--sweep]

Why: the paper twin filled 7 of 8 signals on 25 Oct morning while live filled 2 of 8 - Polymarket's book reprices
~350 ms after a Binance move, the same moment our order lands, so 10-50 ms decide the trade. feedrace.db has every
exchange quote and every Polymarket UP top-of-book change with server arrival times; this replays on exactly that.

Rules as in taker.py live: signal when a feed's mid moved >= bps over the last sec seconds (several feeds: whichever
fires first); skip if < 30 s left, ask outside 0.05-0.95 or spread > 0.03; FAK limit = ask + pad, it matches `match`
seconds after the signal against the book THEN (killed if the ask ran past the limit); 10 s cooldown after every
order; sell at the bid `hold` s after the fill (decided on the bid then, matches `match` later, bid - pad limit,
retried each second). DOWN prices come from the UP book (unified book: DOWN ask = 1 - UP bid). No sizes recorded:
assumes the top level holds our 10 shares. Taker fee on both legs.
"""
import argparse
import collections
import math
import sqlite3
import time
from bisect import bisect_right
from datetime import datetime, timezone

import requests

import config
from engine import taker_fee
from market import MarketFeed
from feedrace import DB, FEEDS, pct, series, signals

CODE, RESP, COOLDOWN, MIN_LEFT, LO, HI, SPREAD, SIZE = 0.005, 0.03, 10, 30, 0.05, 0.95, 0.03, 10


class Poly:
    def __init__(self, db, settle=False):
        rows = db.execute("SELECT t, slug, bid, ask FROM poly ORDER BY t").fetchall()
        self.t = [r[0] for r in rows]
        self.rows = rows
        self.winner = {}                                        # slug -> +1 Up won / -1 Down won (official result)
        if settle:
            mf = MarketFeed(requests.Session())
            for slug in sorted({r[1] for r in rows}):
                if int(slug.rsplit("-", 1)[1]) + 300 < time.time() - 60 and (w := mf.outcome(slug)):
                    self.winner[slug] = 1 if w == "Up" else -1

    def at(self, t):
        """(slug, round start, up bid, up ask) of the book at time t, or None."""
        i = bisect_right(self.t, t) - 1
        if i < 0 or t - self.t[i] > 60:                         # rows only on change: a quiet book is still valid
            return None
        _, slug, b, a = self.rows[i]
        return slug, int(slug.rsplit("-", 1)[1]), b, a


def side_px(book, d):
    """(bid, ask) of the side the move points to: UP for d > 0, DOWN = 1 - UP book otherwise."""
    _, _, b, a = book
    return (b, a) if d > 0 else (round(1 - a, 2), round(1 - b, 2))


def run(sig, poly, a, log=None):
    """--settle (the copied wallet's way, W-3bp): many buys per round, cooldown per side, held to the round's end."""
    trades, attempts, busy, last = [], 0, 0.0, {1: -1e9, -1: -1e9}
    inv = {}                                                    # settle: round -> {+1: UP shares, -1: DOWN shares}
    fee = config.DEFAULT_FEE
    for t, d in sig:
        if t < busy or t - (last[d] if a.settle else max(last.values())) < a.cooldown:
            continue
        bk = poly.at(t)
        if not bk or not MIN_LEFT <= bk[1] + 300 - t <= 300 or (a.settle and bk[0] not in poly.winner):
            continue
        bid, ask = side_px(bk, d)
        if not a.lo <= ask <= a.hi or ask - bid > a.spread + 1e-9:
            continue                                            # refused at signal time, no cooldown (as live)
        h = inv.setdefault(bk[0], {1: 0.0, -1: 0.0})
        if a.settle and a.skew and h[d] + SIZE - h[-d] > a.skew:
            continue                                            # would leave more than `skew` one-sided shares
        attempts += 1
        last[d], limit = t, round(ask + a.pad, 2)
        tm = t + CODE + a.match
        bm = poly.at(tm)
        if not bm or bm[0] != bk[0] or side_px(bm, d)[1] > limit + 1e-9:
            busy = tm + RESP
            log and log(t, d, ask, limit, None, None)
            continue
        px = side_px(bm, d)[1]
        shares = max(SIZE, math.ceil(100 / limit) / 100)
        if a.settle:
            h[d] += shares
            won = poly.winner[bk[0]] == d
            trades.append((t, shares, shares * (won - px) - taker_fee(px, shares, fee), shares * px))
            busy = tm + RESP                                    # only the order in flight blocks; positions don't
            log and log(t, d, ask, limit, px, 1.0 if won else 0.0)
            continue
        te, end, sold = tm + RESP + a.hold, bk[1] + 300, None
        while te < end and sold is None:                        # sale: decided on the bid at te, matches later
            be = poly.at(te)
            bs = poly.at(te + a.match) if be else None
            if be and bs and bs[0] == bk[0] and side_px(bs, d)[0] >= round(side_px(be, d)[0] - a.pad, 2) - 1e-9:
                sold = side_px(bs, d)[0]
            te += 1
        if sold is None:                                        # unsold at round end: rare, count at the last bid
            sold = side_px(poly.at(end - 0.5) or bm, d)[0]
        pnl = shares * (sold - px) - taker_fee(px, shares, fee) - taker_fee(sold, shares, fee)
        trades.append((t, shares, pnl, shares * px))
        busy = te + a.match
        log and log(t, d, ask, limit, px, sold)
    return attempts, trades


def run_pairs(sig, poly, a):
    """Hold to settlement and BUILD PAIRS like the copied wallet (25 Oct 08:15: DOWN at 0.44 on the first Deribit tick,
    then UP at 0.30 at the bottom of the crash = pair 0.74). Taker entries on signals as in run(); after each fill a
    RESTING bid waits on the other side at `pair` - (avg price of the unpaired side), sized to the unpaired shares.
    It fills (maker, no fee) when that side's ask trades below it. Returns (attempts, fills) like run()."""
    fee, out, attempts, busy, last = config.DEFAULT_FEE, [], 0, 0.0, {1: -1e9, -1: -1e9}
    inv, rest = {}, {}                                          # slug -> {+1/-1: [shares, cost]}; slug -> (side, px, n)
    lots = collections.defaultdict(collections.deque)           # slug -> unpaired lots [side, shares, price], oldest first
    ev = sorted([(r[0], 0, r) for r in poly.rows] + [(t, 1, d) for t, d in sig], key=lambda e: (e[0], e[1]))

    def fill(slug, side, n, px):
        """Pair `n` new shares against the oldest unpaired lots of the other side; the rest becomes a new lot."""
        q = lots[slug]
        while n > 1e-9 and q and q[0][0] != side:
            k = min(n, q[0][1])
            q[0][1] -= k
            n -= k
            if q[0][1] <= 1e-9:
                q.popleft()
        if n > 1e-9:
            q.append([side, n, px])

    def place(slug):
        """Resting bid on the short side priced so that pairing the UNPAIRED lots costs at most `pair` (bug fixed
        25 Oct: averaging all shares of a side priced a pair at 1.19)."""
        q = lots[slug]
        un = sum(x[1] for x in q)
        if not a.pair or un < 1:
            rest.pop(slug, None)
            return
        avg = sum(x[1] * x[2] for x in q) / un
        px = math.floor((a.pair - avg) * 100 + 1e-9) / 100
        rest[slug] = (-q[0][0], px, un) if px >= 0.01 else None

    for t, kind, x in ev:
        if kind == 0:                                           # book change: does a resting pair bid fill?
            _, slug, b, ak = x
            o = rest.get(slug)
            if o and t < int(slug.rsplit("-", 1)[1]) + 295 and slug in poly.winner:
                side, q, n = o
                if (ak if side > 0 else round(1 - b, 2)) < q - 1e-9:
                    inv[slug][side][0] += n
                    inv[slug][side][1] += n * q
                    out.append((t, n, n * ((poly.winner[slug] == side) - q), n * q))
                    fill(slug, side, n, q)
                    place(slug)
            continue
        d = x
        if t < busy or t - last[d] < a.cooldown:
            continue
        bk = poly.at(t)
        if not bk or not MIN_LEFT <= bk[1] + 300 - t <= 300 or bk[0] not in poly.winner:
            continue
        bid, ask = side_px(bk, d)
        if not a.lo <= ask <= a.hi or ask - bid > a.spread + 1e-9:
            continue
        h = inv.setdefault(bk[0], {1: [0.0, 0.0], -1: [0.0, 0.0]})
        if a.skew and h[d][0] + SIZE - h[-d][0] > a.skew:
            continue
        attempts += 1
        last[d], limit = t, round(ask + a.pad, 2)
        bm = poly.at(t + CODE + a.match)
        busy = t + CODE + a.match + RESP
        if not bm or bm[0] != bk[0] or side_px(bm, d)[1] > limit + 1e-9:
            continue
        px = side_px(bm, d)[1]
        n = max(SIZE, math.ceil(100 / limit) / 100)
        h[d][0] += n
        h[d][1] += n * px
        out.append((t, n, n * ((poly.winner[bk[0]] == d) - px) - taker_fee(px, n, fee), n * px))
        fill(bk[0], d, n, px)
        place(bk[0])
    return attempts, out


def summary(attempts, trades):
    sh = sum(t[1] for t in trades)
    pnl = sum(t[2] for t in trades)
    cost = sum(t[3] for t in trades)
    return (f"{attempts:5d} orders {len(trades) / attempts * 100 if attempts else 0:4.0f}% filled {len(trades):4d} trades "
            f"{pnl / sh * 100 if sh else 0:+6.2f} c/sh {pnl / cost * 100 if cost else 0:+6.2f}%/$  ${pnl:+7.2f}")


def build(db, feeds, bps, sec, since):
    sig = []
    for f in feeds:
        ts, vs = series(db, f)
        sig += [(t, d) for t, d in signals(ts, vs, bps, sec, gap=0.0) if t >= since]
    return sorted(sig)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(DB))
    p.add_argument("--feeds", default="binance-fut")
    p.add_argument("--bps", type=float, default=2)
    p.add_argument("--sec", type=float, default=1)
    p.add_argument("--hold", type=float, default=12)
    p.add_argument("--pad", type=float, default=0.04)
    p.add_argument("--match", type=float, default=0.34)
    p.add_argument("--since", default="")
    p.add_argument("--settle", action="store_true")
    p.add_argument("--skew", type=float, default=0, help="settle: max UP-DOWN share imbalance per round (0 = none)")
    p.add_argument("--spread", type=float, default=SPREAD, help="skip a signal when the side's ask - bid is wider")
    p.add_argument("--pair", type=float, default=0, help="settle: resting bid on the other side at pair - avg (0 = off)")
    p.add_argument("--cooldown", type=float, default=COOLDOWN)
    p.add_argument("--band", default=f"{LO},{HI}")
    p.add_argument("--list", action="store_true")
    p.add_argument("--sweep", action="store_true")
    p.add_argument("--quick", action="store_true", help="with --sweep: only the PAIR-BUILD block")
    a = p.parse_args()
    a.lo, a.hi = map(float, a.band.split(","))
    db = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    poly = Poly(db, settle=a.settle or a.sweep)
    since = datetime.fromisoformat(a.since).replace(tzinfo=timezone.utc).timestamp() if a.since else 0
    t0, t1 = poly.t[0], poly.t[-1]
    half = max(since, t0) + (t1 - max(since, t0)) / 2
    print(f"{(t1 - max(since, t0)) / 3600:.1f} h of server data, halves split at "
          f"{datetime.fromtimestamp(half, timezone.utc):%H:%M} UTC | match {a.match}s pad {a.pad} hold {a.hold}s\n")

    def show(label, feeds, bps, sec, hold=a.hold, pad=a.pad, **kw):
        if a.quick and not label.startswith("PAIR-BUILD"):
            return
        args = argparse.Namespace(**{**vars(a), "hold": hold, "pad": pad, **kw})
        sig = build(db, feeds, bps, sec, since)
        fn = run_pairs if args.settle and (args.pair or kw.get("pairs")) else run
        out = [fn([s for s in sig if lo <= s[0] < hi], poly, args) for lo, hi in ((0, half), (half, 9e12))]
        allr = fn(sig, poly, args)
        print(f"{label:44} all {summary(*allr)} | 1st {summary(*out[0])[-8:]} 2nd {summary(*out[1])[-8:]}")

    if a.sweep:
        for f in FEEDS:
            show(f"{f} 2bp/1s", [f], 2, 1)
        for f in ("binance-fut", "deribit", "coinbase", "bybit"):
            for bps, sec in ((1, 0.3), (1.5, 0.5), (3, 1)):
                show(f"{f} {bps}bp/{sec}s", [f], bps, sec)
        show("first of binance-fut+deribit 2bp/1s", ["binance-fut", "deribit"], 2, 1)
        show("first of binance-fut+deribit+coinbase 2bp/1s", ["binance-fut", "deribit", "coinbase"], 2, 1)
        w = dict(settle=True, cooldown=2, lo=0.10, hi=0.90)       # the copied wallet's way: walletlab.py found it follows
        for f in ("deribit", "binance-fut", "bybit"):              # Deribit's 3-5 s move, buys 0.10-0.90, never sells
            for bps, sec in ((0.5, 3), (1, 3), (1.5, 3), (2, 3), (3, 3), (1, 5), (2, 5)):
                show(f"WALLET-STYLE {f} {bps}bp/{sec}s settle", [f], bps, sec, **w)
        for cd in (1, 4):
            show(f"WALLET-STYLE deribit 1bp/3s cooldown {cd}s", ["deribit"], 1, 3, **{**w, "cooldown": cd})
        if a.quick:
            w = dict(settle=True, cooldown=2, lo=0.10, hi=0.90)
        else:
            for f in ("deribit", "bybit", "binance-fut"):         # wallet entries, pairs first: cap the one-sided rest
                for bps, sec in ((0.5, 3), (1, 3), (1, 5)):
                    for sk in (10, 20, 40):
                        show(f"PAIRS-FIRST {f} {bps}bp/{sec}s skew<={sk}", [f], bps, sec, **{**w, "skew": sk})
        for f in ("deribit", "bybit", "binance-fut"):             # + no spread filter (A) and resting pair bids (B)
            for bps, sec in ((0.5, 1), (1, 1), (1, 3), (1, 5)):
                for sp in (0.03, 1.0):
                    for pr in (0, 0.96, 0.92):
                        show(f"PAIR-BUILD {f} {bps}bp/{sec}s spread<={sp} pair<={pr or '-'}", [f], bps, sec,
                             **{**w, "skew": 20, "spread": sp, "pair": pr, "pairs": True})
        return
    feeds = a.feeds.split(",")

    def log(t, d, ask, limit, px, sold):
        print(f"  {datetime.fromtimestamp(t, timezone.utc):%H:%M:%S.%f}"[:-3] + f" {'UP  ' if d > 0 else 'DOWN'} ask {ask:.2f} "
              f"limit {limit:.2f} -> " + (f"filled {px:.2f}, sold {sold:.2f}" if px is not None else "no fill"))
    attempts, trades = run(build(db, feeds, a.bps, a.sec, since), poly, a, log if a.list else None)
    print(f"{','.join(feeds)} {a.bps}bp/{a.sec}s: {summary(attempts, trades)}")


if __name__ == "__main__":
    main()
