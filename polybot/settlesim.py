"""Hold-to-settlement replay with the honest fill rule, on any paper ledger that recorded ticks + the signal feed.

    python settlesim.py runs/fav1.db --bps 1 --win 3 --band 0.50-0.90 [--round-usd 10] [--skip 0.15-0.30]
    python settlesim.py runs/fav1.db --grid            # trigger x window x band x round cap, robust ones only

Fill rule = taker.py TK_SAFE_SEC: the ask must stay <= seen + PAD from 0.3 s to SAFE s after the signal, the price is
the ask at MATCH s, and QUEUE x our shares must sit at it (calibrated on 712 live orders, 26 Oct). Winner = the side
whose bid ended >= 0.9 in the last 15 s. 25 Oct: conservative against paper (D-S03N +$23.7 here vs +$64.9 on paper).
A model is "robust" when it is positive in both halves of the data and without its 5 best rounds.
"""
import argparse
import bisect
import itertools
import math
import sqlite3
from collections import defaultdict

import config
from engine import taker_fee

PAD, QUEUE, SIZE, SAFE_FROM, SAFE, MATCH, SPREAD, COOLDOWN, MIN_LEFT = 0.04, 5, 5, 0.3, 1.6, 0.8, 0.03, 2, 30


class Data:
    def __init__(self, path):
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        self.bt, self.bm = zip(*db.execute("SELECT t, mid FROM binance WHERE mid IS NOT NULL ORDER BY t"))
        self.t0, self.t1 = self.bt[0] + 10, self.bt[-1]
        self.by = defaultdict(list)
        for r in db.execute("SELECT t, slug, t_left, up_bid, up_ask, up_ask_size, dn_bid, dn_ask, dn_ask_size FROM ticks "
                            "WHERE t BETWEEN ? AND ? ORDER BY t", (self.t0 - 5, self.t1)):
            self.by[r[1]].append(r)
        self.tt = {s: [r[0] for r in v] for s, v in self.by.items()}
        self.win = {}
        for slug, v in self.by.items():
            last = [r for r in v if r[2] is not None and r[2] < 15]
            if last and last[-1][3] and last[-1][3] >= 0.9:
                self.win[slug] = "Up"
            elif last and last[-1][6] and last[-1][6] >= 0.9:
                self.win[slug] = "Down"
        self._fill = {}
        self._move = {}

    def mid(self, t):
        i = bisect.bisect_right(self.bt, t) - 1
        return self.bm[i] if i >= 0 and t - self.bt[i] < 1 else None

    def at(self, slug, t):
        i = bisect.bisect_right(self.tt[slug], t) - 1
        return self.by[slug][i] if i >= 0 and t - self.by[slug][i][0] < 1 else None

    def move(self, t, w):
        k = (t, w)
        if k not in self._move:
            a, b = self.mid(t - w), self.mid(t)
            self._move[k] = (b / a - 1) * 1e4 if a and b else None
        return self._move[k]

    def fill(self, slug, t, side, ask):
        """(price, shares) the honest rule fills, or None."""
        k = (t, side)
        if k not in self._fill:
            lim = round(ask + PAD, 2)
            want = max(SIZE, math.ceil(100 / lim) / 100)
            v, ts = self.by[slug], self.tt[slug]
            i, j = bisect.bisect_left(ts, t + SAFE_FROM), bisect.bisect_right(ts, t + SAFE)
            res = None
            if j > i and all((a := sv(r, side)[1]) is not None and a <= lim + 1e-9 for r in v[max(0, i - 1):j]):
                m = self.at(slug, t + MATCH)
                if m:
                    _, a, size = sv(m, side)
                    got = math.floor(min(want, (size or 0) - (QUEUE - 1) * want) * 100) / 100
                    res = (a, got) if a is not None and got >= 1 else None
            self._fill[k] = res
        return self._fill[k]


def sv(r, side):
    """(bid, ask, ask size) of one side from a ticks row."""
    return (r[3], r[4], r[5]) if side == "Up" else (r[6], r[7], r[8])


def run(d, bps, win, lo, hi, skip=(1, 0), round_usd=0):
    trades, last, busy, spent = [], {}, 0.0, defaultdict(float)
    for slug, v in d.by.items():
        if slug not in d.win:
            continue
        for r in v:
            t, tl = r[0], r[2]
            if t < busy or tl is None or tl < MIN_LEFT:
                continue
            x = d.move(t, win)
            if x is None or abs(x) < bps:
                continue
            side = "Up" if x > 0 else "Down"
            if t - last.get(side, -99) < COOLDOWN:
                continue
            bid, ask, size = sv(r, side)
            if ask is None or bid is None or not lo <= ask <= hi or skip[0] <= ask < skip[1]:
                continue
            if ask - bid > SPREAD + 1e-9 or (size or 0) < SIZE:
                continue
            lim = round(ask + PAD, 2)
            if round_usd and spent[slug] + max(SIZE, math.ceil(100 / lim) / 100) * lim > round_usd + 1e-9:
                continue
            last[side], busy = t, t + SAFE
            f = d.fill(slug, t, side, ask)
            if not f:
                continue
            px, sh = f
            spent[slug] += px * sh
            pnl = (sh if d.win[slug] == side else 0) - px * sh - taker_fee(px, sh, config.DEFAULT_FEE)
            trades.append((t, slug, px, sh, pnl))
    return sorted(trades)


def score(d, tr):
    if not tr:
        return None
    cost, pnl = sum(p * s for _, _, p, s, _ in tr), sum(x[4] for x in tr)
    mid = (d.t0 + d.t1) / 2
    h1 = sum(x[4] for x in tr if x[0] < mid)
    rnd = defaultdict(float)
    for x in tr:
        rnd[x[1]] += x[4]
    rs = sorted(rnd.values(), reverse=True)
    return dict(n=len(tr), pnl=pnl, pct=100 * pnl / cost, h1=h1, h2=pnl - h1, wo5=sum(rs[5:]), worst=rs[-1],
                rounds=len(rs), rpos=sum(x > 0 for x in rs))


def line(name, s):
    return (f"{name:42} {s['n']:5d} tr ${s['pnl']:+8.2f} {s['pct']:+5.1f}%/$ | halves {s['h1']:+7.2f} {s['h2']:+7.2f} | "
            f"without best 5 rounds {s['wo5']:+7.2f} | worst round {s['worst']:+6.2f} | rounds + {s['rpos']}/{s['rounds']}")


def band(s):
    lo, hi = s.split("-")
    return float(lo), float(hi)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("db")
    p.add_argument("--bps", type=float, default=1.0)
    p.add_argument("--win", type=float, default=3.0)
    p.add_argument("--band", default="0.50-0.90")
    p.add_argument("--skip", default="")
    p.add_argument("--round-usd", type=float, default=0)
    p.add_argument("--grid", action="store_true")
    a = p.parse_args()
    d = Data(a.db)
    print(f"{(d.t1 - d.t0) / 3600:.1f} h, {len(d.win)} rounds with a winner")
    if not a.grid:
        s = score(d, run(d, a.bps, a.win, *band(a.band), band(a.skip) if a.skip else (1, 0), a.round_usd))
        print(line(f"{a.bps}bp/{a.win}s {a.band} cap {a.round_usd or '-'}", s) if s else "no trades")
        return
    res = []
    for bps, win, (lo, hi), cap in itertools.product((0.5, 0.8, 1.0, 1.5), (0.3, 1, 3, 5),
                                                     ((0.10, 0.90), (0.40, 0.90), (0.50, 0.90), (0.50, 0.85)), (0, 10)):
        s = score(d, run(d, bps, win, lo, hi, round_usd=cap))
        if s and s["n"] >= 40 and s["h1"] > 0 and s["h2"] > 0 and s["wo5"] > 0:
            res.append((min(s["h1"], s["h2"]), f"{bps}bp/{win}s {lo:.2f}-{hi:.2f} cap {cap or '-'}", s))
    print(f"{len(res)} robust settings (both halves + and + without the best 5 rounds), best first:")
    for _, name, s in sorted(res, key=lambda r: -r[0])[:20]:
        print(" ", line(name, s))


if __name__ == "__main__":
    main()
