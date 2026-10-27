"""Fast decision maker for the W-SNIPE taker: should we take this Deribit signal? (plan 26 Oct, stdlib only)

Per signal, in logit space so a tiny model can combine them:
  market prior   logit(side mid)                         the book is the best single estimate
  fair value     logit(fair) - logit(mid)                engine.fair_up on Deribit vs the round's Deribit price to beat,
                                                         with realised Deribit vol (the fixed-sigma gate was overconfident)
  lead-lag       R = change in fair - change in mid      the part of the last 0.5 s / 2 s Deribit move Polymarket has
                                                         NOT priced yet: the wallet's edge; R ~ 0 = already priced
  controls       Deribit 0.5 s move z, spread, t_left/300, others' net taker flow on the side in the last 2 s
Two models on the same features: logistic (P(side wins); edge = p - ask - fee) and ridge (P/L per share directly).
State is fed the same way by the replay and by taker.py, so the bot sees exactly the features the model was trained on.

    python predict.py                       self-check
    python predict.py test                  walk-forward on runs/feedrace.db (expanding window, 6 h blocks)
    python predict.py train [--lam 10]      fit both on all data -> runs/model.json, runs/model_ridge.json
"""
import bisect
import collections
import json
import math
import sqlite3
import sys
import time
from pathlib import Path

import config
from engine import fair_up, taker_fee

RUNS = Path(__file__).parent / "runs"
NAMES = ["logit_mid", "fair_minus_mid", "leadlag_0.5s", "leadlag_2s", "move_z", "spread", "t_left", "flow"]
logit = lambda p: math.log(p / (1 - p))
clip = lambda p: min(max(p, 0.01), 0.99)


class State:
    """Rolling market state, O(1) per update: Deribit on a 1 s grid (price to beat, final-minute average, EWMA vol),
    Deribit ticks of the last 3 s, Polymarket UP mid of the last 3 s per round, trade prints of the last 2 s."""

    def __init__(self, half_life=120.0):
        self.sec = collections.deque(maxlen=420)     # (second, last Deribit mid in it)
        self.ticks = collections.deque()             # (t, Deribit mid)
        self.mids = {}                               # slug -> deque of (t, UP mid)
        self.prints = collections.deque()            # (t, slug, outcome, size, side)
        self.var, self.a = None, 1 - 0.5 ** (1 / half_life)
        self.beats = {}

    def deribit(self, t, mid):
        self.ticks.append((t, mid))
        while self.ticks[0][0] < t - 3:
            self.ticks.popleft()
        s = int(t)
        if self.sec and self.sec[-1][0] == s:
            self.sec[-1] = (s, mid)
            return
        if self.sec:
            prev_s, prev = self.sec[-1]
            r2 = math.log(mid / prev) ** 2 / max(1, s - prev_s)   # per-second variance across a gap
            self.var = r2 if self.var is None else self.var + self.a * (r2 - self.var)
        self.sec.append((s, mid))

    def book(self, t, slug, bid, ask):
        d = self.mids.setdefault(slug, collections.deque())
        d.append((t, (bid + ask) / 2))
        while d[0][0] < t - 3:
            d.popleft()

    def trade(self, t, slug, outcome, size, side):
        self.prints.append((t, slug, outcome, size, side))
        while self.prints[0][0] < t - 2:
            self.prints.popleft()

    def drop(self, slug):
        self.mids.pop(slug, None)
        self.beats.pop(slug, None)

    def _mean(self, a, b):
        v = [m for s, m in self.sec if a <= s < b]
        return sum(v) / len(v) if len(v) >= 30 else None   # >= half the minute seen, else unknown (bot just started)

    def beat(self, slug, start):
        if slug not in self.beats:
            b = self._mean(start - 60, start)
            if b is None:
                return None
            self.beats[slug] = b                        # fixed once the round started
        return self.beats[slug]

    @staticmethod
    def _at(d, t):
        """value at or before t from a deque of (t, v), or None"""
        v = None
        for x, y in d:
            if x > t:
                break
            v = y
        return v

    def features(self, now, slug, start, side, bid, ask, t_left):
        """8 floats toward `side` (bid/ask are that side's), or None while the state is warming up."""
        beat = self.beat(slug, start)
        d = self.mids.get(slug)
        if beat is None or not self.ticks or self.var is None or not d:
            return None
        spot = self.ticks[-1][1]
        sig = max(math.sqrt(self.var), 1e-6)
        known = self._mean(start + 240, start + 300) if t_left < 60 else None
        up = side == "Up"
        fair = lambda s: (lambda f: f if up else 1 - f)(fair_up(s, beat, sig, t_left, known))
        mid_up = d[-1][1]
        m = clip(mid_up if up else 1 - mid_up)
        f = clip(fair(spot))
        x = [logit(m), logit(f) - logit(m)]
        for w in (0.5, 2.0):
            s0, m0 = self._at(self.ticks, now - w), self._at(d, now - w)
            if s0 is None or m0 is None:
                x.append(0.0)
            else:
                x.append((f - clip(fair(s0))) - (m - (m0 if up else 1 - m0)))
        s05 = self._at(self.ticks, now - 0.5)
        mv = 0.0 if s05 is None else math.log(spot / s05) / (sig * math.sqrt(0.5))
        x.append(mv if up else -mv)
        x.append(ask - bid)
        x.append(t_left / 300)
        flow = sum((1 if b == "BUY" else -1) * (1 if o == side else -1) * z for t, s, o, z, b in self.prints if s == slug)
        x.append(math.copysign(math.log1p(abs(flow)), flow))
        return x


# ---------------------------------------------------------------- models
def _solve(A, b):
    """Gauss-Jordan with partial pivoting (9x9)."""
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        for r in range(n):
            if r != c and M[r][c]:
                k = M[r][c] / M[c][c]
                M[r] = [a - k * b for a, b in zip(M[r], M[c])]
    return [M[i][n] / M[i][i] for i in range(n)]


def _scale(X):
    n, k = len(X), len(X[0])
    mean = [sum(r[j] for r in X) / n for j in range(k)]
    std = [max(1e-9, math.sqrt(sum((r[j] - mean[j]) ** 2 for r in X) / n)) for j in range(k)]
    return mean, std, [[1.0] + [(r[j] - mean[j]) / std[j] for j in range(k)] for r in X]


def _normal(Z, wts, y):
    k = len(Z[0])
    A = [[0.0] * k for _ in range(k)]
    b = [0.0] * k
    for z, w, t in zip(Z, wts, y):
        for i in range(k):
            wi = w * z[i]
            b[i] += wi * t
            Ai = A[i]
            for j in range(i, k):
                Ai[j] += wi * z[j]
    for i in range(k):
        for j in range(i):
            A[i][j] = A[j][i]
    return A, b


def fit_logit(X, y, l2=1.0, iters=12):
    """L2 logistic regression by Newton/IRLS on standardised X (intercept not penalised)."""
    mean, std, Z = _scale(X)
    k = len(Z[0])
    w = [0.0] * k
    for _ in range(iters):
        p = [1 / (1 + math.exp(-max(-30, min(30, sum(a * b for a, b in zip(w, z)))))) for z in Z]
        q = [pi * (1 - pi) for pi in p]
        # Newton step: (Z'QZ + l2 I) dw = Z'(y-p) - l2 w
        A, _ = _normal(Z, q, [0.0] * len(Z))
        g = [sum(z[i] * (t - pi) for z, t, pi in zip(Z, y, p)) - (l2 * w[i] if i else 0) for i in range(k)]
        for i in range(1, k):
            A[i][i] += l2
        dw = _solve(A, g)
        w = [a + b for a, b in zip(w, dw)]
        if max(abs(d) for d in dw) < 1e-6:
            break
    return {"kind": "logit", "mean": mean, "std": std, "w": w, "names": NAMES, "l2": l2}


def fit_ridge(X, y, lam=10.0):
    """Ridge regression of P/L per share on standardised X, closed form (intercept not penalised)."""
    mean, std, Z = _scale(X)
    A, b = _normal(Z, [1.0] * len(Z), y)
    for i in range(1, len(A)):
        A[i][i] += lam
    return {"kind": "ridge", "mean": mean, "std": std, "w": _solve(A, b), "names": NAMES, "lam": lam}


def score(model, x):
    w, mu, sd = model["w"], model["mean"], model["std"]
    s = w[0]
    for i in range(len(x)):
        s += w[i + 1] * (x[i] - mu[i]) / sd[i]
    return s


def p_win(model, x):
    return 1 / (1 + math.exp(-max(-30, min(30, score(model, x)))))


def edge(model, x, ask, fee=config.DEFAULT_FEE):
    """Expected P/L per share of buying at `ask`: logit -> p - ask - fee; ridge -> its prediction."""
    if model["kind"] == "ridge":
        return score(model, x)
    return p_win(model, x) - ask - taker_fee(ask, 1, fee)


def load(path):
    return json.loads(Path(path).read_text())


# ---------------------------------------------------------------- replay of W-SNIPE on runs/feedrace.db
def winners(slugs):
    """slug -> winner, cached in runs/winners.json (wallet fills first, then Polymarket for the rest)"""
    f = RUNS / "winners.json"
    w = json.loads(f.read_text()) if f.exists() else {}
    cf = RUNS / "clone_fills.json"
    if cf.exists():
        for x in json.loads(cf.read_text()):
            w.setdefault(x["slug"], x["winner"])
    miss = [s for s in slugs if s not in w and int(s.rsplit("-", 1)[1]) + 400 < time.time()]
    if miss:
        import requests
        from market import MarketFeed
        mf = MarketFeed(requests.Session())
        for s in miss:
            try:
                o = mf.outcome(s)
            except Exception:  # noqa: BLE001 - an unknown round is just left out
                o = None
            if o:
                w[s] = o
            time.sleep(0.15)
        f.write_text(json.dumps(w))
    return w


def events(db_path=RUNS / "feedrace.db", bps=0.2, win=0.5, gap=0.5, delay=0.25, lo=0.10, hi=0.90, min_left=30,
           max_spread=0.03):
    """W-SNIPE replayed tick by tick: Deribit moved >= bps in win s toward a side -> features at that moment; filled if
    the side's ask `delay` s later is still <= the ask, at that later ask. -> list of (start, slug, side, ask, fill, x)"""
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows = [(t, 0, mid, None, None, None) for t, mid in db.execute("SELECT t, mid FROM quotes WHERE feed='deribit'")]
    rows += [(t, 1, s, b, a, None) for t, s, b, a in
             db.execute("SELECT t, slug, bid, ask FROM poly WHERE bid IS NOT NULL AND ask IS NOT NULL")]
    rows += [(t, 2, s, o, z, sd) for t, s, o, z, sd in db.execute("SELECT t, slug, outcome, size, side FROM prints")]
    rows.sort(key=lambda r: r[0])
    book = collections.defaultdict(list)                  # slug -> [(t, bid, ask)] for the fill lookahead
    for r in rows:
        if r[1] == 1:
            book[r[2]].append((r[0], r[3], r[4]))
    bt = {s: [x[0] for x in v] for s, v in book.items()}

    def side_ba(slug, t, side):
        i = bisect.bisect_right(bt[slug], t) - 1
        if i < 0 or t - bt[slug][i] > 3:
            return None
        _, b, a = book[slug][i]
        return (b, a) if side == "Up" else (round(1 - a, 2), round(1 - b, 2))

    st, out, last, cur = State(), [], {}, {}            # cur: slug of each round start seen in the book
    for t, kind, a, b, c, d in rows:
        if kind == 1:
            st.book(t, a, b, c)
            cur[int(a.rsplit("-", 1)[1])] = a
            continue
        if kind == 2:
            st.trade(t, a, b, c, d)
            continue
        st.deribit(t, a)
        s05 = st._at(st.ticks, t - win)
        if s05 is None:
            continue
        mv = (a / s05 - 1) * 1e4
        if abs(mv) < bps:
            continue
        start = int(t) // 300 * 300
        slug = cur.get(start)
        if not slug:
            continue
        t_left = start + 300 - t
        if t_left < min_left:
            continue
        side = "Up" if mv > 0 else "Down"
        if t - last.get((slug, side), -99) < gap:
            continue
        ba = side_ba(slug, t, side)
        if not ba or not lo <= ba[1] <= hi or ba[1] - ba[0] > max_spread + 1e-9:
            continue
        x = st.features(t, slug, start, side, ba[0], ba[1], t_left)
        if x is None:
            continue
        last[(slug, side)] = t
        later = side_ba(slug, t + delay, side)
        fill = later[1] if later and later[1] <= ba[1] + 1e-9 else None
        out.append((start, slug, side, ba[1], fill, x))
    return out


def labelled(ev, win):
    """filled events with a known winner -> X, won (0/1), pnl per share after fee, event rows"""
    X, y, pnl, rows = [], [], [], []
    for e in ev:
        start, slug, side, ask, fill, x = e
        if fill is None or slug not in win:
            continue
        won = 1.0 if win[slug] == side else 0.0
        X.append(x); y.append(won); pnl.append(won - fill - taker_fee(fill, 1, config.DEFAULT_FEE)); rows.append(e)
    return X, y, pnl, rows


def report(name, rows, pnl, keep):
    """%/$ of the kept trades, both halves, share of hours positive"""
    k = [(r, p) for r, p, kp in zip(rows, pnl, keep) if kp]
    if not k:
        return f"{name:28} no trades"
    cost = sum(r[4] for r, _ in k)
    tot = sum(p for _, p in k)
    rs = sorted({r[0] for r, _ in k})
    h = rs[len(rs) // 2]
    halves = [sum(p for r, p in k if (r[0] < h) == first) for first in (True, False)]
    hours = collections.defaultdict(float)
    for r, p in k:
        hours[r[0] // 3600] += p
    pos = sum(v > 0 for v in hours.values()) / len(hours)
    rnd = collections.defaultdict(lambda: [0.0, 0.0])
    for r, p in k:
        rnd[r[1]][0] += p
        rnd[r[1]][1] += r[4]
    best5 = sorted(rnd.values(), key=lambda v: -v[0])[5:]
    wo5 = 100 * sum(v[0] for v in best5) / max(1e-9, sum(v[1] for v in best5))
    return (f"{name:28} {len(k):6d} trades ({100 * len(k) / len(rows):3.0f}%) | {100 * tot / cost:+5.2f} %/$ "
            f"(${5 * tot:+8.2f} at 5 sh) | halves {100 * halves[0] / max(1e-9, sum(r[4] for r, _ in k if r[0] < h)):+5.2f}"
            f" / {100 * halves[1] / max(1e-9, sum(r[4] for r, _ in k if r[0] >= h)):+5.2f} %/$ | hours + {100 * pos:3.0f}%"
            f" | rounds + {100 * sum(v[0] > 0 for v in rnd.values()) / len(rnd):3.0f}% of {len(rnd)} | w/o best 5 {wo5:+5.2f} %/$")


def walk_forward(block=6 * 3600, warm=12 * 3600):
    ev = events()
    win = winners(sorted({e[1] for e in ev}))
    X, y, pnl, rows = labelled(ev, win)
    t0 = min(r[0] for r in rows)
    blk = [(r[0] - t0) // block for r in rows]
    first = warm // block
    test = [i for i, b in enumerate(blk) if b >= first]
    print(f"{len(ev)} signals, {len(rows)} filled with a winner, {len({r[1] for r in rows})} rounds; "
          f"test = blocks {first}..{max(blk)} of {block // 3600} h, each trained only on earlier blocks\n")
    res = {}
    for kind, params in (("logit", (0.3, 1, 10)), ("ridge", (1, 10, 100, 1000))):
        for prm in params:
            ed = {}
            for b in range(first, max(blk) + 1):
                tr = [i for i in range(len(rows)) if blk[i] < b]
                te = [i for i in range(len(rows)) if blk[i] == b]
                if not te or len(tr) < 200:
                    continue
                m = (fit_logit([X[i] for i in tr], [y[i] for i in tr], prm) if kind == "logit"
                     else fit_ridge([X[i] for i in tr], [pnl[i] for i in tr], prm))
                for i in te:
                    ed[i] = (edge(m, X[i], rows[i][3]), p_win(m, X[i]) if kind == "logit" else None)
            res[(kind, prm)] = ed
    T = [rows[i] for i in test]
    P = [pnl[i] for i in test]
    print(report("W-SNIPE, every signal", T, P, [True] * len(T)))
    for (kind, prm), ed in res.items():
        print(f"\n{kind} {'l2' if kind == 'logit' else 'lambda'}={prm}")
        for me in (0.0, 0.01, 0.02, 0.03, 0.05):
            print("  " + report(f"edge >= {me:.2f}", T, P, [i in ed and ed[i][0] >= me for i in test]))
        per = []
        for b in sorted({blk[i] for i in test}):
            ii = [i for i in test if blk[i] == b and i in ed and ed[i][0] >= 0.02]
            jj = [i for i in test if blk[i] == b]
            per.append(f"block {b}: {100 * sum(pnl[i] for i in ii) / max(1e-9, sum(rows[i][4] for i in ii)):+5.1f}"
                       f" (all {100 * sum(pnl[i] for i in jj) / max(1e-9, sum(rows[i][4] for i in jj)):+5.1f})")
        print("  edge >= 0.02 per 6 h test block, %/$ (ungated in brackets): " + " | ".join(per))
        if kind == "logit":
            ps = [(ed[i][1], y[i]) for i in test if i in ed]
            print(f"  Brier {sum((p - t) ** 2 for p, t in ps) / len(ps):.4f} vs market mid "
                  f"{sum((1 / (1 + math.exp(-X[i][0])) - y[i]) ** 2 for i in test if i in ed) / len(ps):.4f}")
            dec = sorted(ps)
            n = len(dec)
            print("  calibration (decile: predicted -> actual): " + "  ".join(
                f"{sum(p for p, _ in dec[k * n // 10:(k + 1) * n // 10]) / max(1, len(dec[k * n // 10:(k + 1) * n // 10])):.2f}"
                f"->{sum(t for _, t in dec[k * n // 10:(k + 1) * n // 10]) / max(1, len(dec[k * n // 10:(k + 1) * n // 10])):.2f}"
                for k in range(10)))
    return X, y, pnl, rows


def train(lam=100.0, l2=1.0):
    ev = events()
    win = winners(sorted({e[1] for e in ev}))
    X, y, pnl, rows = labelled(ev, win)
    meta = {"trained_until": max(r[0] for r in rows) + 300, "rounds": len({r[1] for r in rows}), "n": len(rows)}
    for m, f in ((fit_logit(X, y, l2), "model.json"), (fit_ridge(X, pnl, lam), "model_ridge.json")):
        m.update(meta)
        (RUNS / f).write_text(json.dumps(m, indent=1))
        print(f"{f}: {m['kind']} on {meta['n']} trades / {meta['rounds']} rounds | weights (standardised): " +
              ", ".join(f"{n} {w:+.3f}" for n, w in zip(["const"] + NAMES, m["w"])))


def selfcheck():
    import random
    rnd = random.Random(1)
    X = [[rnd.gauss(0, 1), rnd.gauss(0, 1)] for _ in range(4000)]
    y = [1.0 if rnd.random() < 1 / (1 + math.exp(-(0.5 + 2 * a - b))) else 0.0 for a, b in X]
    m = fit_logit(X, y, l2=0.01)
    wa, wb = m["w"][1] / m["std"][0], m["w"][2] / m["std"][1]
    assert abs(wa - 2) < 0.25 and abs(wb + 1) < 0.2, (wa, wb)
    yr = [1 + 3 * a - 2 * b + rnd.gauss(0, 0.1) for a, b in X]
    r = fit_ridge(X, yr, lam=0.0)
    assert abs(r["w"][1] / r["std"][0] - 3) < 0.02 and abs(r["w"][2] / r["std"][1] + 2) < 0.02, r["w"]
    assert 0 < p_win(m, [9, -9]) < 1
    st = State()
    t0 = 1_000_000.0
    for i in range(400):                               # 400 s of a slow drift, round starting at t0 + 300
        st.deribit(t0 + i, 80000 * (1 + 1e-5 * math.sin(i / 7)))
        st.book(t0 + i, "r", 0.49, 0.51)
    st.deribit(t0 + 400.2, 80000 * 1.0003)             # a fast move up
    st.book(t0 + 400.2, "r", 0.49, 0.51)               # book not repriced yet
    up = st.features(t0 + 400.3, "r", t0 + 300, "Up", 0.49, 0.51, 200)
    dn = st.features(t0 + 400.3, "r", t0 + 300, "Down", 0.49, 0.51, 200)
    assert up and dn and len(up) == len(NAMES)
    assert up[2] > 0 > dn[2] and up[4] > 0 > dn[4] and abs(up[0] + dn[0]) < 1e-9, (up, dn)
    rm = {"kind": "logit", "mean": [0.0] * 8, "std": [1.0] * 8, "w": [0.1] * 9}
    n = 10000
    t = time.perf_counter()
    for _ in range(n):
        edge(rm, st.features(t0 + 400.3, "r", t0 + 300, "Up", 0.49, 0.51, 200), 0.51)
    us = (time.perf_counter() - t) / n * 1e6
    assert us < 50, f"decision {us:.1f} us"
    print(f"ok | decision (features + edge) {us:.1f} us")


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        selfcheck()
    elif a[0] == "test":
        walk_forward()
    elif a[0] == "train":
        kw = {k[2:]: float(v) for k, v in zip(a[1::2], a[2::2])}
        train(**kw)
