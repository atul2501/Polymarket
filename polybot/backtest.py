"""Replay past BTC 5m windows: model vs market, and tune the fair-value knobs.

python backtest.py [N]            history mode: Polymarket prices-history (~1 point/min) for the last N windows.
                                  Its price lags the live book by ~8 cents, so trust the model-vs-outcome fit
                                  (VOL_MULT, logloss) here, NOT the PnL.
python backtest.py --ticks DB...  replay the real 1-second books the bot recorded in ledger ticks tables.
                                  This is the mode to tune MODEL_WEIGHT / FAIR_MIN_EDGE / bands on.
Tunes on the older half, reports on the newer half. Downloads are cached in runs/backtest_cache.json.
"""
import itertools
import json
import sqlite3
import sys
import time
from math import log, sqrt
from pathlib import Path
from statistics import stdev

import requests

import config
from engine import Book, Snapshot, check_fair, fair_up
from market import MarketFeed

CACHE = Path(__file__).parent / "runs" / "backtest_cache.json"
HALF_SPREAD = 0.01
STAKE = 5.0


def fetch(n):
    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    http = requests.Session()
    now = int(time.time())
    last = now - now % 300 - 900  # skip windows that may not be resolved yet
    for k in range(n):
        st = last - k * 300
        if str(st) in cache:
            continue
        rows = http.get(f"{config.GAMMA}/markets", params={"slug": f"btc-updown-5m-{st}", "closed": "true"}).json()
        if not rows or rows[0].get("umaResolutionStatus") != "resolved":
            continue
        up_token = json.loads(rows[0]["clobTokenIds"])[0]
        hist = http.get(f"{config.CLOB}/prices-history",
                        params={"market": up_token, "startTs": st, "endTs": st + 300, "fidelity": 1}).json()["history"]
        k1s = http.get(f"{config.BINANCE}/api/v3/klines", params={
            "symbol": "BTCUSDT", "interval": "1s", "startTime": (st - 60) * 1000,
            "endTime": (st + 300) * 1000 - 1, "limit": 1000}).json()
        k1m = http.get(f"{config.BINANCE}/api/v3/klines", params={
            "symbol": "BTCUSDT", "interval": "1m", "endTime": st * 1000 - 1, "limit": 31}).json()
        closes = [float(x[4]) for x in k1m]
        cache[str(st)] = {
            "up": float(json.loads(rows[0]["outcomePrices"])[0]) == 1,
            "hist": [(h["t"], h["p"]) for h in hist],
            "c": {int(x[0]) // 1000: float(x[4]) for x in k1s},
            "sigma": stdev(log(b / a) for a, b in zip(closes, closes[1:])) / sqrt(60),
        }
        print(f"\rfetched {k + 1}/{n}", end="", file=sys.stderr)
    CACHE.parent.mkdir(exist_ok=True)
    CACHE.write_text(json.dumps(cache))
    print(file=sys.stderr)
    out = []
    for st, w in sorted(cache.items(), key=lambda kv: int(kv[0]))[-n:]:
        w["c"] = {int(t): p for t, p in w["c"].items()}
        out.append((int(st), w))
    return out


def points(windows):
    """(window, t_left, model p, market p, up?) at every price-history point."""
    out = []
    for st, w in windows:
        c = w["c"]
        pre = [c[t] for t in range(st - 60, st) if t in c]
        if len(pre) < 50:
            continue
        beat = sum(pre) / len(pre)
        for t, p_mkt in w["hist"]:
            t_left = st + 300 - t
            if not 0 < t_left < 300:
                continue
            spot = next((c[s] for s in range(t, t - 10, -1) if s in c), None)
            if spot is None:
                continue
            fin = [c[s] for s in range(st + 240, t) if s in c]
            p = fair_up(spot, beat, w["sigma"] * config.VOL_MULT, t_left, sum(fin) / len(fin) if fin else None)
            out.append((st, t_left, p, p_mkt, w["up"]))
    return out


def brier(pts, i):
    return sum((r[i] - r[4]) ** 2 for r in pts) / len(pts)


def logloss(pts, i):
    eps = 1e-4
    return -sum(log(max(eps, r[i] if r[4] else 1 - r[i])) for r in pts) / len(pts)


def book(p):
    p = min(max(p, 0.01), 0.99)
    return Book(round(p - HALF_SPREAD, 2), 1e9, round(p + HALF_SPREAD, 2), 1e9)


def tick_points(dbs):
    """(window, t_left, model p, market mid, up?, up Book, down Book) from recorded 1s ticks."""
    feed, outcome, out = MarketFeed(), {}, []
    for db in dbs:
        rows = sqlite3.connect(db).execute("SELECT * FROM ticks ORDER BY t").fetchall()
        for (t, slug, end, t_left, ub, ua, uas, db_, da, das, spot, beat, known, sigma, _p) in rows:
            if slug not in outcome:
                outcome[slug] = feed.outcome(slug)
            if outcome[slug] is None or ub is None or ua is None:
                continue
            p = fair_up(spot, beat, sigma * config.VOL_MULT, t_left, known)
            out.append((end, t_left, p, (ub + ua) / 2, outcome[slug] == "Up",
                        Book(ub, 0, ua, uas), Book(db_, 0, da, das)))
    return out


def simulate(pts):
    """One fair-value trade per window (like the live bot) -> (trades, wins, pnl)."""
    done, trades, wins, pnl = set(), 0, 0, 0.0
    for st, t_left, p, p_mkt, up, *books in pts:
        if st in done:
            continue
        up_b, dn_b = books if books else (book(p_mkt), book(1 - p_mkt))
        s = Snapshot(up_b, dn_b, t_left, config.DEFAULT_FEE, STAKE, p, p_mkt)
        if opp := check_fair(s):
            leg = opp.legs[0]
            won = (leg.side == "Up") == up
            pnl += (leg.shares if won else 0) - leg.price * leg.shares - leg.fee
            trades += 1
            wins += won
            done.add(st)
    return trades, wins, pnl


GRID = {
    "MODEL_WEIGHT": [1.0, 0.75, 0.5, 0.25],
    "FAIR_MIN_EDGE": [0.02, 0.03, 0.05, 0.08, 0.12],
    "FAIR_MIN_ASK": [0.0, 0.1, 0.2, 0.3],
    "FAIR_MAX_T": [300, 240, 180, 120],
    "FAIR_MIN_T": [30, 60],
}


def set_knobs(knobs):
    for k, v in knobs.items():
        setattr(config, k, v)
    config.FAIR_MAX_ASK = 1 - config.FAIR_MIN_ASK


def main():
    if sys.argv[1:2] == ["--ticks"]:
        load = lambda: tick_points(sys.argv[2:])
        allp = load()
        ends = sorted({r[0] for r in allp})
        cut = ends[len(ends) // 2] if ends else 0
        split = lambda pts: ([r for r in pts if r[0] < cut], [r for r in pts if r[0] >= cut])
        print(f"tick mode: {len(allp)} ticks, {len(ends)} resolved windows")
    else:
        windows = fetch(int(sys.argv[1]) if len(sys.argv) > 1 else 200)
        half = len(windows) // 2
        load = lambda: points(windows)
        cut = windows[half][0]
        split = lambda pts: ([r for r in pts if r[0] < cut], [r for r in pts if r[0] >= cut])
        print(f"history mode: {len(windows)} windows")

    def fit(k):
        config.VOL_MULT = k
        return logloss(split(load())[0], 2)
    best_k = min((0.6, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5), key=fit)
    config.VOL_MULT = best_k
    pts_tr, pts_te = split(load())
    print(f"\nVOL_MULT fitted on train: {best_k}")
    print(f"{'':10} {'model brier':>12} {'market brier':>13} {'model logloss':>14} {'market logloss':>15}")
    for name, pts in (("train", pts_tr), ("test", pts_te)):
        print(f"{name:10} {brier(pts, 2):12.4f} {brier(pts, 3):13.4f} {logloss(pts, 2):14.4f} {logloss(pts, 3):15.4f}")
    print("\nby time left (test): model vs market brier")
    for lo, hi in ((240, 300), (180, 240), (120, 180), (60, 120), (0, 60)):
        b = [r for r in pts_te if lo < r[1] <= hi]
        if b:
            print(f"  {lo:3}-{hi:3}s  n={len(b):3}  model {brier(b, 2):.4f}  market {brier(b, 3):.4f}")

    baseline = dict(MODEL_WEIGHT=1.0, FAIR_MIN_EDGE=0.05, FAIR_MIN_ASK=0.0, FAIR_MAX_T=300, FAIR_MIN_T=0)
    set_knobs(baseline)
    print("\ncurrent live settings:  train %d trades %d wins $%+.2f | test %d trades %d wins $%+.2f"
          % (*simulate(pts_tr), *simulate(pts_te)))

    results = []
    for combo in itertools.product(*GRID.values()):
        knobs = dict(zip(GRID, combo))
        set_knobs(knobs)
        tr = simulate(pts_tr)
        if tr[0] >= 10:
            results.append((tr[2], knobs, tr))
    results.sort(key=lambda r: -r[0])
    print("\ntop 5 on train -> same settings on test")
    for pnl, knobs, tr in results[:5]:
        set_knobs(knobs)
        te = simulate(pts_te)
        print(f"  {knobs}\n     train {tr[0]} trades {tr[1]} wins ${tr[2]:+.2f} | test {te[0]} trades {te[1]} wins ${te[2]:+.2f}")


if __name__ == "__main__" and sys.argv[1:2] != ["--v3"]:
    main()


# ---- v3 replay: exits, re-entry/flip, Kelly sizing, vol filter, snipe threshold, on real recorded books ----
def replay_rounds(dbs):
    """{window_end: (up_won, [(t_left, Snapshot-args...)])} from recorded ticks, in time order."""
    feed, rounds = MarketFeed(), {}
    for db in dbs:
        for (t, slug, end, t_left, ub, ua, uas, dbid, da, das, spot, beat, known, sigma, _p) in \
                sqlite3.connect(db).execute("SELECT * FROM ticks ORDER BY t"):
            if end not in rounds:
                rounds[end] = [feed.outcome(slug), [], sigma]
            if ub is None and ua is None:
                continue
            rounds[end][1].append((t_left, ub, ua, uas, dbid, da, das, spot, beat, known, sigma))
    return {e: r for e, r in rounds.items() if r[0] is not None}


def run_v3(rounds, exits=True, reentry=True, kelly=True, vol=True, snipe_min=0.97):
    """Same rules as main.py, one round at a time. Returns (entries, wins, pnl)."""
    from engine import check_exit, check_snipe
    saved = (config.KELLY_SIZING, config.SNIPE_MIN_FAIR)
    config.KELLY_SIZING = kelly
    config.SNIPE_MIN_FAIR = snipe_min
    ends = sorted(rounds)
    sigmas = {e: rounds[e][2] for e in ends}
    n = wins = 0
    pnl = 0.0
    for i, end in enumerate(ends):
        up_won, ticks, sig = rounds[end][0] == "Up", rounds[end][1], sigmas[end]
        prev = sorted(sigmas[e] for e in ends[max(0, i - 12):i])
        hot = vol and len(prev) >= 6 and sig > prev[int(0.8 * (len(prev) - 1))]
        pos, entries = None, {"fair": 0, "snipe": 0}
        for (t_left, ub, ua, uas, dbid, da, das, spot, beat, known, sigma) in ticks:
            p = fair_up(spot, beat, sigma * config.VOL_MULT, t_left, known)
            p_mkt = (ub + ua) / 2 if ub is not None and ua is not None else None
            s = Snapshot(Book(ub, 0, ua, uas or 0), Book(dbid, 0, da, das or 0), t_left, config.DEFAULT_FEE, STAKE, p, p_mkt)
            if pos and exits and (ex := check_exit(pos[0], s)):
                pnl += pos[1] * (ex[0] - ex[1]) - pos[2]
                pos = None
                continue
            if pos:
                continue
            for name, check in (("fair", check_fair), ("snipe", check_snipe)):
                limit = 2 if (name == "fair" and reentry) else 1
                if entries[name] >= limit or (name == "fair" and hot):
                    continue
                if opp := check(s):
                    leg = opp.legs[0]
                    pos = (leg.side, leg.shares, leg.price * leg.shares + leg.fee)
                    entries[name] += 1
                    n += 1
                    break
        if pos:  # held to settlement
            won = (pos[0] == "Up") == up_won
            pnl += (pos[1] if won else 0) - pos[2]
            wins += won
    config.KELLY_SIZING, config.SNIPE_MIN_FAIR = saved
    return n, wins, pnl


def v3_main(dbs):
    rounds = replay_rounds(dbs)
    ends = sorted(rounds)
    test = {e: rounds[e] for e in ends[len(ends) // 2:]}
    train = {e: rounds[e] for e in ends[:len(ends) // 2]}
    print(f"v3 replay: {len(rounds)} resolved rounds (train {len(train)}, test {len(test)}), $5 cap per trade")
    variants = [
        ("v2 (hold, 1 entry, flat $5, no vol filter)", dict(exits=False, reentry=False, kelly=False, vol=False)),
        ("+ exit rule", dict(exits=True, reentry=False, kelly=False, vol=False)),
        ("+ exit + re-entry/flip", dict(exits=True, reentry=True, kelly=False, vol=False)),
        ("+ Kelly sizing", dict(exits=True, reentry=True, kelly=True, vol=False)),
        ("+ vol filter (full v3)", dict(exits=True, reentry=True, kelly=True, vol=True)),
        ("full v3, snipe 0.93", dict(exits=True, reentry=True, kelly=True, vol=True, snipe_min=0.93)),
    ]
    print(f"{'variant':44} {'train':>22} {'test':>22}")
    for name, kw in variants:
        tr, te = run_v3(train, **kw), run_v3(test, **kw)
        print(f"{name:44} {tr[0]:3} entries ${tr[2]:+7.2f}   {te[0]:3} entries ${te[2]:+7.2f}")


if __name__ == "__main__" and sys.argv[1:2] == ["--v3"]:
    v3_main(sys.argv[2:])
