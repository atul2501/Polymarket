"""Tune the taker rules on recorded data without fooling yourself.

A grid search over a few hundred trades always finds a "winner" by luck. So the search runs on the FIRST part of the
time line (train) and every candidate is then scored on the LATER part it never saw (test). Trust a setting only if it
is good on both; if the top train rows fall apart on test, the data cannot tell the settings apart yet.

python tune.py runs/live-t01.db [more.db ...] [--since 2026-10-22T17:00] [--split 0.6] [--delay 0.5] [--pad 0.02]
               [--min-trades 25] [--quick]
Uses the replay of latency_sweep.py (same fill rule: the order is killed when the ask ran past signal ask + pad).
"""
import argparse
import itertools
import statistics
import types

import latency_sweep as ls

GRID = dict(bps=[1.5, 2, 3], sec=[1, 2, 3], hold=[5, 8, 12, 20], band=[(0.05, 0.95), (0.10, 0.90)], max_spread=[None, 0.03])
QUICK = dict(bps=[2, 3], sec=[1, 3], hold=[8, 12], band=[(0.05, 0.95), (0.10, 0.90)], max_spread=[None, 0.03])


def args_for(base, bps, sec, hold, band, max_spread):
    return types.SimpleNamespace(bps=bps, sec=sec, hold=hold, lo=band[0], hi=band[1], max_spread=max_spread,
                                 size=base.size, cooldown=base.cooldown, min_left=base.min_left, pad=base.pad,
                                 settle=False)


def split_at(data, frac):
    """Time that leaves `frac` of the recorded time line before it (over all databases)."""
    lo = min(bn[0][0] for bn, _ in data if bn)
    hi = max(bn[-1][0] for bn, _ in data if bn)
    return lo + (hi - lo) * frac


def part(data, t0, t1):
    out = []
    for bn, ticks in data:
        out.append(([r for r in bn if t0 <= r[0] < t1], [r for r in ticks if t0 <= r[0] < t1]))
    return out


def score(data, a, delay):
    """(trades, c/share) of the replay over several databases."""
    rows = [r for bn, ticks in data for r in ls.replay(bn, ticks, delay, a)[0]]
    if not rows:
        return 0, None
    return len(rows), 100 * sum(r[3] for r in rows) / (len(rows) * a.size)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("dbs", nargs="+")
    p.add_argument("--since")
    p.add_argument("--split", type=float, default=0.6)
    p.add_argument("--delay", type=float, default=0.5)
    p.add_argument("--pad", type=float, default=0.02)
    p.add_argument("--min-trades", type=int, default=25, dest="min_trades")
    p.add_argument("--size", type=float, default=5)
    p.add_argument("--cooldown", type=float, default=10)
    p.add_argument("--min-left", type=float, default=30, dest="min_left")
    p.add_argument("--quick", action="store_true")
    base = p.parse_args()
    data = [ls.load(d, base.since) for d in base.dbs]
    cut = split_at(data, base.split)
    train, test = part(data, -1, cut), part(data, cut, 9e12)
    grid = QUICK if base.quick else GRID
    combos = list(itertools.product(*grid.values()))
    print(f"{len(combos)} settings | train = first {base.split:.0%} of the time line, test = the rest | delay {base.delay}s, pad {base.pad}\n")
    results = []
    for i, c in enumerate(combos, 1):
        kw = dict(zip(grid.keys(), c))
        a = args_for(base, **kw)
        n, cps = score(train, a, base.delay)
        results.append((cps if n >= base.min_trades else None, n, kw))
        if i % 10 == 0:
            print(f"  {i}/{len(combos)} tried", flush=True)
    ranked = sorted((r for r in results if r[0] is not None), key=lambda r: -r[0])
    if not ranked:
        print("no setting reached the minimum number of trades on the train part: lower --min-trades or add data")
        return
    print(f"\n{'rank':>4} {'bps':>4} {'sec':>3} {'hold':>4} {'band':>10} {'spread':>6} | {'train n':>7} {'c/share':>8} | {'test n':>6} {'c/share':>8}")
    tests = []
    for rank, (cps, n, kw) in enumerate(ranked[:10], 1):
        tn, tc = score(test, args_for(base, **kw), base.delay)
        tests.append(tc)
        print(f"{rank:>4} {kw['bps']:>4} {kw['sec']:>3} {kw['hold']:>4} {kw['band'][0]:.2f}-{kw['band'][1]:.2f} "
              f"{str(kw['max_spread'] or '-'):>6} | {n:>7} {cps:>+8.2f} | {tn:>6} {('%+8.2f' % tc) if tc is not None else '       -'}")
    good = [t for t in tests if t is not None]
    all_train = [r[0] for r in ranked]
    if good:
        print(f"\nTop-10 mean: train {statistics.mean(r[0] for r in ranked[:10]):+.2f}  ->  test {statistics.mean(good):+.2f} c/share"
              f"   (all {len(ranked)} settings on train: median {statistics.median(all_train):+.2f})")
        print("A drop from train to test is the price of searching: only believe the test column.")


if __name__ == "__main__":
    main()
