"""Replay the taker strategy on recorded ticks + Binance mids at several order delays: how much edge is left?

For each Binance signal (>= MOVE_BPS in MOVE_SEC) it buys the side's ask seen DELAY seconds later (if >= SIZE shares
sit there and the ask is in the band), sells at the bid HOLD seconds after that, and charges the taker fee on both
legs. Same entry rules as taker.py (cooldown, no entry in the last MIN_LEFT seconds). No money, no orders: read-only.

python latency_sweep.py runs/live-t01.db [more.db ...] [--since 2026-10-23T15:00] [--delays 0.1,0.3,0.5,0.8]
                                          [--bps 2] [--sec 1] [--hold 5] [--size 5] [--band 0.05,0.95]
--max-spread X   skip a signal whose side has ask - bid wider than X at signal time (taker.py: TK_MAX_SPREAD)
--pad X    the order's limit is the ask seen at signal time + X (live: TK_SLIP 0.01); it fills only if the ask at
           arrival is within it, else it is killed (still uses the cooldown). Default: always fills.
--settle   hold to settlement instead of selling (W-3bp style): pays 1 per share if the side wins, else 0, fee on
           the buy only; the cooldown then applies per side, not across both. A round counts only if its last
           recorded tick is within 10 s of the end. W-3bp: --bps 3 --sec 3 --settle --cooldown 2 --band 0.15,0.85
Each db needs the `ticks` and `binance` tables that every taker.py ledger records (~10 Hz).
"""
import argparse
import bisect
import sqlite3
from datetime import datetime, timezone

import config
from engine import taker_fee

GAP = 0.35   # a book/Binance sample further than this from the wanted time counts as missing data


def load(path, since):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    t0 = datetime.fromisoformat(since).replace(tzinfo=timezone.utc).timestamp() if since else 0
    bn = db.execute("SELECT t, mid FROM binance WHERE t >= ? ORDER BY t", (t0,)).fetchall()
    ticks = db.execute("SELECT t, slug, t_left, up_bid, up_ask, up_ask_size, dn_bid, dn_ask, dn_ask_size"
                       " FROM ticks WHERE t >= ? ORDER BY t", (t0,)).fetchall()
    return bn, ticks


def at_or_after(times, rows, t):
    """First row with time >= t, if within GAP of t."""
    i = bisect.bisect_left(times, t)
    return rows[i] if i < len(rows) and rows[i][0] - t <= GAP else None


def replay(bn, ticks, delay, a):
    bt, tt = [r[0] for r in bn], [r[0] for r in ticks]
    by_slug = {}
    for r in ticks:
        by_slug.setdefault(r[1], ([], []))
        by_slug[r[1]][0].append(r[0])
        by_slug[r[1]][1].append(r)
    fee = config.DEFAULT_FEE
    out, misses, last_entry = [], [0], {True: -1e9, False: -1e9}
    busy_until = -1e9                  # taker.py enters only with no open position (not in --settle mode)
    for t, mid in bn:
        i = bisect.bisect_right(bt, t - a.sec) - 1
        if i < 0 or t - a.sec - bt[i] > GAP:
            continue
        bps = (mid / bn[i][1] - 1) * 1e4
        if abs(bps) < a.bps:
            continue
        up = bps > 0
        if t - (last_entry[up] if a.settle else max(last_entry.values())) < a.cooldown or t < busy_until:
            continue
        j = bisect.bisect_right(tt, t) - 1                     # the round being traded: smallest t_left near t
        near = [r for r in ticks[max(0, j - 6):j + 1] if t - r[0] <= GAP and r[2] > 0]
        if not near:
            continue
        slug = min(near, key=lambda r: r[2])[1]
        if min(near, key=lambda r: r[2])[2] < a.min_left:
            continue
        times, rows = by_slug[slug]
        enter = at_or_after(times, rows, t + delay)
        if not enter:
            continue
        ask, size = (enter[4], enter[5]) if up else (enter[7], enter[8])
        if ask is None or not a.lo <= ask <= a.hi or (size or 0) < a.size:
            continue
        if a.max_spread is not None:
            seen = at_or_after(times, rows, t)
            sb, sa = (None, None) if not seen else ((seen[3], seen[4]) if up else (seen[6], seen[7]))
            if sb is None or sa is None or sa - sb > a.max_spread + 1e-9:
                continue                                         # the bot would not have sent an order
        last_entry[up] = t + delay
        if a.pad is not None:
            seen = at_or_after(times, rows, t)               # the book the bot saw when it fired
            sig_ask = None if not seen else (seen[4] if up else seen[7])
            if sig_ask is not None and ask > sig_ask + a.pad + 1e-9:
                misses[0] += 1                               # price ran away: FAK killed, no trade
                continue
        if a.settle:
            end = rows[-1]                                    # the round's last recorded tick decides the winner
            if end[2] > 10 or end[3] is None:
                continue
            win = (end[3] >= 0.5) == up
            pnl = ((1.0 if win else 0.0) - ask) * a.size - taker_fee(ask, a.size, fee)
            out.append((t, ask, 1.0 if win else 0.0, pnl))
            continue
        busy_until = t + delay + a.hold
        leave = at_or_after(times, rows, t + delay + a.hold)
        bid = None if not leave else (leave[3] if up else leave[6])
        if bid is None:
            continue
        pnl = (bid - ask) * a.size - taker_fee(ask, a.size, fee) - taker_fee(bid, a.size, fee)
        out.append((t, ask, bid, pnl))
    return out, misses[0]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("dbs", nargs="+")
    p.add_argument("--since")
    p.add_argument("--delays", default="0.1,0.2,0.3,0.4,0.5,0.7,1.0")
    p.add_argument("--bps", type=float, default=2)
    p.add_argument("--sec", type=float, default=1)
    p.add_argument("--hold", type=float, default=5)
    p.add_argument("--size", type=float, default=5)
    p.add_argument("--cooldown", type=float, default=10)
    p.add_argument("--min-left", type=float, default=30, dest="min_left")
    p.add_argument("--band", default="0.05,0.95")
    p.add_argument("--settle", action="store_true")
    p.add_argument("--pad", type=float, default=None)
    p.add_argument("--max-spread", type=float, default=None, dest="max_spread")
    a = p.parse_args()
    a.lo, a.hi = (float(x) for x in a.band.split(","))
    data = [load(d, a.since) for d in a.dbs]
    hours = sum((bn[-1][0] - bn[0][0]) / 3600 for bn, _ in data if len(bn) > 1)
    print(f"{len(a.dbs)} db(s), {hours:.1f} h of data | {a.bps:g} bp in {a.sec:g}s, size {a.size:g}, "
          f"ask {a.lo}-{a.hi}, cooldown {a.cooldown:g}s{' per side' if a.settle else ''}, "
          f"{'hold to settlement' if a.settle else f'sell after {a.hold:g}s'}\n")
    print(f"{'delay':>6} {'trades':>7} {'fill%':>6} {'win%':>5} {'c/share':>8} {'total $':>8} {'$/hour':>7}")
    for d in (float(x) for x in a.delays.split(",")):
        res = [replay(bn, ticks, d, a) for bn, ticks in data]
        rows, miss = [r for out, _ in res for r in out], sum(m for _, m in res)
        if not rows:
            print(f"{d:6.2f}      0")
            continue
        pnl = sum(r[3] for r in rows)
        print(f"{d:6.2f} {len(rows):7d} {100 * len(rows) / (len(rows) + miss):6.0f} "
              f"{100 * sum(r[3] > 0 for r in rows) / len(rows):5.0f} "
              f"{100 * pnl / (len(rows) * a.size):8.2f} {pnl:8.2f} {pnl / hours if hours else 0:7.2f}")


if __name__ == "__main__":
    main()
