"""Where does a live trade lose against the replay? One row per closed live trade, split into its parts (cents/share):

  move    recorded bid HOLD s after the buy - ask the bot saw at the signal   (what the replay counts as gross)
  entry   price paid - ask seen at the signal                 (+ = paid more than it saw: a cost)
  exit    recorded bid HOLD s after the buy - what the sell really got   (+ = sold below the bid: a cost)
  fees    both taker fees
  net     = move - entry - exit - fees = pnl / shares

Needs the `signals` rows (outcome 'bought') and the 10 Hz `ticks` that taker.py records.

python trade_audit.py runs/live-t01.db [--since 2026-10-23T18:41] [--hold 12]
"""
import argparse
import bisect
import sqlite3
from datetime import datetime, timezone

import config
from engine import taker_fee


def epoch(iso):
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()


def sell_avg(payout, shares):
    """Average sell price from the booked payout (= shares * (avg - fee per share))."""
    avg = payout / shares
    for _ in range(3):
        avg = payout / shares + taker_fee(avg, 1, config.DEFAULT_FEE)
    return avg


def main():
    p = argparse.ArgumentParser()
    p.add_argument("db")
    p.add_argument("--since", default="2026-10-23T18:41")
    p.add_argument("--hold", type=float, default=12)
    a = p.parse_args()
    db = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    trades = db.execute("SELECT id, ts, slug, side, price, shares, fee, payout, pnl, status FROM trades"
                        " WHERE mode='live' AND strategy='taker' AND ts >= ? AND status!='open' ORDER BY id",
                        (a.since,)).fetchall()
    ticks = {}
    for t, slug, ub, ua, db_, da in db.execute("SELECT t, slug, up_bid, up_ask, dn_bid, dn_ask FROM ticks WHERE t >= ?"
                                              " ORDER BY t", (epoch(a.since) - 60,)):
        ticks.setdefault(slug, ([], []))
        ticks[slug][0].append(t)
        ticks[slug][1].append((ub, ua, db_, da))
    head = f"{'time':8} {'side':4} {'paid':>5} {'sigask':>6} {'entry':>6} {'move':>6} {'exit':>6} {'fees':>5} {'net':>6} {'ms':>9}  lag"
    print(head)
    rows = []
    for tid, ts, slug, side, price, shares, fee, payout, pnl, status in trades:
        t = epoch(ts)
        sig = db.execute("SELECT t, sig_ask, ms_to_order, ms_order, bn_lag FROM signals WHERE outcome='bought' AND slug=?"
                         " AND side=? AND t BETWEEN ? AND ? ORDER BY ABS(t - ?) LIMIT 1",
                         (slug, side, t - 6, t + 1.5, t)).fetchone()
        sig_ask = sig[1] if sig else None
        # time the buy was filled: signal + code + order call; fall back to the booked second
        t_buy = sig[0] + ((sig[2] or 0) + (sig[3] or 0)) / 1000 if sig else t
        bid_exit = None
        if status == "sold" and slug in ticks:
            times, vals = ticks[slug]
            i = bisect.bisect_left(times, t_buy + a.hold)
            if i < len(times) and times[i] - (t_buy + a.hold) < 1:
                bid_exit = vals[i][0] if side == "Up" else vals[i][2]
        sold = sell_avg(payout, shares) if status == "sold" and shares else None
        c = lambda x: None if x is None else x * 100
        entry = c(price - sig_ask) if sig_ask is not None else None
        move = c(bid_exit - sig_ask) if bid_exit is not None and sig_ask is not None else None
        exit_ = c(bid_exit - sold) if bid_exit is not None and sold is not None else None
        fees = c(fee / shares + (taker_fee(sold, 1, config.DEFAULT_FEE) if sold else 0))
        net = c(pnl / shares)
        f = lambda x, w: f"{x:{w}.1f}" if x is not None else " " * (w - 1) + "-"
        ms = f"{sig[2]:.0f}+{sig[3]:.0f}" if sig and sig[2] is not None and sig[3] is not None else "-"
        lag = f"{sig[4]:.0f}" if sig and sig[4] is not None else "-"
        print(f"{ts[11:19]:8} {side:4} {price:5.2f} {f(sig_ask, 6)} {f(entry, 6)} {f(move, 6)} {f(exit_, 6)} {f(fees, 5)} "
              f"{f(net, 6)} {ms:>9}  {lag}")
        rows.append((entry, move, exit_, fees, net, shares))
    if not rows:
        return
    print("-" * len(head))
    for name, k in (("entry", 0), ("move", 1), ("exit", 2), ("fees", 3), ("net", 4)):
        xs = [r[k] for r in rows if r[k] is not None]
        print(f"{name:6} mean {sum(xs) / len(xs):+6.2f} c/share over {len(xs)} trades" if xs else f"{name:6} no data")
    full = [r for r in rows if None not in r[:5]]
    if full:
        gap = sum(r[4] - (r[1] - r[0] - r[2] - r[3]) for r in full) / len(full)
        print(f"\nmove - entry - exit - fees vs net: mean gap {gap:+.2f} c/share over {len(full)} complete trades (0 = all explained)")


if __name__ == "__main__":
    main()
