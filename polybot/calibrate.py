"""Does a paper fill model reproduce what the live bot really got? Read-only.

For every real order the live bot sent (rows in `signals` with outcome 'bought' or 'nofill'), look at the recorded
book at the moment the order could have matched and predict: fills if ask <= signal ask + pad, else killed.
Compare with what really happened. The matching moment is unknown inside the order call (it includes a ~150 ms
taker delay and the trip back), so several guesses are tried: signal + code time + FRAC * order call.

python calibrate.py runs/live-t01.db --since 2026-10-23T22:48 --pad 0.02 [--until ...]
     (pad 0.01 for orders before 18:41, 0.02 after; the db needs `signals` and the 10 Hz `ticks`)
"""
import argparse
import bisect
import sqlite3
from datetime import datetime, timezone


def epoch(iso):
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("db")
    p.add_argument("--since", default="2026-10-23T15:59")
    p.add_argument("--until", default="2099-01-01T00:00")
    p.add_argument("--pad", type=float, default=0.02)
    a = p.parse_args()
    db = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    lo, hi = epoch(a.since), epoch(a.until)
    sigs = db.execute("SELECT t, slug, side, sig_ask, ms_to_order, ms_order, outcome, fill_px, reason FROM signals"
                      " WHERE t BETWEEN ? AND ? AND outcome IN ('bought','nofill') AND sig_ask IS NOT NULL"
                      " AND ms_to_order IS NOT NULL ORDER BY t", (lo, hi)).fetchall()
    ticks = {}
    for t, slug, ua, da in db.execute("SELECT t, slug, up_ask, dn_ask FROM ticks WHERE t BETWEEN ? AND ? ORDER BY t",
                                      (lo - 30, hi + 30)):
        ticks.setdefault(slug, ([], []))
        ticks[slug][0].append(t)
        ticks[slug][1].append((ua, da))
    # nofill rows caused by something other than a price run-away (balance, size...) say nothing about the fill model
    sigs = [s for s in sigs if s[6] == "bought" or "no orders found" in (s[8] or "") or (s[8] or "") == "no_fill"]
    print(f"{len(sigs)} real orders from {a.since} (pad {a.pad}): "
          f"{sum(s[6] == 'bought' for s in sigs)} filled, {sum(s[6] == 'nofill' for s in sigs)} killed\n")
    if not sigs:
        return
    print(f"{'match at':>26} {'right':>6} {'wrong':>6} {'accuracy':>9}   predicted fill/real kill   predicted kill/real fill")
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        right = fp = fn = missing = 0
        diffs = []
        for t, slug, side, sig_ask, ms_code, ms_order, outcome, fill_px, _ in sigs:
            if slug not in ticks:
                missing += 1
                continue
            times, vals = ticks[slug]
            tm = t + ms_code / 1000 + frac * (ms_order or 0) / 1000
            i = bisect.bisect_left(times, tm)
            if i >= len(times) or times[i] - tm > 0.5:
                missing += 1
                continue
            ask = vals[i][0] if side == "Up" else vals[i][1]
            if ask is None:
                missing += 1
                continue
            pred = ask <= sig_ask + a.pad + 1e-9
            real = outcome == "bought"
            right += pred == real
            fp += pred and not real
            fn += (not pred) and real
            if real and fill_px:
                diffs.append((fill_px - ask) * 100)
        n = right + fp + fn
        extra = f"   mean (real fill - recorded ask) {sum(diffs) / len(diffs):+.2f} c" if diffs else ""
        print(f"signal + code + {frac:4.2f} x call {right:6d} {fp + fn:6d} {100 * right / n if n else 0:8.0f}%"
              f"   {fp:10d}                 {fn:10d}{extra}" + (f"  ({missing} without book data)" if missing else ""))
    print("\nBest row = the moment the order really matched. If even the best row is far below ~85%, the recorded books"
          "\nare not a good picture of what the order met, and the paper fill model cannot be trusted for this strategy.")


if __name__ == "__main__":
    main()
