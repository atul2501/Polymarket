"""Read-only report on taker ledgers: where signals went (bought / no fill / skipped and why), order latency,
slow loop passes, slippage and P/L by hour and ask band. Needs the `signals` table written by taker.py.

python report.py runs/live-t01.db [runs/tk-fast.db ...] [--since 2026-10-23T10:00]   markdown to stdout
"""
import sqlite3
import sys
from datetime import datetime, timezone


def q(db, sql, *a):
    return db.execute(sql, a).fetchall()


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else None


def fmt(x, nd=0):
    return "-" if x is None else f"{x:.{nd}f}"


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return out


def report(path, since):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    t0 = datetime.fromisoformat(since).replace(tzinfo=timezone.utc).timestamp() if since else 0
    iso = since or ""
    L = [f"## {path}", ""]
    n, = q(db, "SELECT count(*) FROM signals WHERE t >= ? AND outcome NOT IN ('slowloop')", t0)[0]
    L.append(f"Signals: **{n}**")
    L += [""] + table(["outcome", "reason", "count"], q(
        db, "SELECT outcome, reason, count(*) FROM signals WHERE t >= ? AND outcome != 'slowloop'"
            " GROUP BY 1, 2 ORDER BY 3 DESC", t0))
    # a "skip" row is written once a second per reason, so skip counts are seconds of missed opportunity, not signals
    rows = q(db, "SELECT ms_to_order, ms_order, fill_px, ask_now, sig_ask FROM signals WHERE t >= ? AND outcome IN ('bought','nofill')", t0)
    if rows:
        a, b = [r[0] for r in rows], [r[1] for r in rows]
        L += ["", "### Latency (ms)", ""] + table(["", "p50", "p95", "max"], [
            ["signal -> order sent"] + [fmt(pct(a, .5)), fmt(pct(a, .95)), fmt(max((x for x in a if x is not None), default=None))],
            ["order call (post -> response)"] + [fmt(pct(b, .5)), fmt(pct(b, .95)), fmt(max((x for x in b if x is not None), default=None))]])
        sl = [(r[2] - r[4]) * 100 for r in rows if r[2] is not None and r[4] is not None]
        if sl:
            L += ["", f"Slippage, fill vs ask at signal: mean {sum(sl) / len(sl):+.2f} c/share over {len(sl)} fills "
                      f"(p95 {pct(sl, .95):+.2f} c)"]
    try:
        lags = [x[0] for x in q(db, "SELECT bn_lag FROM signals WHERE t >= ? AND outcome IN ('bought','nofill')", t0)]
        if any(x is not None for x in lags):
            L += ["", f"Binance lag when the signal fired (ms): p50 {fmt(pct(lags, .5))}, p95 {fmt(pct(lags, .95))}, "
                      f"max {fmt(max(x for x in lags if x is not None))}"]
    except sqlite3.OperationalError:
        pass                                                  # ledger from before the bn_lag column
    slow = q(db, "SELECT count(*), max(loop_ms), avg(loop_ms) FROM signals WHERE t >= ? AND outcome='slowloop'", t0)[0]
    L += ["", f"Slow loop passes: {slow[0]} (max {fmt(slow[1])} ms, avg {fmt(slow[2])} ms). Each one blocked signals."]
    L += ["", "### Trades", ""]
    L += table(["status", "n", "pnl $", "avg pnl c/share"], [
        (s, n_, fmt(p, 2), fmt(c, 2)) for s, n_, p, c in q(
            db, "SELECT status, count(*), sum(pnl), 100 * sum(pnl) / sum(shares) FROM trades WHERE ts >= ?"
                " GROUP BY 1", iso)])
    tot = q(db, "SELECT count(*), coalesce(sum(pnl),0), sum(pnl > 0) FROM trades WHERE ts >= ? AND status != 'open'", iso)[0]
    if tot[0]:
        L.append(f"\nClosed: {tot[0]} trades, P/L ${tot[1]:+.2f}, {100 * tot[2] / tot[0]:.0f}% positive")
    L += ["", "By hour (UTC)", ""] + table(["hour", "trades", "pnl $"], q(
        db, "SELECT substr(ts, 1, 13), count(*), round(sum(pnl), 2) FROM trades WHERE ts >= ? AND status != 'open'"
            " GROUP BY 1 ORDER BY 1", iso))
    L += ["", "By entry price band", ""] + table(["band", "trades", "pnl $", "c/share"], q(
        db, "SELECT CASE WHEN price < .3 THEN '<0.30' WHEN price < .5 THEN '0.30-0.50' WHEN price < .7 THEN '0.50-0.70'"
            " ELSE '>0.70' END b, count(*), round(sum(pnl), 2), round(100 * sum(pnl) / sum(shares), 2)"
            " FROM trades WHERE ts >= ? AND status != 'open' GROUP BY 1 ORDER BY 1", iso))
    return L


if __name__ == "__main__":
    args = sys.argv[1:]
    since = args.pop(args.index("--since") + 1) if "--since" in args else None
    if since:
        args.remove("--since")
    for p in args or ["runs/taker.db"]:
        print("\n".join(report(p, since)), "\n")
