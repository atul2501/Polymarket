"""W-LEARN: a self-learning filter for W-SNIPE-DOG2, fast enough to sit inside taker.py's signal check.

Every night (polybot-learn.timer) it re-fits a logistic regression P(the bought side wins) on the base bot's settled
trades (runs/wsnipedog2.db: unfiltered, so the filter never learns only from trades it chose itself; every lost trade is
a labelled mistake), on the last TRAIN_DAYS days minus the newest day. The newest day is the exam: the new model replaces
runs/wlearn_model.json only when, on that unseen day, it keeps buys worth >= 0 %/$, positive in all thirds and without its
best 5 rounds, on >= MIN_TRADES trades, and does at least as well as the current model. Each night appends one line to
runs/wlearn_history.jsonl (results, losers avoided, winners missed, weights) - what it learned from its mistakes.

The bot (TK_AI=runs/wlearn_model.json) buys only when p - ask - fee >= margin; no model file = trades like the base bot.
Features use only what the bot knows at the signal: one decision ~10 multiplies + exp (check asserts < 0.05 ms).
    python selflearn.py train [--dry]   fit + exam (+ write unless --dry)
    python selflearn.py compare         live vs live filtered by the model, summed over exam nights: SWITCH yes/not yet
    python selflearn.py check           self-check
Live learner (polybot-learn.service): LEARN_SRC=wsnipedog2live.db LEARN_MIDS=feedrace.db LEARN_OUT=wlearnlive - the live
ledger records no prices (TK_RECORD off), so Deribit mids come from feedrace.db. Once live itself uses the filter it only
learns from trades it chose: then train it on the paper twin's unfiltered trades instead.
"""
import bisect, json, math, os, sqlite3, sys, time
from datetime import datetime
from pathlib import Path

RUNS = Path(__file__).parent / "runs"
SRC = RUNS / os.getenv("LEARN_SRC", "wsnipedog2.db")
MIDS = os.getenv("LEARN_MIDS", "")       # db with feedrace quotes for the signal-feed mid; empty = SRC's binance table
OUT = os.getenv("LEARN_OUT", "wlearn")
MODEL, HIST = RUNS / f"{OUT}_model.json", RUNS / f"{OUT}_history.jsonl"
TRAIN_DAYS, TEST_DAYS = float(os.getenv("LEARN_DAYS", 4)), 1
MIN_TRADES, MARGIN, L2 = 100, float(os.getenv("LEARN_MARGIN", 0.02)), 1.0
NAMES = ["bps", "ask", "spread", "log_size", "t_left", "dist", "z", "n_side", "lag"]


def feats(bps, ask, bid, size, t_left, dist, n_side, lag):
    """Feature vector in NAMES order; dist = signal-feed mid vs price to beat in bp, signed toward the bought side."""
    return [abs(bps or 0), ask, ask - bid if bid is not None else 0.05, math.log(max(size or 1, 1)), t_left, dist,
            dist / math.sqrt(max(t_left, 1)), n_side, lag or 0]


class Model:
    def __init__(self, d):
        self.d, self.margin = d, d["margin"]
        self.w = [w / s for w, s in zip(d["w"], d["sd"])]                    # standardisation folded into the weights
        self.b = d["b"] - sum(w * m for w, m in zip(self.w, d["mu"]))

    @classmethod
    def load(cls, path=MODEL):
        return cls(json.loads(Path(path).read_text())) if Path(path).exists() else None

    def p(self, x):
        z = self.b + sum(w * v for w, v in zip(self.w, x))
        return 1 / (1 + math.exp(-max(-30, min(30, z))))

    def keep(self, x, ask, fee_sh):
        return self.p(x) - ask - fee_sh >= self.margin


def fit(X, y):
    """L2 logistic regression, Newton steps on standardised features (pure Python, ~10 dims)."""
    n, k = len(X), len(X[0])
    mu = [sum(r[j] for r in X) / n for j in range(k)]
    sd = [max(1e-9, math.sqrt(sum((r[j] - mu[j]) ** 2 for r in X) / n)) for j in range(k)]
    Z = [[1.0] + [(r[j] - mu[j]) / sd[j] for j in range(k)] for r in X]
    w = [0.0] * (k + 1)
    for _ in range(25):
        g, H = [0.0] * (k + 1), [[0.0] * (k + 1) for _ in range(k + 1)]
        for z, t in zip(Z, y):
            p = 1 / (1 + math.exp(-max(-30, min(30, sum(a * b for a, b in zip(w, z))))))
            for i in range(k + 1):
                g[i] += (p - t) * z[i]
                for j in range(i + 1):
                    H[i][j] += p * (1 - p) * z[i] * z[j]
        for i in range(k + 1):
            for j in range(i):
                H[j][i] = H[i][j]
            if i:
                g[i] += L2 * w[i]; H[i][i] += L2
        step = solve(H, g)
        w = [a - b for a, b in zip(w, step)]
        if max(map(abs, step)) < 1e-8:
            break
    return {"names": NAMES, "b": w[0], "w": w[1:], "mu": mu, "sd": sd, "margin": MARGIN}


def solve(A, b):
    """Gaussian elimination with partial pivoting."""
    n = len(b)
    M = [row[:] + [v] for row, v in zip(A, b)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        for r in range(c + 1, n):
            f = M[r][c] / M[c][c]
            for j in range(c, n + 1):
                M[r][j] -= f * M[c][j]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (M[r][n] - sum(M[r][j] * x[j] for j in range(r + 1, n))) / M[r][r]
    return x


def load_rows(db, lo, hi, mids=None):
    """Settled base-bot buys in [lo, hi): dicts with features x, label y, slug, t, pnl, cost, ask, fee per share."""
    sigs = db.execute("SELECT t, slug, side, bps, sig_ask, sig_bid, sig_size, t_left, fill_px, bn_lag FROM signals "
                      "WHERE outcome='bought' AND t >= ? AND t < ? ORDER BY t", (lo, hi)).fetchall()
    trades = {}
    for tid, ts, slug, side, px, sh, fee, cost, st, pnl in db.execute(
            "SELECT id, ts, slug, side, price, shares, fee, cost, status, pnl FROM trades WHERE slug IN "
            "(SELECT DISTINCT slug FROM signals WHERE outcome='bought' AND t >= ? AND t < ?) ORDER BY id", (lo, hi)):
        trades.setdefault((slug, side), []).append((datetime.fromisoformat(ts).timestamp(), px, sh, fee, cost, st, pnl))
    # signal-feed mid: latest at or before each signal; all mids kept for the price to beat below
    need = sorted({s[0] for s in sigs})
    starts = {int(s[1].rsplit("-", 1)[1]) for s in sigs}
    ts_, mids_, spot, i, last = [], [], {}, 0, None
    q = ("SELECT t, mid FROM quotes WHERE feed='deribit' AND mid > 0 AND t >= ? AND t < ? ORDER BY t" if mids
         else "SELECT t, mid FROM binance WHERE t >= ? AND t < ? ORDER BY t")
    for t, mid in (mids or db).execute(q, (min(starts, default=lo) - 61, hi)):
        while i < len(need) and need[i] < t:
            spot[need[i]] = last; i += 1
        ts_.append(t); mids_.append(mid)
        last = (t, mid)
    for t in need[i:]:
        spot[t] = last
    rows = []
    for t, slug, side, bps, ask, bid, size, t_left, fill, lag in sigs:
        cands = trades.get((slug, side), [])
        j = next((j for j, c in enumerate(cands) if -1 <= c[0] - t <= 3 and abs(c[1] - fill) < 1e-9), None)
        if j is None or cands[j][5] not in ("won", "lost"):
            continue
        n_side = j                                       # fills before it this round (cands are in id order)
        _, _, sh, fee, cost, st, pnl = cands[j]
        cands[j] = (-1e18,) + cands[j][1:]               # used
        start = int(slug.rsplit("-", 1)[1])
        # mid at each second, carried forward up to 300 s like taker.py's secs (feedrace only logs Deribit changes)
        v = [mids_[j - 1] for k in range(start - 60, start)
             if (j := bisect.bisect_right(ts_, k + 1)) and k + 1 - ts_[j - 1] <= 300]
        sp = spot.get(t)
        if len(v) < 30 or not sp or t - sp[0] > 300:
            continue
        dist = (1 if side == "Up" else -1) * (sp[1] / (sum(v) / len(v)) - 1) * 1e4
        rows.append(dict(x=feats(bps, ask, bid, size, t_left, dist, n_side, lag), y=1 if st == "won" else 0, slug=slug,
                         t=t, pnl=pnl, cost=cost, ask=ask, fee_sh=fee / sh))
    return rows


def score(rows):
    """%/$, trades, per-third %/$, %/$ without the best 5 rounds."""
    pct = lambda rs: 100 * sum(r["pnl"] for r in rs) / sum(r["cost"] for r in rs) if rs else 0.0
    by = {}
    for r in rows:
        by.setdefault(r["slug"], []).append(r)
    order = sorted(by, key=lambda s: by[s][0]["t"])
    thirds = [pct([r for s in order[k * len(order) // 3:(k + 1) * len(order) // 3] for r in by[s]]) for k in range(3)]
    best = set(sorted(by, key=lambda s: -sum(r["pnl"] for r in by[s]))[:5])
    return dict(pct=round(pct(rows), 2), n=len(rows), pnl=round(sum(r["pnl"] for r in rows), 2),
                cost=round(sum(r["cost"] for r in rows), 2),
                thirds=[round(x, 1) for x in thirds], wo5=round(pct([r for r in rows if r["slug"] not in best]), 2))


def passes(s, cur):
    return (s["n"] >= MIN_TRADES and s["pct"] > 0 and all(x > 0 for x in s["thirds"]) and s["wo5"] > 0
            and (cur is None or s["pct"] >= cur["pct"]))


def train(dry=False):
    db = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True, timeout=30)
    end = db.execute("SELECT max(t) FROM signals WHERE outcome='bought'").fetchone()[0]
    if end is None:
        sys.exit(f"no bought signals in {SRC}")
    cut = end - TEST_DAYS * 86400
    mids = sqlite3.connect(f"file:{RUNS / MIDS}?mode=ro", uri=True, timeout=30) if MIDS else None
    tr, te = load_rows(db, cut - TRAIN_DAYS * 86400, cut, mids), load_rows(db, cut, end + 1, mids)
    if len(tr) < 3 * MIN_TRADES or not te:
        sys.exit(f"too little data: {len(tr)} train / {len(te)} test trades")
    new = Model(fit([r["x"] for r in tr], [r["y"] for r in tr]))
    cur = Model.load()
    kept = lambda m: [r for r in te if m.keep(r["x"], r["ask"], r["fee_sh"])]
    keep_new = kept(new)
    base, s_new = score(te), score(keep_new)
    s_cur = score(kept(cur)) if cur else None
    ids = {id(r) for r in keep_new}
    drop = [r for r in te if id(r) not in ids]
    ok = passes(s_new, s_cur)
    line = dict(at=time.strftime("%Y-%m-%d %H:%M", time.gmtime()), train=len(tr), test=len(te), base=base, current=s_cur,
                new=s_new, accepted=ok and not dry,
                avoided_losers=[sum(1 for r in drop if not r["y"]), round(sum(r["pnl"] for r in drop if not r["y"]), 2)],
                missed_winners=[sum(1 for r in drop if r["y"]), round(sum(r["pnl"] for r in drop if r["y"]), 2)],
                weights={n: round(w, 3) for n, w in zip(NAMES, new.d["w"])})
    print(json.dumps(line, indent=1))
    if not dry:
        with open(HIST, "a") as f:
            f.write(json.dumps(line) + "\n")
        if ok:
            new.d.update(trained=[cut - TRAIN_DAYS * 86400, cut], test=s_new)
            tmp = MODEL.with_suffix(".tmp")
            tmp.write_text(json.dumps(new.d))
            tmp.replace(MODEL)                           # atomic: the bot never reads half a file


def compare(lines, min_nights=7):
    """Sum the exam nights that had a model in place (current: trained before that day, so out of sample):
    all base trades vs the ones the model kept. Returns (verdict, text)."""
    ns = [l for l in lines if l.get("current") and "cost" in l["base"] and "cost" in l["current"]]
    pct = lambda k: 100 * sum(l[k]["pnl"] for l in ns) / max(sum(l[k]["cost"] for l in ns), 1e-9)
    won = sum(l["current"]["pct"] > l["base"]["pct"] for l in ns)
    model_pnl = sum(l["current"]["pnl"] for l in ns)
    why = (f"{len(ns)} nights < {min_nights}" if len(ns) < min_nights else
           f"model {pct('current'):+.1f}%/$ <= live {pct('base'):+.1f}%/$" if pct("current") <= pct("base") else
           f"model beat live on only {won}/{len(ns)} nights" if won * 2 <= len(ns) else
           f"model P/L {model_pnl:+.2f} <= 0" if model_pnl <= 0 else "")
    text = (f"{len(ns)} nights | live {pct('base'):+.2f}%/$ ${sum(l['base']['pnl'] for l in ns):+.2f} | "
            f"model {pct('current'):+.2f}%/$ ${model_pnl:+.2f} | model better {won}/{len(ns)} nights\n"
            f"SWITCH: {'yes' if not why else 'not yet: ' + why}")
    return not why, text


def check():
    import random
    rnd = random.Random(1)
    X, y = [], []
    for _ in range(3000):                                # wins depend on z (decidedness) and the price paid
        x = feats(rnd.uniform(0.3, 3), rnd.uniform(0.1, 0.49), 0.0, 50, rnd.uniform(30, 290), rnd.gauss(0, 3), 0, 30)
        x[2] = 0.01
        y.append(1 if rnd.random() < 1 / (1 + math.exp(-(3 * x[6] + 4 * (x[1] - 0.3)))) else 0)
        X.append(x)
    m = Model(fit(X, y))
    assert m.d["w"][NAMES.index("z")] > 0.5 and m.d["w"][NAMES.index("ask")] > 0, m.d["w"]
    assert abs(solve([[2, 1], [1, 3]], [3, 5])[0] - 0.8) < 1e-12
    # gate: a model whose profit is 5 lucky rounds fails, a steady one passes
    lucky = [dict(slug=f"r-{i}", t=i, pnl=(20 if i < 5 else -1), cost=10, y=0) for i in range(150)]
    steady = [dict(slug=f"r-{i}", t=i, pnl=0.5, cost=10, y=1) for i in range(150)]
    assert not passes(score(lucky), None) and passes(score(steady), None) and not passes(score(steady), dict(pct=9))
    night = lambda b, c: dict(base=dict(pct=b, pnl=b, cost=100), current=dict(pct=c, pnl=c, cost=100))
    assert compare([night(-1, 2)] * 7)[0] and not compare([night(-1, 2)] * 6)[0] and not compare([night(2, 1)] * 7)[0]
    x = X[0]
    t0 = time.perf_counter()
    for _ in range(10000):
        m.keep(x, 0.3, 0.01)
    ms = (time.perf_counter() - t0) / 10000 * 1000
    assert ms < 0.05, ms
    print(f"ok | decision {ms:.4f} ms | z weight {m.d['w'][NAMES.index('z')]:.2f}")


if __name__ == "__main__":
    a = sys.argv[1:]
    if a == ["compare"]:
        print(compare([json.loads(l) for l in HIST.read_text().splitlines()] if HIST.exists() else [])[1])
    else:
        check() if a == ["check"] else train(dry="--dry" in a) if a[:1] == ["train"] else sys.exit(__doc__)
