"""DOG-AI (paper, shadow): W-SNIPE-DOG's paper trades, kept only when a frozen wallet-imitation model says the copied
wallet would buy that side there too (P(wallet takes a buy in the next 0.5 s) >= thr).

Why: lab 27 Oct (440 rounds, test = last 88 never seen): W-SNIPE-DOG +15.4 %/$ but -10.7 % without its best 5 rounds;
keeping the top 30 % by this model: +23.6 %/$, thirds +28 / +10 / +27, +3.0 % without the best 5. The choice window was only
29 rounds, so this is the forward test: the model is frozen (dogai_model.json, frozen_at 27 Oct 00:55 UTC) and only scores
signals after that. Model = 300 gradient-boosted trees on 79 features from all recorded feeds, which only feedrace.db has
(the bots listen to Deribit alone), so it runs beside W-SNIPE-DOG instead of inside it: every LOOP s it scores
W-SNIPE-DOG's new bought signals (>= LAG s old, so the recorder has written that moment) and copies the kept trades to
runs/dogai.db (same ledger schema; dashboard + walletclone compare read it). One decision: ~0.2 ms (pure Python).
Features mirror the lab's mlbuild.py exactly (same windows, staleness, NaN rules, float32).
    python dogai.py              run (systemd: polybot-dogai)
    python dogai.py check        self-check
"""
import bisect, json, math, os, sqlite3, sys, time
from array import array
from pathlib import Path

HERE = Path(__file__).parent
RUNS = HERE / "runs"
FR, SRC, OUT = RUNS / "feedrace.db", RUNS / os.getenv("DOGAI_SRC", "wsnipedog.db"), RUNS / os.getenv("DOGAI_OUT", "dogai.db")
LOG = OUT.with_suffix(".log")                                      # DOGAI_SRC/OUT: filter another W-SNIPE-DOG ledger
FEEDS = ["deribit", "binance-fut", "binance-spot", "coinbase", "bybit", "okx", "kraken", "bitstamp", "hyperliquid", "chainlink"]
WINS = (0.5, 1, 2, 5, 15, 60)
LAG, LOOP = 15, 10
NAN = float("nan")
fee = lambda p: 0.07 * p * p * (1 - p)


def nanmean(v):
    v = [x for x in v if x == x]
    return sum(v) / len(v) if v else NAN


class Round:
    """Everything recorded for one round from start-700 s (staleness 300 s + 60 s windows + 300 s sigma) to `upto`."""

    def __init__(self, db, slug, upto):
        self.slug, self.start = slug, int(slug.rsplit("-", 1)[1])
        lo = self.start - 700
        self.q = {f: ([], []) for f in FEEDS}
        for t, f, m in db.execute("SELECT t, feed, mid FROM quotes WHERE t BETWEEN ? AND ? AND mid > 0 ORDER BY t", (lo, upto)):
            if f in self.q:
                self.q[f][0].append(t); self.q[f][1].append(m)
        self.bk = db.execute("SELECT t, bid, ask FROM poly WHERE slug=? AND bid IS NOT NULL AND ask IS NOT NULL AND t <= ? "
                             "ORDER BY t", (slug, upto)).fetchall()
        self.bt = [r[0] for r in self.bk]
        self.pt, self.cum = [], [0.0]
        for t, o, z, sd in db.execute("SELECT t, outcome, size, side FROM prints WHERE slug=? AND t <= ? ORDER BY t", (slug, upto)):
            self.pt.append(t)
            self.cum.append(self.cum[-1] + (1 if sd == "BUY" else -1) * (1 if o == "Up" else -1) * z)   # + = toward Up
        s = self.start
        self.beat_d = nanmean([self.ff("deribit", s - 60 + k) for k in range(60)])
        self.beat_c = nanmean([self.ff("chainlink", s - 60 + k) for k in range(60)])
        g = [x for x in (self.ff("deribit", s - 300 + k) for k in range(301)) if x == x]
        r = [math.log(b / a) for a, b in zip(g, g[1:])]
        self.sig = max(1e-6, math.sqrt(sum(x * x for x in r) / len(r))) if len(r) > 100 else NAN

    def ff(self, feed, t):
        """forward-filled value at t (the recorder logs on change); NaN if none or older than 300 s"""
        T, V = self.q[feed]
        i = bisect.bisect_right(T, t) - 1
        return V[i] if i >= 0 and t - T[i] < 300 else NAN

    def _flow(self, t, w):
        return self.cum[bisect.bisect_right(self.pt, t)] - self.cum[bisect.bisect_right(self.pt, t - w)]

    def features(self, t, side, names):
        """feature vector in `names` order toward `side` at time t, or None where the lab had no row"""
        if self.beat_d != self.beat_d or self.sig != self.sig:
            return None
        i = bisect.bisect_right(self.bt, t) - 1
        if i < 0 or t - self.bt[i] >= 120:
            return None
        _, ub, ua = self.bk[i]
        sg = 1 if side == "Up" else -1
        bid, ask = (ub, ua) if sg == 1 else (round(1 - ua, 2), round(1 - ub, 2))
        if not 0.02 <= ask <= 0.98:
            return None
        s, spot = self.start, self.ff("deribit", t)
        tl = s + 300 - t
        var = (tl - 60) + 20 if tl >= 60 else (tl / 60) ** 2 * tl / 3
        run = nanmean([self.ff("deribit", s + 240 + k) for k in range(int(math.ceil(t - s - 240)))]) if t > s + 241 else NAN
        mean = spot if tl >= 60 else ((60 - tl) * (spot if run != run else run) + tl * spot) / 60
        fu = 0.5 * (1 + math.erf((mean - self.beat_d) / (self.sig * spot * math.sqrt(max(var, 1e-9))) / math.sqrt(2)))
        fair = fu if sg == 1 else 1 - fu
        cl = self.ff("chainlink", t)
        c = {"ask": ask, "spread": ask - bid, "t_left": tl, "fair": fair, "value": fair - ask - fee(ask),
             "dist_d": sg * (spot / self.beat_d - 1) * 1e4, "dist_c": sg * (cl / self.beat_c - 1) * 1e4,
             "basis_c": sg * (spot / cl - 1) * 1e4, "sigma": self.sig * 1e4, "pair_sum": ua + (1 - ub),
             "last_chg": t - self.bt[i], "cnt2": bisect.bisect_right(self.pt, t) - bisect.bisect_right(self.pt, t - 2)}
        for w in (1, 3, 10, 30):
            k = max(bisect.bisect_right(self.bt, t - w) - 1, 0)
            c[f"dmid{w}"] = sg * ((ub + ua) / 2 - (self.bk[k][1] + self.bk[k][2]) / 2) * 100
        for w in (2, 10, 30):
            c[f"flow{w}"] = sg * self._flow(t, w)
        for f in FEEDS:
            now = self.ff(f, t)
            for w in WINS:
                c[f"{f}_{w}"] = sg * (now / self.ff(f, t - w) - 1) * 1e4
        return array("f", [c[n] for n in names]).tolist()            # float32, like the training data


class Model:
    def __init__(self, path=HERE / "dogai_model.json"):
        m = json.loads(Path(path).read_text())
        self.names, self.thr, self.frozen_at, self.base, self.trees = m["features"], m["thr"], m["frozen_at"], m["base"], m["trees"]

    def p(self, x):
        r = self.base
        for tr in self.trees:
            i = 0
            while not tr[i][5]:
                f, th, ml, lf, rt = tr[i][:5]
                v = x[f]
                i = (lf if ml else rt) if v != v else (lf if v <= th else rt)
            r += tr[i][6]
        return 1 / (1 + math.exp(-r))


def log(msg, lvl="I"):
    with open(LOG, "a") as f:
        f.write(f"{time.strftime('%H:%M:%S', time.gmtime())} {lvl} {msg}\n")


def setup_out():
    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
    out = sqlite3.connect(OUT)
    for name, sql in src.execute("SELECT name, sql FROM sqlite_master WHERE type='table'"):
        out.execute(sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
    out.commit()
    return out


def step(model, out):
    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True, timeout=10)
    fr = sqlite3.connect(f"file:{FR}?mode=ro", uri=True, timeout=10)
    done = out.execute("SELECT max(t) FROM signals").fetchone()[0] or model.frozen_at
    upto = time.time() - LAG
    sigs = src.execute("SELECT * FROM signals WHERE outcome='bought' AND t > ? AND t <= ? ORDER BY t", (done, upto)).fetchall()
    used = {r[0] for r in out.execute("SELECT id FROM trades")}
    rounds, kept = {}, 0
    for row in sigs:
        t, slug, side, fill = row[0], row[1], row[2], row[13]
        if slug not in rounds:
            rounds[slug] = Round(fr, slug, upto)
        x = rounds[slug].features(t, side, model.names)
        p = model.p(x) if x else None
        ok = p is not None and p >= model.thr
        sig = list(row)
        sig[8], sig[9] = ("bought", f"p={p:.3f}") if ok else ("skip", "model" if p is None else f"model p={p:.3f}")
        out.execute(f"INSERT INTO signals VALUES ({','.join('?' * len(sig))})", sig)
        if ok:
            ts = lambda d: time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t + d))
            tr = src.execute("SELECT * FROM trades WHERE slug=? AND side=? AND abs(price-?)<1e-9 AND ts BETWEEN ? AND ? "
                             "ORDER BY ts", (slug, side, fill, ts(-1), ts(3) + "~")).fetchall()
            tr = [r for r in tr if r[0] not in used]
            if tr:
                out.execute(f"INSERT INTO trades VALUES ({','.join('?' * len(tr[0]))})", tr[0])
                used.add(tr[0][0]); kept += 1
    for tid, st, pay, pnl in src.execute("SELECT id, status, payout, pnl FROM trades WHERE status != 'open'"):
        if tid in used:
            out.execute("UPDATE trades SET status=?, payout=?, pnl=? WHERE id=? AND status='open'", (st, pay, pnl, tid))
    out.commit()
    return len(sigs), kept


def main():
    model = Model()
    out = setup_out()
    log(f"start | frozen model {len(model.trees)} trees, {len(model.names)} features, keep P >= {model.thr:.4f} | "
        f"scoring W-SNIPE-DOG signals after {time.strftime('%d %b %H:%M', time.gmtime(model.frozen_at))} UTC")
    last = 0.0
    while True:
        try:
            n, k = step(model, out)
            if n:
                log(f"scored {n} signals, kept {k}")
        except sqlite3.OperationalError as e:            # a busy recorder/ledger: next pass
            log(f"sqlite: {e}", "W")
        if time.time() - last > 60:
            c, p, o = out.execute("SELECT count(*), coalesce(sum(pnl), 0), coalesce(sum(cost), 0) FROM trades "
                                  "WHERE status != 'open'").fetchone()
            log(f"dogai status | {c} trades closed | P/L {p:+.2f} on ${o:.2f}")
            last = time.time()
        time.sleep(LOOP)


def check():
    m = Model()
    assert len(m.trees) == 300 and len(m.names) == 79 and 0 < m.thr < 1
    x = [NAN] * len(m.names)
    assert 0 < m.p(x) < 1                                       # all-missing still scores
    assert abs(nanmean([1.0, NAN, 3.0]) - 2.0) < 1e-12 and nanmean([NAN]) != nanmean([NAN])
    t0 = time.perf_counter()
    for _ in range(500):
        m.p(x)
    ms = (time.perf_counter() - t0) / 500 * 1000
    assert ms < 2, ms
    print(f"ok | decision {ms:.3f} ms")


if __name__ == "__main__":
    check() if sys.argv[1:] == ["check"] else main()
