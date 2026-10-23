"""SQLite ledger: one row per filled leg, plus best UP+DOWN ask sum seen per market window."""
import sqlite3
from datetime import datetime, timezone

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades(
    id INTEGER PRIMARY KEY, ts TEXT, mode TEXT, strategy TEXT, asset TEXT, window TEXT,
    window_end INTEGER, slug TEXT, side TEXT, price REAL, shares REAL, fee REAL, cost REAL,
    status TEXT DEFAULT 'open', payout REAL DEFAULT 0, pnl REAL);
CREATE TABLE IF NOT EXISTS scans(
    slug TEXT PRIMARY KEY, asset TEXT, window TEXT, window_start INTEGER, best_sum REAL, ticks INTEGER);
CREATE TABLE IF NOT EXISTS prints(t REAL, asset TEXT, price REAL, size REAL, side TEXT);
CREATE TABLE IF NOT EXISTS binance(t REAL, mid REAL);
CREATE TABLE IF NOT EXISTS ticks(
    t REAL, slug TEXT, window_end INTEGER, t_left REAL, up_bid REAL, up_ask REAL, up_ask_size REAL,
    dn_bid REAL, dn_ask REAL, dn_ask_size REAL, spot REAL, beat REAL, known REAL, sigma REAL, p_model REAL);
CREATE TABLE IF NOT EXISTS signals(
    t REAL, slug TEXT, side TEXT, bps REAL, sig_ask REAL, sig_bid REAL, sig_size REAL, t_left REAL,
    outcome TEXT, reason TEXT, ms_to_order REAL, ms_order REAL, ask_now REAL, fill_px REAL, loop_ms REAL,
    bn_lag REAL);
"""


class Ledger:
    def __init__(self, path=config.LEDGER_PATH):
        self.db = sqlite3.connect(path, timeout=30)   # bots, settler and dashboard share the file
        self.db.execute("PRAGMA journal_mode=WAL")      # dashboard reads never block the bot's writes
        self.db.executescript(SCHEMA)
        try:                                            # ledgers made before 23 Oct 18:00 lack the column
            self.db.execute("ALTER TABLE signals ADD COLUMN bn_lag REAL")
        except sqlite3.OperationalError:
            pass

    def _one(self, sql, *args):
        return self.db.execute(sql, args).fetchone()[0]

    @property
    def wallet(self):
        return config.BASE_BALANCE * config.TRADE_SCALE

    def record(self, m, strategy, leg, mode=None):
        cost = leg.price * leg.shares + leg.fee
        cur = self.db.execute(
            "INSERT INTO trades(ts, mode, strategy, asset, window, window_end, slug, side, price, shares, fee, cost)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(timespec="seconds"), mode or ("paper" if config.DRY_RUN else "live"),
             strategy, m.asset, m.label, m.end, m.slug, leg.side, leg.price, leg.shares, leg.fee, cost))
        self.db.commit()
        return cur.lastrowid

    def close(self, trade_id, price, fee_per_share):
        """Sell an open position at price: status 'sold', payout = proceeds after fee, pnl = payout - cost."""
        self.db.execute("UPDATE trades SET status='sold', payout = shares * (? - ?), pnl = shares * (? - ?) - cost"
                        " WHERE id=? AND status='open'", (price, fee_per_share, price, fee_per_share, trade_id))
        self.db.commit()

    def open_positions(self, slug):
        """Open directional positions in a round: (id, strategy, side, price, shares)."""
        return self.db.execute("SELECT id, strategy, side, price, shares FROM trades WHERE slug=? AND status='open'"
                               " AND strategy IN ('fair', 'snipe')", (slug,)).fetchall()

    def entries(self, slug, strategy):
        return self._one("SELECT count(*) FROM trades WHERE slug=? AND strategy=?", slug, strategy)

    def hour_pnl(self, now):
        """Realised pnl of rounds that ended in the last hour, plus sales made in it."""
        return self._one("SELECT coalesce(sum(pnl), 0) FROM trades WHERE status!='open' AND window_end > ?", now - 3600)

    def scan(self, m, pair_sum):
        self.db.execute(
            "INSERT INTO scans VALUES (?,?,?,?,?,1) ON CONFLICT(slug) DO UPDATE"
            " SET best_sum = min(best_sum, excluded.best_sum), ticks = ticks + 1",
            (m.slug, m.asset, m.label, m.start, pair_sum))
        self.db.commit()

    def tick(self, now, m, s, spot, beat, known, sigma):
        """Every order-book check, so backtest.py can replay real books later."""
        self.db.execute("INSERT INTO ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (now, m.slug, m.end, s.t_left, s.up.bid, s.up.ask, s.up.ask_size, s.down.bid, s.down.ask,
                         s.down.ask_size, spot, beat, known, sigma, s.p_up))
        self.db.commit()

    def signal(self, t, slug="", side="", bps=None, sig_ask=None, sig_bid=None, sig_size=None, t_left=None,
               outcome="", reason="", ms_to_order=None, ms_order=None, ask_now=None, fill_px=None, loop_ms=None,
               bn_lag=None):
        """One row per Binance signal and what became of it (bought / nofill / skip + reason), plus slow loop passes.
        ms_to_order = signal -> order sent, ms_order = the order call itself (post to response),
        bn_lag = how late the newest Binance update was when the signal fired."""
        self.db.execute("INSERT INTO signals(t, slug, side, bps, sig_ask, sig_bid, sig_size, t_left, outcome, reason,"
                        " ms_to_order, ms_order, ask_now, fill_px, loop_ms, bn_lag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (t, slug, side, bps, sig_ask, sig_bid, sig_size, t_left, outcome, reason, ms_to_order,
                         ms_order, ask_now, fill_px, loop_ms, bn_lag))
        self.db.commit()

    def binance(self, t, mid):
        """Binance mid at each bot tick, so future replays can use the fast feed."""
        if mid:
            self.db.execute("INSERT INTO binance VALUES (?, ?)", (t, mid))

    def prints(self, rows):
        """Raw CLOB trade prints (t, asset, price, size, taker side) for future replays."""
        if rows:
            self.db.executemany("INSERT INTO prints VALUES (?,?,?,?,?)", rows)
            self.db.commit()

    def traded(self, slug, strategy):
        return self._one("SELECT count(*) FROM trades WHERE slug=? AND strategy=?", slug, strategy) > 0

    def cash(self):
        return self.wallet - self._one("SELECT coalesce(sum(cost) - sum(payout), 0) FROM trades")

    def cap(self, reserved=0.0):
        """Max $ for the next trade: limited by free cash, per-trade size and open-exposure kill switch."""
        free = self.cash() - reserved
        room = config.MAX_OPEN_EXPOSURE - self.exposure() - reserved
        return max(0.0, min(free, room, self.wallet * config.MAX_PER_TRADE))

    def exposure(self):
        return self._one("SELECT coalesce(sum(cost), 0) FROM trades WHERE status='open'")

    def today_pnl(self):
        today = datetime.now(timezone.utc).date().isoformat()
        return self._one("SELECT coalesce(sum(pnl), 0) FROM trades WHERE status!='open' AND ts >= ?", today)

    def unsettled(self, now):
        return [r[0] for r in self.db.execute(
            "SELECT DISTINCT slug FROM trades WHERE status='open' AND window_end < ?", (now,))]

    def settle(self, slug, winner):
        self.db.execute(
            "UPDATE trades SET status = CASE side WHEN ?1 THEN 'won' ELSE 'lost' END,"
            " payout = CASE side WHEN ?1 THEN shares ELSE 0 END,"
            " pnl = CASE side WHEN ?1 THEN shares - cost ELSE -cost END"
            " WHERE slug = ?2 AND status = 'open'", (winner, slug))
        self.db.commit()

    def report(self):
        lines = [f"wallet ${self.wallet:.2f} | cash ${self.cash():.2f} | open exposure ${self.exposure():.2f}"
                 f" | settled pnl ${self._one('SELECT coalesce(sum(pnl),0) FROM trades'):.2f}", "",
                 f"{'strategy':8} {'coin':5} {'win':4} {'legs':>5} {'won':>4} {'lost':>5} {'open':>5}"
                 f" {'cost $':>8} {'pnl $':>8}"]
        for r in self.db.execute(
                "SELECT strategy, asset, window, count(*), sum(status='won'), sum(status='lost'), sum(status='open'),"
                " sum(cost), coalesce(sum(pnl), 0) FROM trades GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"):
            lines.append(f"{r[0]:8} {r[1]:5} {r[2]:4} {r[3]:5} {r[4]:4} {r[5]:5} {r[6]:5} {r[7]:8.2f} {r[8]:8.2f}")
        lines += ["", f"{'coin':5} {'win':4} {'windows':>7} {'min sum':>8} {'avg sum':>8} {'sum<1':>6}"]
        for r in self.db.execute(
                "SELECT asset, window, count(*), min(best_sum), avg(best_sum), sum(best_sum < 1)"
                " FROM scans GROUP BY 1, 2 ORDER BY 1, 2"):
            lines.append(f"{r[0]:5} {r[1]:4} {r[2]:7} {r[3]:8.3f} {r[4]:8.3f} {r[5]:6}")
        return "\n".join(lines)
