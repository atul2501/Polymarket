"""TK_REAL paper mode, end to end: the real taker.live() loop on a fake clock, fake feeds and scripted books.

python test_real_paper.py        (no network, no keys; takes a few seconds)
"""
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

os.environ.update(TK_REAL="1", TK_LIVE="0", TK_SIZE="5", TK_HOLD="5", TK_COOLDOWN="10", TK_SLIP="0.02",
                  TK_MOVE_BPS="2", TK_MOVE_SEC="1", TK_MAX_SPREAD="1", TK_MIN_LEFT="30")
sys.path.insert(0, str(Path(__file__).parent))

import config  # noqa: E402
import taker  # noqa: E402
from engine import Book  # noqa: E402
from market import Market  # noqa: E402

START = 1_800_000_000 - 1_800_000_000 % 300      # a round start
T0 = START + 10                                   # the fake clock starts 10 s into the round
SIGNAL_AT = T0 + 10


class Stop(BaseException):
    """Ends taker.live()'s endless loop (not caught by its except clause)."""


class Clock:
    def __init__(self, end):
        self.now, self.end = float(T0), end

    def time(self):
        return self.now

    def sleep(self, s):
        self.now += s
        if self.now > self.end:
            raise Stop


def run(up_book, seconds=30, size=100.0):
    """Run the bot; up_book(t) -> (up_bid, up_ask). The Binance move fires once, at SIGNAL_AT. Returns the ledger."""
    clock = Clock(T0 + seconds)
    market = Market(f"btc-updown-5m-{START}", "btc", "5m", START, 300, "UPTOK", "DNTOK", config.DEFAULT_FEE)

    class WS:
        def start(self): return self
        def trades_since(self, t): return []
        def watch(self, a): pass

    class Feed:
        def __init__(self, *a, **k): pass
        def current(self, now): return [market]
        def upcoming(self, now): pass

        def books(self, markets):
            ub, ua = up_book(clock.now)
            return {"UPTOK": Book(ub, size, ua, size), "DNTOK": Book(round(1 - ua, 4), size, round(1 - ub, 4), size)}

    class BN:
        stale, SOURCE = 0, "binance"
        def start(self): return self
        def at(self, t): return 100_000.0
        def lag_ms(self): return 130.0
        def wait(self, s): clock.sleep(s)
        def move_bps(self, window, now=None):
            return 3.0 if SIGNAL_AT <= clock.now < SIGNAL_AT + 0.2 else 0.0

    taker.time = types.SimpleNamespace(time=clock.time, sleep=clock.sleep)
    taker.settler = lambda: None
    taker.BookFeed, taker.MarketFeed, taker.BinanceFeed = WS, Feed, BN
    taker.LEDGER = Path(tempfile.mkdtemp()) / "t.db"
    try:
        taker.live()
    except Stop:
        pass
    from ledger import Ledger
    return Ledger(taker.LEDGER)


class RealPaper(unittest.TestCase):
    def test_fill_then_sell_with_delays(self):
        # ask 0.50 at the signal, 0.51 when the order matches (inside the 2c pad); bid 0.56 when the sale matches
        led = run(lambda t: (0.55, 0.56) if t >= SIGNAL_AT + 5 else (0.49, 0.51) if t >= SIGNAL_AT + 0.2 else (0.49, 0.50))
        rows = led.db.execute("SELECT price, shares, status, pnl FROM trades").fetchall()
        self.assertEqual(len(rows), 1)
        price, shares, status, pnl = rows[0]
        self.assertAlmostEqual(price, 0.51)      # the ask of the matching moment, not of the signal
        self.assertEqual(shares, 5)
        self.assertEqual(status, "sold")
        self.assertAlmostEqual(led.db.execute("SELECT (payout + cost - cost) / shares FROM trades").fetchone()[0] > 0.5, True)
        self.assertEqual(led.db.execute("SELECT outcome FROM signals WHERE outcome IN ('bought','nofill')").fetchall(),
                         [("bought",)])

    def test_order_killed_when_ask_runs_past_the_pad(self):
        # 0.50 at the signal, 0.60 when the order matches: limit 0.52 < 0.60 -> killed, no trade
        led = run(lambda t: (0.59, 0.60) if t >= SIGNAL_AT + 0.2 else (0.49, 0.50))
        self.assertEqual(led.db.execute("SELECT count(*) FROM trades").fetchone()[0], 0)
        self.assertEqual(led.db.execute("SELECT outcome FROM signals WHERE outcome IN ('bought','nofill')").fetchall(),
                         [("nofill",)])

    def test_dollar_minimum_buys_more_shares_on_cheap_tokens(self):
        # ask 0.05: limit 0.07 -> $1 minimum -> ceil(100 / 0.07) / 100 = 14.29 shares
        led = run(lambda t: (0.04, 0.05))
        shares = led.db.execute("SELECT shares FROM trades").fetchone()[0]
        self.assertAlmostEqual(shares, 14.29, places=2)

    def test_second_signal_during_the_hold_is_busy(self):
        led = run(lambda t: (0.49, 0.50))
        self.assertEqual(led.db.execute("SELECT count(*) FROM trades").fetchone()[0], 1)

    def test_live_orders_in_the_signal_pass(self):
        # live timing (no simulated delay): the order goes out in the pass that saw the signal, at that pass's ask
        # 0.50; the next pass (50 ms later) shows 0.70. Before 25 Oct the order waited a pass (05:05:00: 144 ms).
        first = []

        def book(t):
            if t >= SIGNAL_AT and not first:
                first.append(t)
            return (0.49, 0.50) if not first or t == first[0] else (0.69, 0.70)
        saved = taker.REAL, taker.DELAY_SEC
        taker.REAL, taker.DELAY_SEC = False, 0.0
        try:
            led = run(book)
        finally:
            taker.REAL, taker.DELAY_SEC = saved
        self.assertAlmostEqual(led.db.execute("SELECT price FROM trades").fetchone()[0], 0.50)

    def test_thin_ask_is_killed_with_queue_margin(self):
        # 7 shares at the ask, we want 5, QUEUE 2: others ahead of us take 5 first, we get the 2 left (25 Oct 07:27)
        saved = taker.QUEUE
        taker.QUEUE = 2
        try:
            led = run(lambda t: (0.49, 0.50), size=7.0)
        finally:
            taker.QUEUE = saved
        self.assertEqual(led.db.execute("SELECT shares FROM trades").fetchall(), [(2.0,)])
        taker.QUEUE = 2
        try:                                       # 4 shares: nothing left for us -> killed
            self.assertEqual(run(lambda t: (0.49, 0.50), size=4.0).db.execute("SELECT count(*) FROM trades").fetchone()[0], 0)
        finally:
            taker.QUEUE = saved

    def test_settle_skew_cap(self):
        # hold-to-settle with TK_SKEW: a buy that would leave more one-sided shares than the cap is skipped
        saved = taker.SETTLE, taker.SKEW
        try:
            taker.SETTLE, taker.SKEW = True, 6
            self.assertEqual(run(lambda t: (0.49, 0.50)).db.execute("SELECT count(*) FROM trades").fetchone()[0], 1)   # 5 <= 6
            taker.SKEW = 4
            self.assertEqual(run(lambda t: (0.49, 0.50)).db.execute("SELECT count(*) FROM trades").fetchone()[0], 0)   # 5 > 4
        finally:
            taker.SETTLE, taker.SKEW = saved

    def test_match_delay_from_live_ledger(self):
        from ledger import Ledger
        path = Path(tempfile.mkdtemp()) / "live.db"
        live = Ledger(path)
        for ms in (300, 400, 500, 600):
            live.signal(1.0, outcome="nofill", ms_order=ms)
        saved = taker.MATCH_FROM, dict(taker._match)
        taker.MATCH_FROM, taker._match["t"] = str(path), 0.0
        try:
            self.assertAlmostEqual(taker.match_delay(), 0.575)         # 75th pct 600 ms - 25 ms
        finally:
            taker.MATCH_FROM = saved[0]
            taker._match.update(saved[1])

    def test_helpers(self):
        b = Book(0.55, 3.0, 0.56, 3.0)
        self.assertEqual(taker.real_buy(0.57, b, 5), (3.0, 0.56))        # partial fill: only 3 shares sit at the ask
        self.assertEqual(taker.real_buy(0.55, b, 5), (0.0, 0.55))        # ask above the limit: killed
        self.assertEqual(taker.real_sell(0.53, b, 5), (3.0, 0.55))
        self.assertEqual(taker.real_sell(0.56, b, 5), (0.0, 0.56))       # bid below the limit: killed


if __name__ == "__main__":
    unittest.main()
