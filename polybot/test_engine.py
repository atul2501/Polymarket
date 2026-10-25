"""Run: python test_engine.py  (no network)"""
import tempfile
from pathlib import Path

import config
from engine import (Book, Leg, Snapshot, blended, check_arb, check_fair, check_maker, check_snipe, fair_up,
                    hedge_leg, maker_filled, taker_fee)
from ledger import Ledger
from market import Market

FEE = {"rate": 0.07, "exponent": 1}


def snap(up, down, t_left=120, p_up=None, cap=20.0):
    return Snapshot(up, down, t_left, FEE, cap, p_up)


def main():
    # fee formula: 100 shares at 0.50 -> 100 * 0.07 * 0.5 * 0.25
    assert abs(taker_fee(0.5, 100, FEE) - 0.875) < 1e-9

    # arb fires on sum < 1 - fee - MIN_EDGE, sized by depth and cap
    s = snap(Book(0.46, 50, 0.47, 30), Book(0.49, 50, 0.50, 40))
    opp = check_arb(s)
    assert opp and opp.legs[0].shares == opp.legs[1].shares == 20, opp  # cap 20$/~0.987 -> 20 < depth 30
    # mirrored book (the real Polymarket case): sum 1.01 never fires
    assert check_arb(snap(Book(0.47, 50, 0.48, 30), Book(0.52, 30, 0.53, 50))) is None
    # 30s guard
    assert check_arb(snap(s.up, s.down, t_left=30)) is None
    # below min size
    assert check_arb(snap(s.up, s.down, cap=2.0)) is None

    # fair value on the 60s-TWAP rule
    assert abs(fair_up(100, 100, 1e-4, 300) - 0.5) < 1e-9
    assert fair_up(100.5, 100, 1e-4, 20) > 0.97
    # known part of the final minute far above beat: spot dipping below beat with 5s left can't flip it
    assert fair_up(99.9, 100, 1e-4, 5, known_avg=100.3) > 0.99
    assert fair_up(100, 100, 1e-4, 0, known_avg=99.0) == 0.0
    up, down = Book(0.60, 100, 0.62, 100), Book(0.38, 100, 0.40, 100)
    assert check_fair(snap(up, down, p_up=0.75)).legs[0].side == "Up"
    assert check_fair(snap(up, down, p_up=0.75, t_left=10)) is None

    # snipe only inside (30s, 150s]
    up = Book(0.92, 100, 0.94, 100)
    assert check_snipe(snap(up, down, t_left=90, p_up=0.99)).legs[0].side == "Up"
    assert check_snipe(snap(up, down, t_left=180, p_up=0.99)) is None
    assert check_snipe(snap(up, down, t_left=25, p_up=0.99)) is None

    # maker quotes sum to 1 - MAKER_EDGE; fills when ask touches bid
    opp = check_maker(snap(Book(0.49, 100, 0.51, 100), Book(0.49, 100, 0.51, 100)))
    assert abs(sum(l.price for l in opp.legs) - (1 - config.MAKER_EDGE)) < 1e-9, opp
    assert maker_filled(opp.legs[0], Book(None, 0, opp.legs[0].price - 0.01, 10))
    assert not maker_filled(opp.legs[0], Book(None, 0, opp.legs[0].price, 10))  # touching isn't a fill

    # hedge: Up filled @0.47 -> buy Down @0.50 or @0.55 (pair + fee <= 1.06), but not @0.62
    up_leg = Leg("Up", 0.47, 10, 0.0)
    h = hedge_leg(up_leg, Book(0.49, 50, 0.50, 50), FEE)
    assert h and h.side == "Down" and h.shares == 10
    assert hedge_leg(up_leg, Book(0.54, 50, 0.55, 50), FEE)
    assert hedge_leg(up_leg, Book(0.61, 50, 0.62, 50), FEE) is None

    # blend: market at 0.60 pulls a 0.75 model call to 0.675 at w=0.5 -> edge at ask 0.62 drops below 5%
    up, down = Book(0.60, 100, 0.62, 100), Book(0.38, 100, 0.40, 100)
    s2 = Snapshot(up, down, 120, FEE, 20.0, 0.75, 0.60)
    w = config.MODEL_WEIGHT
    config.MODEL_WEIGHT = 1.0
    assert check_fair(s2).legs[0].side == "Up"  # model alone trades
    config.MODEL_WEIGHT = 0.5
    assert abs(blended(s2) - 0.675) < 1e-9 and check_fair(s2) is None
    config.MODEL_WEIGHT = w
    # price band blocks long shots
    lo = config.FAIR_MIN_ASK
    config.FAIR_MIN_ASK = 0.70
    assert check_fair(snap(up, down, p_up=0.75)) is None
    config.FAIR_MIN_ASK = lo

    # ledger: arb pair settles to shares - cost regardless of winner
    with tempfile.TemporaryDirectory() as d:
        led = Ledger(Path(d) / "t.db")
        m = Market("x", "btc", "5m", 0, 300, "u", "d", FEE)
        opp = check_arb(s)
        for leg in opp.legs:
            led.record(m, "arb", leg)
        cost = sum(l.price * l.shares + l.fee for l in opp.legs)
        assert led.unsettled(301) == ["x"] and led.traded("x", "arb")
        led.settle("x", "Down")
        assert abs(led.cash() - (led.wallet - cost + 20)) < 1e-9
        assert abs(led.today_pnl() - (20 - cost)) < 1e-9
        # cap never exceeds free cash after reservations
        assert led.cap(reserved=led.cash()) == 0.0
    # v3: exit rule, Kelly sizing, close() pnl, sold rows not re-settled
    from engine import check_exit, kelly_stake
    held_up = Snapshot(Book(0.70, 100, 0.71, 100), Book(0.29, 100, 0.30, 100), 120, FEE, 20.0, 0.60)
    bid, fee = check_exit("Up", held_up)          # model says Up is worth 0.60, market bids 0.70 -> sell
    assert bid == 0.70 and fee > 0
    assert check_exit("Up", Snapshot(held_up.up, held_up.down, 120, FEE, 20.0, 0.68)) is None  # 0.70 - fee < 0.68 + 0.03
    assert check_exit("Up", Snapshot(held_up.up, held_up.down, 8, FEE, 20.0, 0.60)) is None   # too close to the close
    assert kelly_stake(0.05, 0.50) < kelly_stake(0.15, 0.50) and abs(kelly_stake(0.05, 0.50) - 2.5) < 1e-9
    with tempfile.TemporaryDirectory() as d:
        led = Ledger(Path(d) / "t.db")
        m = Market("y", "btc", "5m", 0, 300, "u", "d", FEE)
        tid = led.record(m, "fair", Leg("Up", 0.50, 10, 0.1))
        assert led.open_positions("y")
        led.close(tid, 0.70, 0.01)
        assert abs(led._one("SELECT pnl FROM trades WHERE id=?", tid) - (10 * 0.69 - 5.1)) < 1e-9
        assert not led.open_positions("y") and led.entries("y", "fair") == 1
        led.settle("y", "Down")  # must not touch the sold row
        assert led._one("SELECT status FROM trades WHERE id=?", tid) == "sold"
    # mm v2: direction filter, never above best bid, lean, pull, queue-aware fills, top-up limits
    import mm
    assert mm.quote("Up", 0.30, 0.32, 0.35, 0, 0, False) is None                 # model gives UP < 0.40 -> no bid
    q = mm.quote("Up", 0.50, 0.52, 0.80, 0, 0, False)
    assert q is not None and q <= 0.50 and q <= 0.51 - mm.BOOK_DELTA + 1e-9       # model says 0.80: price still from the book
    assert mm.quote("Up", 0.50, 0.52, 0.60, 0, 0, True) is None                  # pulled
    mm.LEAN = 0.001
    assert mm.quote("Up", 0.50, 0.52, 0.60, 30, 0, False) < mm.quote("Up", 0.50, 0.52, 0.60, 0, 0, False)  # heavy -> lower
    mm.DIR_K = 0.5
    assert mm.target_skew(0.8, 100) > 0 and mm.target_skew(0.2, 100) < 0
    r = mm.Round(None)
    r.place("Up", 0.45, 20)                         # 20 shares ahead of us at 0.45
    assert r.fill("Up", [("SELL", "Up", 0.45, 15)]) == 0 and r.resting["Up"][1] == 5   # queue eaten first
    assert r.fill("Up", [("SELL", "Up", 0.45, 12)]) == 7                              # 5 more ahead, then 7 to us
    assert r.fill("Up", [("BUY", "Down", 0.60, 10)]) == 3     # DOWN taker at 0.60 hits UP bids at 0.40; skew cap 10 - 7 held
    r.place("Up", 0.45, 99)
    assert r.resting["Up"][1] == 0                  # same price keeps our queue spot
    mm.PULL_MOVE = 0.10
    r2 = mm.Round(None)
    r2.update_pulls(0, 0.50)
    r2.update_pulls(5, 0.65)
    assert r2.pulled_until["Up"] > 5
    ask = lambda a, n: type("B", (), {"ask": a, "ask_size": n})()
    mm.TOPUP_MIN = 0.90
    assert mm.topup(mm.Round(None), 90, 0.95, {"Up": ask(0.84, 50), "Down": ask(0.16, 50)}) == ("Up", 0.84, 10)
    assert mm.topup(mm.Round(None), 90, 0.95, {"Up": ask(0.90, 50), "Down": ask(0.10, 50)}) is None   # too expensive
    assert mm.topup(mm.Round(None), 150, 0.95, {"Up": ask(0.84, 50), "Down": ask(0.16, 50)}) is None  # too early
    # mm pairing: finish a pair at <= PAIR_TARGET, never add to the side already ahead
    rp = mm.Round(None)
    rp.buy("Up", 0.55, 10)                                           # UP filled at 0.55, unpaired
    assert mm.pair_quote(rp, "Up", 0.54, 0.56, 0.60, rp.skew(), 0, False) is None        # heavy: no averaging down
    assert mm.pair_quote(rp, "Down", 0.44, 0.46, 0.20, rp.skew(), 0, False) == 0.42  # 0.97 - 0.55, even though model says 20%
    bk = lambda a, n: type("B", (), {"ask": a, "ask_size": n})()
    take = mm.pair_take(rp, {"Up": bk(0.56, 50), "Down": bk(0.41, 50)}, FEE)
    assert take and take[0] == "Down" and take[2] == 10                              # 0.55 + 0.41 + fee: pair now
    assert mm.pair_take(rp, {"Up": bk(0.56, 50), "Down": bk(0.45, 50)}, FEE)          # ~1.02: still pair (unpaired loses ~45c)
    assert mm.pair_take(rp, {"Up": bk(0.56, 50), "Down": bk(0.50, 50)}, FEE) is None  # 1.05+: too expensive
    rq = mm.Round(None)
    rq.buy("Up", 0.55, 4)
    assert mm.pair_take(rq, {"Up": bk(0.56, 50), "Down": bk(0.41, 50)}, FEE) is None  # under PAIR_MIN ahead: wait
    rb = mm.Round(None)
    assert mm.pair_quote(rb, "Up", 0.80, 0.81, 0.85, 0, 0, False) == mm.OPEN_MAX           # no one-sided bid above 0.60
    assert mm.pair_quote(rb, "Up", 0.50, 0.52, 0.80, 0, 0, False, boosted=True) == \
        mm.pair_quote(rb, "Up", 0.50, 0.52, 0.80, 0, 0, False)                             # boost off by default
    # mm v4: average down on the heavy side, pair cap rises as the heavy average falls, fast-move pull/boost
    mm.AVG_STEP = 0.02                                                                      # v4 averaging, off by default
    r4 = mm.Round(None)
    r4.buy("Up", 0.39, 10)
    assert mm.pair_quote(r4, "Up", 0.38, 0.39, 0.60, r4.skew(), 0, False) <= 0.37          # heavy at avg 0.39: bid <= 0.37
    assert mm.pair_quote(r4, "Up", 0.30, 0.31, 0.60, r4.skew(), 0, False) is not None      # cheaper than avg - 2c: average down
    assert mm.pair_quote(r4, "Down", 0.60, 0.62, 0.40, r4.skew(), 0, False) == 0.58         # 0.97 - 0.39
    r4.buy("Up", 0.13, 10)                                                                  # averaged down to 0.26
    assert mm.pair_quote(r4, "Down", 0.80, 0.82, 0.40, r4.skew(), 0, False) == 0.71         # cap rose: 0.97 - 0.26
    assert r4.fast_move(100, 3.0) == "Up" and r4.pulled_until["Down"] > 100 and r4.boost_until["Up"] > 100
    assert r4.fast_move(100.5, 3.0) is None                                                 # same move: logged once
    assert r4.fast_move(101, 1.0) is None                                                   # small move ignored
    assert r4.no_avg_until["Down"] > 109                                                    # Binance up: no DOWN averaging 10 s
    assert mm.pair_quote(r4, "Up", 0.20, 0.21, 0.60, r4.skew(), 0, False, no_avg=True) is None  # heavy, blocked
    mm.AVG_STEP = 9
    r5 = mm.Round(None)
    r5.buy("Up", 0.65, 40); r5.buy("Down", 0.27, 40); r5.buy("Up", 0.71, 2)  # 11:05PM: $39.85 spent, UP +2
    assert r5.room("Up", 0.53) <= 50 - r5.cost - 2 * 0.47 + 1e-9                             # keeps money to pair
    assert r5.room("Down", 0.47) == 10                                                     # light side unaffected
    boosted = mm.quote("Up", 0.50, 0.52, 0.80, 0, 0, False, 0.0)
    assert boosted == 0.50                                                                  # boost bids at the best bid
    # spot: Binance REST fallback reused for 1 s, not called every 0.1 s tick
    import spot
    sf, calls = spot.SpotFeed.__new__(spot.SpotFeed), []
    sf._memo = {}
    assert sf._cached("k", lambda: calls.append(1) or len(calls)) == 1 == sf._cached("k", lambda: 99) and len(calls) == 1

    # taker: Binance move -> side; entry needs time left, a sane ask and SIZE shares at it
    import taker
    assert taker.signal(2.5) == "Up" and taker.signal(-3) == "Down" and taker.signal(1.9) is None and taker.signal(None) is None
    assert taker.entry_price(Book(0.49, 50, 0.50, 40), 120) == 0.50
    assert taker.entry_price(Book(0.49, 50, 0.50, 5), 120) is None          # fewer than SIZE shares at the ask
    assert taker.entry_price(Book(0.97, 50, 0.98, 40), 120) is None         # ask above MAX_ASK
    assert taker.entry_price(Book(0.49, 50, 0.50, 40), 20) is None          # last 30 s

    # Binance move: never measured across a data gap (reconnect), only over continuous data
    from feeds import BinanceFeed
    bf, t0 = BinanceFeed(), 1000.0
    bf.pts = [(t0 - 9, 100.0), (t0 - 8.5, 100.0)] + [(t0 - 0.2 + i * 0.05, 100.05) for i in range(4)]
    assert bf.move_bps(3, t0) is None
    bf.pts = [(t0 - 3.2 + i * 0.1, 100.0) for i in range(30)] + [(t0, 100.04)]
    assert round(bf.move_bps(3, t0), 1) == 4.0

    # live: API share amounts in either unit (5 or 5000000 = 5 shares)
    from live import amount
    assert amount("5.136985", 13) == 5.136985 and amount("5000000", 13) == 5 and amount("3.749999", 10) == 3.749999
    assert amount(None, 5) == 0

    # live fills, from the real responses of 22 Oct 18:07 (fake client, no network)
    import live as lv

    class FakeClient:
        def __init__(self, r):
            self.r = r

        def create_and_post_order(self, *a, **k):
            if isinstance(self.r, Exception):
                raise self.r
            return self.r
    bot = object.__new__(lv.Live)
    bot.dry, bot.balance, bot.err, bot.have = False, 34.6, "", {}
    bot.c = FakeClient({"success": True, "status": "matched", "orderID": "0xd3", "makingAmount": "3.749999",
                        "takingAmount": "5.136985"})
    sh, px = bot._fak("tok", "BUY", 0.75, 5)
    assert abs(sh - 5.136985) < 1e-9 and abs(px - 3.749999 / 5.136985) < 1e-9      # more shares, avg 0.73
    bot.c = FakeClient({"success": True, "status": "matched", "orderID": "0x85", "makingAmount": "5", "takingAmount": "3.85"})
    sh, px = bot._fak("tok", "SELL", 0.75, 5)
    assert sh == 5 and abs(px - 0.77) < 1e-9
    bot.c = FakeClient({"success": True, "status": "matched", "orderID": "0xb9", "makingAmount": "4.349996",
                        "takingAmount": "5.058135"})
    assert abs(bot._fak("tok", "BUY", 0.87, 5)[0] - 5.058135) < 1e-9                 # the fill logged as "no fill"
    bot.c = FakeClient(Exception("no orders found to match with FAK order"))
    assert bot._fak("tok", "BUY", 0.55, 5)[0] == 0
    bot.c = FakeClient({"success": True, "status": "matched", "orderID": "0x1", "makingAmount": "1.0", "takingAmount": "7.69"})
    assert bot.buy("tok", 0.12, 5)[0] == 7.69                                        # $1 minimum: 7.7 sh at 0.13

    # live sell: never more than the wallet holds; selling all of it closes the recorded size (fee taken in shares)
    sent = []

    class SellClient:
        def create_and_post_order(self, args, **k):
            sent.append(args.size)
            return {"success": True, "status": "matched", "orderID": "0x2", "makingAmount": str(args.size),
                    "takingAmount": str(round(args.size * 0.60, 6))}
    bot.c, bot.have = SellClient(), {"tok": 9.87}
    sh, px = bot.sell("tok", 0.61, 10)
    assert sent == [9.87] and sh == 10 and abs(px - 9.87 * 0.60 / 10) < 1e-9 and "tok" not in bot.have
    bot.c = FakeClient(Exception("not enough balance / allowance"))
    bot.watch = lambda *a, **k: None                                                 # no thread in the test
    assert bot.sell("tok", 0.61, 10)[0] == 0                                         # returns at once, retried next pass

    print("ok")


if __name__ == "__main__":
    main()
