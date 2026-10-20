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
    print("ok")


if __name__ == "__main__":
    main()
