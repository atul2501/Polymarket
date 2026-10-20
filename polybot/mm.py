"""Paper market maker for BTC 5m Up/Down, modelled on wallet 0xc533...69a9 (see wallet.py).

Every tick it rests a bid on UP at centre - DELTA and on DOWN at (1 - centre) - DELTA, so a matched UP+DOWN pair
costs about 1 - 2*DELTA and pays $1. It only ever buys, holds to settlement, stops quoting STOP_SEC before the close,
and stops buying the heavy side once |UP shares - DOWN shares| reaches MAX_SKEW.

Fill model (paper): UP and DOWN share one book, so our UP bid b is hit by a taker SELL of UP at price <= b, or a taker
BUY of DOWN at price >= 1 - b. Better price than the print -> fill up to its size; same price -> half (queue).

python mm.py                         live paper run on the CLOB trade stream (ledger MM_LEDGER, default runs/mm.db)
python mm.py --replay ADDR DB...     replay on recorded books, using that wallet's maker fills as the trade prints
"""
import logging
import os
import sys
import time
from math import floor
from pathlib import Path

import requests

import config
from engine import Leg, fair_up
from feeds import BookFeed, ChainlinkFeed
from ledger import Ledger
from market import MarketFeed
from spot import SpotFeed

log = logging.getLogger("mm")
DELTA = float(os.getenv("MM_DELTA", 0.015))
CLIP = float(os.getenv("MM_CLIP", 10))            # shares per fill (the wallet uses 30; we run ~1/3 size)
MAX_SKEW = float(os.getenv("MM_MAX_SKEW", 40))    # max |UP - DOWN| shares before we stop buying the heavy side
BUDGET = float(os.getenv("MM_BUDGET", 60))        # max $ spent per round
STOP_SEC = float(os.getenv("MM_STOP_SEC", 50))
CENTER = os.getenv("MM_CENTER", "model")          # "model" = Chainlink TWAP model P(up) (replay: + both halves),
# "mid" = UP book mid (replay: -$8 at delta 0.015)
LEDGER = Path(os.getenv("MM_LEDGER", Path(__file__).parent / "runs" / "mm.db"))


def quotes(up_bid, up_ask, dn_ask, p_model=None):
    """(UP bid, DOWN bid) around the centre, each kept one tick under its own ask so it rests on the book."""
    if up_bid is None or up_ask is None:
        return None
    mid = p_model if CENTER == "model" and p_model is not None else (up_bid + up_ask) / 2
    qu = min(floor((mid - DELTA) * 100) / 100, round(up_ask - 0.01, 2))
    qd = floor((1 - mid - DELTA) * 100) / 100
    if dn_ask is not None:
        qd = min(qd, round(dn_ask - 0.01, 2))
    return (qu, qd) if qu >= 0.01 and qd >= 0.01 else None


def fill_size(bid, side, print_side, print_asset_side, price, size):
    """Shares of our `side` bid at `bid` a print fills (see module docstring)."""
    if print_asset_side == side and print_side == "SELL":
        hit = price          # taker sold our side at `price`
    elif print_asset_side != side and print_side == "BUY":
        hit = 1 - price      # taker bought the other side -> matched against bids on ours at 1 - price
    else:
        return 0.0
    if bid > hit + 1e-9:
        return size
    if abs(bid - hit) < 1e-9:
        return size / 2
    return 0.0


class Round:
    """Inventory and quoting for one market window."""

    def __init__(self, m):
        self.m, self.sh, self.cost = m, {"Up": 0.0, "Down": 0.0}, 0.0
        self.resting = None  # (UP bid, DOWN bid) placed on the previous tick: these are what new prints can hit

    def take(self, side, bid, prints):
        """Fill our resting bid on `side` from prints; returns shares bought (capped by clip, skew and budget)."""
        other = "Down" if side == "Up" else "Up"
        room = min(CLIP, max(0.0, MAX_SKEW - (self.sh[side] - self.sh[other])), (BUDGET - self.cost) / bid)
        got = 0.0
        for ps, pa, price, size in prints:
            if room - got < 1:
                break
            got += min(room - got, fill_size(bid, side, ps, pa, price, size))
        got = floor(got)
        if got >= 1:
            self.sh[side] += got
            self.cost += got * bid
        return got


def side_of(m, asset):
    return "Up" if asset == m.up_token else "Down" if asset == m.down_token else None


def live():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname).1s %(message)s", datefmt="%H:%M:%S")
    http = requests.Session()
    ws = BookFeed().start()
    feed, led = MarketFeed(http, ws), Ledger(LEDGER)
    spot = SpotFeed(http, ChainlinkFeed(["btc/usd"]).start())
    rounds, last_seen, last_settle, last_log = {}, time.time(), 0.0, 0.0
    log.info("mm start | PAPER | centre %s delta %.3f clip %g max_skew %g budget $%g/round stop %gs",
             CENTER, DELTA, CLIP, MAX_SKEW, BUDGET, STOP_SEC)
    while True:
        try:
            now = time.time()
            markets = feed.current(now)
            books = feed.books(markets)
            prints = ws.trades_since(last_seen)
            last_seen = now
            for m in markets:
                r = rounds.setdefault(m.slug, Round(m))
                t_left = m.seconds_left(now)
                up, dn = books[m.up_token], books[m.down_token]
                if r.resting:  # prints since the last tick can only hit the quotes that were resting then
                    mine = [(ps, side_of(m, a), price, size) for (_, a, price, size, ps) in prints if side_of(m, a)]
                    for side, bid in (("Up", r.resting[0]), ("Down", r.resting[1])):
                        if n := r.take(side, bid, mine):
                            led.record(m, "mm", Leg(side, bid, n, 0.0))
                            log.info("[BTC 5m] MM FILL %s @ %.2f x%g | inv UP %g DN %g cost $%.2f", side, bid, n,
                                     r.sh["Up"], r.sh["Down"], r.cost)
                pm = None
                if CENTER == "model":
                    sp, beat, known, src = spot.view(m.asset, m.start, m.end, now)
                    if sp is None or beat is None or src != "chainlink":
                        r.resting = None  # no quotes without the resolution price
                        continue
                    pm = fair_up(sp, beat, spot.sigma(m.asset) * config.VOL_MULT, t_left, known)
                q = quotes(up.bid, up.ask, dn.ask, pm) if t_left > STOP_SEC else None
                r.resting = q
                if now - last_log >= 5:
                    last_log = now
                    log.info("[BTC 5m] %3ds | UP %s/%s | model %s | quotes %s | inv UP %g DN %g | cost $%.2f", t_left,
                             up.bid, up.ask, f"{pm:.3f}" if pm is not None else "-", q, r.sh["Up"], r.sh["Down"], r.cost)
            if now - last_settle > 30:
                last_settle = now
                for slug in led.unsettled(now):
                    if w := feed.outcome(slug):
                        led.settle(slug, w)
                        log.info("SETTLED %s -> %s", slug, w)
        except (requests.RequestException, ValueError, KeyError) as e:
            log.warning("api error: %r", e)
        time.sleep(0.25)


def replay(address, dbs):
    """Quote alongside the wallet on recorded books; the wallet's maker fills stand in for the market's trade prints."""
    import sqlite3
    from wallet import fetch_trades, winner
    http = requests.Session()
    trades = fetch_trades(http, address, 3000)
    ticks = {}
    for db in dbs:
        for t, slug, end, t_left, ub, ua, dbid, da, pm in sqlite3.connect(db).execute(
                "SELECT t, slug, window_end, t_left, up_bid, up_ask, dn_bid, dn_ask, p_model FROM ticks ORDER BY t"):
            ticks.setdefault(slug, []).append((t, t_left, ub, ua, dbid, da, pm))
    import bisect
    by = {}
    for x in trades:
        # only the wallet's MAKER fills (price at/below the recorded best bid) prove a taker sold into bids at that price
        rows = ticks.get(x["slug"])
        if not rows:
            continue
        i = bisect.bisect_right([r[0] for r in rows], x["timestamp"]) - 1
        if i < 0 or x["timestamp"] - rows[i][0] > 2:
            continue
        bid = rows[i][2] if x["outcome"] == "Up" else rows[i][4]
        if bid is not None and x["price"] <= bid + 1e-9:
            by.setdefault(x["slug"], []).append((x["timestamp"], "SELL", x["outcome"], x["price"], x["size"]))
    total, rows = 0.0, []
    for slug in sorted(set(ticks) & set(by)):
        w = winner(http, slug)
        if not w:
            continue
        m = type("M", (), {"slug": slug})()
        r = Round(m)
        prints = sorted(by[slug])
        prev_t = None
        for t, t_left, ub, ua, dbid, da, pm in ticks[slug]:
            if r.resting and prev_t is not None:  # prints since the last tick hit the quotes resting since then
                now_prints = [(ps, side, price, size) for (pt, ps, side, price, size) in prints if prev_t < pt <= t]
                for side, bid in (("Up", r.resting[0]), ("Down", r.resting[1])):
                    r.take(side, bid, now_prints)
            r.resting, prev_t = (quotes(ub, ua, da, pm) if t_left > STOP_SEC else None), t
        pnl = r.sh[w] - r.cost
        total += pnl
        rows.append((slug, r.sh["Up"], r.sh["Down"], r.cost, w, pnl))
    return rows, total


if __name__ == "__main__":
    if sys.argv[1:2] == ["--replay"]:
        rows, total = replay(sys.argv[2], sys.argv[3:])
        for slug, us, ds, cost, w, pnl in rows:
            print(f"{slug}  UP {us:5.0f}  DN {ds:5.0f}  cost ${cost:6.2f}  winner {w:4}  pnl ${pnl:+6.2f}")
        print(f"rounds {len(rows)}  total ${total:+.2f}  (delta {DELTA}, clip {CLIP}, max skew {MAX_SKEW}, budget ${BUDGET})")
    else:
        try:
            live()
        except KeyboardInterrupt:
            pass
