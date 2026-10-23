"""Paper market maker v2 for BTC 5m Up/Down: model gives the direction, the order book gives the price.

v1 (commit 6729088) centred bids on the model and lost -$65.29 overnight: it chased the model above the market
(pairs averaged ~$1.03) and its leftover imbalance was a coin flip. The wallet it copies (0xc533...69a9) earned +$767 in
the same hours, 94% from spread, bidding 1.6-2.2c under the book mid. v2:

- direction:  no bid on a side the model gives < MIN_SIDE; imbalance leans toward the model's favourite
              (target = DIR_K * (p_up - 0.5) * shares); late top-up of the favourite (>= TOPUP_MIN, ask <= TOPUP_MAX_ASK)
- price:      bid = min(book mid - BOOK_DELTA, best bid, model value - DELTA) - inventory lean; never above the best bid
- safety:     pull a side for PULL_SEC after its model value moves > PULL_MOVE in PULL_WINDOW s; stop at STOP_SEC left;
              stop quoting after -LOSS_HOUR in the last hour or -LOSS_DAY today
- paper fills: queue-aware. A bid joins behind the size already resting at its price; a print at our price eats that
              queue first, a print through our price (worse for the seller) fills us. UP and DOWN share one book, so a taker
              BUY of the other side at q hits bids on ours at 1 - q.

python mm.py                       live paper run (ledger MM_LEDGER, default runs/mm.db), records ticks and trade prints
python mm.py --replay ADDR DB...   replay on recorded books with that wallet's maker fills as prints (no queue info)
"""
import bisect
import logging
import os
import sys
import time
from math import floor
from pathlib import Path

import requests

import config
from engine import Leg, Snapshot, fair_up, taker_fee
from feeds import BinanceFeed, BookFeed, ChainlinkFeed
from ledger import Ledger
from market import MarketFeed
from spot import SpotFeed

log = logging.getLogger("mm")
_f = lambda k, d: float(os.getenv(k, d))
DELTA = _f("MM_DELTA", 0.015)          # bid at most this far under the model's value of the side
BOOK_DELTA = _f("MM_BOOK_DELTA", 0.015)  # ...and at least this far under the book mid (the wallet: 1.4c)
CLIP = _f("MM_CLIP", 10)               # shares per fill
MAX_SKEW = _f("MM_MAX_SKEW", 10)       # hard cap on |UP - DOWN| shares (v2 at 40 lost $94 in 5 reversal rounds)
PAIR_TARGET = _f("MM_PAIR_TARGET", 0.97)  # complete a pair while UP avg + DOWN price (+ fee) <= this
PAIR_MIN = _f("MM_PAIR_MIN", 5)           # once one side is this many shares ahead, it is "heavy"
# 21 Oct, 32 rounds: pairs earned +$22..+$66, unpaired extra shares lost -$97..-$212 and sat on the losing side in
# 43 of 44 rounds (bids fill on the side that is falling). So: pair at once as taker, never add to the heavy side.
PAIR_TAKE_MAX = _f("MM_PAIR_TAKE_MAX", 1.02)  # taker-complete a pair up to this (2c/share beats ~45c lost unpaired)
AVG_STEP = _f("MM_AVG_STEP", 9)           # heavy side may only add at <= its average - this; 9 = off (was 0.02-0.04)
OPEN_MAX = _f("MM_OPEN_MAX", 0.60)        # never bid above this except to pair (bought DOWN @0.95 one-sided, UP won)
BOOST = os.getenv("MM_BOOST", "0") != "0"  # fast-move bid-up to the mid; off: it opened one-sided at 0.71-0.95
FAST_BPS, FAST_WINDOW, FAST_SEC = _f("MM_FAST_BPS", 2), _f("MM_FAST_WINDOW", 3), _f("MM_FAST_SEC", 3)
FAST_AVG_SEC = _f("MM_FAST_AVG_SEC", 10)  # no averaging down this long after Binance moves against a side
RESERVE = os.getenv("MM_RESERVE", "1") != "0"  # adding to the heavy side keeps budget to pair it (21 Oct 11:05PM: -$7.75)
NOWCAST = os.getenv("MM_NOWCAST", "1") != "0"  # model spot = Chainlink + Binance move since the Chainlink point
LOOP_SEC = _f("MM_LOOP_SEC", 0.1)         # Polymarket follows Binance within ~0.4 s
LAT = _f("MM_LAT", 0)                     # seconds for a quote change to reach the book (real: post 0.085, cancel 0.075)
BUDGET = _f("MM_BUDGET", 50)           # $ per round
STOP_SEC = _f("MM_STOP_SEC", 20)
MIN_SIDE = _f("MM_MIN_SIDE", 0.40)     # real fills overnight: -$65.29 -> +$5.58 with this alone
DIR_K = _f("MM_DIR_K", 0)              # imbalance target = DIR_K * (p_up - 0.5) * shares; off: replay mixed by half
LEAN = _f("MM_LEAN", 0)                # $/share off the heavy side's bid; off: replay better in both halves without it
PULL_MOVE, PULL_WINDOW, PULL_SEC = _f("MM_PULL_MOVE", 9), _f("MM_PULL_WINDOW", 10), _f("MM_PULL_SEC", 5)  # 9 = off
# (pull at 0.10: replay worse in both halves)
TOPUP_FROM, TOPUP_TO = _f("MM_TOPUP_FROM", 120), _f("MM_TOPUP_TO", 30)
TOPUP_MIN, TOPUP_MAX_ASK, TOPUP_SIZE = _f("MM_TOPUP_MIN", 9), _f("MM_TOPUP_MAX_ASK", 0.85), _f("MM_TOPUP_SIZE", 10)
# top-up off (9): replay -$8.93 / +$8.37 by half; enable with MM_TOPUP_MIN=0.90
LOSS_HOUR, LOSS_DAY = _f("MM_LOSS_HOUR", 25), _f("MM_LOSS_DAY", 50)
LEDGER = Path(os.getenv("MM_LEDGER", Path(__file__).parent / "runs" / "mm.db"))
SIDES = ("Up", "Down")


def other(side):
    return "Down" if side == "Up" else "Up"


def target_skew(p_up, total):
    return max(-MAX_SKEW, min(MAX_SKEW, DIR_K * (p_up - 0.5) * total))


def quote(side, bid, ask, value, skew, target, pulled, book_delta=None):
    """Our bid for one side, or None. skew = UP - DOWN shares; lean pushes inventory toward `target`."""
    if bid is None or ask is None or value < MIN_SIDE or pulled:
        return None
    excess = (skew - target) if side == "Up" else (target - skew)   # > 0: we already hold too much of this side
    bd = BOOK_DELTA if book_delta is None else book_delta
    px = min((bid + ask) / 2 - bd, bid, value - DELTA) - LEAN * excess
    px = min(floor(px * 100 + 1e-9) / 100, bid)                       # never above the best bid
    return px if px >= 0.01 else None


def pair_quote(r, side, bid, ask, value, skew, target, pulled, boosted=False, no_avg=False):
    """Bid for `side`, pairing first, like the copied wallet (round 10:10PM ET: it bought UP all the way down to an
    average of $0.24, which let DOWN at $0.67 still pair at $0.91).
    - light side: bid up to PAIR_TARGET - heavy side's average (that cap rises as the heavy side averages down),
      ignoring the direction filter, since a pair at <= PAIR_TARGET is profit either way
    - heavy side: may keep buying, but only at <= its own average - AVG_STEP, and not while `no_avg`
      (Binance just moved against it)
    - otherwise: normal quote; `boosted` (Binance just moved toward this side) bids up to the book mid"""
    o = other(side)
    if r.sh[o] - r.sh[side] >= PAIR_MIN:
        if bid is None:
            return None
        px = min(floor((PAIR_TARGET - r.avg(o)) * 100 + 1e-9) / 100, bid)
        return px if px >= 0.01 else None
    px = quote(side, bid, ask, value, skew, target, pulled, 0.0 if boosted and BOOST else None)
    if px is not None:
        px = min(px, OPEN_MAX)
    if px is not None and r.sh[side] - r.sh[o] >= PAIR_MIN:
        if no_avg:
            return None
        px = min(px, floor((r.avg(side) - AVG_STEP) * 100 + 1e-9) / 100)
        return px if px >= 0.01 else None
    return px


def pair_take(r, books_by_side, fee):
    """(side, ask, shares, fee) to buy as taker now, if it completes pairs at <= PAIR_TAKE_MAX."""
    heavy = "Up" if r.skew() > 0 else "Down"
    light, excess = other(heavy), abs(r.skew())
    book = books_by_side[light]
    if excess < PAIR_MIN or book.ask is None:
        return None
    f = taker_fee(book.ask, 1, fee)
    if r.avg(heavy) + book.ask + f > PAIR_TAKE_MAX:
        return None
    n = floor(min(excess, book.ask_size))
    return (light, book.ask, n, f * n) if n >= 1 else None


def hit_price(side, print_side, print_on, price):
    """Price at which a print trades against bids on `side`, or None if it can't touch them."""
    if print_on == side and print_side == "SELL":
        return price
    if print_on == other(side) and print_side == "BUY":
        return round(1 - price, 3)
    return None


class Round:
    """Inventory, resting bids and queue position for one market window."""

    def __init__(self, m):
        self.m = m
        self.sh = {"Up": 0.0, "Down": 0.0}
        self.side_cost = {"Up": 0.0, "Down": 0.0}
        self.cost = 0.0
        self.resting = {"Up": None, "Down": None}  # side -> [price, size ahead of us in the queue]
        self.wish = {"Up": None, "Down": None}     # MM_LAT: the quote we last asked for
        self.pend = {"Up": None, "Down": None}     # MM_LAT: (effective at, price, queue ahead) still travelling
        self.hist = []                               # (t, p_up) for the pull rule
        self.pulled_until = {"Up": 0.0, "Down": 0.0}
        self.boost_until = {"Up": 0.0, "Down": 0.0}
        self.no_avg_until = {"Up": 0.0, "Down": 0.0}
        self.topped = False

    def fast_move(self, now, bps):
        """Binance moved `bps` in FAST_WINDOW: step away from the side about to be sold, lean into the other."""
        if bps is None or abs(bps) < FAST_BPS:
            return None
        rising = "Up" if bps > 0 else "Down"
        started = now >= self.boost_until[rising]
        self.pulled_until[other(rising)] = now + FAST_SEC
        self.boost_until[rising] = now + FAST_SEC
        self.no_avg_until[other(rising)] = now + FAST_AVG_SEC
        return rising if started else None   # report only the start of a move

    def skew(self):
        return self.sh["Up"] - self.sh["Down"]

    def avg(self, side):
        return self.side_cost[side] / self.sh[side] if self.sh[side] else 0.0

    def buy(self, side, px, n, fee=0.0):
        self.sh[side] += n
        self.side_cost[side] += px * n + fee
        self.cost += px * n + fee

    def room(self, side, px):
        d = self.sh[side] - self.sh[other(side)]
        left = BUDGET - self.cost
        n = min(CLIP, MAX_SKEW - d, left / px)
        if RESERVE and d >= 0:  # each extra share also needs ~(1 - px) later to pair it, as does the current excess
            n = min(n, left - d * (1 - px))
        return max(0.0, n)

    def place(self, side, px, ahead, now=None):
        """Ask for a bid (None = cancel). With MM_LAT > 0 the change takes effect only LAT seconds later, like a real
        post (85 ms) or cancel (75 ms, measured from Stockholm 23 Oct): until then the old quote can still be hit."""
        if LAT > 0 and now is not None:
            if px == self.wish[side]:
                return
            self.wish[side] = px
            self.pend[side] = (now + LAT, px, ahead)
            return
        self._place(side, px, ahead)

    def apply(self, now):
        """Make the pending quote changes whose trip has finished."""
        for side in SIDES:
            p = self.pend[side]
            if p and p[0] <= now:
                self.pend[side] = None
                self._place(side, p[1], p[2])

    def _place(self, side, px, ahead):
        """Rest a bid; keep our queue spot if the price didn't change."""
        cur = self.resting[side]
        if px is None:
            self.resting[side] = None
        elif cur is None or abs(cur[0] - px) > 1e-9:
            self.resting[side] = [px, ahead]

    def fill(self, side, prints):
        """Queue-aware fills of our resting bid on `side`; returns shares bought."""
        cur = self.resting[side]
        if cur is None:
            return 0.0
        px, got = cur[0], 0.0
        room = self.room(side, px)
        for print_side, print_on, price, size in prints:
            h = hit_price(side, print_side, print_on, price)
            if h is None or h > px + 1e-9:
                continue
            if abs(h - px) < 1e-9:           # trades at our price: the queue ahead of us goes first
                eat = min(cur[1], size)
                cur[1] -= eat
                size -= eat
            got += min(max(0.0, room - got), size)
        got = floor(got)
        if got >= 1:
            self.sh[side] += got
            self.side_cost[side] += got * px
            self.cost += got * px
        return got

    def update_pulls(self, now, p_up):
        self.hist = [x for x in self.hist if now - x[0] <= PULL_WINDOW] + [(now, p_up)]
        moved = p_up - self.hist[0][1]
        if abs(moved) > PULL_MOVE:  # fast move: step out of the way on both sides for a moment
            for s in SIDES:
                self.pulled_until[s] = now + PULL_SEC
        return moved


def side_of(m, asset):
    return "Up" if asset == m.up_token else "Down" if asset == m.down_token else None


def topup(r, t_left, p_up, books_by_side):
    """Late taker buy of the model's favourite when it is cheap enough to pay at ~90% accuracy."""
    if r.topped or not TOPUP_TO < t_left <= TOPUP_FROM:
        return None
    fav = "Up" if p_up >= 0.5 else "Down"
    value = p_up if fav == "Up" else 1 - p_up
    book = books_by_side[fav]
    if value < TOPUP_MIN or book.ask is None or book.ask > TOPUP_MAX_ASK:
        return None
    n = floor(min(TOPUP_SIZE, book.ask_size))
    return (fav, book.ask, n) if n >= config.MIN_SHARES else None


def live():
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname).1s %(message)s", datefmt="%H:%M:%S",
                        handlers=[logging.StreamHandler(), logging.FileHandler(LEDGER.with_suffix(".log"))])
    http = requests.Session()
    ws = BookFeed().start()
    feed, led = MarketFeed(http, ws), Ledger(LEDGER)
    chain = ChainlinkFeed(["btc/usd"]).start()
    spot = SpotFeed(http, chain)
    bn = BinanceFeed().start()
    rounds, last_seen, last_settle, last_log = {}, time.time(), 0.0, 0.0
    log.info("mm v4 start | PAPER | pair target %.2f avg step %.2f fast %g bp/%gs nowcast %s loop %.2fs |",
             PAIR_TARGET, AVG_STEP, FAST_BPS, FAST_WINDOW, NOWCAST, LOOP_SEC)
    log.info("mm settings | min_side %.2f dir_k %.2f book_delta %.3f delta %.3f clip %g max_skew %g budget $%g "
             "stop %gs pull %.2f/%gs topup >=%.2f @<=%.2f loss stop $%g/h $%g/day",
             MIN_SIDE, DIR_K, BOOK_DELTA, DELTA, CLIP, MAX_SKEW, BUDGET, STOP_SEC, PULL_MOVE, PULL_WINDOW,
             TOPUP_MIN, TOPUP_MAX_ASK, LOSS_HOUR, LOSS_DAY)
    while True:
        try:
            now = time.time()
            markets = feed.current(now)
            books = feed.books(markets)
            raw = ws.trades_since(last_seen)
            last_seen = now
            led.prints([(t, a, p, s, side) for (t, a, p, s, side) in raw])
            halted = led.hour_pnl(now) < -LOSS_HOUR or led.today_pnl() < -LOSS_DAY
            for m in markets:
                r = rounds.setdefault(m.slug, Round(m))
                t_left = m.seconds_left(now)
                up, dn = books[m.up_token], books[m.down_token]
                mine = [(ps, side_of(m, a), price, size) for (_, a, price, size, ps) in raw if side_of(m, a)]
                r.apply(now)
                for side in SIDES:          # prints since the last tick can only hit bids that were resting then
                    if n := r.fill(side, mine):
                        led.record(m, "mm", Leg(side, r.resting[side][0], n, 0.0))
                        log.info("[BTC 5m] MM FILL %s @ %.2f x%g | inv UP %g DN %g cost $%.2f", side,
                                 r.resting[side][0], n, r.sh["Up"], r.sh["Down"], r.cost)
                sp, beat, known, src = spot.view(m.asset, m.start, m.end, now)
                if sp is None or beat is None or src != "chainlink":
                    r.place("Up", None, 0, now)
                    r.place("Down", None, 0, now)
                    continue
                cl, bnow = chain.last_t("btc/usd"), bn.at(now)
                if NOWCAST and cl and bnow and (bthen := bn.at(cl[0] + 1, max_age=5)):
                    sp = cl[1] * bnow / bthen              # Chainlink lags Binance ~2.6 s: carry Binance's move forward
                sig = spot.sigma(m.asset)
                p_up = fair_up(sp, beat, sig * config.VOL_MULT, t_left, known)
                led.tick(now, m, Snapshot(up, dn, t_left, m.fee, 0.0, p_up), sp, beat, known, sig)
                led.binance(now, bnow)
                moved = r.update_pulls(now, p_up)
                if rising := r.fast_move(now, bn.move_bps(FAST_WINDOW, now)):
                    log.info("[BTC 5m] FAST move: Binance %+.1f bp in %gs -> lean %s", bn.move_bps(FAST_WINDOW, now),
                             FAST_WINDOW, rising)
                target = target_skew(p_up, r.sh["Up"] + r.sh["Down"])
                quoting = t_left > STOP_SEC and not halted
                if quoting and (pt := pair_take(r, {"Up": up, "Down": dn}, m.fee)):
                    side, ask, n, fee = pt
                    led.record(m, "mm", Leg(side, ask, n, fee))
                    r.buy(side, ask, n, fee)
                    log.info("[BTC 5m] PAIR take %s @ %.2f x%g (pair %.3f incl fee) | inv UP %g DN %g", side, ask, n,
                             r.avg(other(side)) + ask + fee / n, r.sh["Up"], r.sh["Down"])
                for side, book in (("Up", up), ("Down", dn)):
                    value = p_up if side == "Up" else 1 - p_up
                    px = pair_quote(r, side, book.bid, book.ask, value, r.skew(), target, now < r.pulled_until[side],
                                    now < r.boost_until[side], now < r.no_avg_until[side]) if quoting else None
                    r.place(side, px, ws.depth(m.up_token if side == "Up" else m.down_token, px) if px else 0, now)
                if not halted and (t := topup(r, t_left, p_up, {"Up": up, "Down": dn})):
                    fav, ask, n = t
                    fee = taker_fee(ask, n, m.fee)
                    led.record(m, "topup", Leg(fav, ask, n, fee))
                    r.sh[fav] += n
                    r.cost += ask * n + fee
                    r.side_cost[fav] += ask * n + fee
                    r.topped = True
                    log.info("[BTC 5m] TOPUP %s @ %.2f x%g (model %.3f, %ds left)", fav, ask, n, max(p_up, 1 - p_up), t_left)
                if now - last_log >= 5:
                    last_log = now
                    q = {s: (round(v[0], 2) if v else None) for s, v in r.resting.items()}
                    log.info("[BTC 5m] %3ds | UP %s/%s | model %.3f (moved %+.2f) | target skew %+.0f | bids %s | "
                             "inv UP %g DN %g | cost $%.2f%s", t_left, up.bid, up.ask, p_up, moved, target, q,
                             r.sh["Up"], r.sh["Down"], r.cost, " | HALTED (loss stop)" if halted else "")
            if now - last_settle > 30:
                last_settle = now
                for slug in led.unsettled(now):
                    if w := feed.outcome(slug):
                        led.settle(slug, w)
                        log.info("SETTLED %s -> %s", slug, w)
        except (requests.RequestException, ValueError, KeyError) as e:
            log.warning("api error: %r", e)
        time.sleep(LOOP_SEC)


def replay(address, dbs):
    """v2 rules on recorded books; the wallet's maker fills stand in for prints. Queue size ahead is unknown -> 0."""
    import sqlite3
    from wallet import fetch_trades, winner
    http = requests.Session()
    trades = fetch_trades(http, address, 3000)
    ticks = {}
    for db in dbs:
        for t, slug, t_left, ub, ua, uas, dbid, da, das, pm in sqlite3.connect(db).execute(
                "SELECT t, slug, t_left, up_bid, up_ask, up_ask_size, dn_bid, dn_ask, dn_ask_size, p_model FROM ticks ORDER BY t"):
            ticks.setdefault(slug, []).append((t, t_left, ub, ua, uas, dbid, da, das, pm))
    by = {}
    for x in trades:  # keep only the wallet's maker fills: proof a taker sold into bids at that price
        rows = ticks.get(x["slug"])
        if not rows:
            continue
        i = bisect.bisect_right([r[0] for r in rows], x["timestamp"]) - 1
        if i < 0 or x["timestamp"] - rows[i][0] > 2:
            continue
        bid = rows[i][2] if x["outcome"] == "Up" else rows[i][5]
        if bid is not None and x["price"] <= bid + 1e-9:
            by.setdefault(x["slug"], []).append((x["timestamp"], "SELL", x["outcome"], x["price"], x["size"]))
    total, out = 0.0, []
    for slug in sorted(set(ticks) & set(by)):
        w = winner(http, slug)
        if not w:
            continue
        r, prints, prev_t = Round(None), sorted(by[slug]), None
        for t, t_left, ub, ua, uas, dbid, da, das, pm in ticks[slug]:
            if prev_t is not None:
                now_prints = [(ps, side, price, size) for (pt, ps, side, price, size) in prints if prev_t < pt <= t]
                for side in SIDES:
                    r.fill(side, now_prints)
            prev_t = t
            if pm is None:
                continue
            r.update_pulls(t, pm)
            target = target_skew(pm, r.sh["Up"] + r.sh["Down"])
            book = type("B", (), {})
            bu, bd = book(), book()
            bu.ask, bu.ask_size, bd.ask, bd.ask_size = ua, uas or 0, da, das or 0
            if t_left > STOP_SEC and (pt := pair_take(r, {"Up": bu, "Down": bd}, config.DEFAULT_FEE)):
                r.buy(pt[0], pt[1], pt[2], pt[3])
            for side, b, a in (("Up", ub, ua), ("Down", dbid, da)):
                v = pm if side == "Up" else 1 - pm
                r.place(side, pair_quote(r, side, b, a, v, r.skew(), target, t < r.pulled_until[side]) if t_left > STOP_SEC else None, 0)
            if tp := topup(r, t_left, pm, {"Up": bu, "Down": bd}):
                fav, ask, n = tp
                c = ask * n + taker_fee(ask, n, config.DEFAULT_FEE)
                r.sh[fav] += n
                r.cost += c
                r.side_cost[fav] += c
                r.topped = True
        pnl = r.sh[w] - r.cost
        total += pnl
        out.append((slug, r.sh["Up"], r.sh["Down"], r.cost, w, pnl))
    return out, total


if __name__ == "__main__":
    if sys.argv[1:2] == ["--replay"]:
        rows, total = replay(sys.argv[2], sys.argv[3:])
        for slug, us, ds, cost, w, pnl in rows:
            print(f"{slug}  UP {us:5.0f}  DN {ds:5.0f}  cost ${cost:6.2f}  winner {w:4}  pnl ${pnl:+6.2f}")
        h = len(rows) // 2
        print(f"rounds {len(rows)}  total ${total:+.2f}  first ${sum(r[5] for r in rows[:h]):+.2f}  "
              f"second ${sum(r[5] for r in rows[h:]):+.2f}")
    else:
        try:
            live()
        except KeyboardInterrupt:
            pass
