"""Paper fast taker for BTC 5m Up/Down: react to Binance before Polymarket's book does.

Binance BTC/USDT leads the Polymarket UP price by ~0.4 s (live capture 21 Oct). When the Binance mid moves
>= MOVE_BPS within MOVE_SEC, buy SIZE shares of the side it moved toward at the ask, then sell them at the bid
HOLD_SEC later. Taker fee on both legs. Replays (scratchpad phase1.py / filters.py), 0.3 s reaction:
last night +5.4c/share over 56 trades, 21 Oct afternoon +3.0c over 53; ~1-1.5c/share lost per extra 0.1 s; no entry
filter and no limit exit beat this plain version on both days.

Paper realism: the entry waits DELAY_SEC after the signal (our order's trip to Polymarket) and takes the ask seen
then, only if >= SIZE shares sit at it; the exit takes the bid seen at the exit time.

python taker.py        live paper run (ledger TK_LEDGER, default runs/taker.db); records ticks, Binance and prints
"""
import logging
import os
import time
from pathlib import Path

import requests

import config
from engine import Leg, Snapshot, taker_fee
from feeds import BinanceFeed, BookFeed
from ledger import Ledger
from market import MarketFeed

log = logging.getLogger("taker")
_f = lambda k, d: float(os.getenv(k, d))
MOVE_BPS, MOVE_SEC = _f("TK_MOVE_BPS", 2), _f("TK_MOVE_SEC", 1)
DELAY_SEC = _f("TK_DELAY", 0.3)          # paper stand-in for order latency (Mac -> Polymarket ~0.17 s)
HOLD_SEC = _f("TK_HOLD", 5)
SIZE = _f("TK_SIZE", 10)
MIN_ASK, MAX_ASK = _f("TK_MIN_ASK", 0.05), _f("TK_MAX_ASK", 0.95)
MIN_LEFT = _f("TK_MIN_LEFT", 30)         # no entries in the last 30 s
COOLDOWN = _f("TK_COOLDOWN", 10)
LOOP_SEC = _f("TK_LOOP", 0.05)
LEDGER = Path(os.getenv("TK_LEDGER", Path(__file__).parent / "runs" / "taker.db"))


def signal(bps):
    """Side the Binance move points to, or None."""
    if bps is None or abs(bps) < MOVE_BPS:
        return None
    return "Up" if bps > 0 else "Down"


def entry_price(book, t_left):
    """Ask to pay for SIZE shares, or None if the book doesn't allow an entry."""
    if t_left < MIN_LEFT or book is None or book.ask is None or not MIN_ASK <= book.ask <= MAX_ASK:
        return None
    return book.ask if book.ask_size >= SIZE else None


def live():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname).1s %(message)s", datefmt="%H:%M:%S")
    http = requests.Session()
    ws = BookFeed().start()
    feed, led = MarketFeed(http, ws), Ledger(LEDGER)
    bn = BinanceFeed().start()
    pending, pos, last_entry = None, None, 0.0   # pending: (fire_at, market, side); pos: (id, market, side, exit_at)
    last_seen, last_tick, last_settle, last_log = time.time(), 0.0, 0.0, 0.0
    log.info("taker start | PAPER | %g bp in %gs -> buy %g @ ask after %.2fs, sell at bid after %gs | ask %.2f-%.2f "
             "| no entry < %gs left | cooldown %gs | ledger %s", MOVE_BPS, MOVE_SEC, SIZE, DELAY_SEC, HOLD_SEC,
             MIN_ASK, MAX_ASK, MIN_LEFT, COOLDOWN, LEDGER.name)
    while True:
        try:
            now = time.time()
            markets = feed.current(now)
            books = feed.books(markets)
            raw = ws.trades_since(last_seen)
            last_seen = now
            led.prints([(t, a, p, s, side) for (t, a, p, s, side) in raw])
            for m in markets:
                up, dn = books[m.up_token], books[m.down_token]
                t_left = m.seconds_left(now)
                side_book = {"Up": up, "Down": dn}
                if now - last_tick >= 0.1:
                    last_tick = now
                    led.tick(now, m, Snapshot(up, dn, t_left, m.fee, 0.0, None), None, None, None, None)
                    led.binance(now, bn.at(now))
                if pos and pos[1].slug == m.slug and now >= pos[3]:
                    pid, _, side, _ = pos
                    bid = side_book[side].bid
                    if bid is not None:
                        led.close(pid, bid, taker_fee(bid, 1, m.fee))
                        pnl = led._one("SELECT pnl FROM trades WHERE id=?", pid)
                        log.info("[BTC 5m] SELL %s @ %.2f x%g | P/L %+.2f | total %+.2f", side, bid, SIZE, pnl,
                                 led._one("SELECT coalesce(sum(pnl),0) FROM trades WHERE status!='open'"))
                        pos = None
                if pending and pending[1].slug == m.slug and now >= pending[0]:
                    _, _, side = pending
                    pending = None
                    if pos is None and (ask := entry_price(side_book[side], t_left)) is not None:
                        fee = taker_fee(ask, SIZE, m.fee)
                        pid = led.record(m, "taker", Leg(side, ask, SIZE, fee))
                        pos, last_entry = (pid, m, side, now + HOLD_SEC), now
                        log.info("[BTC 5m] BUY %s @ %.2f x%g (fee %.3f) | %ds left", side, ask, SIZE, fee, t_left)
                if pending is None and pos is None and now - last_entry >= COOLDOWN and t_left >= MIN_LEFT:
                    bps = bn.move_bps(MOVE_SEC, now)
                    if side := signal(bps):
                        pending = (now + DELAY_SEC, m, side)
                        log.info("[BTC 5m] SIGNAL Binance %+.1f bp in %gs -> %s (UP %s/%s)", bps, MOVE_SEC, side,
                                 up.bid, up.ask)
            if now - last_log >= 300:
                last_log = now
                n, pnl = led.db.execute("SELECT count(*), coalesce(sum(pnl),0) FROM trades WHERE status!='open'").fetchone()
                log.info("taker status | %d trades closed | P/L %+.2f", n, pnl)
            if now - last_settle > 30:
                last_settle = now
                for slug in led.unsettled(now):   # only if a sale never happened (e.g. no bid at the exit)
                    if w := feed.outcome(slug):
                        led.settle(slug, w)
                        log.info("SETTLED %s -> %s", slug, w)
        except (requests.RequestException, ValueError, KeyError) as e:
            log.warning("api error: %r", e)
        time.sleep(LOOP_SEC)


if __name__ == "__main__":
    live()
