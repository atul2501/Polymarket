"""Paper fast taker for BTC 5m Up/Down: react to Binance before Polymarket's book does.

Binance BTC/USDT leads the Polymarket UP price by ~0.4 s (live capture 21 Oct). When the Binance mid moves
>= MOVE_BPS within MOVE_SEC, buy SIZE shares of the side it moved toward at the ask, then sell them at the bid
HOLD_SEC later. Taker fee on both legs. Replays (scratchpad phase1.py / filters.py), 0.3 s reaction:
last night +5.4c/share over 56 trades, 21 Oct afternoon +3.0c over 53; ~1-1.5c/share lost per extra 0.1 s; no entry
filter and no limit exit beat this plain version on both days.

Paper realism: the entry waits DELAY_SEC after the signal (our order's trip to Polymarket) and takes the ask seen
then, only if >= SIZE shares sit at it; the exit takes the bid seen at the exit time.

TK_SETTLE=1: the copied wallet's way (it never sells; 21 Sep-21 Oct it bought on the side Binance moved toward over
the last ~3 s and held to settlement, +2.1c/share as taker). Many entries per round, a cooldown per side, no exit.
Replay at our speed (0.3 s): 0.5 bp trigger ~0c/share, 3 bp +3.2c over 502 trades (t=1.6, not yet significant).

python taker.py        live paper run (ledger TK_LEDGER, default runs/taker.db); records ticks, Binance and prints
TK_LIVE=1              REAL orders through live.py (FAK, no paper delay); see live.py for keys and safety limits
"""
import logging
import os
import sqlite3
import threading
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
MAX_SPREAD = _f("TK_MAX_SPREAD", 1.0)    # skip a side whose ask-bid is wider (23 Oct: bought a new round's first second at 0.83/0.91)
LOOP_SEC = _f("TK_LOOP", 0.05)
SLOW_LOOP_MS = _f("TK_SLOW_LOOP_MS", 250)   # a loop pass longer than this is written to the signals table
SETTLE = os.getenv("TK_SETTLE", "0") != "0"   # hold to settlement; cooldown applies per side
LEDGER = Path(os.getenv("TK_LEDGER", Path(__file__).parent / "runs" / "taker.db"))
LIVE = os.getenv("TK_LIVE", "0") != "0"
if LIVE:
    DELAY_SEC = 0.0                          # real orders pay the real network delay, no simulated one


def signal(bps):
    """Side the Binance move points to, or None."""
    if bps is None or abs(bps) < MOVE_BPS:
        return None
    return "Up" if bps > 0 else "Down"


def entry_price(book, t_left):
    """Ask to pay for SIZE shares, or None if the book doesn't allow an entry."""
    if t_left < MIN_LEFT or book is None or book.ask is None or not MIN_ASK <= book.ask <= MAX_ASK:
        return None
    if MAX_SPREAD < 1 and (book.bid is None or book.ask - book.bid > MAX_SPREAD + 1e-9):
        return None
    return book.ask if book.ask_size >= SIZE else None


def entry_block(book, t_left):
    """Why entry_price() refused: a short reason for the signals table."""
    if t_left < MIN_LEFT:
        return "late"
    if book is None or book.ask is None:
        return "no_ask"
    if not MIN_ASK <= book.ask <= MAX_ASK:
        return "ask_band"
    if MAX_SPREAD < 1 and (book.bid is None or book.ask - book.bid > MAX_SPREAD + 1e-9):
        return "spread"
    return "ask_size"


def token(m, side):
    return m.up_token if side == "Up" else m.down_token


def settler():
    """Settle finished rounds off the trading loop: a slow Polymarket API froze the loop for ~10 s (21 Oct 22:4x)."""
    led, feed = Ledger(LEDGER), MarketFeed(requests.Session())   # own connection: sqlite objects stay in their thread
    while True:
        try:
            for slug in led.unsettled(time.time()):   # settle mode, or a sale that never happened (no bid at the exit)
                if w := feed.outcome(slug):
                    led.settle(slug, w)
                    log.info("SETTLED %s -> %s", slug, w)
        except (requests.RequestException, ValueError, KeyError, sqlite3.Error) as e:   # "database is locked" killed it 21 Oct 23:14
            log.warning("settle error: %r", e)
        time.sleep(30)


def live():
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname).1s %(message)s", datefmt="%H:%M:%S",
                        handlers=[logging.StreamHandler(), logging.FileHandler(LEDGER.with_suffix(".log"))])   # dashboard tails it
    http = requests.Session()
    ws = BookFeed().start()
    feed, led = MarketFeed(http, ws), Ledger(LEDGER)
    bn = BinanceFeed().start()
    pending, pos = None, None   # pending: (fire_at, market, side); pos: (id, market, side, exit_at, shares, sold_usd)
    lx = None
    if LIVE:
        from live import Live                  # imported only here: paper runs don't need the CLOB client
        lx = Live()
        lx.bn = bn
    blocked_log = 0.0
    last_entry = {"Up": 0.0, "Down": 0.0}
    last_seen, last_tick, last_log = time.time(), 0.0, 0.0
    sig, skipped = None, {}   # sig: the pending signal (t, row of book values at signal time); skipped: dedupe times

    def context(now, m, side, bps, t_left):
        b = side_book_of(m, side)
        return {"t": now, "row": dict(slug=m.slug, side=side, bps=bps, sig_ask=b.ask, sig_bid=b.bid,
                                      sig_size=b.ask_size, t_left=t_left, bn_lag=bn.lag_ms())}

    def side_book_of(m, side):
        return books[m.up_token if side == "Up" else m.down_token]

    def note(ctx, outcome, reason="", dedupe=False, **kw):
        """Write a signals row. dedupe: at most one per reason and side a second (a move lasts many loop passes)."""
        if dedupe:
            key = (reason, ctx["row"]["side"])
            if time.time() - skipped.get(key, 0) < 1:
                return
            skipped[key] = time.time()
        led.signal(ctx["t"], **{**ctx["row"], **kw}, outcome=outcome, reason=reason)

    threading.Thread(target=settler, daemon=True).start()
    log.info("taker start | %s | %g bp in %gs -> buy %g @ ask after %.2fs, %s | ask %.2f-%.2f "
             "| no entry < %gs left | cooldown %gs | ledger %s",
             ("LIVE-DRY" if lx.dry else "LIVE") if lx else "PAPER", MOVE_BPS, MOVE_SEC, SIZE, DELAY_SEC,
             "hold to settlement" if SETTLE else f"sell at bid after {HOLD_SEC:g}s", MIN_ASK, MAX_ASK, MIN_LEFT,
             COOLDOWN, LEDGER.name)
    while True:
        try:
            now = time.time()
            if lx:
                lx.check()                         # geoblock / closed-only, every 10 min; exits if blocked
            if pos and now >= pos[1].end:          # unsold at round end (no bid): the settler books it; stop holding
                log.info("[BTC 5m] %s position unsold at round end, left to settlement", pos[2])
                pos = None                         # 22 Oct 18:19: never cleared, froze T-0.3 for 1.5 h
            markets = feed.current(now)
            books = feed.books(markets)
            raw = ws.trades_since(last_seen)
            last_seen = now
            led.prints([(t, a, p, s, side) for (t, a, p, s, side) in raw])
            for m in markets:
                if lx:
                    lx.prepare((m.up_token, m.down_token))   # new round: fetch order metadata now, not at the order
                up, dn = books[m.up_token], books[m.down_token]
                t_left = m.seconds_left(now)
                side_book = {"Up": up, "Down": dn}
                if now - last_tick >= 0.1:
                    last_tick = now
                    led.tick(now, m, Snapshot(up, dn, t_left, m.fee, 0.0, None), None, None, None, None)
                    led.binance(now, bn.at(now))
                if pos and pos[1].slug == m.slug and now >= pos[3]:
                    pid, _, side, _, left, got = pos
                    bid = side_book[side].bid
                    if bid is not None:
                        sold, px = lx.sell(token(m, side), bid, left) if lx else (left, bid)
                        left, got = left - sold, got + sold * px
                        if left >= 0.01:           # live: not (fully) sold yet, retry in 1 s until the round ends
                            pos = (pid, m, side, now + 1, left, got)
                        else:
                            shares = led._one("SELECT shares FROM trades WHERE id=?", pid)
                            avg = got / shares
                            led.close(pid, avg, taker_fee(avg, 1, m.fee))
                            pnl = led._one("SELECT pnl FROM trades WHERE id=?", pid)
                            log.info("[BTC 5m] SELL %s @ %.2f x%g | P/L %+.2f | total %+.2f", side, avg, shares, pnl,
                                     led._one("SELECT coalesce(sum(pnl),0) FROM trades WHERE status!='open'"))
                            pos = None
                if pending and pending[1].slug == m.slug and now >= pending[0]:
                    _, _, side = pending
                    pending = None
                    if pos is None and (ask := entry_price(side_book[side], t_left)) is not None:
                        why = lx.blocked_reason(led) if lx else None
                        if why:
                            note(sig, "skip", f"blocked: {why}", dedupe=True, t_left=t_left)
                            if now - blocked_log > 60:
                                blocked_log = now
                                log.warning("[BTC 5m] entry skipped: %s", why)
                            continue
                        t_order = time.time()
                        shares, px = lx.buy(token(m, side), ask, SIZE) if lx else (SIZE, ask)
                        last_entry[side] = now
                        timing = dict(ms_to_order=((lx.t_post or t_order) - sig["t"]) * 1000 if lx else DELAY_SEC * 1000,
                                      ms_order=lx.ms if lx else None, ask_now=ask, t_left=t_left)
                        if shares < 0.01:
                            note(sig, "nofill", (lx.err[:80] if lx and lx.err else "no_fill"), **timing)
                            log.info("[BTC 5m] BUY %s @ %.2f got no fill", side, px)
                            continue
                        note(sig, "bought", fill_px=px, **timing)
                        fee = taker_fee(px, shares, m.fee)
                        pid = led.record(m, "taker", Leg(side, px, shares, fee), "paper" if lx and lx.dry else None)   # dry fills must not use the loss cap
                        if not SETTLE:
                            pos = (pid, m, side, now + HOLD_SEC, shares, 0.0)
                        log.info("[BTC 5m] BUY %s @ %.2f x%g (fee %.3f) | %ds left", side, px, shares, fee, t_left)
                    else:
                        note(sig, "skip", "pos_busy" if pos else entry_block(side_book[side], t_left), t_left=t_left)
                bps = bn.move_bps(MOVE_SEC, now) if t_left >= MIN_LEFT else None
                side = signal(bps)
                if side:
                    ctx = context(now, m, side, bps, t_left)
                    if pending is not None or pos is not None:
                        note(ctx, "skip", "busy", dedupe=True)
                    elif now - (last_entry[side] if SETTLE else max(last_entry.values())) < COOLDOWN:
                        note(ctx, "skip", "cooldown", dedupe=True)
                    elif DELAY_SEC == 0 and entry_price(side_book[side], t_left) is None:   # live: the order would be refused anyway
                        note(ctx, "skip", entry_block(side_book[side], t_left), dedupe=True)
                    else:
                        pending, sig = (now + DELAY_SEC, m, side), ctx
                        log.info("[BTC 5m] SIGNAL Binance %+.1f bp in %gs -> %s (UP %s/%s)", bps, MOVE_SEC, side,
                                 up.bid, up.ask)
            if now - last_log >= 300:
                last_log = now
                n, pnl = led.db.execute("SELECT count(*), coalesce(sum(pnl),0) FROM trades WHERE status!='open'").fetchone()
                log.info("taker status | %d trades closed | P/L %+.2f | binance lag %s ms, %d late updates dropped", n, pnl,
                         "?" if bn.lag_ms() is None else f"{bn.lag_ms():.0f}", bn.stale)
            if (loop_ms := (time.time() - now) * 1000) > SLOW_LOOP_MS:   # a blocking call in the loop = missed signals
                led.signal(now, outcome="slowloop", reason="loop", loop_ms=loop_ms)
                log.warning("slow loop pass: %.0f ms", loop_ms)
        except (requests.RequestException, ValueError, KeyError, sqlite3.Error) as e:   # a locked db killed W-0.5 22 Oct 06:00
            log.warning("api error: %r", e)
        time.sleep(LOOP_SEC)


if __name__ == "__main__":
    live()
