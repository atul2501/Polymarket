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
import math
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
# TK_SKEW (settle): max one-sided shares per round. The copied wallet's pairs earn (+$540 in 23 rounds 25 Oct) and its
# one-sided extras lose (-$228); racesim: capping the imbalance at 20 kept the pairs and cut the extras.
SKEW = _f("TK_SKEW", 0)
# TK_HOLD_IF_UP: at the sell time, a position whose bid is at or above its entry price is held to settlement instead of
# sold (the settler books it); a losing one is sold as before. Live-D's 12 s scalp lost most on BTC reversals inside the
# hold (25 Oct: -2.38, -1.64, -1.78) while the copied wallet earns by holding to settlement. LIVE: settled winning
# shares must be redeemed on Polymarket (Portfolio -> Claim); the bot does not redeem.
HOLD_IF_UP = os.getenv("TK_HOLD_IF_UP", "0") != "0"
# corrected after the live test (25 Oct 09:43-10:23: held a 0.55 bid and lost $1.93): hold only when the bid is at least
# TK_HOLD_MIN (a near-certain winner), and never more than TK_MAX_HOLDS open held positions in one round
HOLD_MIN, MAX_HOLDS = _f("TK_HOLD_MIN", 0), int(_f("TK_MAX_HOLDS", 99))
# TK_MAX_HELD_USD: total money in held positions (all rounds) never above this; e.g. 5 with $5 trades = one hold at a time,
# so a hold can never cost more than ~$5 (user, 25 Oct: "hold to settlement only with a small amount")
MAX_HELD_USD = _f("TK_MAX_HELD_USD", 0)
LEDGER = Path(os.getenv("TK_LEDGER", Path(__file__).parent / "runs" / "taker.db"))
LIVE = os.getenv("TK_LIVE", "0") != "0"
if LIVE:
    DELAY_SEC = 0.0                          # real orders pay the real network delay, no simulated one
# books/Binance/prints for replays, committed every pass; off by default when live (a paper twin records them)
RECORD = os.getenv("TK_RECORD", "0" if LIVE else "1") != "0"
# TK_REAL=1: paper fills follow the execution measured live on 23 Oct 2026 (Stockholm), see realistic.env:
# decide CODE_DELAY after the signal, the order matches MATCH_DELAY later against the book of THAT moment (a taker
# delay of ~150 ms + the trip), is killed when the ask ran past the pad, buys >= $1, and the sale decided HOLD after
# the bot learns the fill (RESP_DELAY) matches MATCH_DELAY later too. Without it the paper bot fills at the ask seen
# DELAY_SEC after the signal, which over-stated live results by ~7 c/share.
REAL = os.getenv("TK_REAL", "0") != "0" and not LIVE
CODE_DELAY, MATCH_DELAY, RESP_DELAY = _f("TK_CODE_DELAY", 0.07), _f("TK_MATCH_DELAY", 0.20), _f("TK_RESP_DELAY", 0.15)
PAD = _f("TK_SLIP", 0.02)                    # limit = ask + PAD on buys, bid - PAD on sells (same name as live.py)
# 25 Oct 07:27: paper bought at 0.06 (match assumed at +340 ms), live's order took 571 ms and found the cheap shares gone
# (129 -> 15 shares in 250 ms: other bots took them). So paper follows live's real order times, and needs room in the
# queue: QUEUE x our shares must sit at the price for a fill (others ahead of us take the rest).
MATCH_FROM = os.getenv("TK_MATCH_FROM", "")  # a live ledger: match delay = 75th pct of its last 30 order calls - 25 ms
QUEUE = _f("TK_QUEUE", 1)
# cheap asks are raced hardest: 25 Oct paper filled DOWN at 0.06 and 0.09 (+$7.41) where live's orders found them gone;
# below TK_CHEAP the queue margin is TK_QUEUE_CHEAP x our shares
CHEAP, QUEUE_CHEAP = _f("TK_CHEAP", 0.15), _f("TK_QUEUE_CHEAP", 0)
_match = {"t": 0.0, "v": MATCH_DELAY}


def match_delay():
    """Seconds from sending an order to its match: from the live ledger's recent order calls when TK_MATCH_FROM is set."""
    if MATCH_FROM and time.time() - _match["t"] > 60:
        _match["t"] = time.time()
        try:
            db = sqlite3.connect(f"file:{MATCH_FROM}?mode=ro", uri=True, timeout=1)
            ms = sorted(r[0] for r in db.execute("SELECT ms_order FROM signals WHERE ms_order > 0 AND outcome IN "
                                                 "('bought', 'nofill') ORDER BY t DESC LIMIT 30"))
            db.close()
            if len(ms) >= 3:
                _match["v"] = max(0.1, ms[int(len(ms) * 0.75)] / 1000 - 0.025)
        except sqlite3.Error as e:                 # no live ledger yet: keep the last value
            log.debug("match delay from %s: %r", MATCH_FROM, e)
    return _match["v"]
if REAL:
    DELAY_SEC = CODE_DELAY


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


def plan_shares(limit):
    """Shares a live buy sends: SIZE, or more when SIZE x limit is under Polymarket's $1 minimum for a marketable buy."""
    return max(SIZE, math.ceil(100 / limit) / 100)


def real_buy(limit, book, planned):
    """Paper FAK BUY at matching time: (shares, price) filled from the best ask, or (0, limit) when killed."""
    if book is None or book.ask is None or book.ask > limit + 1e-9:
        return 0.0, limit
    q = QUEUE_CHEAP if QUEUE_CHEAP and book.ask < CHEAP else QUEUE
    got = math.floor(min(planned, book.ask_size - (q - 1) * planned) * 100) / 100
    return (got, book.ask) if got >= 1 else (0.0, limit)


def real_sell(limit, book, left):
    """Paper FAK SELL at matching time: (shares, price) filled from the best bid, or (0, limit) when killed."""
    if book is None or book.bid is None or book.bid < limit - 1e-9:
        return 0.0, limit
    q = QUEUE_CHEAP if QUEUE_CHEAP and book.bid < CHEAP else QUEUE   # cheap bids are raced as hard as cheap asks
    got = math.floor(min(left, book.bid_size - (q - 1) * left) * 100) / 100
    return (got, book.bid) if got >= 0.01 else (0.0, limit)


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
    matching, sell_plan = None, {}   # TK_REAL: matching: (match_at, market, side, limit, planned shares); sell_plan: id -> sell limit
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

    def skew_after(slug, side):
        """One-sided shares this round if we buy SIZE more of `side` (open positions from the ledger)."""
        held = dict(led.db.execute("SELECT side, coalesce(sum(shares), 0) FROM trades WHERE slug=? AND status='open' "
                                   "GROUP BY side", (slug,)).fetchall())
        return held.get(side, 0) + SIZE - held.get("Down" if side == "Up" else "Up", 0)

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
            feed.upcoming(now)                     # next round's market + book subscription before it starts
            books = feed.books(markets)
            raw = ws.trades_since(last_seen)
            last_seen = now
            if RECORD:
                led.prints([(t, a, p, s, side) for (t, a, p, s, side) in raw])
            for m in markets:
                if lx:
                    lx.prepare((m.up_token, m.down_token))   # new round: fetch order metadata now, not at the order
                up, dn = books[m.up_token], books[m.down_token]
                t_left = m.seconds_left(now)
                side_book = {"Up": up, "Down": dn}
                if RECORD and now - last_tick >= 0.1:
                    last_tick = now
                    led.tick(now, m, Snapshot(up, dn, t_left, m.fee, 0.0, None), None, None, None, None)
                    led.binance(now, bn.at(now))
                if pos and pos[1].slug == m.slug and now >= pos[3]:
                    pid, _, side, _, left, got = pos
                    bid = side_book[side].bid
                    if (HOLD_IF_UP and not got and pid not in sell_plan and bid is not None
                            and bid >= max(HOLD_MIN, led._one("SELECT price FROM trades WHERE id=?", pid))
                            and led._one("SELECT count(*) FROM trades WHERE slug=? AND status='open' AND id!=?",
                                         m.slug, pid) < MAX_HOLDS
                            and (not MAX_HELD_USD or led._one(
                                "SELECT coalesce(sum(cost), 0) FROM trades WHERE status='open'") <= MAX_HELD_USD + 1e-9)):
                        log.info("[BTC 5m] HOLD %s to settlement: bid %.2f >= entry", side, bid)
                        pos = None                     # stays open in the ledger; the settler books it at round end
                    elif bid is not None and REAL and pid not in sell_plan:
                        sell_plan[pid] = round(max(bid - PAD, 0.01), 2)   # decided on this bid; it matches MATCH_DELAY later
                        pos = (pid, m, side, now + match_delay(), left, got)
                    elif bid is not None:
                        if lx:
                            sold, px = lx.sell(token(m, side), bid, left)
                        elif REAL:
                            sold, px = real_sell(sell_plan.get(pid, 0.01), side_book[side], left)   # book of the matching moment
                        else:
                            sold, px = left, bid
                        left, got = left - sold, got + sold * px
                        if left >= 0.01:           # live: not (fully) sold yet, retry in 1 s until the round ends
                            pos = (pid, m, side, now + 1, left, got)
                            sell_plan.pop(pid, None)   # a retry decides again on a fresh bid
                        else:
                            sell_plan.pop(pid, None)
                            shares = led._one("SELECT shares FROM trades WHERE id=?", pid)
                            avg = got / shares
                            led.close(pid, avg, taker_fee(avg, 1, m.fee))
                            pnl = led._one("SELECT pnl FROM trades WHERE id=?", pid)
                            log.info("[BTC 5m] SELL %s @ %.2f x%g | P/L %+.2f | total %+.2f", side, avg, shares, pnl,
                                     led._one("SELECT coalesce(sum(pnl),0) FROM trades WHERE status!='open'"))
                            pos = None
                bps = bn.move_bps(MOVE_SEC, now) if t_left >= MIN_LEFT else None
                side = signal(bps)
                if side:
                    ctx = context(now, m, side, bps, t_left)
                    if pending is not None or pos is not None or matching is not None:
                        note(ctx, "skip", "busy", dedupe=True)
                    elif now - (last_entry[side] if SETTLE else max(last_entry.values())) < COOLDOWN:
                        note(ctx, "skip", "cooldown", dedupe=True)
                    elif SETTLE and SKEW and skew_after(m.slug, side) > SKEW:
                        note(ctx, "skip", "skew", dedupe=True)
                    elif entry_price(side_book[side], t_left) is None:   # refused at signal time, in paper too: paper used to
                        # retry every pass until the spread closed and bought the already-priced move (25 Oct 05:51: 0.62)
                        note(ctx, "skip", entry_block(side_book[side], t_left), dedupe=True)
                    else:
                        pending, sig = (now + DELAY_SEC, m, side), ctx   # live (DELAY_SEC 0): ordered below in this same pass
                        log.info("[BTC 5m] SIGNAL %s %+.1f bp in %gs -> %s (UP %s/%s)", BinanceFeed.SOURCE, bps, MOVE_SEC, side,
                                 up.bid, up.ask)
                if matching and matching[1].slug == m.slug and now >= matching[0]:   # TK_REAL: the order reaches the book
                    _, _, side, limit, planned = matching
                    matching = None
                    shares, px = real_buy(limit, side_book[side], planned)
                    timing = dict(ms_to_order=CODE_DELAY * 1000, ms_order=match_delay() * 1000, ask_now=side_book[side].ask,
                                  t_left=t_left)
                    if shares < 0.01:
                        note(sig, "nofill", "no_fill", **timing)
                        log.info("[BTC 5m] BUY %s @ %.2f got no fill (ask now %s)", side, limit, side_book[side].ask)
                    else:
                        note(sig, "bought", fill_px=px, **timing)
                        fee = taker_fee(px, shares, m.fee)
                        pid = led.record(m, "taker", Leg(side, px, shares, fee), "paper")
                        if not SETTLE:
                            pos = (pid, m, side, now + RESP_DELAY + HOLD_SEC, shares, 0.0)
                        log.info("[BTC 5m] BUY %s @ %.2f x%g (fee %.3f) | %ds left", side, px, shares, fee, t_left)
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
                        if REAL:                       # paper: the order is in flight, matching later against the book then
                            limit = round(ask + PAD, 2)
                            matching = (now + match_delay(), m, side, limit, plan_shares(limit))
                            last_entry[side] = now
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
                        pid = led.record(m, "taker", Leg(side, px, shares, fee),
                                         "live" if lx and not lx.dry else "paper")   # only real fills count for the loss cap (DRY_RUN=1 default made them "paper")
                        if not SETTLE:
                            pos = (pid, m, side, now + HOLD_SEC, shares, 0.0)
                        log.info("[BTC 5m] BUY %s @ %.2f x%g (fee %.3f) | %ds left", side, px, shares, fee, t_left)
                    else:
                        note(sig, "skip", "pos_busy" if pos else entry_block(side_book[side], t_left), t_left=t_left)
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
        bn.wait(LOOP_SEC)                          # wakes on the next Binance update (was a fixed 50 ms sleep)


if __name__ == "__main__":
    live()
