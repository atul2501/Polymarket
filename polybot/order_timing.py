"""Where do the ~400 ms of an order go? Times each part separately; places NO real order that can fill.

  signing        create_order(): local signature (CPU)
  plain GET      GET /time on a kept-open connection (network + Cloudflare only)
  auth GET       an authenticated read (network + API-key check)
  post (no fill) a FAK BUY of 100 shares at 0.01 ($1 max) on a token whose ask is far above that, so it cannot
                 match: this is the real POST /order path including the matching engine. Opt-in with --post.

  --maker        a resting post-only order (100 shares at 0.01, never fills) and its cancel, each timed: do resting
                 orders skip the delay a taker order pays?

  --feeds        also run the bot's own Binance and order-book feeds in this process while timing: does the order slow
                 down when ~1000 messages/s compete for Python's lock (live orders took 350-800 ms, this test 250)?
  --ping KIND    keep the connection warm while timing: KIND = time (GET /time through the client) or auth (an
                 authenticated read, which reaches Polymarket's servers) every 8 s. Compare the "idle" lines with and
                 without it: an order after 45 s of quiet took 374-911 ms against ~215 ms warm (23 Oct).
  --switch S     with --feeds: sys.setswitchinterval(S), e.g. 0.0005, to let the threads trade turns more often

~/.venv/bin/python order_timing.py [--post] [--maker] [--feeds [--switch 0.0005]]   keys from the environment
"""
import os
import statistics
import sys
import time

import requests
from py_clob_client_v2 import AssetType, BalanceAllowanceParams, OrderArgs, OrderPayload, OrderType

import live
from market import MarketFeed

N = 12


def ms(f):
    t = time.perf_counter()
    try:
        f()
    except Exception as e:  # noqa: BLE001 - a rejected post is still a measurement
        return (time.perf_counter() - t) * 1000, repr(e)[:90]
    return (time.perf_counter() - t) * 1000, ""


def show(name, f, n=N):
    xs, errs = [], set()
    for _ in range(n):
        x, e = ms(f)
        xs.append(x)
        errs.add(e)
        time.sleep(0.3)
    xs.sort()
    print(f"{name:22} median {statistics.median(xs):6.0f} ms | min {xs[0]:5.0f} | p90 {xs[int(len(xs) * .9)]:5.0f} "
          f"| max {xs[-1]:5.0f}" + (f" | {sorted(errs - {''})[:1]}" if errs - {''} else ""))


def main():
    c = live.Client(live.HOST, live.CHAIN, key=os.environ["PM_PRIVATE_KEY"], funder=os.environ["PM_FUNDER"],
                    signature_type=int(os.environ["PM_SIGNATURE_TYPE"]))
    c.set_api_creds(c.create_or_derive_api_key())
    http = requests.Session()
    feed = MarketFeed(http)
    for _ in range(80):                       # wait for a round with 60+ s left and a token whose ask is 0.10-0.90
        now = time.time()
        pick = None
        for m in feed.current(now):
            books = feed.books([m])
            for tok in (m.up_token, m.down_token):
                if m.seconds_left(now) > 60 and books[tok].ask and 0.10 <= books[tok].ask <= 0.90:
                    pick = (m, tok, books[tok])
        if pick:
            break
        time.sleep(5)
    else:
        sys.exit("no suitable market in 6 minutes")
    m, tok, b = pick
    if "--feeds" in sys.argv:
        from feeds import BinanceFeed, BookFeed
        if "--switch" in sys.argv:
            sys.setswitchinterval(float(sys.argv[sys.argv.index("--switch") + 1]))
        ws = BookFeed().start()
        BinanceFeed().start()
        ws.watch([m.up_token, m.down_token])
        time.sleep(8)                                     # let both feeds run at full rate
        print(f"feeds on: {ws.msgs} book messages in 8 s | switch interval {sys.getswitchinterval()}")
    print(f"market {m.slug} | {m.seconds_left(time.time()):.0f} s left | token ask {b.ask} bid {b.bid}\n")
    c.get_tick_size(tok), c.get_neg_risk(tok), c._ClobClient__resolve_version()   # warm, as live.prepare() does
    args = OrderArgs(token_id=tok, price=0.01, size=100, side="BUY")
    if "--ping" in sys.argv:
        import threading
        kind = sys.argv[sys.argv.index("--ping") + 1]
        ping = (c.get_server_time if kind == "time" else
                lambda: c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)))

        def keep():
            while True:
                time.sleep(8)
                try:
                    ping()
                except Exception:  # noqa: BLE001 - a missed ping only means a colder next order
                    pass
        threading.Thread(target=keep, daemon=True).start()
        print(f"keep-alive: {kind} every 8 s")
    show("signing (local)", lambda: c.create_order(args))
    show("plain GET /time", lambda: http.get("https://clob.polymarket.com/time", timeout=10))
    show("auth GET (balance)", lambda: c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)))
    if "--post" in sys.argv:
        post = lambda: c.create_and_post_order(args, order_type=OrderType.FAK)
        show("POST /order (no fill)", post, n=8)
        for idle in (45, 45, 90):                 # the first order after a quiet spell: does a cold connection cost time?
            time.sleep(idle)
            x, _ = ms(post)
            print(f"POST after {idle:2d} s idle   {x:6.0f} ms")
    if "--maker" in sys.argv:
        # A resting post-only BUY of 100 shares at 0.01 ($1) far below the bid: it rests, cannot take, and is cancelled
        # at once. If a taker order carries a matching delay and a resting one doesn't, the gap shows here.
        rest = OrderArgs(token_id=tok, price=0.01, size=100, side="BUY")
        posts, cancels = [], []
        for _ in range(6):
            oid = None
            try:
                t = time.perf_counter()
                r = c.create_and_post_order(rest, order_type=OrderType.GTC, post_only=True)
                posts.append((time.perf_counter() - t) * 1000)
                oid = (r or {}).get("orderID")
            except Exception as e:  # noqa: BLE001
                print("maker post failed:", repr(e)[:120])
            finally:
                if oid:
                    t = time.perf_counter()
                    c.cancel_order(OrderPayload(orderID=oid))
                    cancels.append((time.perf_counter() - t) * 1000)
            time.sleep(0.5)
        for name, xs in (("POST resting (post-only)", posts), ("CANCEL resting", cancels)):
            if xs:
                xs.sort()
                print(f"{name:24} median {statistics.median(xs):6.0f} ms | min {xs[0]:5.0f} | max {xs[-1]:5.0f}")


if __name__ == "__main__":
    main()
