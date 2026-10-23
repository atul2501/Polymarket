"""Where do the ~400 ms of an order go? Times each part separately; places NO real order that can fill.

  signing        create_order(): local signature (CPU)
  plain GET      GET /time on a kept-open connection (network + Cloudflare only)
  auth GET       an authenticated read (network + API-key check)
  post (no fill) a FAK BUY of 100 shares at 0.01 ($1 max) on a token whose ask is far above that, so it cannot
                 match: this is the real POST /order path including the matching engine. Opt-in with --post.

~/.venv/bin/python order_timing.py [--post]      keys from the environment, as for live.py (source ~/.polybot-secrets)
"""
import os
import statistics
import sys
import time

import requests
from py_clob_client_v2 import AssetType, BalanceAllowanceParams, OrderArgs, OrderType

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
    now = time.time()
    m = feed.current(now)[0]
    books = feed.books([m])
    tok = m.up_token if books[m.up_token].ask and books[m.up_token].ask > 0.05 else m.down_token
    b = books[tok]
    print(f"market {m.slug} | token ask {b.ask} bid {b.bid}\n")
    c.get_tick_size(tok), c.get_neg_risk(tok), c._ClobClient__resolve_version()   # warm, as live.prepare() does
    args = OrderArgs(token_id=tok, price=0.01, size=100, side="BUY")
    show("signing (local)", lambda: c.create_order(args))
    show("plain GET /time", lambda: http.get("https://clob.polymarket.com/time", timeout=10))
    show("auth GET (balance)", lambda: c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)))
    if "--post" in sys.argv:
        if not b.ask or b.ask <= 0.05:
            sys.exit("ask too low for a safe no-fill test, try again in a minute")
        post = lambda: c.create_and_post_order(args, order_type=OrderType.FAK)
        show("POST /order (no fill)", post, n=8)
        for idle in (10, 45):                 # the first order after a quiet spell: does a cold connection cost time?
            time.sleep(idle)
            x, _ = ms(post)
            print(f"POST after {idle:2d} s idle   {x:6.0f} ms")


if __name__ == "__main__":
    main()
