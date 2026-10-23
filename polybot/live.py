"""Real orders for taker.py (TK_LIVE=1), through Polymarket's official py-clob-client-v2.

Secrets come only from the environment (systemd EnvironmentFile, chmod 600), never from code or git:
    PM_PRIVATE_KEY      key of the account's signer (export from Polymarket: Settings -> Export private key)
    PM_FUNDER           the Polymarket wallet address that holds the USDC (Profile -> deposit address)
    PM_SIGNATURE_TYPE   3 deposit wallet (accounts since May 2026), 1 email/Google, 2 MetaMask/Safe, 0 plain EOA
TK_LIVE_DRY=1 signs every order but never posts it (fills simulated at the limit price): use it to check keys/setup.

Orders are FAK (fill-and-kill): fill what the book has now at the limit or better, cancel the rest, never rest.
Fills (shares and USDC) come from the order response; GET /order only for orders the CLOB delays.
Nothing slow sits between a signal and its order: market metadata is fetched when a round starts (prepare), the
USDC balance is cached (refreshed each minute), and the client doesn't wait for on-chain settlement of fills.
"""
import json
import logging
import math
import os
import threading
import time

import requests
from py_clob_client_v2 import AssetType, BalanceAllowanceParams, ClobClient, OrderArgs, OrderType

log = logging.getLogger("live")
logging.getLogger("httpx").setLevel(logging.WARNING)   # the CLOB client logs every request at INFO
HOST, CHAIN = "https://clob.polymarket.com", 137
_f = lambda k, d: float(os.getenv(k, d))
MAX_TRADE_USD = _f("TK_MAX_TRADE_USD", 10)   # never spend more than this on one buy
MAX_DAY_LOSS = _f("TK_MAX_DAY_LOSS", 15)     # stop opening trades after this realised loss today (UTC)
KEEPALIVE = _f("TK_KEEPALIVE", 0)            # seconds between warm-up requests, 0 = off (an order after 45 s of quiet took 374-911 ms vs ~215)
KEEPALIVE_KIND = os.getenv("TK_KEEPALIVE_KIND", "auth")   # "auth" reaches Polymarket's servers, "time" only the edge
SLIP = _f("TK_SLIP", 0.01)                   # limit = ask + 1 tick on buys, bid - 1 tick on sells
LOSS_SINCE = os.getenv("TK_LOSS_SINCE", "")   # ISO time (UTC): the loss cap counts only trades from then on (a reset)
STOP_FILE = "runs/STOP"                      # touch it to stop new entries at once (open positions still exit)
STATUS_FILE = "runs/live.json"               # balance for the dashboard, refreshed every minute


class Client(ClobClient):
    def _resolve_transactions_hashes(self, response):
        # the stock client polls up to 30 s for on-chain settlement hashes after every fill, freezing the bot;
        # the fill itself is final once matched, so return at once
        return response


def geoblocked():
    g = requests.get("https://polymarket.com/api/geoblock", timeout=10).json()
    return bool(g.get("blocked")), g


def amount(x, cap):
    """Shares or USDC from an API amount string; guards against 6-decimal fixed-point units (5000000 = 5)."""
    v = float(x or 0)
    return v / 1e6 if v > cap * 100 else v


class Live:
    def __init__(self):
        self.dry = os.getenv("TK_LIVE_DRY", "0") != "0"
        self.c = Client(HOST, CHAIN, key=os.environ["PM_PRIVATE_KEY"], funder=os.environ["PM_FUNDER"],
                        signature_type=int(os.environ["PM_SIGNATURE_TYPE"]))
        self.checked = self.published = 0.0
        self.balance = None                           # cached USDC, refreshed every minute, minus our own buys
        self.bn = None                                # taker.py sets its BinanceFeed here
        self.ready, self.err = set(), ""              # tokens with metadata cached; last order error text
        self.t_post, self.ms = 0.0, 0.0               # last order: when it was sent (epoch) and how long the call took
        if not self.dry:
            self.c.set_api_creds(self.c.create_or_derive_api_key())
        self.check(force=True)
        if KEEPALIVE > 0 and not self.dry:
            threading.Thread(target=self._keepalive, daemon=True).start()

    def _keepalive(self):
        """Keep the HTTP/2 connection to the CLOB warm: the bot trades a few times an hour, so without this nearly
        every order pays for a cold connection (23 Oct: 374-911 ms after 45 s idle, 214 ms warm)."""
        log.info("keep-alive on: %s request every %gs", KEEPALIVE_KIND, KEEPALIVE)
        while True:
            time.sleep(KEEPALIVE)
            try:
                if KEEPALIVE_KIND == "time":
                    self.c.get_server_time()
                else:
                    self.c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            except Exception as e:  # noqa: BLE001 - a missed ping only makes the next order colder
                log.debug("keep-alive failed: %r", e)

    def check(self, force=False):
        """Refuse to trade when geoblocked or closed-only; re-checked every 10 minutes. Balance file every minute."""
        if force or time.time() - self.published >= 60:
            self.publish()
        if not force and time.time() - self.checked < 600:
            return
        try:
            blocked, g = geoblocked()
            closed = not self.dry and (self.c.get_closed_only_mode() or {}).get("closed_only")
        except Exception as e:  # noqa: BLE001 - a failed check is retried next minute, it must not kill the bot
            log.warning("live check failed, retrying: %r", e)
            self.checked = time.time() - 540
            return
        if blocked:
            raise SystemExit(f"geoblocked here {g} - not trading")
        if closed:
            raise SystemExit("account is in closed-only mode - not trading")
        log.info("live check ok | %s %s | USDC %s", g.get("country"), g.get("region"),
                 "dry" if self.dry else f"{self.balance:.2f}" if self.balance is not None else "?")
        self.checked = time.time()

    def publish(self):
        self.published = time.time()
        try:
            if not self.dry:
                r = self.c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
                self.balance = int(r["balance"]) / 1e6
            out = {"t": self.published, "mode": "LIVE-DRY" if self.dry else "LIVE",
                   "usdc": None if self.dry or self.balance is None else round(self.balance, 2)}
            if self.bn:                                   # the bot's own Binance delay, for the dashboard
                with self.bn.lock:
                    lags = sorted(l for t, l in self.bn.lags if t > self.published - 60)
                out["binance_lag"] = [lags[len(lags) // 2], lags[int(len(lags) * .95)]] if lags else None
            with open(STATUS_FILE + ".tmp", "w") as f:
                json.dump(out, f)
            os.replace(STATUS_FILE + ".tmp", STATUS_FILE)
        except Exception as e:  # noqa: BLE001 - display/cache only, never stop trading for it
            log.warning("balance refresh failed: %r", e)

    def prepare(self, tokens):
        """Cache tick size / neg-risk / version for a new round's tokens in the background, so orders don't wait."""
        new = [t for t in tokens if t not in self.ready]
        if not new:
            return
        self.ready.update(new)

        def warm():
            for t in new:
                try:
                    self.c.get_tick_size(t)
                    self.c.get_neg_risk(t)
                    self.c._ClobClient__resolve_version()   # caches it (plain get_version() does not)
                except Exception as e:  # noqa: BLE001 - the order path fetches it itself if this failed
                    log.warning("prepare %s failed: %r", t[:8], e)
                    self.ready.discard(t)
        threading.Thread(target=warm, daemon=True).start()

    def held(self, token):
        """Outcome shares of token in the wallet (refreshed from chain first), or None if the CLOB can't say."""
        try:
            p = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token)
            self.c.update_balance_allowance(p)
            return int(self.c.get_balance_allowance(p)["balance"]) / 1e6
        except Exception as e:  # noqa: BLE001 - any client/HTTP error: fall back to our own count
            log.warning("share balance read failed: %r", e)
            return None

    def blocked_reason(self, led):
        if os.path.exists(STOP_FILE):
            return "STOP file present"
        day = time.strftime("%Y-%m-%d", time.gmtime())
        since = max(day, LOSS_SINCE)                  # ISO strings compare in time order
        lost = led._one("SELECT coalesce(sum(pnl), 0) FROM trades WHERE mode='live' AND status!='open' AND ts >= ?", since)
        if lost <= -MAX_DAY_LOSS:
            return f"loss limit hit since {since} ({lost:+.2f} <= -{MAX_DAY_LOSS:g})"
        return None

    def _fak(self, token, side, price, size):
        """One FAK order; returns (shares filled, average price). (0, limit) on no fill or any error (text in self.err).

        Fills come from the response: BUY makingAmount = USDC paid, takingAmount = shares got (a BUY spends
        size x limit, so a better price buys MORE shares - 22 Oct: 5.137 sh for $3.75 at limit 0.75);
        SELL makingAmount = shares sold, takingAmount = USDC got.
        """
        price = round(min(max(price, 0.01), 0.99), 2)
        size = int(size * 100) / 100                       # 2 size decimals, rounded down
        self.err = ""
        if size < 0.01:
            return 0.0, price
        t0 = time.perf_counter()
        self.t_post = time.time()
        try:
            if self.dry:
                order = self.c.create_order(OrderArgs(token_id=token, price=price, size=size, side=side))
                log.info("DRY %s %s x%.2f @ %.2f signed (not posted) %s", side, token[:8], size, price,
                         type(order).__name__)
                return size, price
            r = self.c.create_and_post_order(OrderArgs(token_id=token, price=price, size=size, side=side),
                                             order_type=OrderType.FAK)
        except Exception as e:  # noqa: BLE001 - a rejected/failed order must never kill the bot
            self.err = repr(e)
            ms = self.ms = (time.perf_counter() - t0) * 1000
            if "no orders found to match" in self.err:
                log.info("order %s x%.2f @ %.2f: no fill (book moved) | %.0f ms", side, size, price, ms)
            else:
                log.warning("order %s x%.2f @ %.2f failed | %.0f ms: %s", side, size, price, ms, self.err)
            return 0.0, price
        ms = self.ms = (time.perf_counter() - t0) * 1000
        log.info("order %s x%.2f @ %.2f -> %s | %.0f ms", side, size, price,
                 {k: r.get(k) for k in ("success", "status", "orderID", "makingAmount", "takingAmount", "errorMsg")}, ms)
        if not r.get("success") or not r.get("orderID"):
            self.err = str(r.get("errorMsg"))
            return 0.0, price
        usd_key, sh_key = ("makingAmount", "takingAmount") if side == "BUY" else ("takingAmount", "makingAmount")
        shares = amount(r.get(sh_key), size * 2 / price)
        usd = amount(r.get(usd_key), size * 2)
        if not shares and str(r.get("status", "")).lower() in ("delayed", "live"):   # matches a moment later
            for _ in range(12):
                time.sleep(0.25)
                try:
                    o = self.c.get_order(r["orderID"]) or {}
                except Exception as e:  # noqa: BLE001
                    log.warning("order lookup failed: %r", e)
                    break
                shares = amount(o.get("size_matched"), size * 2 / price)
                if str(o.get("status", "")).upper() not in ("LIVE", "DELAYED"):
                    break
            usd = shares * price                           # limit price: the lookup has no paid amount
        if not shares:
            return 0.0, price
        return shares, (usd / shares if usd else price)

    def buy(self, token, ask, size):
        limit = round(ask + SLIP, 2)
        size = max(size, math.ceil(100 / limit) / 100)     # marketable BUYs must be >= $1 (22 Oct: $0.65 rejected)
        self.err, self.ms, self.t_post = "", 0.0, 0.0      # why a buy returns 0 without an order shows in the signals table
        if size * limit > MAX_TRADE_USD:
            log.warning("buy %.2f x %.2f = $%.2f over TK_MAX_TRADE_USD %.2f, skipped", size, limit, size * limit, MAX_TRADE_USD)
            self.err = "over_max_trade_usd"
            return 0.0, limit
        if size < 5:                                       # Polymarket min order size (orderMinSize) is 5 shares
            self.err = "under_min_size"
            return 0.0, limit
        if not self.dry and (self.balance is None or self.balance < size * limit * 1.05):
            log.warning("not enough USDC (cached %s) for %.2f x %.2f", self.balance, size, limit)
            self.err = "low_balance"
            return 0.0, limit
        got, px = self._fak(token, "BUY", limit, size)
        if got and self.balance is not None:
            self.balance -= got * px                       # until the next refresh
        return got, px

    def sell(self, token, bid, size):
        got, px = self._fak(token, "SELL", bid - SLIP, size)
        if not got and "balance" in self.err.lower():      # fewer shares than we think (fee in shares / not settled)
            have = self.held(token)
            if have:
                got, px = self._fak(token, "SELL", bid - SLIP, min(size, have))
        return got, px
