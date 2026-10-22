"""Real orders for taker.py (TK_LIVE=1), through Polymarket's official py-clob-client-v2.

Secrets come only from the environment (systemd EnvironmentFile, chmod 600), never from code or git:
    PM_PRIVATE_KEY      key of the account's signer (export from Polymarket: Settings -> Export private key)
    PM_FUNDER           the Polymarket wallet address that holds the USDC (Profile -> deposit address)
    PM_SIGNATURE_TYPE   3 deposit wallet (accounts since May 2026), 1 email/Google, 2 MetaMask/Safe, 0 plain EOA
TK_LIVE_DRY=1 signs every order but never posts it (fills simulated at the limit price): use it to check keys/setup.

Orders are FAK (fill-and-kill): fill what the book has now at the limit or better, cancel the rest, never rest.
Fill sizes come from GET /order (size_matched); cost is booked at the limit price, a conservative upper bound.
"""
import json
import logging
import os
import time

import requests
from py_clob_client_v2 import AssetType, BalanceAllowanceParams, ClobClient, OrderArgs, OrderType

log = logging.getLogger("live")
logging.getLogger("httpx").setLevel(logging.WARNING)   # the CLOB client logs every request at INFO
HOST, CHAIN = "https://clob.polymarket.com", 137
_f = lambda k, d: float(os.getenv(k, d))
MAX_TRADE_USD = _f("TK_MAX_TRADE_USD", 10)   # never spend more than this on one buy
MAX_DAY_LOSS = _f("TK_MAX_DAY_LOSS", 15)     # stop opening trades after this realised loss today (UTC)
SLIP = _f("TK_SLIP", 0.01)                   # limit = ask + 1 tick on buys, bid - 1 tick on sells
STOP_FILE = "runs/STOP"                      # touch it to stop new entries at once (open positions still exit)
STATUS_FILE = "runs/live.json"               # balance for the dashboard, refreshed every minute


def geoblocked():
    g = requests.get("https://polymarket.com/api/geoblock", timeout=10).json()
    return bool(g.get("blocked")), g


class Live:
    def __init__(self):
        self.dry = os.getenv("TK_LIVE_DRY", "0") != "0"
        self.c = ClobClient(HOST, CHAIN, key=os.environ["PM_PRIVATE_KEY"], funder=os.environ["PM_FUNDER"],
                            signature_type=int(os.environ["PM_SIGNATURE_TYPE"]))
        self.checked = self.published = 0.0
        self.bn = None                                # taker.py sets its BinanceFeed here
        if not self.dry:
            self.c.set_api_creds(self.c.create_or_derive_api_key())
        self.check(force=True)

    def check(self, force=False):
        """Refuse to trade when geoblocked or closed-only; re-checked every 10 minutes. Balance file every minute."""
        if force or time.time() - self.published >= 60:
            self.publish()
        if not force and time.time() - self.checked < 600:
            return
        blocked, g = geoblocked()
        if blocked:
            raise SystemExit(f"geoblocked here {g} - not trading")
        if not self.dry:
            if (self.c.get_closed_only_mode() or {}).get("closed_only"):
                raise SystemExit("account is in closed-only mode - not trading")
            log.info("live check ok | %s %s | USDC %.2f", g.get("country"), g.get("region"), self.usdc())
        self.checked = time.time()

    def publish(self):
        self.published = time.time()
        try:
            out = {"t": self.published, "mode": "LIVE-DRY" if self.dry else "LIVE",
                   "usdc": None if self.dry else round(self.usdc(), 2)}
            if self.bn:                                   # the bot's own Binance delay, for the dashboard
                with self.bn.lock:
                    lags = sorted(l for t, l in self.bn.lags if t > self.published - 60)
                out["binance_lag"] = [lags[len(lags) // 2], lags[int(len(lags) * .95)]] if lags else None
            with open(STATUS_FILE + ".tmp", "w") as f:
                json.dump(out, f)
            os.replace(STATUS_FILE + ".tmp", STATUS_FILE)
        except Exception as e:  # noqa: BLE001 - display only, never stop trading for it
            log.warning("balance publish failed: %r", e)

    def usdc(self):
        if self.dry:
            return 1e9
        r = self.c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        return int(r["balance"]) / 1e6

    def held(self, token):
        """Outcome shares of token in the wallet, or None if the CLOB can't say."""
        if self.dry:
            return None
        try:
            r = self.c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token))
            return int(r["balance"]) / 1e6
        except Exception as e:  # noqa: BLE001 - any client/HTTP error: fall back to our own count
            log.warning("share balance read failed: %r", e)
            return None

    def blocked_reason(self, led):
        if os.path.exists(STOP_FILE):
            return "STOP file present"
        day = time.strftime("%Y-%m-%d", time.gmtime())
        lost = led._one("SELECT coalesce(sum(pnl), 0) FROM trades WHERE mode='live' AND status!='open' AND ts >= ?", day)
        if lost <= -MAX_DAY_LOSS:
            return f"daily loss limit hit ({lost:+.2f} <= -{MAX_DAY_LOSS:g})"
        return None

    def _fak(self, token, side, price, size):
        """One FAK order; returns shares matched (0.0 on no fill or any error)."""
        price = round(min(max(price, 0.01), 0.99), 2)
        size = int(size * 100) / 100                       # 2 size decimals, rounded down
        if size < 0.01:
            return 0.0, price
        try:
            if self.dry:
                order = self.c.create_order(OrderArgs(token_id=token, price=price, size=size, side=side))
                log.info("DRY %s %s x%.2f @ %.2f signed (not posted) %s", side, token[:8], size, price,
                         type(order).__name__)
                return size, price
            r = self.c.create_and_post_order(OrderArgs(token_id=token, price=price, size=size, side=side),
                                             order_type=OrderType.FAK)
            log.info("order %s x%.2f @ %.2f -> %s", side, size, price,
                     {k: r.get(k) for k in ("success", "status", "orderID", "makingAmount", "takingAmount", "errorMsg")})
            if not r.get("success") or not r.get("orderID"):
                return 0.0, price
            o = self.c.get_order(r["orderID"]) or {}
            return float(o.get("size_matched") or 0), price
        except Exception as e:  # noqa: BLE001 - a rejected/failed order must never kill the bot
            log.warning("order %s x%.2f @ %.2f failed: %r", side, size, price, e)
            return 0.0, price

    def buy(self, token, ask, size):
        limit = ask + SLIP
        if size * limit > MAX_TRADE_USD:
            log.warning("buy %.2f x %.2f = $%.2f over TK_MAX_TRADE_USD %.2f, skipped", size, limit, size * limit, MAX_TRADE_USD)
            return 0.0, limit
        if size < 5:                                       # Polymarket min order size (orderMinSize) is 5 shares
            return 0.0, limit
        if self.usdc() < size * limit * 1.05:
            log.warning("not enough USDC for %.2f x %.2f", size, limit)
            return 0.0, limit
        return self._fak(token, "BUY", limit, size)

    def sell(self, token, bid, size):
        have = self.held(token)
        if have is not None:
            size = min(size, have)
        return self._fak(token, "SELL", bid - SLIP, size)
