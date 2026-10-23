"""Websocket feeds, each on a background thread:
- ChainlinkFeed: Chainlink prices (what Polymarket resolves on) from Polymarket's public live-data socket.
- BookFeed: live order books from Polymarket's CLOB market socket.
Both reconnect on any error; callers fall back to REST when data is missing or stale."""
import asyncio
import collections
import json
import logging
import os
import ssl
import threading
import time

import certifi
import websockets

from engine import Book

log = logging.getLogger("feeds")
CTX = ssl.create_default_context(cafile=certifi.where())
RTDS = "wss://ws-live-data.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
# Fastest public BTC feeds (live capture 21 Oct): Binance futures ~315 updates/s, moves with spot (<20 ms) and ~0.2 s
# before Coinbase; Binance moves ~2.6 s before Chainlink and ~0.4 s before Polymarket's book.
BINANCE_WS = ("wss://fstream.binance.com/ws/btcusdt@bookTicker", "wss://stream.binance.com:9443/ws/btcusdt@bookTicker")
STALE_SEC = 10


def _thread(coro_fn):
    threading.Thread(target=lambda: asyncio.run(coro_fn()), daemon=True).start()


class ChainlinkFeed:
    def __init__(self, symbols):
        self.symbols = set(symbols)          # e.g. {"btc/usd"}
        self.points = {s: {} for s in symbols}  # symbol -> {unix second: price}
        self.lock = threading.Lock()
        self.last_msg = 0.0  # when we last received a price for one of our symbols

    def start(self):
        _thread(self._run)
        return self

    def _add(self, sym, ts_ms, value):
        if sym in self.points:
            self.last_msg = time.time()
            with self.lock:
                pts = self.points[sym]
                pts[int(ts_ms) // 1000] = float(value)
                if len(pts) > 1200:
                    for k in sorted(pts)[:-900]:
                        del pts[k]

    async def _run(self):
        while True:
            try:
                async with websockets.connect(RTDS, ssl=CTX, open_timeout=10, ping_interval=5) as ws:
                    # unfiltered = live updates for every symbol; filtered = ~60s backfill snapshot per symbol
                    subs = [{"topic": "crypto_prices_chainlink", "type": "*"}] + [
                        {"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": s})}
                        for s in self.symbols]
                    await ws.send(json.dumps({"action": "subscribe", "subscriptions": subs}))
                    self.last_msg = time.time()
                    while True:
                        # watchdog: the socket can stay open but stop sending (seen 20 Oct 21:00 IST)
                        if time.time() - self.last_msg > STALE_SEC:
                            log.warning("chainlink socket silent for %ds, reconnecting", STALE_SEC)
                            break
                        try:
                            msg = await asyncio.wait_for(ws.recv(), 1)
                        except asyncio.TimeoutError:
                            continue
                        if not msg.strip():
                            continue
                        p = json.loads(msg).get("payload") or {}
                        sym = p.get("symbol")
                        for d in p.get("data") or []:
                            self._add(sym, d["timestamp"], d["value"])
                        if p.get("value") is not None:
                            self._add(sym, p.get("timestamp") or time.time() * 1000, p["value"])
            except Exception as e:  # ponytail: blind reconnect with 2s backoff; add jitter/alerts if it flaps
                log.warning("chainlink socket: %r, reconnecting", e)
                await asyncio.sleep(2)

    def last_t(self, sym, max_age=5):
        """(unix second, price) of the latest Chainlink point, or None if stale."""
        with self.lock:
            pts = self.points.get(sym) or {}
            if not pts:
                return None
            t = max(pts)
            return (t, pts[t]) if time.time() - t <= max_age else None

    def last(self, sym, max_age=5):
        with self.lock:
            pts = self.points.get(sym) or {}
            if not pts:
                return None
            t = max(pts)
            return pts[t] if time.time() - t <= max_age else None

    def avg(self, sym, start, end, min_cover=0.8):
        """Mean of 1s prices in [start, end), or None if under min_cover of the seconds are present."""
        with self.lock:
            pts = self.points.get(sym) or {}
            vals = [pts[t] for t in range(int(start), int(end)) if t in pts]
        need = max(1, int((end - start) * min_cover))
        return sum(vals) / len(vals) if len(vals) >= need else None


class BookFeed:
    def __init__(self):
        self.books = {}        # asset_id -> {"bids": {price: size}, "asks": {price: size}}
        self.updated = {}      # asset_id -> unix time of last message
        self.assets = set()
        self.lock = threading.Lock()
        self.prints = []       # (unix time, asset_id, price, size, taker side) from last_trade_price events
        self.msgs = 0          # events received (the dashboard shows the rate)
        self.lags = collections.deque(maxlen=4000)  # (local time, ms between Polymarket's event stamp and arrival)

    def start(self):
        _thread(self._run)
        return self

    def trades_since(self, t):
        with self.lock:
            return [x for x in self.prints if x[0] > t]

    def watch(self, assets):
        self.assets = set(assets)  # a change makes _run reconnect with the new list

    def _apply(self, ev):
        now = time.time()
        with self.lock:
            self.msgs += 1
            if ev.get("timestamp"):
                self.lags.append((now, now * 1000 - float(ev["timestamp"])))
            if ev.get("event_type") == "last_trade_price":
                self.prints.append((now, ev["asset_id"], float(ev["price"]), float(ev["size"]), ev["side"]))
                if len(self.prints) > 5000:
                    del self.prints[:-2000]
            if "bids" in ev and "asks" in ev and "asset_id" in ev:  # full snapshot
                self.books[ev["asset_id"]] = {
                    "bids": {float(o["price"]): float(o["size"]) for o in ev["bids"]},
                    "asks": {float(o["price"]): float(o["size"]) for o in ev["asks"]}}
                self.updated[ev["asset_id"]] = now
            for ch in ev.get("price_changes") or []:
                b = self.books.get(ch["asset_id"])
                if b is None:
                    continue
                side = b["bids"] if ch["side"] == "BUY" else b["asks"]
                price, size = float(ch["price"]), float(ch["size"])
                if size == 0:
                    side.pop(price, None)
                else:
                    side[price] = size
                self.updated[ch["asset_id"]] = now

    async def _run(self):
        while True:
            assets = set(self.assets)
            if not assets:
                await asyncio.sleep(0.2)
                continue
            try:
                async with websockets.connect(CLOB_WS, ssl=CTX, open_timeout=10, ping_interval=10,
                                              max_size=None) as ws:
                    await ws.send(json.dumps({"assets_ids": sorted(assets), "type": "market"}))
                    # plain recv (no per-message timeout) keeps up with ~1000 msg/s; wait_for here caused
                    # "slow consumer" disconnects. Messages arrive constantly, so the asset check still runs often.
                    while self.assets == assets:
                        data = json.loads(await ws.recv())
                        for ev in data if isinstance(data, list) else [data]:
                            self._apply(ev)
            except Exception as e:  # ponytail: blind reconnect; REST fallback covers the gap
                log.warning("book socket: %r, reconnecting", e)
                await asyncio.sleep(1)

    def depth(self, asset, price):
        """Size resting on the bid at `price` (what sits ahead of a new bid we place there)."""
        with self.lock:
            return (self.books.get(asset) or {}).get("bids", {}).get(round(price, 3), 0.0)

    def book(self, asset, max_age=15):
        """engine.Book from the live socket, or None if we have no fresh snapshot for it."""
        with self.lock:
            b = self.books.get(asset)
            if b is None or time.time() - self.updated.get(asset, 0) > max_age:
                return None
            bid = max(b["bids"]) if b["bids"] else None
            ask = min(b["asks"]) if b["asks"] else None
            return Book(bid, b["bids"].get(bid, 0.0), ask, b["asks"].get(ask, 0.0))


class BinanceFeed:
    """Binance futures BTC/USDT best bid/ask mid over SOCKETS hedged connections; last ~15 s kept.

    Each update has an id (u): the first copy to arrive wins and older ids are dropped, so a slow or reconnecting
    socket never stalls the feed. Futures only: spot trades ~$40 apart, mixing the two would fake bp moves.
    """
    SOCKETS = 1                # 2 tested 22 Oct 16:10: both lag together (the home link is the bottleneck), no gain
    BEHIND_MS = 3000           # a socket this far behind Binance's event time is replaced
    MAX_LAG_MS = float(os.getenv("BN_MAX_LAG_MS", 600))   # an update later than this is dropped, never traded on:
    # after a 1-3 s stall the late burst looks like a sudden move, but Polymarket priced it long ago (23 Oct: the
    # feed stalls every 3-6 min from Stockholm; median lag 130 ms, p95 ~210 ms, so 600 ms only cuts stalls)

    def __init__(self):
        self.pts = []          # (unix time, mid)
        self.lock = threading.Lock()
        self.lags = collections.deque(maxlen=4000)  # (local time, ms between Binance's event time E and arrival)
        self.last_u = 0        # newest update id accepted
        self.won = collections.Counter()            # socket -> updates it delivered first
        self.stale = 0                              # updates dropped for arriving later than MAX_LAG_MS

    def start(self):
        _thread(self._run)
        return self

    async def _run(self):
        await asyncio.gather(*(self._socket(k) for k in range(self.SOCKETS)))

    async def _socket(self, k):
        await asyncio.sleep(k * 2)                  # stagger so both are never fresh (or stale) together
        while True:
            try:
                async with websockets.connect(BINANCE_WS[0], ssl=CTX, open_timeout=10, ping_interval=20) as ws:
                    while True:
                        d = json.loads(await asyncio.wait_for(ws.recv(), 10))  # silent 10 s -> reconnect
                        now = time.time()
                        if now * 1000 - d["E"] > self.BEHIND_MS:
                            log.warning("binance socket %d %.1fs behind, reconnecting", k, now - d["E"] / 1000)
                            break
                        if now * 1000 - d["E"] > self.MAX_LAG_MS:
                            self.stale += 1
                            continue
                        with self.lock:
                            if d["u"] <= self.last_u:          # the other socket already delivered this update
                                continue
                            self.last_u = d["u"]
                            self.won[k] += 1
                            self.pts.append((now, (float(d["b"]) + float(d["a"])) / 2))
                            self.lags.append((now, now * 1000 - d["E"]))
                            if len(self.pts) > 6000:
                                cut = now - 15
                                self.pts = [x for x in self.pts if x[0] >= cut]
            except Exception as e:  # ponytail: fixed 1 s retry, no backoff growth
                log.warning("binance socket %d: %r, reconnecting", k, e)
                await asyncio.sleep(1)

    def lag_ms(self):
        """Delay of the newest update we accepted (Binance event time to arrival), or None."""
        with self.lock:
            return self.lags[-1][1] if self.lags else None

    def at(self, t, max_age=2):
        """Mid at time t (latest point at or before t), or None."""
        with self.lock:
            pts = self.pts
            lo, hi = 0, len(pts)
            while lo < hi:
                m = (lo + hi) // 2
                if pts[m][0] <= t:
                    lo = m + 1
                else:
                    hi = m
            if lo == 0 or t - pts[lo - 1][0] > max_age:
                return None
            return pts[lo - 1][1]

    def move_bps(self, window, now=None):
        """Mid change over the last `window` seconds in basis points, or None."""
        now = now or time.time()
        # start point must be from ~window ago, not before a data gap: max_age=window+2 let a 3 s move span an 8 s
        # reconnect gap, so the bot bought moves Polymarket had already priced (22 Oct: -$1.02/buy vs +$0.76 otherwise)
        a, b = self.at(now - window, max_age=1), self.at(now)
        return (b / a - 1) * 1e4 if a and b else None
