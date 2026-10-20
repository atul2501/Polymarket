"""Websocket feeds, each on a background thread:
- ChainlinkFeed: Chainlink prices (what Polymarket resolves on) from Polymarket's public live-data socket.
- BookFeed: live order books from Polymarket's CLOB market socket.
Both reconnect on any error; callers fall back to REST when data is missing or stale."""
import asyncio
import json
import logging
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

    def start(self):
        _thread(self._run)
        return self

    def watch(self, assets):
        self.assets = set(assets)  # a change makes _run reconnect with the new list

    def _apply(self, ev):
        now = time.time()
        with self.lock:
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

    def book(self, asset, max_age=15):
        """engine.Book from the live socket, or None if we have no fresh snapshot for it."""
        with self.lock:
            b = self.books.get(asset)
            if b is None or time.time() - self.updated.get(asset, 0) > max_age:
                return None
            bid = max(b["bids"]) if b["bids"] else None
            ask = min(b["asks"]) if b["asks"] else None
            return Book(bid, b["bids"].get(bid, 0.0), ask, b["asks"].get(ask, 0.0))
