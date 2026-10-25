"""Polymarket data: market discovery (Gamma) and order books (CLOB). Public endpoints, no auth."""
import json
import logging
import threading
from dataclasses import dataclass

import requests

import config
from engine import Book

log = logging.getLogger("market")


@dataclass
class Market:
    slug: str
    asset: str
    label: str               # "5m" | "15m"
    start: int
    length: int
    up_token: str
    down_token: str
    fee: dict

    @property
    def end(self):
        return self.start + self.length

    def seconds_left(self, now):
        return self.end - now


def parse_book(raw):
    bids = sorted(raw.get("bids", []), key=lambda o: -float(o["price"]))
    asks = sorted(raw.get("asks", []), key=lambda o: float(o["price"]))
    return Book(float(bids[0]["price"]) if bids else None, float(bids[0]["size"]) if bids else 0.0,
                float(asks[0]["price"]) if asks else None, float(asks[0]["size"]) if asks else 0.0)


class MarketFeed:
    def __init__(self, session=None, ws=None):
        self.http = session or requests.Session()
        self.ws = ws  # feeds.BookFeed or None
        self._cache = {}  # slug -> Market | None
        self._next = {}   # slug -> Market | None, next round fetched early by upcoming() in a thread

    def _fetch(self, slug, asset, label, start, length):
        rows = self.http.get(f"{config.GAMMA}/markets", params={"slug": slug}, timeout=10).json()
        if not rows:
            log.warning("no market for %s", slug)
            return None
        m = rows[0]
        up, down = json.loads(m["clobTokenIds"])
        assert json.loads(m["outcomes"]) == ["Up", "Down"], m["outcomes"]
        fee = m.get("feeSchedule") or config.DEFAULT_FEE
        if not m.get("feesEnabled", True):
            fee = {"rate": 0.0, "exponent": 1}
        return Market(slug, asset, label, start, length, up, down, fee)

    def current(self, now):
        out = []
        for label, length in config.WINDOWS.items():
            start = int(now) - int(now) % length
            for asset in config.ASSETS:
                slug = f"{asset}-updown-{label}-{start}"
                if slug not in self._cache:
                    self._cache = {k: v for k, v in self._cache.items() if v is None or v.end > now - 3600}
                    self._cache[slug] = self._next.pop(slug, None) or self._fetch(slug, asset, label, start, length)
                if self._cache[slug]:
                    out.append(self._cache[slug])
        return out

    def upcoming(self, now, early=15):
        """Fetch the next round's market `early` s before it starts, off the loop; books() then subscribes its tokens,
        so the round change needs no REST call and no socket reconnect (05:05:00 25 Oct: 144 ms to an order)."""
        for label, length in config.WINDOWS.items():
            start = int(now) - int(now) % length + length
            if start - now > early:
                continue
            for asset in config.ASSETS:
                slug = f"{asset}-updown-{label}-{start}"
                if slug in self._next or slug in self._cache:
                    continue
                self._next[slug] = None

                def get(slug=slug, asset=asset, label=label, start=start, length=length):
                    try:
                        self._next[slug] = self._fetch(slug, asset, label, start, length)
                    except Exception as e:  # noqa: BLE001 - current() fetches it itself at the round change
                        log.warning("prefetch %s failed: %r", slug, e)
                threading.Thread(target=get, daemon=True).start()

    def books(self, markets):
        """{token_id: Book} from the live socket, or one POST /books call if the socket has no fresh book."""
        tokens = [t for m in markets for t in (m.up_token, m.down_token)]
        if not tokens:
            return {}
        if self.ws:
            self.ws.watch(tokens + [t for m in list(self._next.values()) if m for t in (m.up_token, m.down_token)])
            live = {t: self.ws.book(t) for t in tokens}
            if all(live.values()):
                return live
        raw = self.http.post(f"{config.CLOB}/books", json=[{"token_id": t} for t in tokens], timeout=10).json()
        return {r["asset_id"]: parse_book(r) for r in raw}

    def outcome(self, slug):
        """'Up' / 'Down' once resolved, else None."""
        rows = self.http.get(f"{config.GAMMA}/markets", params={"slug": slug, "closed": "true"}, timeout=10).json()
        if not rows or rows[0].get("umaResolutionStatus") != "resolved":
            return None
        prices = [float(p) for p in json.loads(rows[0]["outcomePrices"])]
        return json.loads(rows[0]["outcomes"])[prices.index(max(prices))] if max(prices) == 1 else None
