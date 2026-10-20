"""Window prices (spot, price to beat, final-minute average) from Chainlink, falling back to Binance; Binance volatility."""
import logging
import time
from math import log, sqrt
from statistics import stdev

import requests

import config

logger = logging.getLogger("spot")
GRACE_SEC = 5  # a Chainlink gap shorter than this just skips ticks


class SpotFeed:
    def __init__(self, session=None, chainlink=None):
        self.http = session or requests.Session()
        self.chainlink = chainlink  # feeds.ChainlinkFeed or None
        self._sigma = {}  # asset -> (fetched_at, per-second vol)
        self._open = {}   # (asset, start) -> (price to beat, source)
        self._stale = {}  # (asset, start) -> when Chainlink went quiet
        self._sig_hist = {}  # asset -> recent per-minute sigma values (last hour)

    def _klines(self, asset, interval="1m", **params):
        return self.http.get(f"{config.BINANCE}/api/v3/klines", timeout=10,
                             params={"symbol": config.ASSETS[asset], "interval": interval, **params}).json()

    def _avg_1s(self, asset, start, end):
        closes = [float(k[4]) for k in self._klines(asset, "1s", startTime=int(start * 1000),
                                                     endTime=int(end * 1000) - 1, limit=60)]
        return sum(closes) / len(closes) if closes else None

    def _binance_price(self, asset):
        return float(self.http.get(f"{config.BINANCE}/api/v3/ticker/price", timeout=10,
                                   params={"symbol": config.ASSETS[asset]}).json()["price"])

    def view(self, asset, start, end, now):
        """(spot, beat, known_avg, source) for one window, all from ONE source. Chainlink (what markets resolve on)
        when it covers the minute before the open, else Binance for the whole window: Chainlink BTC/USD and Binance
        BTC/USDT differ by ~$50-70, so mixing them would bias every probability. spot is None during a short Chainlink gap;
        a gap over GRACE_SEC moves the rest of the window to Binance."""
        sym = f"{asset}/usd"
        if (asset, start) not in self._open:
            beat = self.chainlink.avg(sym, start - 60, start) if self.chainlink else None
            self._open[(asset, start)] = (beat, "chainlink") if beat else (self._avg_1s(asset, start - 60, start), "binance")
        beat, src = self._open[(asset, start)]
        in_final = now > end - 60
        if src == "chainlink":
            spot = self.chainlink.last(sym)
            if spot is not None:
                self._stale.pop((asset, start), None)
                known = self.chainlink.avg(sym, end - 60, now, min_cover=0.5) if in_final else None
                return spot, beat, known, src
            if now - self._stale.setdefault((asset, start), now) < GRACE_SEC:
                return None, beat, None, src
            # Chainlink lost mid-window: move the WHOLE window to Binance, beat included, to keep one source
            beat, src = self._avg_1s(asset, start - 60, start), "binance"
            self._open[(asset, start)] = (beat, src)
            logger.warning("[%s] chainlink quiet >%ds, window %d switched to binance", asset.upper(), GRACE_SEC, start)
        return self._binance_price(asset), beat, self._avg_1s(asset, end - 60, now) if in_final else None, src

    def sigma(self, asset):
        """Per-second vol from the last 30 one-minute log returns, refreshed once a minute."""
        at, value = self._sigma.get(asset, (0, 0.0))
        if time.time() - at > 60:
            closes = [float(k[4]) for k in self._klines(asset, limit=31)]
            value = stdev(log(b / a) for a, b in zip(closes, closes[1:])) / sqrt(60)
            self._sigma[asset] = (time.time(), value)
            self._sig_hist[asset] = (self._sig_hist.get(asset, []) + [value])[-60:]
        return value

    def vol_high(self, asset):
        """True when current sigma is above the VOL_PCTL percentile of the last hour (needs 20+ minutes of history)."""
        hist = sorted(self._sig_hist.get(asset, []))
        if len(hist) < 20:
            return False
        return self.sigma(asset) > hist[min(len(hist) - 1, int(config.VOL_PCTL * len(hist)))]

