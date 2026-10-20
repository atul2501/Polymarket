"""Polymarket 5m crypto Up/Down paper-trading bot.  Run: python main.py   |   python main.py --report"""
import logging
import sys
import time

import requests

import config
from engine import CHECKS, Snapshot, arb_edge, check_exit, fair_up
from executor import Executor
from feeds import BookFeed, ChainlinkFeed
from ledger import Ledger
from market import MarketFeed
from spot import SpotFeed

log = logging.getLogger("bot")


class Bot:
    def __init__(self):
        http = requests.Session()
        chainlink = ChainlinkFeed([f"{a}/usd" for a in config.ASSETS]).start() if config.CHAINLINK else None
        self.ledger = Ledger()
        self.feed = MarketFeed(http, BookFeed().start() if config.BOOK_WS else None)
        self.spot = SpotFeed(http, chainlink)
        self.exec = Executor(self.ledger)
        self.last_settle = 0.0
        self.last_log = {}  # slug -> time of last status line (we check 4x/s, log 1x/s)

    def tick(self):
        now = time.time()
        markets = self.feed.current(now)
        books = self.feed.books(markets)
        for m in markets:
            up, down, t_left = books[m.up_token], books[m.down_token], m.seconds_left(now)
            spot, beat, known, src = self.spot.view(m.asset, m.start, m.end, now)
            if spot is None or beat is None:
                log.warning("[%s %s] no fresh %s price, skipping tick", m.asset.upper(), m.label, src)
                continue
            sigma = self.spot.sigma(m.asset) * config.VOL_MULT
            p_up = fair_up(spot, beat, sigma, t_left, known)
            p_mkt = (up.bid + up.ask) / 2 if up.bid is not None and up.ask is not None else None
            s = Snapshot(up, down, t_left, m.fee, 0.0, p_up, p_mkt)
            self.ledger.tick(now, m, s, spot, beat, known, sigma / config.VOL_MULT)
            arb = arb_edge(s)
            if arb:
                self.ledger.scan(m, arb[0])
            if now - self.last_log.get(m.slug, 0) >= 1:
                self.last_log[m.slug] = now
                log.info("[%s %-3s] %3ds | UP %s/%s DN %s/%s | sum %s | %s spot %.6g beat %.6g twap_known %s"
                         " σ/s %.1e fair_up %.3f%s",
                         m.asset.upper(), m.label, t_left, _p(up.bid), _p(up.ask), _p(down.bid), _p(down.ask),
                         f"{arb[0]:.2f} fee {arb[1]:.4f} edge {arb[2]:+.4f}" if arb else "n/a", src,
                         spot, beat, f"{known:.6g}" if known else "--", sigma, p_up,
                         "" if t_left > config.NO_TRADE_LAST_SEC else " | LOCKED")
            self.exec.tick_maker(m, up, down, t_left)
            # 1) exits first: sell when the market pays more than the model's value for what we hold
            for pos in self.ledger.open_positions(m.slug):
                if ex := check_exit(pos[2], s):
                    self.exec.sell(m, pos, *ex)
            # 2) entries, only on the resolution price source and inside the risk limits
            if src != "chainlink" and not config.TRADE_ON_BINANCE:
                continue
            if self.ledger.hour_pnl(now) < -config.MAX_HOURLY_LOSS:
                if now - self.last_log.get("pause", 0) >= 60:
                    self.last_log["pause"] = now
                    log.warning("hourly loss limit $%.0f hit: pausing new entries", config.MAX_HOURLY_LOSS)
                continue
            for name in config.STRATEGIES:
                if self.ledger.entries(m.slug, name) >= config.MAX_ENTRIES.get(name, 1):
                    continue
                if name in ("fair", "snipe") and self.ledger.open_positions(m.slug):
                    continue  # max 1 open directional position per round; re-entry/flip only after an exit
                if name == "maker" and self.exec.has_pending(m.slug):
                    continue
                if name == "fair" and config.VOL_FILTER and self.spot.vol_high(m.asset):
                    continue
                s.cap = self.ledger.cap(self.exec.reserved())
                if opp := CHECKS[name](s):
                    self.exec.execute(m, opp)
        if now - self.last_settle > 30:
            self.settle(now)
            self.last_settle = now

    def settle(self, now):
        for slug in self.ledger.unsettled(now):
            if winner := self.feed.outcome(slug):
                self.ledger.settle(slug, winner)
                log.info("SETTLED %s -> %s | cash $%.2f", slug, winner, self.ledger.cash())

    def run(self):
        log.info("start | %s | wallet $%.2f | strategies %s", "PAPER" if config.DRY_RUN else "LIVE",
                 self.ledger.wallet, config.STRATEGIES)
        errors = 0
        while True:
            try:
                self.tick()
                errors = 0
            except (requests.RequestException, ValueError, KeyError) as e:
                errors += 1
                log.warning("api error %d/%d: %r", errors, config.MAX_API_ERRORS, e)
                if errors >= config.MAX_API_ERRORS:
                    log.error("KILL SWITCH: too many consecutive API errors")
                    return
            if self.ledger.today_pnl() < -config.MAX_DAILY_LOSS:
                log.error("KILL SWITCH: daily loss $%.2f exceeds $%.2f", -self.ledger.today_pnl(), config.MAX_DAILY_LOSS)
                return
            time.sleep(config.POLL_SEC)


def _p(x):
    return f"{x:.2f}" if x is not None else "--"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname).1s %(message)s", datefmt="%H:%M:%S")
    if "--report" in sys.argv:
        print(Ledger().report())
    else:
        try:
            Bot().run()
        except KeyboardInterrupt:
            pass
