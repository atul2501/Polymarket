"""Turns Opportunities into ledger rows. Paper-only in v1: same path, no real orders."""
import logging

import config
from engine import hedge_leg, maker_filled

log = logging.getLogger("exec")


class Executor:
    def __init__(self, ledger):
        self.ledger = ledger
        self.pending = {}  # slug -> (market, unfilled maker legs, filled maker legs)

    def execute(self, m, opp):
        if not config.DRY_RUN:
            raise NotImplementedError("live trading is not implemented in v1; set DRY_RUN=1")
        tag = f"[{m.asset.upper()} {m.label}] {opp.strategy.upper()}"
        if opp.strategy == "maker":
            self.pending[m.slug] = (m, list(opp.legs), [])
            log.info("%s QUOTE %s", tag, opp.note)
            return
        for leg in opp.legs:
            self.ledger.record(m, opp.strategy, leg)
        log.info("%s FILL %s", tag, opp.note)

    def sell(self, m, pos, price, fee):
        """Close an open directional position at the bid (paper)."""
        if not config.DRY_RUN:
            raise NotImplementedError("live trading is not implemented in v1; set DRY_RUN=1")
        trade_id, strategy, side, entry, shares = pos
        self.ledger.close(trade_id, price, fee)
        pnl = self.ledger._one("SELECT pnl FROM trades WHERE id=?", trade_id)
        log.info("[%s %s] SELL %s %s @ %.2f x%g (bought %.2f) -> pnl $%+.2f", m.asset.upper(), m.label,
                 strategy.upper(), side, price, shares, entry, pnl)

    def has_pending(self, slug):
        return slug in self.pending

    def reserved(self):
        """Cash held back for resting maker bids."""
        return sum(l.price * l.shares for _, legs, _f in self.pending.values() for l in legs)

    def tick_maker(self, m, up, down, t_left):
        if m.slug not in self.pending:
            return
        _, legs, filled = self.pending[m.slug]
        tag = f"[{m.asset.upper()} {m.label}]"
        for leg in list(legs):
            if maker_filled(leg, up if leg.side == "Up" else down):
                self.ledger.record(m, "maker", leg)
                legs.remove(leg)
                filled.append(leg)
                log.info("%s MAKER FILL %s @ %.2f x%d", tag, leg.side, leg.price, leg.shares)
        if len(legs) == 1 and filled and t_left > config.NO_TRADE_LAST_SEC:
            first = filled[0]
            if hedge := hedge_leg(first, up if legs[0].side == "Up" else down, m.fee):
                self.ledger.record(m, "maker", hedge)
                log.info("%s MAKER HEDGE %s @ %.2f x%d (pair %.2f + fee %.3f)", tag, hedge.side, hedge.price,
                         hedge.shares, first.price + hedge.price, hedge.fee)
                legs.clear()
        if legs and t_left <= config.NO_TRADE_LAST_SEC:
            log.info("[%s %s] MAKER CANCEL unfilled %s", m.asset.upper(), m.label, [l.side for l in legs])
            legs.clear()
        if not legs:
            del self.pending[m.slug]
