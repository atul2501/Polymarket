"""Pure strategy math. Each check_* takes a Snapshot and returns an Opportunity or None."""
from dataclasses import dataclass
from math import floor, sqrt
from statistics import NormalDist

import config


@dataclass
class Book:
    bid: float | None
    bid_size: float
    ask: float | None
    ask_size: float


@dataclass
class Snapshot:
    up: Book
    down: Book
    t_left: float
    fee: dict
    cap: float               # max $ this trade may spend (cash / exposure / per-trade limits)
    p_up: float | None = None  # model P(up)
    p_mkt: float | None = None  # market P(up), UP book mid


@dataclass
class Leg:
    side: str                # "Up" | "Down"
    price: float
    shares: int
    fee: float


@dataclass
class Opportunity:
    strategy: str
    legs: list[Leg]
    edge: float
    note: str


def taker_fee(price, shares, fee):
    # Polymarket crypto fee (market.feeSchedule): shares * rate * p * (p*(1-p))^exponent
    return shares * fee["rate"] * price * (price * (1 - price)) ** fee["exponent"]


def can_trade(t_left):
    return t_left > config.NO_TRADE_LAST_SEC


def fair_up(spot, beat, sigma_s, t_left, known_avg=None):
    """P(final 60s TWAP >= beat), beat = 60s TWAP before the window start (how these markets resolve).
    Driftless random walk with per-second vol sigma_s; known_avg = average of the final minute so far."""
    t = max(t_left, 0.0)
    if t >= 60:
        mean, var = spot, (t - 60) + 60 / 3
    else:
        a = spot if known_avg is None else known_avg
        mean, var = ((60 - t) * a + t * spot) / 60, (t / 60) ** 2 * t / 3
    sd = sigma_s * spot * sqrt(var)
    if sd <= 0:
        return 1.0 if mean >= beat else 0.0
    return NormalDist().cdf((mean - beat) / sd)


def arb_edge(s):
    """(pair_sum, fee_per_pair, edge) or None if a side has no asks."""
    if s.up.ask is None or s.down.ask is None:
        return None
    pair = s.up.ask + s.down.ask
    fee = taker_fee(s.up.ask, 1, s.fee) + taker_fee(s.down.ask, 1, s.fee)
    return pair, fee, 1 - pair - fee


def _shares(cap, unit_cost, depth):
    n = floor(min(depth, cap / unit_cost)) if unit_cost > 0 else 0
    return n if n >= config.MIN_SHARES else 0


def check_arb(s):
    e = arb_edge(s)
    if not can_trade(s.t_left) or e is None or e[2] < config.MIN_EDGE:
        return None
    pair, fee, edge = e
    n = _shares(s.cap, pair + fee, min(s.up.ask_size, s.down.ask_size))
    if not n:
        return None
    legs = [Leg("Up", s.up.ask, n, taker_fee(s.up.ask, n, s.fee)),
            Leg("Down", s.down.ask, n, taker_fee(s.down.ask, n, s.fee))]
    return Opportunity("arb", legs, edge,
                       f"{s.up.ask:.2f}+{s.down.ask:.2f}={pair:.3f} fee {fee:.4f} edge {edge:.4f} x{n} "
                       f"-> locked pnl ${edge * n:.2f}")


def _directional(name, s, side, p, book, min_edge):
    if book.ask is None:
        return None
    fee = taker_fee(book.ask, 1, s.fee)
    edge = p - book.ask - fee
    if edge < min_edge:
        return None
    stake = min(s.cap, kelly_stake(edge, book.ask)) if config.KELLY_SIZING else s.cap
    n = _shares(stake, book.ask + fee, book.ask_size)
    if not n:
        return None
    return Opportunity(name, [Leg(side, book.ask, n, fee * n)], edge,
                       f"{side} fair {p:.3f} - ask {book.ask:.2f} - fee {fee:.4f} = edge {edge:.4f} x{n}")


def kelly_stake(edge, price):
    """Fractional Kelly for a binary paying $1: small edge -> small bet."""
    return config.KELLY * edge / max(1 - price, 0.01) * config.BASE_BALANCE * config.TRADE_SCALE


def check_exit(side, s):
    """(bid, fee per share) to sell an open position at, when the market pays more than the model's value for it."""
    book = s.up if side == "Up" else s.down
    if book.bid is None or s.p_up is None or s.t_left <= config.EXIT_LAST_SEC:
        return None
    held = s.p_up if side == "Up" else 1 - s.p_up
    fee = taker_fee(book.bid, 1, s.fee)
    return (book.bid, fee) if book.bid - fee >= held + config.EXIT_MARGIN else None


def _sides(s, p):
    return (("Up", p, s.up), ("Down", 1 - p, s.down))


def blended(s):
    """Model P(up) pulled toward the market's: the book knows things Binance doesn't."""
    if s.p_mkt is None:
        return s.p_up
    return config.MODEL_WEIGHT * s.p_up + (1 - config.MODEL_WEIGHT) * s.p_mkt


def check_fair(s):
    if not can_trade(s.t_left) or s.p_up is None or not config.FAIR_MIN_T < s.t_left <= config.FAIR_MAX_T:
        return None
    for side, p, book in _sides(s, blended(s)):
        if book.ask is None or not config.FAIR_MIN_ASK <= book.ask <= config.FAIR_MAX_ASK:
            continue
        if opp := _directional("fair", s, side, p, book, config.FAIR_MIN_EDGE):
            return opp
    return None


def check_snipe(s):
    if not (can_trade(s.t_left) and s.t_left <= config.SNIPE_FROM_SEC) or s.p_up is None:
        return None
    for side, p, book in _sides(s, s.p_up):
        if p >= config.SNIPE_MIN_FAIR and book.ask is not None and book.ask <= config.SNIPE_MAX_ASK:
            return _directional("snipe", s, side, p, book, 0.0)
    return None


def check_maker(s):
    """Quote bids on both sides around a centre (model or book mid) so bid_up + bid_down <= 1 - MAKER_EDGE.
    Bids stay one tick under the ask so they rest on the book. Makers pay no fee."""
    if not can_trade(s.t_left) or s.up.bid is None or s.up.ask is None:
        return None
    mid = (s.up.bid + s.up.ask) / 2
    centre = blended(s) if config.MAKER_CENTER == "model" and s.p_up is not None else mid
    bid_up = min(floor((centre - config.MAKER_EDGE / 2) * 100) / 100, round(s.up.ask - 0.01, 2))
    bid_down = floor((1 - centre - config.MAKER_EDGE / 2) * 100) / 100
    if s.down.ask is not None:
        bid_down = min(bid_down, round(s.down.ask - 0.01, 2))
    if bid_up < 0.01 or bid_down < 0.01:
        return None
    n = _shares(s.cap, bid_up + bid_down, float("inf"))
    if not n:
        return None
    edge = 1 - bid_up - bid_down
    return Opportunity("maker", [Leg("Up", bid_up, n, 0.0), Leg("Down", bid_down, n, 0.0)], edge,
                       f"bids {bid_up:.2f}+{bid_down:.2f}={bid_up + bid_down:.2f} edge {edge:.2f} x{n} if both fill")


def maker_filled(leg, book):
    # ponytail: fill only when the ask trades *through* our bid; still ignores queue position
    return book.ask is not None and book.ask < leg.price


def hedge_leg(filled, book, fee):
    """After one maker leg fills, take the other side while pair + taker fee <= MAKER_HEDGE_MAX per share.
    Above $1 that locks a small loss, which beats carrying a one-sided bet to settlement."""
    if book.ask is None:
        return None
    f = taker_fee(book.ask, filled.shares, fee)
    if (filled.price + book.ask) * filled.shares + f > config.MAKER_HEDGE_MAX * filled.shares:
        return None
    return Leg("Down" if filled.side == "Up" else "Up", book.ask, filled.shares, f)


CHECKS = {"arb": check_arb, "fair": check_fair, "snipe": check_snipe, "maker": check_maker}
