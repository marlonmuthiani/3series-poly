"""Strategy state machine — series reversal logic."""

import logging
from dataclasses import dataclass, field
from typing import Optional, Literal

from config import TRIGGER_N

logger = logging.getLogger("strategy")


@dataclass
class CoinState:
    state: Literal["SCANNING", "WAITING_FOR_CHANGE", "BET_ACTIVE"] = "SCANNING"
    wait_side: Optional[str] = None
    history: list[str] = field(default_factory=list)
    verified: list[bool] = field(default_factory=list)  # FIX #17: track verified/unverified outcomes
    total_bets: int = 0
    wins: int = 0
    losses: int = 0
    balance: float = 0.0

    def to_dict(self) -> dict:
        return {
            "state": self.state,
            "wait_side": self.wait_side,
            "history": list(self.history),
            "verified": list(self.verified),  # FIX #17
            "bets": self.total_bets,
            "wins": self.wins,
            "losses": self.losses,
            "balance": self.balance,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CoinState":
        cs = cls(
            state=d.get("state", "SCANNING"),
            wait_side=d.get("wait_side"),
            history=d.get("history", []),
            verified=d.get("verified", []),  # FIX #17
            total_bets=d.get("bets", 0),
            wins=d.get("wins", 0),
            losses=d.get("losses", 0),
            balance=d.get("balance", 0.0),
        )
        # FIX #17: backfill verified list if shorter than history
        while len(cs.verified) < len(cs.history):
            cs.verified.append(False)
        return cs


@dataclass
class BetSignal:
    coin: str
    side: str  # "Up" or "Down" — the side we BET ON (opposite of streak)
    token_type: str  # "up" or "down" — which token to buy


class Strategy:
    """Manages per-coin state machines."""

    def __init__(self, coins: list[str], trigger_n: int = TRIGGER_N):
        self.coins = coins
        self.trigger_n = trigger_n
        self.states: dict[str, CoinState] = {c: CoinState() for c in coins}

    def load_states(self, data: dict):
        for coin in self.coins:
            if coin in data:
                self.states[coin] = CoinState.from_dict(data[coin])

    def dump_states(self) -> dict:
        return {c: s.to_dict() for c, s in self.states.items()}

    def on_outcome(self, coin: str, outcome: str, verified: bool = True) -> Optional[BetSignal]:
        """Process a new outcome. Returns BetSignal if we should bet.
        verified=True means self-resolved with boundary prices. FIX #17."""
        cs = self.states.get(coin)
        if not cs:
            return None

        cs.history.append(outcome)
        cs.verified.append(verified)  # FIX #17
        if len(cs.history) > self.trigger_n + 5:
            cs.history = cs.history[-(self.trigger_n + 5):]
            cs.verified = cs.verified[-(self.trigger_n + 5):]  # FIX #17

        if cs.state == "WAITING_FOR_CHANGE":
            if outcome != cs.wait_side:
                logger.info(f"{coin}: Side changed from {cs.wait_side} → {outcome}, back to SCANNING")
                cs.state = "SCANNING"
                cs.wait_side = None
            else:
                logger.info(f"{coin}: Still waiting for change (got {outcome}, waiting for !{cs.wait_side})")
                return None

        # BET_ACTIVE: already have a pending bet, wait for result
        if cs.state == "BET_ACTIVE":
            logger.debug(f"{coin}: BET_ACTIVE, waiting for result")
            return None

        # SCANNING: check for streak (GAP markers break streaks)
        if cs.state == "SCANNING" and len(cs.history) >= self.trigger_n:
            last_n = cs.history[-self.trigger_n:]
            if "GAP" not in last_n and len(set(last_n)) == 1:
                streak_side = last_n[0]
                bet_side = "Down" if streak_side == "Up" else "Up"
                token_type = "down" if streak_side == "Up" else "up"
                cs.state = "BET_ACTIVE"  # FIX #25: prevent duplicate bets
                logger.info(
                    f"{coin}: TRIGGER! {self.trigger_n}x {streak_side} → bet {bet_side}"
                )
                return BetSignal(coin=coin, side=bet_side, token_type=token_type)

        return None

    def record_bet_result(self, coin: str, won: bool, pnl: float, bet_side: str = ""):
        """Record result of a bet. bet_side = the side we bet ON (e.g. 'Down')."""
        cs = self.states.get(coin)
        if not cs:
            return
        cs.total_bets += 1
        if won:
            cs.wins += 1
            cs.balance += pnl
            cs.state = "SCANNING"  # Back to scanning after win
            logger.info(f"{coin}: WIN +${pnl:.2f} (total: {cs.wins}W/{cs.losses}L)")
        else:
            cs.losses += 1
            cs.balance -= abs(pnl)
            # After loss: wait for the STREAK side to change
            # Streak side = opposite of what we bet on
            # e.g. if we bet Down (against Up streak), wait_side = Up
            if bet_side:
                streak_side = "Down" if bet_side == "Up" else "Up"
            else:
                streak_side = cs.history[-1] if cs.history else None
            cs.state = "WAITING_FOR_CHANGE"
            cs.wait_side = streak_side
            logger.info(
                f"{coin}: LOSS -${abs(pnl):.2f}, waiting for change from {cs.wait_side}"
            )
