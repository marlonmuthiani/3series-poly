"""Self-resolve 15-minute crypto windows using Chainlink prices.
Also handles startup recovery: fetches recent outcomes from Gamma API."""

import asyncio
import json
import logging
import time
from typing import Optional

import httpx

from config import GAMMA_API, COINS
from http_client import get_client  # FIX #14

logger = logging.getLogger("resolver")

WINDOW = 900  # 15 minutes in seconds
RECOVERY_WINDOWS = 6  # fetch last 6 windows on startup for history


def current_window_start() -> int:
    """Return epoch of current 15-min window start."""
    now = int(time.time())
    return now - (now % WINDOW)


def next_window_end() -> float:
    """Return timestamp when current window ends."""
    return float(current_window_start() + WINDOW)


class MarketResolver:
    """Resolves 15-min windows by comparing start/end Chainlink prices."""

    def __init__(self, feed):
        self.feed = feed
        self.stats = {"total": 0, "matches": 0, "mismatches": 0}
        # Store boundary prices: end price of window N = start price of window N+1
        self._boundary_prices: dict[str, dict[int, float]] = {}  # {coin: {epoch: price}}
        # Store priceToBeat + slug for last markets (from Gamma verification)
        # {coin: [{epoch, slug, priceToBeat, outcome, resolved_at}]}
        self.market_records: dict[str, list[dict]] = {c: [] for c in COINS}

    def record_boundary(self, coin: str, epoch: int, price: float):
        """Record a boundary price (at exact window boundary timestamp)."""
        if coin not in self._boundary_prices:
            self._boundary_prices[coin] = {}
        self._boundary_prices[coin][epoch] = price
        logger.info(f"BOUNDARY {coin} epoch={epoch}: ${price:.6f}")

    def cleanup_boundaries(self, current_epoch: int):
        """Remove boundary prices older than 2 windows."""
        cutoff = current_epoch - 2 * WINDOW
        for coin in COINS:
            if coin in self._boundary_prices:
                old = [e for e in self._boundary_prices[coin] if e < cutoff]
                for e in old:
                    del self._boundary_prices[coin][e]

    def resolve_window(self, coin: str, epoch: int) -> Optional[str]:
        """Resolve a window. Returns 'Up' or 'Down' or None if data missing."""
        end_epoch = epoch + WINDOW

        # Start price = boundary at epoch (= end price of previous window)
        start_price = self._boundary_prices.get(coin, {}).get(epoch)
        # End price = boundary at epoch + WINDOW
        end_price = self._boundary_prices.get(coin, {}).get(end_epoch)

        # Fallback to RTDS buffer if boundary not recorded
        if start_price is None:
            start_price = self.feed.get_price_exact(coin, epoch * 1000, tolerance_ms=2000)
            if start_price is not None:
                logger.warning(f"Used RTDS fallback for start price {coin} epoch={epoch}")
        if end_price is None:
            end_price = self.feed.get_price_exact(coin, end_epoch * 1000, tolerance_ms=2000)
            if end_price is not None:
                logger.warning(f"Used RTDS fallback for end price {coin} epoch={epoch}")

        if start_price is None or end_price is None:
            logger.warning(f"Missing price data for {coin} epoch={epoch} start={start_price} end={end_price}")
            return None

        outcome = "Up" if end_price >= start_price else "Down"
        logger.info(
            f"RESOLVED {coin} epoch={epoch}: ${start_price:.6f} → ${end_price:.6f} = {outcome}"
        )
        return outcome

    def _record_market(self, coin: str, epoch: int, outcome: str,
                       price_to_beat: Optional[float] = None, source: str = "gamma"):
        """Store market record for audit trail. Keep last RECOVERY_WINDOWS.
        source: 'self' for Chainlink self-resolve, 'gamma' for Gamma fallback."""
        slug = f"{coin}-updown-15m-{epoch}"
        rec = {
            "epoch": epoch,
            "slug": slug,
            "outcome": outcome,
            "priceToBeat": price_to_beat,
            "resolved_at": time.time(),
            "source": source,
        }
        records = self.market_records.setdefault(coin, [])
        records.append(rec)
        # Keep only last N
        if len(records) > RECOVERY_WINDOWS:
            self.market_records[coin] = records[-RECOVERY_WINDOWS:]

    async def recover_history(self, strategy) -> tuple[int, int]:
        """On startup, fetch recent resolved outcomes from Gamma up to current_epoch.
        Fills ALL gaps so strategy history is complete.
        Returns (number_recovered, last_recovered_epoch)."""
        now = int(time.time())
        current_epoch = now - (now % WINDOW)
        recovered = 0
        last_recovered_epoch = 0

        # Fetch all coins in parallel
        async def _recover_coin(coin: str):
            outcomes = []
            # Fetch RECOVERY_WINDOWS back, PLUS current_epoch (might just have resolved)
            epochs = [current_epoch - i * WINDOW for i in range(RECOVERY_WINDOWS + 1)]
            epochs.reverse()  # oldest first

            client = get_client()  # FIX #14: shared httpx client
            if True:  # kept block indent
                # Fire all requests
                resp_futures = {
                    ep: client.get(f"{GAMMA_API}/events", params={"slug": f"{coin}-updown-15m-{ep}"})
                    for ep in epochs
                }
                results = {}
                for ep in epochs:
                    try:
                        results[ep] = await resp_futures[ep]
                    except Exception as e:
                        logger.warning(f"Recovery fetch failed {coin} epoch={ep}: {e}")

            for ep in epochs:
                resp = results.get(ep)
                if not resp or resp.status_code != 200:
                    continue
                events = resp.json()
                if not events:
                    continue
                event = events[0]
                meta = event.get("eventMetadata", {})
                ptb = meta.get("priceToBeat")
                markets = event.get("markets", [])
                if not markets:
                    continue
                market = markets[0]
                if market.get("umaResolutionStatus") != "resolved":
                    continue

                outcome_prices = market.get("outcomePrices", "[]")
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)
                outcomes_list = market.get("outcomes", "[]")
                if isinstance(outcomes_list, str):
                    outcomes_list = json.loads(outcomes_list)

                resolved_outcome = None
                for oc, price in zip(outcomes_list, outcome_prices):
                    if price == "1":
                        resolved_outcome = oc
                        break

                if resolved_outcome:
                    outcomes.append((ep, resolved_outcome, ptb))

            return coin, outcomes

        # Run all coins in parallel
        coin_results = await asyncio.gather(
            *[_recover_coin(c) for c in COINS], return_exceptions=True
        )

        for result in coin_results:
            if isinstance(result, Exception):
                logger.error(f"Recovery error: {result}")
                continue
            coin, outcomes = result
            if outcomes:
                # Check for gaps — insert None marker to break false streaks
                recovered_with_gaps = []
                prev_epoch = None
                for ep, outcome, ptb in outcomes:
                    if prev_epoch is not None and ep - prev_epoch > WINDOW:
                        # Gap detected — insert streak-breaker
                        recovered_with_gaps.append("GAP")
                        logger.warning(f"Gap in {coin} history between {prev_epoch} and {ep}")
                    recovered_with_gaps.append(outcome)
                    prev_epoch = ep
                recovered_outcomes = recovered_with_gaps
                strategy.states[coin].history = recovered_outcomes
                for epoch, outcome, ptb in outcomes:
                    self._record_market(coin, epoch, outcome, ptb)
                    last_recovered_epoch = max(last_recovered_epoch, epoch)
                recovered += len(outcomes)
                logger.info(
                    f"Recovered {coin}: {recovered_outcomes} "
                    f"(history={strategy.states[coin].history})"
                )
            else:
                logger.warning(f"No resolved markets found for {coin} recovery")

        return recovered, last_recovered_epoch

    async def verify_with_gamma(self, coin: str, epoch: int, our_outcome: str) -> Optional[bool]:
        """Check our resolution against Gamma API.
        Returns True if match, False if mismatch, None if unavailable.
        Also stores priceToBeat."""
        slug = f"{coin}-updown-15m-{epoch}"
        try:
            client = get_client()  # FIX #14: shared httpx client
            resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
            if resp.status_code != 200:
                return None
            events = resp.json()
            if not events:
                return None
            event = events[0]
            meta = event.get("eventMetadata", {})
            ptb = meta.get("priceToBeat")
            markets = event.get("markets", [])
            if not markets:
                return None
            market = markets[0]
            uma_status = market.get("umaResolutionStatus")
            if uma_status != "resolved":
                return None

            # Determine winner from outcomePrices
            outcome_prices = market.get("outcomePrices", "[]")
            if isinstance(outcome_prices, str):
                outcome_prices = json.loads(outcome_prices)
            outcomes_list = market.get("outcomes", "[]")
            if isinstance(outcomes_list, str):
                outcomes_list = json.loads(outcomes_list)

            resolved_outcome = None
            for oc, price in zip(outcomes_list, outcome_prices):
                if price == "1":
                    resolved_outcome = oc
                    break

            if not resolved_outcome:
                return None

            # Store market record with priceToBeat
            self._record_market(coin, epoch, resolved_outcome, ptb)

            match = (resolved_outcome == our_outcome)
            self.stats["total"] += 1
            if match:
                self.stats["matches"] += 1
                if ptb is not None:
                    logger.info(f"Verified {coin} epoch={epoch}: ✓ priceToBeat=${ptb:.2f}")
            else:
                self.stats["mismatches"] += 1
                logger.error(
                    f"MISMATCH {coin} epoch={epoch}: ours={our_outcome} gamma={resolved_outcome} ptb=${ptb}"
                )
            return match
        except Exception as e:
            logger.warning(f"Gamma verify failed for {coin} epoch={epoch}: {e}")
            return None

    def dump_records(self) -> dict:
        """Dump market records for state persistence."""
        return {c: list(recs) for c, recs in self.market_records.items()}

    def load_records(self, data: dict):
        """Load market records from state."""
        for coin in COINS:
            if coin in data:
                self.market_records[coin] = data[coin][-RECOVERY_WINDOWS:]
