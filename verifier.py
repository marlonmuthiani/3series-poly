#!/usr/bin/env python3
"""
Parallel Verifier for Series Reversal Bot.

Runs alongside the bot and independently verifies:
1. Chainlink self-resolve accuracy vs Gamma API (ground truth)
2. Strategy logic correctness (independent state machine)

Logs to logs/verification.jsonl and sends Telegram alerts on issues.
"""

import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────

COINS = os.getenv("COINS", "btc,eth,sol,xrp").split(",")
TRIGGER_N = int(os.getenv("TRIGGER_N", "3"))
WINDOW = 900  # 15 min
GAMMA_API = "https://gamma-api.polymarket.com"
# FIX #21: Server in US (SFO2), no proxy needed
PROXY_URL = ""
STATE_FILE = os.path.join(os.path.dirname(__file__), "state.json")
BOT_LOG_FILE = os.path.join(os.path.dirname(__file__), "logs", "bot.log")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
VERIFICATION_LOG = os.path.join(LOG_DIR, "verification.jsonl")
TG_TOKEN = os.getenv("TRADE_BOT_TOKEN", "")
TG_CHAT = os.getenv("TRADE_CHAT_ID", "")

# Gamma retry schedule: seconds after window end
GAMMA_RETRY_DELAYS = [180, 300, 420, 540, 660]  # 3, 5, 7, 9, 11 min

os.makedirs(LOG_DIR, exist_ok=True)

# ── Logging ─────────────────────────────────────────────

logger = logging.getLogger("verifier")
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(fmt)
logger.addHandler(ch)

fh = RotatingFileHandler(
    os.path.join(LOG_DIR, "verifier.log"),
    maxBytes=10 * 1024 * 1024,
    backupCount=3,
)
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
logger.addHandler(fh)


# ── Telegram notify ─────────────────────────────────────

async def tg_notify(text: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"},
            )
    except Exception as e:
        logger.warning(f"TG notify failed: {e}")


# ── Verification log writer ─────────────────────────────

def log_event(event: dict):
    """Append event to verification.jsonl."""
    event["ts"] = time.time()
    event["ts_human"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    try:
        with open(VERIFICATION_LOG, "a") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        logger.error(f"Failed to write verification log: {e}")


# ── Pure Strategy Verifier (independent state machine) ──

@dataclass
class VerifierCoinState:
    state: str = "SCANNING"  # SCANNING or WAITING_FOR_CHANGE
    wait_side: Optional[str] = None
    history: list[str] = field(default_factory=list)
    triggers: list[dict] = field(default_factory=list)  # log of all triggers
    total_bets: int = 0
    wins: int = 0
    losses: int = 0
    balance: float = 0.0


class PureStrategyVerifier:
    """Independent strategy state machine using ONLY Gamma ground truth."""

    def __init__(self, coins: list[str], trigger_n: int = TRIGGER_N):
        self.coins = coins
        self.trigger_n = trigger_n
        self.states: dict[str, VerifierCoinState] = {c: VerifierCoinState() for c in coins}

    def on_outcome(self, coin: str, outcome: str, epoch: int) -> Optional[dict]:
        """Process outcome. Returns trigger dict if we should bet, else None."""
        cs = self.states.get(coin)
        if not cs:
            return None

        cs.history.append(outcome)
        if len(cs.history) > self.trigger_n + 5:
            cs.history = cs.history[-(self.trigger_n + 5):]

        if cs.state == "WAITING_FOR_CHANGE":
            if outcome != cs.wait_side:
                logger.debug(f"[V] {coin}: Side changed {cs.wait_side}→{outcome}, back to SCANNING")
                cs.state = "SCANNING"
                cs.wait_side = None
            else:
                logger.debug(f"[V] {coin}: Still waiting (got {outcome}, want !{cs.wait_side})")
                return None

        # Check for streak
        if cs.state == "SCANNING" and len(cs.history) >= self.trigger_n:
            last_n = cs.history[-self.trigger_n:]
            if "GAP" not in last_n and len(set(last_n)) == 1:
                streak_side = last_n[0]
                bet_side = "Down" if streak_side == "Up" else "Up"
                trigger = {
                    "coin": coin,
                    "epoch": epoch,
                    "streak": list(last_n),
                    "streak_side": streak_side,
                    "bet_side": bet_side,
                }
                cs.triggers.append(trigger)
                logger.info(f"[V] {coin}: TRIGGER {self.trigger_n}x {streak_side} → bet {bet_side}")
                return trigger

        return None

    def record_result(self, coin: str, won: bool, pnl: float):
        """Record bet result."""
        cs = self.states.get(coin)
        if not cs:
            return
        cs.total_bets += 1
        if won:
            cs.wins += 1
            cs.balance += pnl
        else:
            cs.losses += 1
            cs.balance -= abs(pnl)
            cs.state = "WAITING_FOR_CHANGE"
            cs.wait_side = cs.history[-1] if cs.history else None
            logger.info(f"[V] {coin}: LOSS, waiting for change from {cs.wait_side}")


# ── Gamma API helper ────────────────────────────────────

async def fetch_gamma_outcome(client: httpx.AsyncClient, coin: str, epoch: int) -> Optional[dict]:
    """Fetch resolved outcome from Gamma API. Returns dict or None if not resolved."""
    slug = f"{coin}-updown-15m-{epoch}"
    try:
        resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None

        event = data[0]
        market = event["markets"][0]

        if market.get("umaResolutionStatus") != "resolved":
            return None

        outcome_prices = market.get("outcomePrices", "[]")
        if isinstance(outcome_prices, str):
            outcome_prices = json.loads(outcome_prices)
        outcomes_list = market.get("outcomes", "[]")
        if isinstance(outcomes_list, str):
            outcomes_list = json.loads(outcomes_list)

        resolved = None
        for oc, price in zip(outcomes_list, outcome_prices):
            if price == "1":
                resolved = oc
                break

        if not resolved:
            return None

        meta = event.get("eventMetadata", {})
        return {
            "outcome": resolved,
            "price_to_beat": meta.get("priceToBeat"),
            "slug": slug,
        }
    except Exception as e:
        logger.warning(f"Gamma fetch error for {coin} epoch={epoch}: {e}")
        return None


# ── Read bot state (read-only) ──────────────────────────

def read_bot_state() -> dict:
    """Read bot's state.json (read-only)."""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Cannot read bot state: {e}")
    return {}


def check_bot_log_for_trigger(coin: str, epoch: int) -> bool:
    """Check bot.log for a TRIGGER line for this coin after resolving the given epoch.

    More reliable than checking state.json orders, because OBSERVATION mode
    orders may not persist and there's a race condition with create_task + save_state.
    """
    try:
        if not os.path.exists(BOT_LOG_FILE):
            return False
        # Read last 500 lines (enough to cover recent windows)
        with open(BOT_LOG_FILE, "rb") as f:
            # Seek to end, read last ~100KB
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 100_000)
            f.seek(size - read_size)
            tail = f.read().decode("utf-8", errors="replace")

        # Look for: RESOLVED {coin} epoch={epoch} ... followed by TRIGGER for same coin
        # Bot logs pattern:
        #   [resolver] INFO: RESOLVED eth epoch=1772104500: ... = Down
        #   [strategy] INFO: eth: TRIGGER! 3x Down → bet Up
        lines = tail.split("\n")
        found_resolve = False
        for line in lines:
            if f"RESOLVED {coin} epoch={epoch}" in line:
                found_resolve = True
                continue
            if found_resolve:
                if f"{coin}: TRIGGER!" in line:
                    return True
                # If we hit the next RESOLVED or BOUNDARY for this coin, stop looking
                if f"RESOLVED {coin}" in line or f"BOUNDARY {coin}" in line:
                    break
        return False
    except Exception as e:
        logger.warning(f"Cannot check bot log for trigger: {e}")
        return False


# ── Stats tracker ───────────────────────────────────────

class Stats:
    def __init__(self):
        self.chainlink_matches = 0
        self.chainlink_mismatches = 0
        self.chainlink_skips = 0  # bot had no self-resolve
        self.strategy_matches = 0
        self.strategy_mismatches = 0
        self.windows_checked = 0
        self.recent_accuracy = []  # last 50 results (True/False)

    def add_chainlink_result(self, match: bool):
        if match:
            self.chainlink_matches += 1
        else:
            self.chainlink_mismatches += 1
        self.recent_accuracy.append(match)
        if len(self.recent_accuracy) > 50:
            self.recent_accuracy.pop(0)

    @property
    def chainlink_accuracy(self) -> float:
        total = self.chainlink_matches + self.chainlink_mismatches
        return self.chainlink_matches / total if total > 0 else 1.0

    @property
    def recent_chainlink_accuracy(self) -> float:
        if not self.recent_accuracy:
            return 1.0
        return sum(self.recent_accuracy) / len(self.recent_accuracy)

    @property
    def strategy_accuracy(self) -> float:
        total = self.strategy_matches + self.strategy_mismatches
        return self.strategy_matches / total if total > 0 else 1.0

    def summary(self) -> str:
        total_cl = self.chainlink_matches + self.chainlink_mismatches
        total_st = self.strategy_matches + self.strategy_mismatches
        return (
            f"📊 Verification Summary (windows={self.windows_checked})\n"
            f"  Chainlink: {self.chainlink_matches}/{total_cl} match "
            f"({self.chainlink_accuracy:.1%}) | skips={self.chainlink_skips}\n"
            f"  Recent(50): {self.recent_chainlink_accuracy:.1%}\n"
            f"  Strategy:  {self.strategy_matches}/{total_st} match "
            f"({self.strategy_accuracy:.1%})"
        )


# ── Main verification loop ──────────────────────────────

async def verify_window(
    epoch: int,
    client: httpx.AsyncClient,
    strategy_v: PureStrategyVerifier,
    stats: Stats,
):
    """Verify a single window for all coins."""
    stats.windows_checked += 1
    bot_state = read_bot_state()
    bot_coins = bot_state.get("coins", {})
    bot_records = bot_state.get("market_records", {})

    for coin in COINS:
        # ── Step 1: Get Gamma ground truth (with retries) ──
        gamma_result = None
        gamma_delay = 0

        for delay in GAMMA_RETRY_DELAYS:
            # Calculate how long to wait from now
            target_time = epoch + WINDOW + delay
            now = time.time()
            wait = target_time - now
            if wait > 0:
                logger.debug(f"Waiting {wait:.0f}s for Gamma resolve ({coin} epoch={epoch})")
                await asyncio.sleep(wait)

            gamma_result = await fetch_gamma_outcome(client, coin, epoch)
            if gamma_result:
                gamma_delay = delay
                break

        if not gamma_result:
            logger.warning(f"Gamma never resolved {coin} epoch={epoch} after all retries")
            log_event({
                "type": "gamma_timeout",
                "coin": coin,
                "epoch": epoch,
            })
            continue

        gamma_outcome = gamma_result["outcome"]
        logger.info(f"Gamma: {coin} epoch={epoch} → {gamma_outcome} (delay={gamma_delay}s)")

        # ── Step 2: Compare with bot's self-resolve ──
        bot_coin_state = bot_coins.get(coin, {})

        # Try to get bot's self-resolve from its market_records
        # Look through list of records for this coin to find matching epoch
        coin_records = bot_records.get(coin, [])
        bot_record = None
        for rec in coin_records:
            if isinstance(rec, dict) and rec.get("epoch") == epoch:
                bot_record = rec
                break
        bot_self_resolve = bot_record.get("outcome") if bot_record and bot_record.get("source") == "self" else None

        if bot_self_resolve:
            match = (bot_self_resolve == gamma_outcome)
            stats.add_chainlink_result(match)
            status = "✅" if match else "❌ MISMATCH"
            logger.info(f"Chainlink check {coin} epoch={epoch}: bot={bot_self_resolve} gamma={gamma_outcome} → {status}")

            log_event({
                "type": "chainlink_accuracy",
                "coin": coin,
                "epoch": epoch,
                "bot_resolve": bot_self_resolve,
                "gamma_resolve": gamma_outcome,
                "match": match,
                "gamma_delay_s": gamma_delay,
                "price_to_beat": gamma_result.get("price_to_beat"),
            })

            if not match:
                await tg_notify(
                    f"🚨 <b>CHAINLINK MISMATCH</b>\n"
                    f"Coin: {coin.upper()}\n"
                    f"Epoch: {epoch}\n"
                    f"Bot said: {bot_self_resolve}\n"
                    f"Gamma says: {gamma_outcome}\n"
                    f"PriceToBeat: {gamma_result.get('price_to_beat')}"
                )
        else:
            stats.chainlink_skips += 1
            logger.info(f"Chainlink check {coin} epoch={epoch}: bot had no self-resolve (skip)")
            log_event({
                "type": "chainlink_skip",
                "coin": coin,
                "epoch": epoch,
                "gamma_resolve": gamma_outcome,
                "gamma_delay_s": gamma_delay,
            })

        # ── Step 3: Feed into independent strategy verifier ──
        # Check what bot's strategy state is BEFORE this outcome
        bot_strategy_state = bot_coin_state.get("state", "SCANNING")
        bot_wait_side = bot_coin_state.get("wait_side")
        v_state = strategy_v.states[coin]
        v_state_before = v_state.state
        v_wait_before = v_state.wait_side

        # Check if states match BEFORE processing
        state_match = (bot_strategy_state == v_state_before)
        if not state_match:
            logger.warning(
                f"State mismatch BEFORE outcome {coin} epoch={epoch}: "
                f"bot={bot_strategy_state}(wait={bot_wait_side}) "
                f"verifier={v_state_before}(wait={v_wait_before})"
            )

        # Process outcome in verifier
        v_trigger = strategy_v.on_outcome(coin, gamma_outcome, epoch)

        # Check if bot also triggered (parse bot.log for TRIGGER line)
        bot_triggered = check_bot_log_for_trigger(coin, epoch)

        trigger_match = (bool(v_trigger) == bot_triggered)
        if v_trigger or bot_triggered:
            stats.strategy_matches += (1 if trigger_match else 0)
            stats.strategy_mismatches += (0 if trigger_match else 1)

            status = "✅" if trigger_match else "❌ MISMATCH"
            logger.info(
                f"Trigger check {coin} epoch={epoch}: "
                f"verifier={'YES' if v_trigger else 'NO'} "
                f"bot={'YES' if bot_triggered else 'NO'} → {status}"
            )

            log_event({
                "type": "trigger_check",
                "coin": coin,
                "epoch": epoch,
                "verifier_triggered": bool(v_trigger),
                "bot_triggered": bot_triggered,
                "match": trigger_match,
                "verifier_streak": v_trigger.get("streak") if v_trigger else None,
                "verifier_bet_side": v_trigger.get("bet_side") if v_trigger else None,
            })

            if not trigger_match:
                await tg_notify(
                    f"🚨 <b>STRATEGY MISMATCH</b>\n"
                    f"Coin: {coin.upper()}\n"
                    f"Epoch: {epoch}\n"
                    f"Verifier trigger: {'YES' if v_trigger else 'NO'}\n"
                    f"Bot trigger: {'YES' if bot_triggered else 'NO'}\n"
                    f"Verifier history: {v_state.history[-5:]}"
                )

        # Record bet result in verifier if there was a trigger on THIS epoch
        # (the trigger was placed in the previous window for THIS epoch)
        v_bet_for_this = None
        for t in v_state.triggers:
            if t.get("epoch") == epoch - WINDOW:  # trigger was set last window
                v_bet_for_this = t
                break
        if v_bet_for_this:
            won = (v_bet_for_this["bet_side"] == gamma_outcome)
            pnl = 2.50  # 5 shares × $0.50
            strategy_v.record_result(coin, won, pnl)
            log_event({
                "type": "bet_result",
                "coin": coin,
                "epoch": epoch,
                "bet_side": v_bet_for_this["bet_side"],
                "outcome": gamma_outcome,
                "won": won,
                "pnl": pnl if won else -pnl,
            })

        # Log state check
        log_event({
            "type": "state_check",
            "coin": coin,
            "epoch": epoch,
            "outcome": gamma_outcome,
            "bot_state": bot_strategy_state,
            "bot_wait_side": bot_wait_side,
            "verifier_state": v_state.state,
            "verifier_wait_side": v_state.wait_side,
            "verifier_history": v_state.history[-6:],
            "state_match": v_state.state == bot_strategy_state,
        })

    # ── Accuracy alert ──
    if stats.recent_chainlink_accuracy < 0.95 and len(stats.recent_accuracy) >= 10:
        await tg_notify(
            f"🚨 <b>LOW CHAINLINK ACCURACY</b>\n"
            f"Recent: {stats.recent_chainlink_accuracy:.1%}\n"
            f"Total: {stats.chainlink_accuracy:.1%}\n"
            f"Matches: {stats.chainlink_matches}, Mismatches: {stats.chainlink_mismatches}"
        )


async def run():
    """Main loop — fires after each 15-min window."""
    logger.info("=" * 60)
    logger.info("Series Reversal Bot — Parallel Verifier starting")
    logger.info(f"Coins: {COINS} | Trigger: {TRIGGER_N} | Window: {WINDOW}s")
    logger.info("=" * 60)

    strategy_v = PureStrategyVerifier(COINS, TRIGGER_N)
    stats = Stats()

    kwargs = {"timeout": 15}
    if PROXY_URL:
        kwargs["proxy"] = PROXY_URL
    client = httpx.AsyncClient(**kwargs)

    # ── Initial recovery: get last 6 windows from Gamma ──
    logger.info("Recovering verifier history from Gamma...")
    now = int(time.time())
    current_epoch = now - (now % WINDOW)

    for lookback in range(6, 0, -1):
        ep = current_epoch - (lookback * WINDOW)
        for coin in COINS:
            result = await fetch_gamma_outcome(client, coin, ep)
            if result:
                strategy_v.on_outcome(coin, result["outcome"], ep)
                logger.debug(f"Recovered {coin} epoch={ep}: {result['outcome']}")
            else:
                strategy_v.states[coin].history.append("GAP")
                logger.debug(f"Recovered {coin} epoch={ep}: GAP (not resolved)")
        await asyncio.sleep(0.5)

    for coin in COINS:
        h = strategy_v.states[coin].history
        logger.info(f"  {coin}: history={h[-6:]} state={strategy_v.states[coin].state}")

    await tg_notify(
        "🔍 <b>Verifier started</b>\n"
        f"Coins: {', '.join(c.upper() for c in COINS)}\n"
        f"Mode: parallel verification\n"
        "Checking: Chainlink accuracy + strategy logic"
    )

    # ── FIX #19: Verify the in-progress epoch at startup ──
    # Recovery only fetches fully resolved past epochs. The epoch that was
    # in-progress when we started may not be resolved yet. Wait for it to end
    # and resolve via Gamma so we don't have a gap in history.
    now = time.time()
    current_epoch = int(now) - (int(now) % WINDOW)
    boundary = current_epoch + WINDOW
    wait_for_current = boundary - now + 10  # wait until end + 10s buffer
    if wait_for_current > 0:
        logger.info(f"FIX #19: Waiting {wait_for_current:.0f}s for in-progress epoch={current_epoch} to end")
        await asyncio.sleep(wait_for_current)
    # Now try to fetch and feed the epoch that just ended
    for coin in COINS:
        result = await fetch_gamma_outcome(client, coin, current_epoch)
        if result:
            strategy_v.on_outcome(coin, result["outcome"], current_epoch)
            logger.debug(f"FIX #19: Recovered in-progress {coin} epoch={current_epoch}: {result['outcome']}")
        else:
            strategy_v.states[coin].history.append("GAP")
            logger.debug(f"FIX #19: {coin} epoch={current_epoch}: GAP")
    # Update current_epoch to now (after waiting)
    now2 = time.time()
    current_epoch = int(now2) - (int(now2) % WINDOW)

    # ── Main loop ──
    last_epoch = current_epoch
    report_counter = 0

    while True:
        try:
            # Wait until next window boundary + 10s buffer
            next_end = current_epoch + WINDOW + WINDOW  # next window end
            # Actually: wait until the end of the current window
            now = time.time()
            current_epoch = int(now) - (int(now) % WINDOW)
            next_boundary = current_epoch + WINDOW
            wait = next_boundary - now + 10  # 10s buffer after boundary

            if wait > 0:
                logger.info(f"Waiting {wait:.0f}s until window boundary + 10s")
                await asyncio.sleep(wait)

            # The epoch that just ended
            epoch = current_epoch
            if epoch == last_epoch:
                await asyncio.sleep(30)
                continue

            last_epoch = epoch
            logger.info(f"=== Verifying window epoch={epoch} ===")

            # Verify this window (has internal retry logic for Gamma)
            await verify_window(epoch, client, strategy_v, stats)

            # Periodic summary (every 4 windows = 1 hour)
            report_counter += 1
            if report_counter % 4 == 0:
                summary = stats.summary()
                logger.info(summary)

                # Build verifier PnL summary
                v_lines = []
                for coin in COINS:
                    cs = strategy_v.states[coin]
                    if cs.total_bets > 0:
                        v_lines.append(
                            f"  {coin.upper()}: {cs.wins}W/{cs.losses}L ${cs.balance:+.2f}"
                        )

                tg_text = f"📊 <b>Verifier Report</b> ({report_counter} windows)\n"
                tg_text += f"🎯 Chainlink: {stats.chainlink_accuracy:.1%} ({stats.chainlink_matches}✅ {stats.chainlink_mismatches}❌ {stats.chainlink_skips}⏭)\n"
                tg_text += f"🧠 Strategy: {stats.strategy_accuracy:.1%} ({stats.strategy_matches}✅ {stats.strategy_mismatches}❌)\n"
                if v_lines:
                    tg_text += "📈 Verifier PnL:\n" + "\n".join(v_lines)
                await tg_notify(tg_text)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Verifier loop error: {e}")
            await asyncio.sleep(30)

    await client.aclose()


if __name__ == "__main__":
    print("🔍 Series Reversal Bot — Parallel Verifier")
    print(f"   Coins: {', '.join(c.upper() for c in COINS)}")
    print(f"   Trigger: {TRIGGER_N} consecutive")
    print(f"   Gamma retry: {GAMMA_RETRY_DELAYS}")
    print()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n👋 Verifier stopped.")
