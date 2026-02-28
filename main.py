#!/usr/bin/env python3
"""Series Reversal Bot — main orchestrator."""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time

from config import (
    setup_logging, STATE_FILE, COINS, MODE, BET_SIZE, BET_PRICE, TRIGGER_N,
    GAMMA_API, GAMMA_BET_ALLOWED,  # FIX #11
)
from http_client import get_client, close_client  # FIX #14
from chainlink import ChainlinkFeed
from market_resolver import MarketResolver, WINDOW, current_window_start, next_window_end
from token_cache import TokenCache
from strategy import Strategy
from order_engine import OrderEngine
from user_ws import UserWebSocket
from redeemer import Redeemer
from notifier import notify

logger: logging.Logger = None  # type: ignore


# ---------- State persistence ----------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    return {}


def save_state(strategy: Strategy, engine: OrderEngine, resolver: MarketResolver, last_epoch: int, redeemer=None):
    data = {
        "coins": strategy.dump_states(),
        "orders": engine.dump_orders(),
        "verification": resolver.stats,
        "market_records": resolver.dump_records(),
        "last_epoch": last_epoch,
        "updated_at": time.time(),
        "redeemer_pending": redeemer.dump_pending() if redeemer else [],  # FIX #9
    }
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


# ---------- Main loop ----------

async def window_loop(
    feed: ChainlinkFeed,
    resolver: MarketResolver,
    cache: TokenCache,
    strategy: Strategy,
    engine: OrderEngine,
    redeemer: Redeemer,
    user_ws=None,  # FIX #2: user WS for market subscription
):
    """Main loop — fires at each 15-min window boundary."""
    global _recovered  # FIX #10
    last_epoch = 0
    state = load_state()
    if state:
        strategy.load_states(state.get("coins", {}))
        engine.load_orders(state.get("orders", []))
        resolver.stats = state.get("verification", resolver.stats)
        resolver.load_records(state.get("market_records", {}))
        last_epoch = state.get("last_epoch", 0)
        # FIX #9: restore redeemer pending
        redeemer_pending = state.get("redeemer_pending", [])
        if redeemer_pending:
            redeemer.load_pending(redeemer_pending)
        logger.info(f"Resumed from state. Last epoch: {last_epoch}")

    # Always recover history from Gamma on startup (ground truth)
    logger.info("Recovering recent outcomes from Gamma API...")
    recovered_count, last_recovered_epoch = await resolver.recover_history(strategy)
    logger.info(f"Recovered {recovered_count} outcomes total")
    for coin in COINS:
        h = strategy.states[coin].history
        logger.info(f"  {coin}: history={h[-6:]} (len={len(h)})")

    # FIX #1: Recovery state sync — check if WAITING_FOR_CHANGE needs update
    for coin in COINS:
        cs = strategy.states[coin]
        if cs.state == "WAITING_FOR_CHANGE" and cs.history and cs.history[-1] != cs.wait_side:
            cs.state = "SCANNING"
            cs.wait_side = None
            logger.info(f"{coin}: Recovery detected side change, back to SCANNING")

    # CRITICAL: set last_epoch to CURRENT window start, not state.json
    now = int(time.time())
    last_epoch = now - (now % WINDOW)

    # FIX #12: Handle blind epochs between last_recovered and current
    if last_recovered_epoch > 0 and last_recovered_epoch < last_epoch - WINDOW:
        blind_ep = last_recovered_epoch + WINDOW
        while blind_ep < last_epoch:
            for coin in COINS:
                strategy.states[coin].history.append("GAP")
                strategy.states[coin].verified.append(False)  # FIX #17
            logger.warning(f"Blind epoch {blind_ep} — GAP inserted for all coins")
            blind_ep += WINDOW

    # FIX #19: Wait for in-progress epoch to end and resolve via Gamma
    # Recovery only fetches fully resolved past epochs. The epoch that was
    # in-progress when we started may not be resolved yet. Wait for it
    # so we don't have a gap that causes false triggers.
    boundary = last_epoch + WINDOW
    wait_for_current = boundary - time.time() + 15  # wait until end + 15s for Gamma
    if wait_for_current > 0 and wait_for_current < WINDOW + 30:
        logger.info(f"FIX #19: Waiting {wait_for_current:.0f}s for in-progress epoch={last_epoch} to end and resolve")
        await asyncio.sleep(wait_for_current)
    # Now try to resolve the epoch that just ended via Gamma
    from http_client import get_client
    gamma_client = get_client()
    for coin in COINS:
        for attempt in range(10):  # retry up to 10 times (Gamma may be slow)
            try:
                slug = f"{coin}-updown-15m-{last_epoch}"
                resp = await gamma_client.get(f"https://gamma-api.polymarket.com/events?slug={slug}")
                data = resp.json()
                if data:
                    for m in data[0].get("markets", []):
                        if m.get("umaResolutionStatus") == "resolved":
                            import json as _json
                            op = _json.loads(m["outcomePrices"]) if isinstance(m["outcomePrices"], str) else m["outcomePrices"]
                            oc = _json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
                            outcome = oc[0] if float(op[0]) > 0.5 else oc[1]
                            strategy.on_outcome(coin, outcome, verified=False)
                            logger.info(f"FIX #19: Resolved in-progress {coin} epoch={last_epoch}: {outcome}")
                            break
                    else:
                        if attempt < 9:
                            logger.debug(f"FIX #19: {coin} epoch={last_epoch} not yet resolved, retry {attempt+1}/10 in 45s")
                            await asyncio.sleep(45)
                            continue
                        else:
                            strategy.states[coin].history.append("GAP")
                            strategy.states[coin].verified.append(False)
                            logger.warning(f"FIX #19: {coin} epoch={last_epoch} still unresolved after 10 retries, GAP")
                break
            except Exception as e:
                logger.warning(f"FIX #19: Gamma error {coin}: {e}")
                if attempt < 9:
                    await asyncio.sleep(15)
    # Update last_epoch — set to PREVIOUS window so main loop processes the current one
    now2 = int(time.time())
    last_epoch = (now2 - (now2 % WINDOW)) - WINDOW

    logger.info(f"last_epoch set to current window: {last_epoch} (last_recovered: {last_recovered_epoch})")
    # FIX #27: Reset BET_ACTIVE to SCANNING if no active order exists for that coin
    for coin in COINS:
        cs = strategy.states[coin]
        if cs.state == "BET_ACTIVE":
            has_active = any(
                r.coin == coin and r.status in ("live", "matched", "confirmed", "pending")
                for r in engine.orders
            )
            if not has_active:
                logger.info(f"FIX #27: {coin} was BET_ACTIVE but no active order found, resetting to SCANNING")
                cs.state = "SCANNING"
    for coin in COINS:
        h = strategy.states[coin].history
        logger.info(f"  {coin}: final history={h[-6:]} state={strategy.states[coin].state}")
    # FIX #24: Check any live orders from before restart and settle them
    # Recovery resolved epochs via Gamma but didn't check if we had bets on those epochs
    settled = []
    for rec in list(engine.orders):
        if rec.status not in ("live", "matched", "confirmed"):
            continue
        # Find the outcome for this order's epoch from history
        # We need to check what the resolved outcome was
        for coin_name in COINS:
            if rec.coin != coin_name:
                continue
            # Query Gamma for this specific epoch
            try:
                slug = f"{coin_name}-updown-15m-{rec.epoch}"
                resp_settle = await gamma_client.get(f"https://gamma-api.polymarket.com/events?slug={slug}")
                data_settle = resp_settle.json()
                if data_settle:
                    for m_settle in data_settle[0].get("markets", []):
                        if m_settle.get("umaResolutionStatus") == "resolved":
                            import json as _json2
                            op2 = _json2.loads(m_settle["outcomePrices"]) if isinstance(m_settle["outcomePrices"], str) else m_settle["outcomePrices"]
                            oc2 = _json2.loads(m_settle["outcomes"]) if isinstance(m_settle["outcomes"], str) else m_settle["outcomes"]
                            outcome2 = oc2[0] if float(op2[0]) > 0.5 else oc2[1]
                            won = (rec.side == outcome2)
                            actual_cost = rec.size * rec.price
                            pnl = actual_cost if won else actual_cost
                            strategy.record_bet_result(coin_name, won, pnl, bet_side=rec.side)
                            rec.status = "settled"
                            emoji = "✅" if won else "❌"
                            logger.info(f"FIX #24: Settled {coin_name} epoch={rec.epoch}: bet {rec.side}, result {outcome2} → {emoji} {'+'if won else '-'}${pnl:.2f}")
                            asyncio.create_task(notify(
                                f"{emoji} <b>{coin_name.upper()}</b> (recovery settle) "
                                f"bet {rec.side}, result {outcome2} → {'+'if won else '-'}${pnl:.2f}"
                            ))
                            settled.append(rec.order_id)
                            break
            except Exception as e:
                logger.warning(f"FIX #24: Could not settle {coin_name} epoch={rec.epoch}: {e}")
    if settled:
        logger.info(f"FIX #24: Settled {len(settled)} orders from before restart")

    _recovered = True  # FIX #10: mark recovery done
    save_state(strategy, engine, resolver, last_epoch, redeemer)

    while True:
        try:
            # Wait until ~2s before window end
            end_ts = next_window_end()
            wait = end_ts - time.time() - 2.0
            if wait > 0:
                logger.debug(f"Sleeping {wait:.1f}s until window end")
                await asyncio.sleep(wait)

            # Wait for exact window boundary + 2s for Chainlink delivery
            # RTDS delivers boundary-timestamp prices ~1-1.5s after the actual second
            remaining = end_ts - time.time() + 2.0
            if remaining > 0:
                await asyncio.sleep(remaining)

            # Record boundary prices RIGHT at window boundary
            # End price of current window = Start price of next window
            boundary_epoch = int(end_ts)
            for coin in COINS:
                price = feed.get_price_exact(coin, boundary_epoch * 1000, tolerance_ms=2000)
                if price is not None:
                    resolver.record_boundary(coin, boundary_epoch, price)
                else:
                    logger.warning(f"No boundary price for {coin} at {boundary_epoch}")

            epoch = int(end_ts) - WINDOW
            if epoch == last_epoch:
                await asyncio.sleep(5)
                continue

            last_epoch = epoch
            logger.info(f"=== Window {epoch} ended ===")
            resolver.cleanup_boundaries(epoch)

            # FIX #7: Check for stale price data
            for coin in COINS:
                if feed.is_stale(coin, max_age=60):
                    logger.warning(f"Stale price data for {coin} (>60s old)")

            # FIX #8 + FIX #34: Cancel stale orders and rollback state
            cancelled_coins = await engine.cancel_stale_orders()
            for cc in cancelled_coins:
                cs = strategy.states.get(cc)
                if cs and cs.state == "BET_ACTIVE":
                    cs.state = "SCANNING"
                    logger.info(f"FIX #34: {cc} rolled back to SCANNING after stale cancel (history={cs.history[-3:]})")
                    await notify(f"⏰ <b>{cc.upper()}</b> ордер не заполнился, отменён. Продолжаю scanning.")

            # Coins that self-resolved vs need Gamma fallback
            gamma_pending = []

            for coin in COINS:
                outcome = resolver.resolve_window(coin, epoch)
                if outcome is None:
                    # No boundary price (e.g. restart mid-window) → Gamma fallback
                    logger.warning(f"Could not self-resolve {coin} epoch={epoch}, will use Gamma")
                    gamma_pending.append(coin)
                    continue

                # Record self-resolve for verifier audit trail
                resolver._record_market(coin, epoch, outcome, source="self")
                _process_outcome(coin, epoch, outcome, strategy, cache, engine, resolver, redeemer, user_ws=user_ws)  # FIX #2

            # For coins without boundary: wait for Gamma resolve (async)
            if gamma_pending:
                asyncio.create_task(
                    _gamma_fallback_resolve(
                        gamma_pending, epoch, resolver, strategy,
                        cache, engine, redeemer, user_ws=user_ws,  # FIX #2
                    )
                )

            # FIX #33: Resolve tokens and batch all pending bets
            if _pending_bets:
                batch_ready = []
                for info in _pending_bets:
                    token_info = await cache.get(info["coin"], info["epoch"])
                    if token_info:
                        token_id = (
                            token_info.up_token
                            if info["token_type"] == "up"
                            else token_info.down_token
                        )
                        info["token_id"] = token_id
                        batch_ready.append(info)
                    else:
                        logger.error(f"No token for {info['coin']} epoch={info['epoch']}")
                        cs = info.get("coin_state")
                        if cs and cs.state == "BET_ACTIVE":
                            cs.state = "SCANNING"
                            logger.info(f"FIX #32: {info['coin']} rolled back to SCANNING (no token)")

                if batch_ready:
                    batch_results = await engine.place_orders_batch(batch_ready)
                    for i, rec in enumerate(batch_results):
                        if rec:
                            info = batch_ready[i]
                            await notify(
                                f"🎯 <b>{info['coin'].upper()}</b> BET {info['side']} "
                                f"({rec.size:.1f} shares @ ${info['price']})"
                            )
                _pending_bets.clear()

            save_state(strategy, engine, resolver, last_epoch, redeemer)  # FIX #9

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Window loop error: {e}")
            await asyncio.sleep(10)


def _process_outcome(
    coin: str, epoch: int, outcome: str,
    strategy: Strategy, cache: TokenCache, engine: OrderEngine,
    resolver: MarketResolver, redeemer: Redeemer,
    verified: bool = True,
    user_ws=None,  # FIX #2
):
    """Process a resolved outcome.
    verified=True: self-resolved with boundary prices, can place bets.
    verified=False: from Gamma fallback, history only, NO bets (unless GAMMA_BET_ALLOWED)."""

    # Check if we had a bet on THIS epoch and record result
    bet = engine.get_bet_for_epoch(coin, epoch)
    if bet:
        won = (bet.side == outcome)
        # FIX #3: PnL based on real order size, not BET_SIZE
        actual_cost = bet.size * bet.price
        pnl = actual_cost if won else -actual_cost
        strategy.record_bet_result(coin, won, pnl, bet_side=bet.side)
        emoji = "✅" if won else "❌"
        asyncio.create_task(notify(
            f"{emoji} <b>{coin.upper()}</b> {'WIN' if won else 'LOSS'} "
            f"(bet {bet.side}, result {outcome}) → {'+'if won else '-'}${pnl:.2f}"
        ))

    # Feed outcome into strategy — FIX #17: pass verified flag
    signal_bet = strategy.on_outcome(coin, outcome, verified=verified)

    # FIX #11: allow bets on Gamma fallback if GAMMA_BET_ALLOWED
    can_bet = verified or GAMMA_BET_ALLOWED

    if signal_bet and can_bet:
        # FIX #33: Collect bet info for batch (token lookup in async context later)
        next_epoch = epoch + WINDOW
        _pending_bets.append({
            "coin": coin,
            "side": signal_bet.side,
            "token_type": signal_bet.token_type,
            "price": BET_PRICE,
            "epoch": next_epoch,
            "coin_state": strategy.states.get(coin),
        })
    elif signal_bet and not can_bet:
        logger.info(
            f"{coin}: TRIGGER detected but window unverified (Gamma fallback) — NO BET"
        )

    # Schedule Gamma verification + redeem (only if we had a bet)
    if verified:
        asyncio.create_task(
            _verify_and_redeem(resolver, redeemer, cache, engine, coin, epoch, outcome)
        )


async def _place_bet(coin, epoch, signal_bet, cache, engine, user_ws=None, coin_state=None):  # FIX #2
    """Place a bet order for the NEXT window."""
    next_epoch = epoch + WINDOW
    # Guard: no double bets on same coin/epoch (FIX #31: enhanced check)
    existing = engine.get_bet_for_epoch(coin, next_epoch)
    if existing:
        logger.warning(f"Already have bet for {coin} epoch={next_epoch}, skipping")
        return
    if engine.has_active_order_for_epoch(coin, next_epoch):
        logger.warning(f"FIX #31: Active order exists for {coin} epoch={next_epoch}, skipping")
        return
    token_info = await cache.get(coin, next_epoch)
    if token_info:
        token_id = (
            token_info.up_token
            if signal_bet.token_type == "up"
            else token_info.down_token
        )
        rec = await engine.place_order(
            coin=coin,
            token_id=token_id,
            side=signal_bet.side,
            price=BET_PRICE,
            epoch=next_epoch,
        )
        if rec:
            await notify(
                f"🎯 <b>{coin.upper()}</b> BET {signal_bet.side} "
                f"({rec.size:.1f} shares @ ${BET_PRICE})"
            )
            # User WS streams all events automatically — no per-market subscribe needed
        else:
            # FIX #32: Rollback BET_ACTIVE → SCANNING if order placement failed
            logger.error(f"FIX #32: Order placement failed for {coin} epoch={next_epoch}, rolling back to SCANNING")
            if coin_state and coin_state.state == "BET_ACTIVE":
                coin_state.state = "SCANNING"
                logger.info(f"FIX #32: {coin} rolled back to SCANNING")
    else:
        logger.error(f"No token for {coin} epoch={next_epoch}")
        # FIX #32: Also rollback if no token found
        if coin_state and coin_state.state == "BET_ACTIVE":
            coin_state.state = "SCANNING"
            logger.info(f"FIX #32: {coin} rolled back to SCANNING (no token)")


async def _gamma_fallback_resolve(
    coins: list[str], epoch: int,
    resolver: MarketResolver, strategy: Strategy,
    cache: TokenCache, engine: OrderEngine,
    redeemer: Redeemer,
    user_ws=None,  # FIX #2
):
    """Wait for Gamma API to resolve markets we couldn't self-resolve.
    Retries every 30s for up to 5 minutes."""
    import json as _json
    remaining = set(coins)
    max_retries = 20  # 20 × 30s = 10 min

    client = get_client()  # FIX #14: shared httpx client
    if True:  # kept block indent
        for attempt in range(max_retries):
            await asyncio.sleep(30)

            for coin in list(remaining):
                slug = f"{coin}-updown-15m-{epoch}"
                try:
                    resp = await client.get(
                        f"{GAMMA_API}/events", params={"slug": slug}
                    )
                    if resp.status_code != 200 or not resp.json():
                        continue
                    event = resp.json()[0]
                    market = event["markets"][0]

                    if market.get("umaResolutionStatus") != "resolved":
                        continue

                    outcome_prices = market.get("outcomePrices", "[]")
                    if isinstance(outcome_prices, str):
                        outcome_prices = _json.loads(outcome_prices)
                    outcomes_list = market.get("outcomes", "[]")
                    if isinstance(outcomes_list, str):
                        outcomes_list = _json.loads(outcomes_list)

                    resolved_outcome = None
                    for oc, price in zip(outcomes_list, outcome_prices):
                        if price == "1":
                            resolved_outcome = oc
                            break

                    if not resolved_outcome:
                        continue

                    meta = event.get("eventMetadata", {})
                    ptb = meta.get("priceToBeat")

                    logger.info(
                        f"GAMMA RESOLVED {coin} epoch={epoch}: {resolved_outcome} "
                        f"(priceToBeat=${ptb})"
                    )
                    resolver._record_market(coin, epoch, resolved_outcome, ptb)

                    _process_outcome(
                        coin, epoch, resolved_outcome, strategy,
                        cache, engine, resolver, redeemer,
                        verified=False, user_ws=user_ws,  # FIX #2
                    )
                    remaining.remove(coin)

                except Exception as e:
                    logger.warning(f"Gamma fallback error {coin} epoch={epoch}: {e}")

            if not remaining:
                logger.info(f"All Gamma fallbacks resolved for epoch={epoch}")
                return

    for coin in remaining:
        logger.error(f"FAILED to resolve {coin} epoch={epoch} after {max_retries} retries — SKIPPING")


async def _verify_and_redeem(
    resolver: MarketResolver, redeemer: Redeemer,
    cache: TokenCache, engine: OrderEngine,
    coin: str, epoch: int, outcome: str,
):
    """Verify resolution with Gamma and schedule redemption if we had a bet."""
    await asyncio.sleep(90)  # Wait for Gamma to update
    try:
        match = await resolver.verify_with_gamma(coin, epoch, outcome)
        if match is True:
            logger.info(f"Verified {coin} epoch={epoch}: ✓ match")
        elif match is False:
            logger.error(f"MISMATCH {coin} epoch={epoch}!")
            print(f"⚠️ RESOLUTION MISMATCH: {coin} epoch={epoch}")
            await notify(f"⚠️ <b>MISMATCH</b> {coin.upper()} epoch={epoch}")

        # Schedule redeem only if we had a bet on this epoch
        bet = engine.get_bet_for_epoch(coin, epoch)
        if bet:
            token_info = await cache.get(coin, epoch)
            if token_info:
                redeemer.schedule_redeem(coin, epoch, token_info.condition_id)
    except Exception as e:
        logger.warning(f"Verify/redeem scheduling error: {e}")


# FIX #10: Recovery flag — prevents watchdog from triggering duplicate recovery
_recovered = False

# FIX #18: Collect pending bet coroutines to await before save_state
_pending_bets: list = []


# FIX #16: Periodic health check to Telegram
async def _health_check_loop(strategy, engine, feed):
    """Send a brief status to Telegram every 15 minutes."""
    while True:
        try:
            await asyncio.sleep(900)  # 15 min
            lines = ["📊 <b>Health Check</b>"]
            for coin in COINS:
                cs = strategy.states[coin]
                stale = feed.is_stale(coin, max_age=60)
                stale_mark = "⚠️" if stale else "✅"
                lines.append(
                    f"  {stale_mark} {coin.upper()}: {cs.state} | "
                    f"H={cs.history[-3:]} | {cs.wins}W/{cs.losses}L | ${cs.balance:+.2f}"
                )
            # FIX #18: Count observation orders too; only current/next window
            now_epoch = current_window_start()
            active = sum(
                1 for o in engine.orders
                if o.status in ("live", "pending", "observation")
                and o.epoch >= now_epoch
            )
            lines.append(f"  📋 Active orders: {active}")
            await notify("\n".join(lines))
        except asyncio.CancelledError:
            break
        except Exception:
            pass


# ---------- Entry point ----------

async def main(args):
    global logger
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info(f"Series Reversal Bot starting — MODE={MODE}")
    logger.info(f"Coins: {COINS} | Trigger: {TRIGGER_N} | Bet: ${BET_SIZE} @ ${BET_PRICE}")
    logger.info("=" * 60)

    print(f"🤖 Series Reversal Bot [{MODE}]")
    print(f"   Coins: {', '.join(c.upper() for c in COINS)}")
    print(f"   Trigger: {TRIGGER_N} consecutive → bet reversal")
    print(f"   Bet size: ${BET_SIZE} @ ${BET_PRICE}")
    print()

    # Init components
    feed = ChainlinkFeed()
    resolver = MarketResolver(feed)
    cache = TokenCache()
    strategy = Strategy(COINS)
    engine = OrderEngine()
    engine.init_client()
    redeemer = Redeemer()

    def handle_user_event(data):
        """FIX #30/#31: Proper status mapping from User WS events with FAILED retry."""
        etype = data.get("type", "").upper()
        event_type = data.get("event_type", "").lower()
        
        if event_type == "trade" or etype == "TRADE":
            taker_oid = data.get("taker_order_id", "")
            ws_status = data.get("status", "").upper()
            
            if taker_oid:
                # Record every WS status in order history
                engine.record_ws_status(taker_oid, ws_status)
                
                # Map to internal status on terminal states only
                if ws_status == "CONFIRMED":
                    engine.update_order_status(taker_oid, "confirmed")
                elif ws_status == "FAILED":
                    engine.update_order_status(taker_oid, "failed")
                    # FIX #31: Trigger async retry for FAILED orders
                    failed_rec = engine.find_order(taker_oid)
                    if failed_rec and failed_rec.retry_count < 2:
                        logger.info(f"FIX #31: Scheduling retry for FAILED order {taker_oid[:20]}...")
                        asyncio.create_task(_handle_failed_order(failed_rec))
                    elif failed_rec:
                        logger.warning(f"FIX #31: Max retries reached for {failed_rec.coin}, notifying user")
                        asyncio.create_task(notify(
                            f"❌ <b>{failed_rec.coin.upper()}</b> ордер FAILED {failed_rec.retry_count + 1}x — "
                            f"retry лимит достигнут. Проверьте вручную."
                        ))
                elif ws_status == "MATCHED":
                    engine.update_order_status(taker_oid, "matched")
                # MINED, RETRYING — log only, don't change status
                
                logger.info(f"Trade (taker): {taker_oid[:20]} ws_status={ws_status}")
            
            for mo in data.get("maker_orders", []):
                mo_id = mo.get("order_id", "")
                if mo_id:
                    logger.debug(f"Trade (maker): {mo_id[:20]}")
        elif etype == "CANCELLATION":
            oid = data.get("id", "")
            engine.update_order_status(oid, "cancelled")
            logger.info(f"Order CANCELLED (ws): {oid[:20]}")

    async def _handle_failed_order(failed_rec):
        """FIX #31: Handle a FAILED order — wait, then retry if safe."""
        coin = failed_rec.coin
        logger.info(f"FIX #31: Handling FAILED order for {coin}, waiting 3s for potential RETRYING...")
        
        # Wait 3 seconds — Polymarket might send RETRYING → MINED → CONFIRMED
        await asyncio.sleep(3)
        
        # Check if status changed during wait (RETRYING → CONFIRMED)
        if failed_rec.status == "confirmed":
            logger.info(f"FIX #31: {coin} order confirmed during wait — no retry needed")
            return
        
        # Still failed — attempt retry
        new_rec = await engine.retry_failed_order(failed_rec)
        if new_rec:
            await notify(
                f"🔄 <b>{coin.upper()}</b> retry #{new_rec.retry_count} — "
                f"новый ордер {new_rec.order_id[:12]}..."
            )
        else:
            logger.info(f"FIX #31: Retry skipped/blocked for {coin}")

    user_ws = UserWebSocket(on_event=handle_user_event)

    # FIX #29: Expose user_ws globally for tg_commands position tracking
    import tg_commands as _tg_cmd
    _tg_cmd.USER_WS = user_ws

    # FIX #31: Connect PositionTracker to engine for duplicate protection
    engine.set_position_tracker(user_ws.positions)

    # Shutdown handler
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _shutdown(sig):
        logger.info(f"Received {sig}, shutting down...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    # Launch tasks
    from tg_commands import polling_loop as tg_polling_loop
    tasks = [
        asyncio.create_task(feed.run(), name="chainlink"),
        asyncio.create_task(cache.refresh_loop(), name="cache"),
        asyncio.create_task(user_ws.run(), name="user_ws"),
        asyncio.create_task(redeemer.run(), name="redeemer"),
        asyncio.create_task(tg_polling_loop(), name="tg_commands"),
    ]

    # Wait for price feed
    try:
        print("⏳ Connecting to Chainlink feed...")
        await feed.wait_connected(timeout=30)
        print("✅ Chainlink connected")
    except asyncio.TimeoutError:
        logger.error("Failed to connect to Chainlink within 30s")
        print("❌ Chainlink connection timeout")

    # Preload tokens (3 windows ahead only)
    print("📦 Preloading token cache...")
    await cache.preload(count=3)
    print(f"✅ Token cache ready")

    # Main loop — FIX #2: pass user_ws
    loop_task = asyncio.create_task(
        window_loop(feed, resolver, cache, strategy, engine, redeemer, user_ws=user_ws),
        name="main_loop",
    )
    tasks.append(loop_task)

    # FIX #16: Health check task
    health_task = asyncio.create_task(
        _health_check_loop(strategy, engine, feed), name="health_check"
    )
    tasks.append(health_task)

    logger.info("All tasks launched. Entering main wait loop...")
    print("🚀 Bot running. Waiting for window events...")

    # Monitor tasks and restart if dead
    task_factories = {
        "chainlink": lambda: asyncio.create_task(feed.run(), name="chainlink"),
        "cache": lambda: asyncio.create_task(cache.refresh_loop(), name="cache"),
        "user_ws": lambda: asyncio.create_task(user_ws.run(), name="user_ws"),
        "redeemer": lambda: asyncio.create_task(redeemer.run(), name="redeemer"),
        "main_loop": lambda: asyncio.create_task(
            window_loop(feed, resolver, cache, strategy, engine, redeemer, user_ws=user_ws),  # FIX #2
            name="main_loop",
        ),
        "health_check": lambda: asyncio.create_task(  # FIX #16
            _health_check_loop(strategy, engine, feed), name="health_check"
        ),
        "tg_commands": lambda: asyncio.create_task(
            tg_polling_loop(), name="tg_commands"
        ),
    }

    async def watchdog():
        while not shutdown_event.is_set():
            for i, t in enumerate(tasks):
                if t.done() and not t.cancelled():
                    exc = t.exception() if not t.cancelled() else None
                    name = t.get_name()
                    logger.error(f"Task {name} exited unexpectedly! exc={exc}")
                    print(f"❌ Task {name} died: {exc}")
                    # FIX #10: Don't restart main_loop if recovery already done
                    if name == "main_loop" and _recovered:
                        logger.info(f"main_loop died post-recovery, restarting without re-recovery")
                    # Restart if we have a factory
                    if name in task_factories:
                        logger.info(f"Restarting task {name}...")
                        tasks[i] = task_factories[name]()
                        await notify(f"🔄 Task <b>{name}</b> restarted (was: {exc})")
            await asyncio.sleep(5)

    wd = asyncio.create_task(watchdog(), name="watchdog")

    # Wait for shutdown
    await shutdown_event.wait()
    logger.info("Shutdown event received")
    wd.cancel()

    logger.info("Shutting down gracefully...")
    # FIX #28: Close WebSockets FIRST with proper close code, THEN cancel tasks
    # This prevents 429 rate limits on reconnect — server sees clean disconnect
    logger.info("Closing WebSockets gracefully...")
    await user_ws.stop()
    await feed.stop()
    await asyncio.sleep(1)  # Give server time to process close frames
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await close_client()  # FIX #14: close shared httpx client

    # Final save — FIX #9: include redeemer
    save_state(strategy, engine, resolver, current_window_start(), redeemer)
    logger.info("Bot stopped.")
    print("👋 Bot stopped.")


def cli():
    parser = argparse.ArgumentParser(
        description="Series Reversal Bot for Polymarket 15-min crypto markets"
    )
    parser.add_argument(
        "--mode", choices=["OBSERVATION", "LIVE"], default=None,
        help="Override MODE (default: from .env or OBSERVATION)"
    )
    parser.add_argument(
        "--coins", type=str, default=None,
        help="Comma-separated coin list (default: btc,eth,sol,xrp)"
    )
    parser.add_argument(
        "--trigger", type=int, default=None,
        help="Number of consecutive outcomes to trigger (default: 3)"
    )
    parser.add_argument(
        "--bet-size", type=float, default=None,
        help="Bet size in USDC (default: 1.0)"
    )
    args = parser.parse_args()

    # Override config BEFORE any other imports use them
    import config as _cfg
    if args.mode:
        _cfg.MODE = args.mode
    if args.coins:
        _cfg.COINS = args.coins.split(",")
    if args.trigger:
        _cfg.TRIGGER_N = args.trigger
    if args.bet_size:
        _cfg.BET_SIZE = args.bet_size

    # Re-export to module-level names used by this file
    global MODE, COINS, TRIGGER_N, BET_SIZE
    MODE = _cfg.MODE
    COINS = _cfg.COINS
    TRIGGER_N = _cfg.TRIGGER_N
    BET_SIZE = _cfg.BET_SIZE

    asyncio.run(main(args))


if __name__ == "__main__":
    cli()
