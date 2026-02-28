"""Order engine — places and manages CLOB orders via py_clob_client.
FIX #30: Prefix matching for truncated WS order IDs.
FIX #31: FAILED retry logic with duplicate protection via PositionTracker.
"""

import asyncio
import logging
import time
from typing import Optional
from dataclasses import dataclass, field

from config import (
    CLOB_URL, PRIVATE_KEY, FUNDER_ADDRESS,
    API_KEY, API_SECRET, API_PASSPHRASE,
    SIGNATURE_TYPE, BET_SIZE, BET_PRICE, FILL_TIMEOUT,
    MODE, CHAIN_ID, PROXY_URL, ORDER_PROXY,
)

logger = logging.getLogger("order_engine")

# FIX #31: Max retry attempts for FAILED orders
MAX_RETRY = 2
RETRY_DELAY = 5  # seconds between retries


@dataclass
class OrderRecord:
    order_id: str = ""
    coin: str = ""
    side: str = ""  # "Up" or "Down" — what we bet ON
    token_id: str = ""
    price: float = 0.0
    size: float = 0.0
    status: str = "pending"  # pending, live, matched, confirmed, failed, cancelled, settled
    placed_at: float = 0.0
    filled_at: float = 0.0
    pnl: float = 0.0
    epoch: int = 0  # the NEXT window epoch this bet is for
    retry_count: int = 0  # FIX #31: how many times this order was retried
    ws_history: list = field(default_factory=list)  # FIX #31: [MATCHED, MINED, CONFIRMED] etc.

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "coin": self.coin,
            "side": self.side,
            "token_id": self.token_id,
            "price": self.price,
            "size": self.size,
            "status": self.status,
            "placed_at": self.placed_at,
            "filled_at": self.filled_at,
            "pnl": self.pnl,
            "epoch": self.epoch,
            "retry_count": self.retry_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "OrderRecord":
        known = {k for k in cls.__dataclass_fields__}
        filtered = {k: d[k] for k in d if k in known}
        return cls(**filtered)


class OrderEngine:
    """Manages order placement and tracking."""

    def __init__(self):
        self._client = None
        self.orders: list[OrderRecord] = []
        self._initialized = False
        self._position_tracker = None  # FIX #31: set by main.py

    def set_position_tracker(self, tracker):
        """Set reference to PositionTracker from UserWebSocket. Called once by main.py."""
        self._position_tracker = tracker

    def init_client(self):
        """Initialize the CLOB client. Call once after config is loaded."""
        if not PRIVATE_KEY:
            logger.warning("No PRIVATE_KEY configured — order engine disabled")
            return

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            import py_clob_client.http_helpers.helpers as clob_helpers
            import httpx as _httpx

            if ORDER_PROXY:
                clob_helpers._http_client = _httpx.Client(http2=True, proxy=ORDER_PROXY)
                logger.info(f"CLOB client using ORDER_PROXY")

            creds = ApiCreds(
                api_key=API_KEY,
                api_secret=API_SECRET,
                api_passphrase=API_PASSPHRASE,
            )
            self._client = ClobClient(
                CLOB_URL,
                key=PRIVATE_KEY,
                chain_id=CHAIN_ID,
                signature_type=SIGNATURE_TYPE,
                funder=FUNDER_ADDRESS,
                creds=creds,
            )
            self._initialized = True
            logger.info("CLOB client initialized")
        except Exception as e:
            logger.error(f"Failed to init CLOB client: {e}")

    # --- FIX #31: Duplicate protection ---

    def has_confirmed_position(self, token_id: str) -> bool:
        """Check if PositionTracker shows a CONFIRMED position for this token.
        This is the ONLY duplicate check — based on User WS events, not REST."""
        if self._position_tracker:
            snap = self._position_tracker.get_snapshot()
            for pos in snap["positions"]:
                if pos.get("asset_id") == token_id and pos.get("size", 0) > 0:
                    logger.info(f"FIX #31: PositionTracker shows existing position for token {token_id[:20]}...")
                    return True
        return False

    def has_active_order_for_epoch(self, coin: str, epoch: int) -> bool:
        """Check if there's already a live/matched/confirmed order for this coin+epoch.
        Prevents double ordering."""
        for rec in self.orders:
            if rec.coin == coin and rec.epoch == epoch and rec.status in ("live", "matched", "confirmed"):
                logger.info(f"FIX #31: Active order exists for {coin} epoch={epoch}: {rec.order_id[:20]}... status={rec.status}")
                return True
        return False

    # --- FIX #30: Find order by prefix match ---

    def find_order(self, order_id: str) -> Optional[OrderRecord]:
        """Find order by exact or prefix match (User WS sends truncated IDs)."""
        for rec in self.orders:
            if rec.order_id == order_id or rec.order_id.startswith(order_id) or order_id.startswith(rec.order_id):
                return rec
        return None

    # --- Order status updates ---

    def record_ws_status(self, order_id: str, ws_status: str):
        """Record a WS status event in order's history. FIX #31."""
        rec = self.find_order(order_id)
        if rec:
            rec.ws_history.append(ws_status)
            logger.info(f"WS history {rec.coin} {rec.order_id[:20]}...: {rec.ws_history}")

    def update_order_status(self, order_id: str, status: str):
        """FIX #30: Match by prefix — User WS sends truncated order IDs."""
        rec = self.find_order(order_id)
        if rec:
            old = rec.status
            # FIX #31: Don't downgrade from confirmed
            if old == "confirmed" and status != "settled":
                logger.info(f"Ignoring status change {old}->{status} for confirmed order {rec.coin}")
                return
            rec.status = status
            if status == "confirmed":
                rec.filled_at = time.time()
            logger.info(f"Order status: {rec.coin} {rec.order_id[:20]}... {old}->{status}")

    # --- Place order ---

    async def place_order(
        self, coin: str, token_id: str, side: str, price: float = BET_PRICE,
        epoch: int = 0, is_retry: bool = False,
    ) -> Optional[OrderRecord]:
        """Place a GTC limit buy order.
        
        FIX #31: Before placing, checks:
        1. PositionTracker for existing CONFIRMED position (no REST!)
        2. Active orders for same coin+epoch
        """
        # === DUPLICATE PROTECTION (FIX #31) ===
        if not is_retry:
            # Check 1: Do we already have a confirmed position for this token via User WS?
            if self.has_confirmed_position(token_id):
                logger.warning(f"FIX #31: BLOCKED duplicate order — confirmed position exists for {coin} token {token_id[:20]}...")
                return None

            # Check 2: Do we already have an active order for this coin+epoch?
            if self.has_active_order_for_epoch(coin, epoch):
                logger.warning(f"FIX #31: BLOCKED duplicate order — active order exists for {coin} epoch={epoch}")
                return None
        else:
            # Retry: still check PositionTracker (maybe RETRYING→CONFIRMED happened)
            if self.has_confirmed_position(token_id):
                logger.warning(f"FIX #31: BLOCKED retry — position already confirmed via WS for {coin}")
                return None

        if MODE == "OBSERVATION":
            size = BET_SIZE / price
            if size < 5:
                size = 5.0
            rec = OrderRecord(
                order_id=f"OBS-{int(time.time())}",
                coin=coin, side=side, token_id=token_id,
                price=price, size=size,
                status="observation", placed_at=time.time(),
                epoch=epoch,
            )
            self.orders.append(rec)
            logger.info(f"[OBSERVATION] Would place: BUY {rec.size:.2f} {side} @ ${price:.2f} for {coin}")
            return rec

        if not self._initialized or not self._client:
            logger.error("CLOB client not initialized")
            return None

        try:
            from py_clob_client.order_builder.constants import BUY
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions

            size = 17.0  # Fixed 17 contracts per bet

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=BUY,
                fee_rate_bps=0,
            )
            options = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)

            loop = asyncio.get_event_loop()
            signed = await loop.run_in_executor(
                None, lambda: self._client.create_and_post_order(order_args, options=options)
            )

            order_id = signed.get("orderID", signed.get("id", ""))
            rec = OrderRecord(
                order_id=order_id, coin=coin, side=side,
                token_id=token_id, price=price, size=size,
                status="live", placed_at=time.time(),
                epoch=epoch,
            )
            self.orders.append(rec)
            if len(self.orders) > 200:
                self.orders = self.orders[-100:]
            retry_tag = f" [RETRY #{rec.retry_count}]" if is_retry else ""
            logger.info(f"ORDER PLACED: {order_id} — {coin} {side} {size:.1f}@{price}{retry_tag}")
            print(f"🎯 ORDER: {coin.upper()} BUY {size:.1f} {side} @ ${price:.2f} [ID: {order_id[:12]}]{retry_tag}")
            return rec

        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    async def place_orders_batch(
        self, orders_info: list[dict],
    ) -> list[Optional["OrderRecord"]]:
        """FIX #33: Place multiple orders in a single batch API call.
        
        orders_info: list of {coin, token_id, side, price, epoch, coin_state}
        Returns list of OrderRecord (or None for failed ones).
        """
        if not orders_info:
            return []

        # Pre-filter: duplicate protection per order
        valid = []
        results: list[Optional[OrderRecord]] = [None] * len(orders_info)
        for i, info in enumerate(orders_info):
            coin = info["coin"]
            token_id = info["token_id"]
            epoch = info["epoch"]
            if self.has_confirmed_position(token_id):
                logger.warning(f"FIX #33: BLOCKED batch order — confirmed position for {coin}")
                continue
            if self.has_active_order_for_epoch(coin, epoch):
                logger.warning(f"FIX #33: BLOCKED batch order — active order for {coin} epoch={epoch}")
                continue
            existing = self.get_bet_for_epoch(coin, epoch)
            if existing:
                logger.warning(f"FIX #33: BLOCKED batch order — bet exists for {coin} epoch={epoch}")
                continue
            valid.append((i, info))

        if not valid:
            return results

        if MODE == "OBSERVATION":
            for i, info in valid:
                size = BET_SIZE / info["price"]
                if size < 5:
                    size = 5.0
                rec = OrderRecord(
                    order_id=f"OBS-{int(time.time())}-{info['coin']}",
                    coin=info["coin"], side=info["side"], token_id=info["token_id"],
                    price=info["price"], size=size,
                    status="observation", placed_at=time.time(), epoch=info["epoch"],
                )
                self.orders.append(rec)
                results[i] = rec
            return results

        if not self._initialized or not self._client:
            logger.error("CLOB client not initialized for batch")
            return results

        try:
            from py_clob_client.order_builder.constants import BUY
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, PostOrdersArgs

            size = 17.0  # Fixed 17 contracts

            # Build signed orders
            signed_orders = []
            order_map = []  # track which valid index maps to which signed order
            loop = asyncio.get_event_loop()

            for i, info in valid:
                order_args = OrderArgs(
                    token_id=info["token_id"],
                    price=info["price"],
                    size=size,
                    side=BUY,
                    fee_rate_bps=0,
                )
                options = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)
                # Create (sign) the order locally
                signed = await loop.run_in_executor(
                    None, lambda oa=order_args, opt=options: self._client.create_order(oa, options=opt)
                )
                signed_orders.append(PostOrdersArgs(order=signed, orderType="GTC"))
                order_map.append((i, info))

            # Post all in one batch call
            logger.info(f"FIX #33: Posting batch of {len(signed_orders)} orders")
            response = await loop.run_in_executor(
                None, lambda: self._client.post_orders(signed_orders)
            )

            # Parse response — expect list of {orderID, ...} or error
            if isinstance(response, list):
                for j, resp in enumerate(response):
                    idx, info = order_map[j]
                    order_id = resp.get("orderID", resp.get("id", f"BATCH-{int(time.time())}-{j}"))
                    rec = OrderRecord(
                        order_id=order_id, coin=info["coin"], side=info["side"],
                        token_id=info["token_id"], price=info["price"], size=size,
                        status="live", placed_at=time.time(), epoch=info["epoch"],
                    )
                    self.orders.append(rec)
                    results[idx] = rec
                    logger.info(f"FIX #33: BATCH ORDER PLACED: {order_id[:20]} — {info['coin']} {info['side']} {size:.1f}@{info['price']}")
            elif isinstance(response, dict):
                # Single response or error
                order_ids = response.get("orderIDs", [])
                for j, (idx, info) in enumerate(order_map):
                    oid = order_ids[j] if j < len(order_ids) else f"BATCH-{int(time.time())}-{j}"
                    rec = OrderRecord(
                        order_id=oid, coin=info["coin"], side=info["side"],
                        token_id=info["token_id"], price=info["price"], size=size,
                        status="live", placed_at=time.time(), epoch=info["epoch"],
                    )
                    self.orders.append(rec)
                    results[idx] = rec
                    logger.info(f"FIX #33: BATCH ORDER PLACED: {oid[:20]} — {info['coin']} {info['side']} {size:.1f}@{info['price']}")
            else:
                logger.error(f"FIX #33: Unexpected batch response: {str(response)[:200]}")

            if len(self.orders) > 200:
                self.orders = self.orders[-100:]

            # Rollback coin_state for any that didn't get placed
            for idx, info in order_map:
                if results[idx] is None and info.get("coin_state"):
                    cs = info["coin_state"]
                    if cs.state == "BET_ACTIVE":
                        cs.state = "SCANNING"
                        logger.info(f"FIX #33: Rolled back {info['coin']} to SCANNING (batch failed)")

            return results

        except Exception as e:
            logger.error(f"FIX #33: Batch order failed: {e}")
            # Rollback all
            for i, info in valid:
                cs = info.get("coin_state")
                if cs and cs.state == "BET_ACTIVE":
                    cs.state = "SCANNING"
                    logger.info(f"FIX #33: Rolled back {info['coin']} to SCANNING")
            return results

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        if MODE == "OBSERVATION" or not self._client:
            return True
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self._client.cancel(order_id))
            logger.info(f"Order cancelled: {order_id}")
            rec = self.find_order(order_id)
            if rec:
                rec.status = "cancelled"
            return True
        except Exception as e:
            logger.error(f"Cancel failed for {order_id}: {e}")
            return False

    # --- FIX #31: Retry failed order ---

    async def retry_failed_order(self, failed_rec: OrderRecord) -> Optional[OrderRecord]:
        """Retry a FAILED order. Called from main.py handle_user_event.
        
        Safety checks:
        1. Max retry count not exceeded
        2. Epoch not expired
        3. No confirmed position via PositionTracker (User WS)
        4. No other active order for same coin+epoch
        """
        coin = failed_rec.coin
        epoch = failed_rec.epoch
        token_id = failed_rec.token_id
        side = failed_rec.side
        retry_num = failed_rec.retry_count + 1

        logger.info(f"FIX #31: Retry #{retry_num} for {coin} epoch={epoch} (failed order {failed_rec.order_id[:20]}...)")

        # Check 1: Max retries
        if retry_num > MAX_RETRY:
            logger.warning(f"FIX #31: Max retries ({MAX_RETRY}) reached for {coin} epoch={epoch}")
            return None

        # Check 2: Epoch not expired (must be within the 15-min window)
        now = time.time()
        epoch_end = epoch + 900  # 15-min window
        if now >= epoch_end:
            logger.warning(f"FIX #31: Epoch {epoch} already ended, no retry for {coin}")
            return None

        # Check 3: PositionTracker — maybe RETRYING→CONFIRMED happened while we waited
        if self.has_confirmed_position(token_id):
            logger.info(f"FIX #31: Position confirmed via WS during wait — no retry needed for {coin}")
            return None

        # Check 4: No other active order for same coin+epoch
        if self.has_active_order_for_epoch(coin, epoch):
            logger.info(f"FIX #31: Another active order exists for {coin} epoch={epoch} — no retry")
            return None

        # Cancel the old failed order on CLOB (cleanup, may already be gone)
        await self.cancel_order(failed_rec.order_id)

        # Wait before retry
        logger.info(f"FIX #31: Waiting {RETRY_DELAY}s before retry...")
        await asyncio.sleep(RETRY_DELAY)

        # === FINAL SAFETY CHECK before placing ===
        # Re-check PositionTracker one more time right before placing
        if self.has_confirmed_position(token_id):
            logger.info(f"FIX #31: Position confirmed during delay — ABORT retry for {coin}")
            return None

        # Place new order
        new_rec = await self.place_order(
            coin=coin, token_id=token_id, side=side,
            price=failed_rec.price, epoch=epoch, is_retry=True,
        )
        if new_rec:
            new_rec.retry_count = retry_num
            logger.info(f"FIX #31: Retry order placed for {coin}: {new_rec.order_id[:20]}... (attempt #{retry_num})")
        else:
            logger.warning(f"FIX #31: Retry order BLOCKED or failed for {coin}")

        return new_rec

    def get_bet_for_epoch(self, coin: str, epoch: int) -> Optional[OrderRecord]:
        """Find an active bet for a specific coin and epoch."""
        for rec in reversed(self.orders):
            if rec.coin == coin and rec.epoch == epoch and rec.status in ("live", "matched", "confirmed", "observation"):
                return rec
        return None

    # FIX #8: Cancel orders older than FILL_TIMEOUT
    async def cancel_stale_orders(self) -> list[str]:
        """Cancel orders that have been pending/live longer than FILL_TIMEOUT.
        Returns list of coin names whose orders were cancelled (for state rollback).
        """
        cancelled_coins = []
        now = time.time()
        for rec in self.orders:
            if rec.status in ("pending", "live") and rec.placed_at > 0:
                age = now - rec.placed_at
                if age > FILL_TIMEOUT:
                    logger.warning(f"FIX #34: Order {rec.order_id} stale ({age:.0f}s > {FILL_TIMEOUT}s), cancelling {rec.coin}")
                    await self.cancel_order(rec.order_id)
                    cancelled_coins.append(rec.coin)
        return cancelled_coins

    def load_orders(self, data: list):
        self.orders = [OrderRecord.from_dict(d) for d in data]

    def dump_orders(self) -> list:
        return [o.to_dict() for o in self.orders[-100:]]
