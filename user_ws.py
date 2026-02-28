"""User Channel WebSocket — tracks order fills, positions, and updates.
FIX #26: Simple auth-only connection. User channel streams ALL events
for the authenticated API key automatically. No per-market subscription needed.
FIX #29: Position tracking from trade events for /balance command.
Always close WS cleanly on shutdown/reconnect.
Docs: https://docs.polymarket.com/market-data/websocket/user-channel
"""

import asyncio
import json
import logging
import threading
from typing import Callable, Optional

import websockets

from config import USER_WS_URL, API_KEY, API_SECRET, API_PASSPHRASE

logger = logging.getLogger("user_ws")


class PositionTracker:
    """Thread-safe position tracker built from User WS trade events.
    
    Tracks:
    - Open positions (asset_id → {size, avg_price, side, outcome, market})
    - Pending orders (order_id → {size, price, side, status})
    - Trade history for audit
    
    Read-only access from /balance command (never blocks trading).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._positions: dict[str, dict] = {}  # asset_id → position info
        self._orders: dict[str, dict] = {}  # order_id → order info
        self._trades: list[dict] = []  # recent trade events (capped)
        self._seen_trades: set[str] = set()  # dedup: (order_id, status) pairs
        self._max_trades = 200

    def handle_event(self, data: dict):
        """Process a User WS event and update positions/orders."""
        event_type = data.get("event_type", data.get("type", "")).lower()
        
        if event_type == "trade" or data.get("type", "").upper() == "TRADE":
            self._handle_trade(data)
        elif event_type == "order":
            self._handle_order(data)

    def _handle_trade(self, data: dict):
        """Update positions from trade events. Only CONFIRMED updates positions.
        Deduplicates repeated CONFIRMED events (Polymarket sends them 2-3 times).
        """
        status = data.get("status", "").upper()
        asset_id = data.get("asset_id", "")
        side = data.get("side", "").upper()  # BUY or SELL
        size = float(data.get("size", 0))
        price = float(data.get("price", 0))
        outcome = data.get("outcome", "")
        market = data.get("market", "")
        order_id = data.get("taker_order_id", data.get("id", ""))

        if not asset_id or size <= 0:
            return

        with self._lock:
            # Dedup: skip if we already processed this (order_id, status) pair
            dedup_key = f"{order_id}:{status}"
            if dedup_key in self._seen_trades:
                logger.debug(f"PositionTracker: skip duplicate {dedup_key}")
                return
            self._seen_trades.add(dedup_key)
            # Cap dedup set size
            if len(self._seen_trades) > 1000:
                self._seen_trades = set(list(self._seen_trades)[-500:])

            # Store trade for history
            self._trades.append({
                "asset_id": asset_id,
                "side": side,
                "size": size,
                "price": price,
                "outcome": outcome,
                "market": market,
                "status": status,
                "timestamp": data.get("timestamp", ""),
                "taker_order_id": order_id,
            })
            if len(self._trades) > self._max_trades:
                self._trades = self._trades[-self._max_trades:]

            # Update position on CONFIRMED only (final state)
            if status == "CONFIRMED":
                pos = self._positions.get(asset_id, {
                    "size": 0.0, "cost": 0.0, "outcome": outcome,
                    "market": market, "asset_id": asset_id,
                    "buy_price": 0.0,
                })

                if side == "BUY":
                    new_size = pos["size"] + size
                    pos["cost"] = pos["cost"] + (size * price)
                    pos["size"] = new_size
                    # Track avg buy price for value display
                    pos["buy_price"] = pos["cost"] / new_size if new_size > 0 else 0
                elif side == "SELL":
                    new_size = pos["size"] - size
                    if new_size > 0:
                        ratio = new_size / pos["size"] if pos["size"] > 0 else 0
                        pos["cost"] = pos["cost"] * ratio
                    else:
                        pos["cost"] = 0.0
                    pos["size"] = max(0.0, new_size)
                    pos["buy_price"] = pos["cost"] / pos["size"] if pos["size"] > 0 else 0

                pos["outcome"] = outcome
                pos["market"] = market

                if pos["size"] > 0.001:
                    self._positions[asset_id] = pos
                elif asset_id in self._positions:
                    del self._positions[asset_id]

    def _handle_order(self, data: dict):
        """Track order lifecycle (PLACEMENT/UPDATE/CANCELLATION)."""
        etype = data.get("type", "").upper()
        order_id = data.get("id", "")
        if not order_id:
            return

        with self._lock:
            if etype == "PLACEMENT":
                self._orders[order_id] = {
                    "asset_id": data.get("asset_id", ""),
                    "price": float(data.get("price", 0)),
                    "size": float(data.get("original_size", data.get("size", 0))),
                    "side": data.get("side", ""),
                    "outcome": data.get("outcome", ""),
                    "status": "open",
                }
            elif etype == "CANCELLATION":
                if order_id in self._orders:
                    del self._orders[order_id]
            elif etype == "UPDATE":
                if order_id in self._orders:
                    matched = float(data.get("size_matched", 0))
                    self._orders[order_id]["size_matched"] = matched

    def get_snapshot(self) -> dict:
        """Thread-safe snapshot for /balance. Never blocks trading."""
        with self._lock:
            positions = list(self._positions.values())
            open_orders = [o for o in self._orders.values() if o.get("status") == "open"]
            return {
                "positions": positions,
                "positions_count": len(positions),
                "positions_cost": sum(p.get("cost", 0) for p in positions),
                "positions_value": sum(p.get("cost", 0) for p in positions),  # = cost spent
                "open_orders": len(open_orders),
                "total_trades": len(self._trades),
            }


class UserWebSocket:
    """Connects to Polymarket User WS for order tracking.
    
    Single authenticated connection — receives ALL order/trade events
    for this API key as a stream. No per-market subscription needed.
    """

    def __init__(self, on_event: Optional[Callable] = None):
        self._ws = None
        self._running = False
        self.on_event = on_event
        self.positions = PositionTracker()  # FIX #29: position tracking

    async def run(self):
        if not API_KEY:
            logger.warning("No API credentials — user WS disabled")
            return

        self._running = True
        backoff = 5
        while self._running:
            try:
                await self._connect()
                backoff = 5
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"User WS error: {e} (retry in {backoff}s)")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def stop(self):
        self._running = False
        await self._close()

    async def _close(self):
        """Always close WS cleanly with proper close code."""
        if self._ws:
            try:
                await self._ws.close(code=1000, reason="graceful shutdown")
                logger.info("User WS closed gracefully (1000)")
            except Exception as e:
                logger.debug(f"User WS close error: {e}")
            self._ws = None

    async def _connect(self):
        logger.info("Connecting to User WS...")
        ws = None
        try:
            ws = await websockets.connect(
                USER_WS_URL,
                ping_interval=30,
                ping_timeout=60,
            )
            self._ws = ws
            logger.info("User WS connected")

            # Single auth message — streams all events for this API key
            auth_msg = json.dumps({
                "auth": {
                    "apiKey": API_KEY,
                    "secret": API_SECRET,
                    "passphrase": API_PASSPHRASE,
                },
                "type": "user",
            })
            await ws.send(auth_msg)
            logger.info("User WS authenticated, listening for events...")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    if not raw or not raw.strip():
                        continue
                    stripped = raw.strip()
                    if not stripped.startswith('{') and not stripped.startswith('['):
                        logger.warning(f"User WS non-JSON: {stripped[:100]}")
                        continue

                    data = json.loads(raw)
                    event_type = data.get("type", data.get("event_type", "unknown"))
                    oid = data.get("taker_order_id", data.get("id", ""))
                    logger.info(f"User WS event: {event_type} order={oid[:20] if oid else '?'}")
                    logger.debug(f"User WS payload: {json.dumps(data)[:500]}")

                    # FIX #29: Track positions from events
                    self.positions.handle_event(data)

                    if self.on_event:
                        self.on_event(data)
                except json.JSONDecodeError as e:
                    logger.warning(f"User WS parse error: {e} | raw ({len(raw)}b): {raw[:200]}")
                except Exception as e:
                    logger.warning(f"User WS handler error: {e}")
        finally:
            self._ws = None
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass
