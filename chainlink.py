"""Chainlink RTDS WebSocket — real-time crypto price feed."""

import asyncio
import bisect  # FIX #13: bisect search
import json
import time
import logging
from collections import defaultdict
from typing import Optional

import websockets
from websockets_proxy import Proxy, proxy_connect

from config import RTDS_URL, COINS, PROXY_URL

logger = logging.getLogger("chainlink")

# FIX #20: If no data received for this many seconds, force WS reconnect
STALE_TIMEOUT = 90

# Maps coin name → Chainlink pair ID
PAIR_MAP = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}


# FIX #13: Helper for bisect on PriceEntry list by timestamp
class _TSKeyList:
    """Allows bisect to work on PriceEntry list by timestamp without copying."""
    def __init__(self, buf):
        self._buf = buf
    def __len__(self):
        return len(self._buf)
    def __getitem__(self, i):
        return self._buf[i].ts


class PriceEntry:
    __slots__ = ("price", "ts")

    def __init__(self, price: float, ts: int):
        self.price = price
        self.ts = ts  # milliseconds


class ChainlinkFeed:
    """Connects to Polymarket RTDS and streams Chainlink prices."""

    def __init__(self):
        self._prices: dict[str, list[PriceEntry]] = defaultdict(list)
        self._latest: dict[str, PriceEntry] = {}
        self._ws = None
        self._running = False
        self._max_buffer = 1500  # ~20 min at ~1/s
        self._connected = asyncio.Event()

    # -- public API --

    def get_latest_price(self, coin: str) -> Optional[float]:
        entry = self._latest.get(coin)
        return entry.price if entry else None

    def get_price_at(self, coin: str, timestamp_ms: int) -> Optional[float]:
        """Return closest price ≤ timestamp_ms."""
        buf = self._prices.get(coin)
        if not buf:
            return None
        best = None
        for e in reversed(buf):
            if e.ts <= timestamp_ms:
                best = e.price
                break
        return best

    def get_price_exact(self, coin: str, timestamp_ms: int, tolerance_ms: int = 2000) -> Optional[float]:
        """Return price with timestamp exactly matching (within tolerance).
        Polymarket uses the Chainlink price at exact window boundary."""
        buf = self._prices.get(coin)
        if not buf:
            return None
        # FIX #13: bisect search instead of linear scan
        idx = bisect.bisect_left(_TSKeyList(buf), timestamp_ms)
        best = None
        best_diff = float('inf')
        for i in (idx - 1, idx):
            if 0 <= i < len(buf):
                diff = abs(buf[i].ts - timestamp_ms)
                if diff < best_diff:
                    best_diff = diff
                    best = buf[i]
        if best and best_diff <= tolerance_ms:
            return best.price
        return None

    # FIX #7: Detect stale price data
    def is_stale(self, coin: str, max_age: int = 60) -> bool:
        """Return True if no fresh price data for coin in max_age seconds."""
        entry = self._latest.get(coin)
        if entry is None:
            return True
        age_ms = time.time() * 1000 - entry.ts
        return age_ms > max_age * 1000

    async def wait_connected(self, timeout: float = 30.0):
        await asyncio.wait_for(self._connected.wait(), timeout)

    # -- lifecycle --

    async def run(self):
        self._running = True
        backoff = 5
        while self._running:
            try:
                await self._connect()
                backoff = 5  # reset on clean disconnect
            except asyncio.CancelledError:
                await self.stop()
                break
            except Exception as e:
                self._connected.clear()  # FIX #6: reset connected on error
                logger.error(f"RTDS error: {e} (retry in {backoff}s)")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def stop(self):
        self._running = False
        if self._ws:
            try:
                await self._ws.close(code=1000, reason="graceful shutdown")
                logger.info("RTDS WebSocket closed gracefully (1000)")
            except Exception as e:
                logger.warning(f"RTDS close error: {e}")
            self._ws = None

    # -- internals --

    async def _connect(self):
        self._connected.clear()  # FIX #6: reset connected state on new connection attempt
        logger.info(f"Connecting to RTDS: {RTDS_URL}")
        if PROXY_URL:
            proxy = Proxy.from_url(PROXY_URL)
            ctx = proxy_connect(RTDS_URL, proxy=proxy, open_timeout=15, ping_interval=20, ping_timeout=30)  # FIX #5: open_timeout
        else:
            ctx = websockets.connect(RTDS_URL, open_timeout=15, ping_interval=20, ping_timeout=30)  # FIX #5: open_timeout
        async with ctx as ws:
            self._ws = ws
            # Subscribe using documented RTDS format
            sub_msg = json.dumps({
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": ""
                    }
                ]
            })
            await ws.send(sub_msg)
            logger.info(f"Subscribed to crypto_prices_chainlink (all coins)")
            self._connected.set()

            # FIX #20: Watchdog — reconnect if no data for STALE_TIMEOUT seconds
            self._last_data_ts = time.time()

            async def _watchdog():
                while self._running:
                    await asyncio.sleep(30)
                    silence = time.time() - self._last_data_ts
                    if silence > STALE_TIMEOUT:
                        logger.warning(f"Watchdog: no data for {silence:.0f}s, forcing reconnect")
                        await ws.close()
                        return

            watchdog_task = asyncio.create_task(_watchdog())
            try:
                async for raw in ws:
                    if not self._running:
                        break
                    self._last_data_ts = time.time()
                    try:
                        self._handle_message(raw)
                    except Exception as e:
                        logger.warning(f"Parse error: {e}")
            finally:
                watchdog_task.cancel()

    def _handle_message(self, raw: str):
        if not raw or not raw.strip():
            return
        data = json.loads(raw)
        topic = data.get("topic", "")
        if "crypto_prices_chainlink" not in topic:
            return

        payload = data.get("payload", {})
        symbol = payload.get("symbol", "")  # e.g. "btc/usd"
        value = payload.get("value")
        ts = payload.get("timestamp")

        if not symbol or value is None:
            return

        coin = symbol.split("/")[0].lower()
        if coin not in COINS:
            return

        ts_ms = int(ts) if ts and ts > 1e12 else int((ts or time.time()) * 1000)
        entry = PriceEntry(float(value), ts_ms)
        self._latest[coin] = entry

        buf = self._prices[coin]
        buf.append(entry)
        if len(buf) > self._max_buffer:
            self._prices[coin] = buf[-self._max_buffer:]

        # Log at INFO level for window boundary prices (every 900 sec = 15 min)
        ts_sec = ts_ms // 1000
        if ts_sec % 900 == 0:
            logger.info(f"BOUNDARY {coin} ${value:.6f} @ {ts_ms} (epoch={ts_sec})")
        else:
            logger.debug(f"{coin} ${value:.2f} @ {ts_ms}")
