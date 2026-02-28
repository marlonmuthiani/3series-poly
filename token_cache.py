"""Token ID cache — preloads and caches CLOB token IDs from Gamma API."""

import asyncio
import json
import logging
import time
from typing import Optional

import httpx

from config import GAMMA_API, COINS
from http_client import get_client  # FIX #14

logger = logging.getLogger("token_cache")

WINDOW = 900
LOOKAHEAD = 3  # only cache 3 windows ahead


class TokenInfo:
    __slots__ = ("up_token", "down_token", "condition_id")

    def __init__(self, up_token: str, down_token: str, condition_id: str):
        self.up_token = up_token
        self.down_token = down_token
        self.condition_id = condition_id


class TokenCache:
    """Caches token IDs for 15-min crypto markets. Keeps only LOOKAHEAD windows."""

    def __init__(self):
        # {coin: {epoch: TokenInfo}}
        self._cache: dict[str, dict[int, TokenInfo]] = {c: {} for c in COINS}
        self._lock = asyncio.Lock()

    async def get(self, coin: str, epoch: int) -> Optional[TokenInfo]:
        """Get token info, fetching from Gamma if not cached."""
        info = self._cache.get(coin, {}).get(epoch)
        if info:
            return info
        return await self._fetch_single(coin, epoch)

    async def preload(self, count: int = LOOKAHEAD):
        """Preload tokens for next N windows (default 3)."""
        now = int(time.time())
        start_epoch = now - (now % WINDOW)
        tasks = []
        for coin in COINS:
            for i in range(count):
                epoch = start_epoch + i * WINDOW
                if epoch not in self._cache.get(coin, {}):
                    tasks.append(self._fetch_single(coin, epoch))
        if tasks:
            logger.info(f"Preloading {len(tasks)} token entries...")
            await asyncio.gather(*tasks, return_exceptions=True)
        # Evict old entries
        self._evict(start_epoch)
        total = sum(len(v) for v in self._cache.values())
        logger.info(f"Cache size: {total} entries")

    def _evict(self, current_epoch: int):
        """Remove entries older than current window."""
        for coin in COINS:
            old_keys = [e for e in self._cache.get(coin, {}) if e < current_epoch]
            for k in old_keys:
                del self._cache[coin][k]

    async def _fetch_single(self, coin: str, epoch: int) -> Optional[TokenInfo]:
        slug = f"{coin}-updown-15m-{epoch}"
        try:
            client = get_client()  # FIX #14: shared httpx client
            resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
            if resp.status_code != 200:
                logger.debug(f"Gamma {resp.status_code} for {slug}")
                return None
            events = resp.json()
            if not events:
                return None
            event = events[0]
            markets = event.get("markets", [])
            if not markets:
                return None
            market = markets[0]
            token_ids = json.loads(market.get("clobTokenIds", "[]"))
            if len(token_ids) < 2:
                return None
            condition_id = market.get("conditionId", "")
            info = TokenInfo(
                up_token=token_ids[0],
                down_token=token_ids[1],
                condition_id=condition_id,
            )
            async with self._lock:
                self._cache.setdefault(coin, {})[epoch] = info
            logger.debug(f"Cached {slug}: up={token_ids[0][:12]}... down={token_ids[1][:12]}...")
            return info
        except Exception as e:
            logger.warning(f"Failed to fetch {slug}: {e}")
            return None

    async def refresh_loop(self, interval: int = 900):
        """Background loop to refresh cache every window."""
        while True:
            try:
                await self.preload(count=LOOKAHEAD)
            except Exception as e:
                logger.error(f"Cache refresh error: {e}")
            await asyncio.sleep(interval)
