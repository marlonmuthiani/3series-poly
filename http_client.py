"""Shared httpx.AsyncClient with proxy support — FIX #14."""

import logging
from typing import Optional

import httpx

from config import PROXY_URL

logger = logging.getLogger("http_client")

_client: Optional[httpx.AsyncClient] = None


def get_client() -> httpx.AsyncClient:
    """Return a shared httpx.AsyncClient. Creates one if needed."""
    global _client
    if _client is None or _client.is_closed:
        kwargs: dict = {"timeout": 15}
        if PROXY_URL:
            kwargs["proxy"] = PROXY_URL
        _client = httpx.AsyncClient(**kwargs)
        logger.info(f"Shared httpx client created (proxy={'yes' if PROXY_URL else 'no'})")
    return _client


async def close_client():
    """Close the shared client gracefully."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None
