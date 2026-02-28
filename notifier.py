"""Telegram notifier — sends messages via TRADE_BOT_TOKEN."""

import logging
import os

import httpx

from http_client import get_client  # FIX #14

logger = logging.getLogger("notifier")

BOT_TOKEN = os.getenv("TRADE_BOT_TOKEN", "")
CHAT_ID = os.getenv("TRADE_CHAT_ID", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


async def notify(text: str, parse_mode: str = "HTML"):
    """Send a Telegram message. Fire-and-forget; logs errors but never raises."""
    if not BOT_TOKEN or not CHAT_ID:
        logger.debug(f"Notify (no TG creds): {text}")
        return
    try:
        client = get_client()  # FIX #14: shared httpx client
        resp = await client.post(
            f"{API_URL}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode},
        )
        if resp.status_code != 200:
            logger.warning(f"TG send failed ({resp.status_code}): {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"TG notify error: {e}")
