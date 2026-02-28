"""Configuration for Series Reversal Bot."""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Strategy
TRIGGER_N = int(os.getenv("TRIGGER_N", "3"))
BET_SIZE = float(os.getenv("BET_SIZE", "1.0"))  # USDC per bet
BET_PRICE = float(os.getenv("BET_PRICE", "0.50"))
FILL_TIMEOUT = int(os.getenv("FILL_TIMEOUT", "120"))  # seconds
COINS = os.getenv("COINS", "btc,eth,sol,xrp").split(",")
MODE = os.getenv("MODE", "OBSERVATION")  # OBSERVATION or LIVE
GAMMA_BET_ALLOWED = os.getenv("GAMMA_BET_ALLOWED", "false").lower() == "true"  # FIX #11: allow bets on Gamma fallback

# CLOB credentials
PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "")
API_KEY = os.getenv("POLY_API_KEY", "")
API_SECRET = os.getenv("POLY_API_SECRET", "")
API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "")
SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "2"))

# RPC
RPC_URL = os.getenv("RPC_URL", os.getenv("POLY_RPC_URL", "https://polygon-rpc.com"))
CHAIN_ID = 137

# Endpoints
CLOB_URL = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
RTDS_URL = "wss://ws-live-data.polymarket.com"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

# Proxy
# FIX #21b: Separate proxies — ORDER_PROXY for CLOB orders only, PROXY_URL for WS (empty = direct)
PROXY_URL = ""  # WS connections — direct (proxy caused instability)
ORDER_PROXY = os.getenv("PROXY_TURKEY", "")  # CLOB order placement — geoblocked without proxy

# Paths
STATE_FILE = os.path.join(os.path.dirname(__file__), "state.json")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")

# Contracts
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging(level=logging.INFO):
    """Configure logging to file (rotating) and console."""
    from logging.handlers import RotatingFileHandler
    fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)

    # FIX #15: RotatingFileHandler — 10MB max, 3 backups
    fh = RotatingFileHandler(
        os.path.join(LOG_DIR, "bot.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=3,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(fh)

    return logging.getLogger("series-bot")
