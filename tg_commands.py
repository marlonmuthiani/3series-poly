"""Telegram /balance command handler — fully async, standalone polling loop.

Runs in a separate asyncio task. Does NOT interfere with trading logic.
Only reads state.json (never writes). Fire-and-forget design.
"""

import asyncio
import json
import logging
import os
import time

import httpx
from web3 import Web3

logger = logging.getLogger("tg-commands")

BOT_TOKEN = os.getenv("TRADE_BOT_TOKEN", "")
CHAT_ID = os.getenv("TRADE_CHAT_ID", "")
ALLOWED_USERS = {int(CHAT_ID)} if CHAT_ID else set()
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
STATE_FILE = os.path.join(os.path.dirname(__file__), "state.json")

# Set by main.py at startup — reference to UserWebSocket for position tracking
USER_WS = None  # type: ignore
FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "")
RPC_URL = os.getenv("RPC_URL", os.getenv("POLY_RPC_URL", "https://polygon-rpc.com"))
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
GAMMA_API = "https://gamma-api.polymarket.com"
PROXY_TURKEY = os.getenv("PROXY_TURKEY", "")

# Minimal ABIs
USDC_ABI = [{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
CTF_ABI = [{"inputs":[{"name":"account","type":"address"},{"name":"id","type":"uint256"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]

# Polling config
POLL_TIMEOUT = 30  # long-poll seconds
POLL_INTERVAL = 1  # between polls on error


async def _get_wallet_balance() -> dict:
    """Get USDC balance of proxy wallet (on-chain). Runs in thread to avoid blocking."""
    result = {"usdc": 0.0, "error": None}
    try:
        loop = asyncio.get_event_loop()
        def _query():
            w3 = Web3(Web3.HTTPProvider(RPC_URL))
            wallet = Web3.to_checksum_address(FUNDER_ADDRESS)
            usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=USDC_ABI)
            raw = usdc.functions.balanceOf(wallet).call()
            return raw / 1e6  # USDC has 6 decimals
        result["usdc"] = await loop.run_in_executor(None, _query)
    except Exception as e:
        result["error"] = str(e)
    return result


def _get_positions_from_ws() -> dict:
    """Get live positions from User WS PositionTracker. Thread-safe, non-blocking."""
    if USER_WS and hasattr(USER_WS, 'positions'):
        return USER_WS.positions.get_snapshot()
    return {"positions": [], "positions_count": 0, "positions_value": 0.0, "open_orders": 0, "total_trades": 0}


async def _get_redeemable_count(state: dict) -> int:
    """Count redeemable positions from state."""
    return len(state.get("redeemer_pending", []))


def _read_state() -> dict | None:
    """Read state.json — read-only, never modifies."""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to read state: {e}")
        return None


async def _fetch_live_positions() -> dict:
    """Fetch fresh positions from Polymarket Data API. Runs in executor."""
    DATA_API = "https://data-api.polymarket.com"
    wallet = FUNDER_ADDRESS
    if not wallet:
        return {"active": [], "redeemable": [], "error": None}

    def _query():
        proxy = PROXY_TURKEY if PROXY_TURKEY else None
        resp = httpx.get(
            f"{DATA_API}/positions",
            params={"user": wallet, "limit": 500, "sizeThreshold": 0.01},
            timeout=15,
            proxy=proxy,
        )
        if resp.status_code != 200:
            return {"active": [], "redeemable": [], "error": f"API {resp.status_code}"}

        positions = resp.json()
        active = []
        redeemable = []
        for pos in positions:
            size = float(pos.get("size", 0))
            if size < 0.01:
                continue
            cur_price = float(pos.get("curPrice", 0))
            entry = {
                "title": pos.get("title", "?")[:40],
                "outcome": pos.get("outcome", "?"),
                "size": size,
                "cur_price": cur_price,
                "value": size * cur_price,
                "redeemable": pos.get("redeemable", False),
            }
            if entry["redeemable"]:
                redeemable.append(entry)
            else:
                active.append(entry)
        return {"active": active, "redeemable": redeemable, "error": None}

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _query)
    except Exception as e:
        return {"active": [], "redeemable": [], "error": str(e)}


async def _format_balance(state: dict) -> str:
    """Format full balance report: wallet + trading P&L + positions + redeemable."""
    coins = state.get("coins", {})

    # On-chain wallet balance + fresh positions (parallel)
    wallet_task = _get_wallet_balance()
    positions_task = _fetch_live_positions()
    wallet, pos_data = await asyncio.gather(wallet_task, positions_task)
    usdc_balance = wallet["usdc"]

    active_positions = pos_data["active"]
    redeemable_positions = pos_data["redeemable"]
    positions_value = sum(p["value"] for p in active_positions)
    redeemable_count = len(redeemable_positions)
    redeemable_value = sum(p["size"] for p in redeemable_positions)  # winning tokens = $1 each

    # Trading stats per coin
    total_bets = 0
    total_wins = 0
    total_losses = 0
    total_pnl = 0.0
    lines = []

    for coin, data in sorted(coins.items()):
        b = data.get("bets", 0)
        w = data.get("wins", 0)
        l = data.get("losses", 0)
        bal = data.get("balance", 0.0)
        st = data.get("state", "?")
        hist = data.get("history", [])

        total_bets += b
        total_wins += w
        total_losses += l
        total_pnl += bal

        # Current streak
        streak = ""
        if hist:
            last = hist[-1]
            count = 0
            for h in reversed(hist):
                if h == last:
                    count += 1
                else:
                    break
            streak = f"{count}×{last}"

        emoji = "🟢" if bal >= 0 else "🔴"
        status_icon = "⏳" if st == "BET_ACTIVE" else "👁" if st == "SCANNING" else "❓"
        lines.append(
            f"{emoji} <b>{coin.upper()}</b> {status_icon}  "
            f"{w}W/{l}L  ${bal:+.2f}  [{streak}]"
        )

    # Total estimated balance
    total_value = usdc_balance + positions_value + redeemable_value

    # Uptime
    updated = state.get("updated_at", 0)
    if updated:
        ago = int(time.time() - updated)
        if ago < 60:
            age = f"{ago}s"
        elif ago < 3600:
            age = f"{ago // 60}m"
        else:
            age = f"{ago // 3600}h {(ago % 3600) // 60}m"
    else:
        age = "?"

    wallet_err = f"\n⚠️ RPC error: {wallet['error']}" if wallet.get("error") else ""

    # Position details from fresh API data
    pos_lines = []
    for p in active_positions:
        oc = p["outcome"]
        sz = p["size"]
        cp = p["cur_price"]
        pos_lines.append(f"  {oc} {sz:.1f}шт × ${cp:.2f} = ${p['value']:.2f}")

    pos_detail = "\n".join(pos_lines) if pos_lines else "  нет"
    pos_err = f"\n⚠️ {pos_data['error']}" if pos_data.get("error") else ""

    # Redeemable details
    redeem_lines = []
    for p in redeemable_positions:
        redeem_lines.append(f"  {p['outcome']} {p['size']:.1f}шт ~${p['size']:.2f}")
    redeem_detail = "\n".join(redeem_lines) if redeem_lines else ""

    return (
        f"📊 <b>Martin Balance</b>\n"
        f"{'─' * 28}\n"
        f"💵 Кошелёк: <b>${usdc_balance:,.2f}</b> USDC{wallet_err}\n"
        f"📈 Позиции: <b>${positions_value:,.2f}</b> ({len(active_positions)} шт){pos_err}\n"
        f"<pre>{pos_detail}</pre>\n"
        + (f"🔓 Redeemable: <b>${redeemable_value:,.2f}</b> ({redeemable_count} шт)\n<pre>{redeem_detail}</pre>\n" if redeemable_count else f"🔓 Redeemable: 0\n")
        + f"{'─' * 28}\n"
        + "\n".join(lines)
        + f"\n{'─' * 28}\n"
        f"📊 P&L: <b>${total_pnl:+.2f}</b> ({total_bets} ставок: {total_wins}W/{total_losses}L)\n"
        f"{'─' * 28}\n"
        f"💰 <b>Итого: ~${total_value:,.2f}</b>\n"
        f"🕐 {age} назад"
    )


async def _send(text: str, chat_id: int | str):
    """Send TG message — fire and forget."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{API_URL}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            )
    except Exception as e:
        logger.warning(f"TG send error: {e}")


async def _fetch_15m_outcomes() -> str:
    """Fetch last 5 resolved outcomes per coin from 15-min markets via Gamma API. Fully async."""
    WINDOW = 900
    now = int(time.time())
    current_ep = now - (now % WINDOW)

    COINS_MAP = {"btc": "BTC", "eth": "ETH", "sol": "SOL", "xrp": "XRP"}
    lines = []

    async with httpx.AsyncClient(timeout=10, proxy=PROXY_TURKEY if PROXY_TURKEY else None) as client:
        for coin, short in COINS_MAP.items():
            dots = []
            for i in range(5, 0, -1):
                epoch = current_ep - i * WINDOW
                slug = f"{coin}-updown-15m-{epoch}"
                try:
                    resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
                    if resp.status_code != 200 or not resp.json():
                        dots.append("⏳")
                        continue
                    event = resp.json()[0]
                    market = event["markets"][0]
                    if market.get("umaResolutionStatus") != "resolved":
                        dots.append("⏳")
                        continue
                    raw_prices = market.get("outcomePrices", "[]")
                    raw_outcomes = market.get("outcomes", "[]")
                    prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                    outcomes_list = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
                    winner = None
                    for oc, price in zip(outcomes_list, prices):
                        if str(price) == "1":
                            winner = oc
                            break
                    dots.append("🟢" if winner == "Up" else "🔴" if winner else "⏳")
                except Exception:
                    dots.append("⏳")
            lines.append(f"<b>{short}</b>  {''.join(dots)}")

    return "\n".join(lines)


async def _handle_update(update: dict):
    """Process a single TG update."""
    msg = update.get("message", {})
    text = msg.get("text", "")
    user_id = msg.get("from", {}).get("id", 0)
    chat_id = msg.get("chat", {}).get("id", 0)

    # Security: only allowed users
    if user_id not in ALLOWED_USERS:
        return

    cmd = text.strip().lower()

    if cmd == "/balance":
        state = _read_state()
        if state:
            reply = await _format_balance(state)
        else:
            reply = "⚠️ Не удалось прочитать state.json"
        await _send(reply, chat_id)

    elif cmd == "/15m":
        result = await _fetch_15m_outcomes()
        reply = f"<b>15m markets — last 5</b>\n\n{result}\n\n🟢 Up  🔴 Down  ⏳ Pending"
        await _send(reply, chat_id)

    elif cmd == "/redeem":
        await _send("🔓 Запускаю редим всех resolved позиций...", chat_id)
        from redeemer_cmd import execute_redeem
        result = await execute_redeem()
        await _send(result, chat_id)

    elif cmd == "/status":
        state = _read_state()
        if state:
            coins = state.get("coins", {})
            states = {c: d.get("state", "?") for c, d in coins.items()}
            reply = f"🤖 Martin: LIVE\n" + "\n".join(
                f"  {c.upper()}: {s}" for c, s in sorted(states.items())
            )
        else:
            reply = "⚠️ state.json недоступен"
        await _send(reply, chat_id)


async def polling_loop():
    """Long-poll Telegram for commands. Fully independent — never touches trading."""
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("TG commands disabled — no TRADE_BOT_TOKEN/TRADE_CHAT_ID")
        return

    logger.info("TG command handler started (polling)")
    offset = 0

    # Set bot commands menu
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{API_URL}/setMyCommands",
                json={
                    "commands": [
                        {"command": "balance", "description": "📊 Баланс и статистика"},
                        {"command": "15m", "description": "🕐 Последние 5 исходов 15m"},
                        {"command": "status", "description": "🤖 Статус бота"},
                        {"command": "redeem", "description": "🔓 Редим всех resolved позиций"},
                    ]
                },
            )
            logger.info("TG bot menu commands set")
    except Exception as e:
        logger.warning(f"Failed to set bot commands: {e}")

    while True:
        try:
            async with httpx.AsyncClient(timeout=POLL_TIMEOUT + 5) as client:
                resp = await client.get(
                    f"{API_URL}/getUpdates",
                    params={
                        "offset": offset,
                        "timeout": POLL_TIMEOUT,
                        "allowed_updates": '["message"]',
                    },
                )
                if resp.status_code != 200:
                    logger.warning(f"TG poll error {resp.status_code}")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                data = resp.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        await _handle_update(update)
                    except Exception as e:
                        logger.warning(f"Handle update error: {e}")

        except (httpx.TimeoutException, httpx.ConnectError):
            await asyncio.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.warning(f"TG poll loop error: {e}")
            await asyncio.sleep(5)
