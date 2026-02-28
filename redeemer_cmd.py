"""
/redeem command — async redeem of ALL resolved positions.

Fully isolated: runs blocking web3 calls in thread executor,
never touches trading state, order engine, or WebSocket connections.
Uses file lock to prevent concurrent redemptions.
"""

import asyncio
import fcntl
import json
import logging
import os
import time
from datetime import datetime

import httpx
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account

logger = logging.getLogger("redeem-cmd")

# --- Config from env ---
PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "")
SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "2"))
RPC_URL = os.getenv("RPC_URL", os.getenv("POLY_RPC_URL", "https://polygon-rpc.com"))
PROXY_TURKEY = os.getenv("PROXY_TURKEY", "")

DATA_API = "https://data-api.polymarket.com"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

LOCK_FILE = "/tmp/redeem.lock"

# --- ABIs (minimal) ---
CTF_ABI = json.loads('[{"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"id","type":"uint256"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"conditionId","type":"bytes32"}],"name":"payoutDenominator","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"indexSets","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}]')

NEG_RISK_ABI = json.loads('[{"inputs":[{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}]')

GNOSIS_SAFE_ABI = json.loads('[{"inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},{"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},{"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},{"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},{"name":"refundReceiver","type":"address"},{"name":"signatures","type":"bytes"}],"name":"execTransaction","outputs":[{"name":"success","type":"bool"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"nonce","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},{"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},{"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},{"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},{"name":"refundReceiver","type":"address"},{"name":"_nonce","type":"uint256"}],"name":"getTransactionHash","outputs":[{"name":"","type":"bytes32"}],"stateMutability":"view","type":"function"}]')

# ---------- file lock (non-blocking check) ----------

class _AsyncRedeemLock:
    """Async-friendly file lock. Acquire/release run in executor."""

    def __init__(self):
        self._fd = None

    async def acquire(self, timeout: float = 60.0) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._acquire_sync, timeout)

    def _acquire_sync(self, timeout: float) -> bool:
        start = time.time()
        try:
            self._fd = open(LOCK_FILE, "w")
            while time.time() - start < timeout:
                try:
                    fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._fd.write(f"{os.getpid()}\n")
                    self._fd.flush()
                    return True
                except (IOError, OSError):
                    time.sleep(0.5)
            self._fd.close()
            self._fd = None
            return False
        except Exception:
            if self._fd:
                self._fd.close()
                self._fd = None
            return False

    async def release(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._release_sync)

    def _release_sync(self):
        if self._fd:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                self._fd.close()
            except Exception:
                pass
            self._fd = None


# ---------- helpers (all sync — run in executor) ----------

def _connect_w3():
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if not w3.is_connected():
        raise ConnectionError(f"Cannot connect to RPC: {RPC_URL}")
    return w3


def _find_redeemable(wallet: str) -> list[dict]:
    """Fetch redeemable positions from Data API."""
    proxies = {"https://": PROXY_TURKEY, "http://": PROXY_TURKEY} if PROXY_TURKEY else None
    resp = httpx.get(
        f"{DATA_API}/positions",
        params={"user": wallet, "limit": 500, "sizeThreshold": 0.01, "redeemable": True},
        timeout=30,
        proxy=PROXY_TURKEY if PROXY_TURKEY else None,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Data API {resp.status_code}: {resp.text[:200]}")

    positions = resp.json()
    # Group by conditionId
    by_cond: dict[str, dict] = {}
    for pos in positions:
        cid = pos.get("conditionId")
        if not cid:
            continue
        if cid not in by_cond:
            by_cond[cid] = {
                "slug": pos.get("slug", "?"),
                "title": pos.get("title", "?"),
                "condition_id": cid,
                "neg_risk": pos.get("negativeRisk", False),
                "up_balance": 0,
                "down_balance": 0,
            }
        size_wei = int(float(pos.get("size", 0)) * 1e6)
        outcome = pos.get("outcome", "")
        if outcome in ("Up", "YES", "Higher"):
            by_cond[cid]["up_balance"] += size_wei
        else:
            by_cond[cid]["down_balance"] += size_wei

    return [v for v in by_cond.values() if v["up_balance"] > 0 or v["down_balance"] > 0]


def _redeem_one(w3, position: dict) -> tuple[bool, str]:
    """Redeem a single position on-chain. Returns (success, message)."""
    cid = position["condition_id"]
    up_bal = position["up_balance"]
    dn_bal = position["down_balance"]
    neg_risk = position["neg_risk"]
    slug = position["slug"]

    # Oracle check
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
    payout = ctf.functions.payoutDenominator(Web3.to_bytes(hexstr=cid)).call()
    if payout == 0:
        return False, f"⏳ {slug}: oracle not resolved"

    account = Account.from_key(PRIVATE_KEY)
    owner = account.address
    safe_addr = Web3.to_checksum_address(FUNDER_ADDRESS)

    # Encode inner redeem call
    if neg_risk:
        adapter = w3.eth.contract(address=Web3.to_checksum_address(NEG_RISK_ADAPTER), abi=NEG_RISK_ABI)
        tmp = adapter.functions.redeemPositions(Web3.to_bytes(hexstr=cid), [up_bal, dn_bal]).build_transaction({"from": safe_addr})
        redeem_data = tmp["data"]
        target = NEG_RISK_ADAPTER
    else:
        tmp = ctf.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS), bytes(32),
            Web3.to_bytes(hexstr=cid), [1, 2]
        ).build_transaction({"from": safe_addr})
        redeem_data = tmp["data"]
        target = CTF_ADDRESS

    # Gnosis Safe execTransaction
    safe = w3.eth.contract(address=safe_addr, abi=GNOSIS_SAFE_ABI)
    safe_nonce = safe.functions.nonce().call()
    eoa_nonce = w3.eth.get_transaction_count(owner)
    gas_price = w3.eth.gas_price

    zero = "0x0000000000000000000000000000000000000000"
    to = Web3.to_checksum_address(target)
    tx_hash_to_sign = safe.functions.getTransactionHash(
        to, 0, redeem_data, 0, 0, 0, 0, zero, zero, safe_nonce
    ).call()

    signed_msg = account.unsafe_sign_hash(tx_hash_to_sign)
    r = signed_msg.r.to_bytes(32, byteorder="big")
    s = signed_msg.s.to_bytes(32, byteorder="big")
    sig = r + s + bytes([signed_msg.v])

    tx = safe.functions.execTransaction(
        to, 0, redeem_data, 0, 0, 0, 0, zero, zero, sig
    ).build_transaction({
        "chainId": 137, "from": owner, "nonce": eoa_nonce,
        "gas": 1_000_000, "gasPrice": int(gas_price * 1.2),
    })

    signed_tx = w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)

    # Wait receipt with retry
    for attempt in range(3):
        try:
            time.sleep(2)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            break
        except Exception as e:
            if "rate limit" in str(e).lower() and attempt < 2:
                time.sleep(3)
                continue
            raise

    status = receipt.get("status")
    gas_used = receipt.get("gasUsed", 0)
    value = (up_bal + dn_bal) / 1e6

    if status == 1:
        return True, f"✅ {slug}: ~${value:.2f} (gas {gas_used})"
    else:
        return False, f"❌ {slug}: TX reverted (gas {gas_used})\nhttps://polygonscan.com/tx/{tx_hash.hex()}"


def _run_redeem_all() -> str:
    """Synchronous: find redeemable → redeem each → return summary text."""
    if not PRIVATE_KEY or not FUNDER_ADDRESS:
        return "⚠️ POLY_PRIVATE_KEY / POLY_FUNDER_ADDRESS not configured"

    wallet = Web3.to_checksum_address(FUNDER_ADDRESS)

    # 1. Find redeemable
    redeemable = _find_redeemable(wallet)
    if not redeemable:
        return "✅ Нет позиций для редима"

    # 2. Connect w3
    w3 = _connect_w3()

    # 3. Redeem each
    results = []
    ok_count = 0
    total_value = 0.0
    for pos in redeemable:
        try:
            success, msg = _redeem_one(w3, pos)
            results.append(msg)
            if success:
                ok_count += 1
                total_value += (pos["up_balance"] + pos["down_balance"]) / 1e6
            time.sleep(2)  # pause between TXs
        except Exception as e:
            results.append(f"❌ {pos['slug']}: {e}")
            logger.exception(f"Redeem error: {pos['slug']}")

    header = f"🔓 <b>Redeem: {ok_count}/{len(redeemable)}</b>"
    if ok_count:
        header += f" (~${total_value:.2f} USDC)"
    return header + "\n\n" + "\n".join(results)


# ---------- public async API ----------

async def execute_redeem() -> str:
    """Run full redeem cycle asynchronously. Safe to call from any async context."""
    lock = _AsyncRedeemLock()
    if not await lock.acquire(timeout=10):
        return "⏳ Другой редим уже выполняется, подожди..."

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _run_redeem_all)
        return result
    except Exception as e:
        logger.exception("Redeem command error")
        return f"❌ Ошибка редима: {e}"
    finally:
        await lock.release()
