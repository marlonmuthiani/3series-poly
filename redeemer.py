"""Redeemer module — redeems winning positions after market resolution."""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import httpx
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account

from config import (
    PRIVATE_KEY, FUNDER_ADDRESS, RPC_URL, GAMMA_API,
    CTF_ADDRESS, USDC_ADDRESS, NEG_RISK_ADAPTER, CHAIN_ID,
)
from notifier import notify

logger = logging.getLogger("redeemer")

# ---------- ABIs ----------

CTF_ABI = json.loads('''[
    {"inputs":[{"name":"account","type":"address"},{"name":"id","type":"uint256"}],
     "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[{"name":"conditionId","type":"bytes32"}],
     "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[{"name":"collateralToken","type":"address"},{"name":"parentCollectionId","type":"bytes32"},
     {"name":"conditionId","type":"bytes32"},{"name":"indexSets","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
]''')

NEG_RISK_ABI = json.loads('''[
    {"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"amounts","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
]''')

GNOSIS_SAFE_ABI = json.loads('''[
    {"inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},
     {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
     {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
     {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
     {"name":"refundReceiver","type":"address"},{"name":"signatures","type":"bytes"}],
     "name":"execTransaction","outputs":[{"name":"success","type":"bool"}],
     "stateMutability":"payable","type":"function"},
    {"inputs":[],"name":"nonce","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},
     {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
     {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
     {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
     {"name":"refundReceiver","type":"address"},{"name":"_nonce","type":"uint256"}],
     "name":"getTransactionHash","outputs":[{"name":"","type":"bytes32"}],
     "stateMutability":"view","type":"function"}
]''')

# ---------- Lock ----------

LOCK_FILE = "/tmp/redeem.lock"


class RedeemLock:
    """File-based lock to prevent concurrent redemptions."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout
        self._acquired = False

    def acquire(self) -> bool:
        try:
            if os.path.exists(LOCK_FILE):
                mtime = os.path.getmtime(LOCK_FILE)
                if time.time() - mtime > self.timeout:
                    logger.warning("Stale lock found, removing")
                    os.unlink(LOCK_FILE)
                else:
                    logger.info("Lock held by another process")
                    return False
            # Atomic create — O_EXCL fails if file exists (no TOCTOU)
            fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            self._acquired = True
            return True
        except FileExistsError:
            logger.info("Lock held by another process (atomic)")
            return False
        except Exception as e:
            logger.error(f"Lock acquire error: {e}")
            return False

    def release(self):
        if self._acquired:
            try:
                os.unlink(LOCK_FILE)
            except FileNotFoundError:
                pass
            self._acquired = False


# ---------- Redeemer ----------

class Redeemer:
    """Async redeemer that watches for resolved markets and redeems."""

    def __init__(self):
        self._w3: Optional[Web3] = None
        self._pending: list[dict] = []  # {coin, epoch, condition_id, scheduled_at}

    def _get_w3(self) -> Web3:
        if self._w3 is None:
            self._w3 = Web3(Web3.HTTPProvider(RPC_URL))
            self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return self._w3

    # FIX #9: Persistence for pending redemptions
    def dump_pending(self) -> list:
        """Return pending redemptions for state persistence."""
        return list(self._pending)

    def load_pending(self, data: list):
        """Restore pending redemptions from state."""
        self._pending = data
        logger.info(f"Loaded {len(data)} pending redemptions from state")

    def schedule_redeem(self, coin: str, epoch: int, condition_id: str):
        """Schedule a redemption attempt ~2 minutes after resolution."""
        self._pending.append({
            "coin": coin,
            "epoch": epoch,
            "condition_id": condition_id,
            "scheduled_at": time.time() + 120,  # 2 min delay
        })
        logger.info(f"Scheduled redeem for {coin} epoch={epoch} in ~2 min")

    async def run(self):
        """Background loop processing pending redemptions."""
        logger.info("Redeemer started")
        while True:
            try:
                now = time.time()
                ready = [p for p in self._pending if now >= p["scheduled_at"]]
                for item in ready:
                    self._pending.remove(item)
                    await self._try_redeem(item)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Redeemer loop error: {e}")
            await asyncio.sleep(10)

    async def _try_redeem(self, item: dict):
        """Attempt redemption for a single market."""
        coin = item["coin"]
        epoch = item["epoch"]
        condition_id = item["condition_id"]
        logger.info(f"Attempting redeem: {coin} epoch={epoch}")

        if not PRIVATE_KEY or not FUNDER_ADDRESS:
            logger.warning("No credentials for redemption")
            return

        w3 = self._get_w3()
        ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)

        # Check oracle resolution
        try:
            loop = asyncio.get_event_loop()
            payout_denom = await loop.run_in_executor(
                None,
                lambda: ctf.functions.payoutDenominator(
                    Web3.to_bytes(hexstr=condition_id)
                ).call()
            )
            if payout_denom == 0:
                logger.warning(f"Oracle not resolved yet for {coin} epoch={epoch}, rescheduling")
                self._pending.append({**item, "scheduled_at": time.time() + 60})
                return
        except Exception as e:
            logger.error(f"Oracle check failed: {e}")
            return

        # Check balances on proxy wallet
        wallet = Web3.to_checksum_address(FUNDER_ADDRESS)
        slug = f"{coin}-updown-15m-{epoch}"

        try:
            # Fetch token IDs from Gamma
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
                if resp.status_code != 200 or not resp.json():
                    logger.warning(f"Cannot fetch market info for {slug}")
                    return
                event = resp.json()[0]
                market = event["markets"][0]
                token_ids = json.loads(market.get("clobTokenIds", "[]"))
                if len(token_ids) < 2:
                    return
                neg_risk = market.get("negRisk", False)

            # Check balances
            up_balance = await loop.run_in_executor(
                None, lambda: ctf.functions.balanceOf(wallet, int(token_ids[0])).call()
            )
            down_balance = await loop.run_in_executor(
                None, lambda: ctf.functions.balanceOf(wallet, int(token_ids[1])).call()
            )

            if up_balance == 0 and down_balance == 0:
                logger.info(f"No balance to redeem for {slug}")
                return

            logger.info(f"Balances for {slug}: UP={up_balance} DOWN={down_balance}")

            # Execute redemption via Gnosis Safe
            await self._execute_redeem(
                w3, condition_id, up_balance, down_balance, neg_risk, slug
            )

        except Exception as e:
            logger.error(f"Redeem failed for {slug}: {e}")

    async def _execute_redeem(
        self, w3: Web3, condition_id: str,
        up_balance: int, down_balance: int,
        neg_risk: bool, slug: str,
    ):
        """Execute on-chain redemption via Gnosis Safe execTransaction."""
        lock = RedeemLock(timeout=60.0)
        if not lock.acquire():
            logger.warning("Redeem lock held, skipping")
            return

        try:
            account = Account.from_key(PRIVATE_KEY)
            owner_address = account.address
            safe_address = Web3.to_checksum_address(FUNDER_ADDRESS)
            ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)

            # Encode redeemPositions call
            if neg_risk:
                adapter = w3.eth.contract(
                    address=Web3.to_checksum_address(NEG_RISK_ADAPTER), abi=NEG_RISK_ABI
                )
                tx_data = adapter.functions.redeemPositions(
                    Web3.to_bytes(hexstr=condition_id), [up_balance, down_balance]
                ).build_transaction({"from": safe_address})["data"]
                target = NEG_RISK_ADAPTER
            else:
                tx_data = ctf.functions.redeemPositions(
                    Web3.to_checksum_address(USDC_ADDRESS),
                    bytes(32),
                    Web3.to_bytes(hexstr=condition_id),
                    [1, 2],
                ).build_transaction({"from": safe_address})["data"]
                target = CTF_ADDRESS

            safe = w3.eth.contract(address=safe_address, abi=GNOSIS_SAFE_ABI)
            loop = asyncio.get_event_loop()

            safe_nonce = await loop.run_in_executor(None, safe.functions.nonce().call)
            eoa_nonce = await loop.run_in_executor(
                None, lambda: w3.eth.get_transaction_count(owner_address)
            )
            gas_price = await loop.run_in_executor(None, lambda: w3.eth.gas_price)

            to = Web3.to_checksum_address(target)
            zero_addr = "0x0000000000000000000000000000000000000000"

            tx_hash_to_sign = await loop.run_in_executor(
                None,
                lambda: safe.functions.getTransactionHash(
                    to, 0, tx_data, 0, 0, 0, 0, zero_addr, zero_addr, safe_nonce
                ).call()
            )

            signed_msg = account.unsafe_sign_hash(tx_hash_to_sign)
            r = signed_msg.r.to_bytes(32, byteorder="big")
            s = signed_msg.s.to_bytes(32, byteorder="big")
            signature = r + s + bytes([signed_msg.v])

            tx = safe.functions.execTransaction(
                to, 0, tx_data, 0, 0, 0, 0, zero_addr, zero_addr, signature
            ).build_transaction({
                "chainId": CHAIN_ID,
                "from": owner_address,
                "nonce": eoa_nonce,
                "gas": 1000000,
                "gasPrice": int(gas_price * 1.2),
            })

            signed_tx = w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
            tx_hash = await loop.run_in_executor(
                None, lambda: w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            )
            logger.info(f"Redeem TX sent: {tx_hash.hex()}")
            print(f"💰 Redeem TX for {slug}: {tx_hash.hex()}")

            receipt = await loop.run_in_executor(
                None, lambda: w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            )

            if receipt.get("status") == 1:
                logger.info(f"Redeemed {slug} successfully! Gas: {receipt.get('gasUsed')}")
                print(f"✅ Redeemed {slug}")
                await notify(f"💰 <b>Redeemed</b> {slug}\nTX: <code>{tx_hash.hex()[:16]}…</code>")
            else:
                logger.error(f"Redeem TX reverted for {slug}")
                print(f"❌ Redeem failed for {slug}")
                await notify(f"❌ Redeem FAILED for {slug}")

        except Exception as e:
            logger.error(f"Redeem execution error for {slug}: {e}")
        finally:
            lock.release()
