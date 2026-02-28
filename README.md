# 🎰 3 Series

**An automated series-reversal trading bot for Polymarket binary markets.**

3 Series watches consecutive outcome streaks on Polymarket's short-duration binary markets (BTC, ETH, SOL, XRP — 15-minute windows) and bets on the reversal when a streak reaches a configurable trigger length.

The idea is simple: after N consecutive same-direction outcomes, the probability of continuation drops. 3 Series places a bet against the streak and rides the mean reversion.

---

## ✨ Features

- **🔄 Series Reversal Strategy** — Detects N consecutive same-direction outcomes (Up or Down) and bets on the reversal
- **📊 Multi-Asset** — Simultaneously tracks BTC, ETH, SOL, and XRP binary markets
- **⚡ Real-Time WebSocket** — Prices via Chainlink RTDS, order fills via Polymarket User Channel WS — zero REST polling
- **📦 Batch Orders** — Single API call for multiple assets (reduces rate-limiting risk)
- **🔐 Gnosis Safe** — All orders signed via Gnosis Safe proxy wallet (SIGNATURE_TYPE=2)
- **🤖 Telegram Bot** — Built-in commands: `/status`, `/balance`, `/15m`, `/redeem`
- **💰 Auto-Redeem** — Automatically redeems winning positions on-chain after market resolution
- **🛡️ Duplicate Prevention** — Multi-layer dedup: epoch tracking, order ID matching, state machine guards
- **📈 Verifier Module** — Independently verifies outcomes via Gamma API (trust but verify)
- **🔧 Systemd-Ready** — Ships with service file for auto-restart on crash + boot persistence

---

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Chainlink   │────▶│   Strategy   │────▶│  Order Engine   │
│  RTDS (WS)   │     │  (series     │     │  (CLOB API +    │
│  Live prices  │     │   tracker)   │     │   batch orders) │
└─────────────┘     └──────┬───────┘     └────────┬────────┘
                           │                       │
┌─────────────┐     ┌──────▼───────┐     ┌────────▼────────┐
│  Gamma API   │────▶│  Verifier    │     │  User WS        │
│  Outcomes    │     │  (confirms   │     │  (fill tracking, │
│              │     │   results)   │     │   position mgmt) │
└─────────────┘     └──────────────┘     └─────────────────┘
                           │
                    ┌──────▼───────┐     ┌─────────────────┐
                    │  Notifier    │     │  TG Commands     │
                    │  (Telegram)  │     │  /status /balance│
                    └──────────────┘     └─────────────────┘
```

### Core Files

| File | Purpose |
|---|---|
| `main.py` | Entry point — orchestrates strategy loop, state machine, WebSocket lifecycle |
| `strategy.py` | Series detection engine — tracks streaks per coin, decides when to bet |
| `order_engine.py` | CLOB order placement, cancellation, prefix-matching fill tracking |
| `user_ws.py` | Persistent User Channel WebSocket — real-time order/trade events |
| `chainlink.py` | Chainlink RTDS price feed — streaming live prices via WebSocket |
| `market_resolver.py` | Discovers next market window, resolves token IDs via Gamma API |
| `token_cache.py` | Caches condition_id → token_id mappings to reduce API calls |
| `verifier.py` | Independent outcome verification against Gamma API |
| `notifier.py` | Telegram notifications for trades, errors, and status updates |
| `tg_commands.py` | Telegram bot command handler (`/status`, `/balance`, `/15m`, `/redeem`) |
| `redeemer.py` | Background auto-redemption of resolved winning positions |
| `redeemer_cmd.py` | On-demand redemption via `/redeem` command |
| `config.py` | Centralized configuration from environment variables |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- A Polymarket account with CLOB API credentials
- A Gnosis Safe proxy wallet on Polygon (this is what Polymarket uses)
- A Telegram bot token (for notifications)

### Installation

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/3series.git
cd 3series

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure
cp .env.example .env
nano .env  # Fill in your credentials
```

### Running

```bash
# Paper trading mode (recommended first!)
# Set MODE=OBSERVATION in .env, then:
python main.py

# Live trading
# Set MODE=LIVE in .env, then:
python main.py
```

### Running as a Service (recommended for production)

```bash
# Create systemd service
sudo tee /etc/systemd/system/3series.service << 'EOF'
[Unit]
Description=3 Series Trading Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/path/to/3series
ExecStart=/path/to/3series/venv/bin/python main.py
Restart=always
RestartSec=10
TimeoutStopSec=15
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now 3series
```

---

## 🎯 How the Strategy Works

### The Core Idea

Polymarket runs 15-minute binary markets on crypto assets: "Will BTC go up or down in the next 15 minutes?"

Each market resolves to either **Up** or **Down**. 3 Series tracks consecutive same-direction outcomes:

```
BTC: Up, Up, Up  ← 3 streak detected! → Bet DOWN on next window
ETH: Down, Down, Down  ← 3 streak! → Bet UP on next window
SOL: Up, Down, Up  ← no streak → skip
```

### Why It Works (When It Works)

Binary outcome markets tend to exhibit **mean reversion** over short timeframes. After 3+ consecutive moves in one direction, the probability of continuation drops below 50% — creating a slight edge.

At a bet price of $0.49 per contract:
- **Risk:** $8.33 per trade (17 contracts × $0.49)
- **Reward:** $8.67 per trade (17 × ($1.00 - $0.49))
- **Breakeven win rate:** 49%
- **Observed win rate (7-day):** ~55%

### State Machine

```
SCANNING ──[streak detected]──▶ BET_ACTIVE
    ▲                               │
    │                               │
    └──[outcome resolved]───────────┘
    └──[order failed → rollback]────┘
```

---

## 🤖 Telegram Commands

The bot includes a built-in Telegram command interface:

| Command | Description |
|---|---|
| `/status` | Current state of all coins, active bets, streaks |
| `/balance` | Live USDC balance + open positions from Polymarket Data API |
| `/15m` | Last 5 resolved outcomes per coin (from Gamma API) |
| `/redeem` | Manually trigger redemption of resolved winning positions |

---

## ⚙️ Configuration

All configuration is via environment variables (`.env` file). See `.env.example` for the full list.

### Key Parameters

| Variable | Default | Description |
|---|---|---|
| `MODE` | `OBSERVATION` | `LIVE` for real trading, `OBSERVATION` for paper |
| `TRIGGER_N` | `3` | How many consecutive same-direction outcomes trigger a bet |
| `BET_SIZE` | `10.00` | USD amount per bet |
| `BET_PRICE` | `0.49` | Limit order price (lower = better odds but lower fill rate) |
| `FILL_TIMEOUT` | `900` | Seconds to wait for limit order fill (one full epoch) |
| `WINDOW` | `900` | Market window duration in seconds (900 = 15 minutes) |
| `COINS` | `btc,eth,sol,xrp` | Comma-separated list of assets to track |

### Price Strategy

The `BET_PRICE` parameter controls the risk/reward tradeoff:

| Price | Risk/Trade | Reward/Trade | Breakeven WR | Fill Rate |
|---|---|---|---|---|
| $0.70 | $11.90 | $5.10 | 70% | High |
| $0.55 | $9.35 | $7.65 | 55% | Medium |
| $0.49 | $8.33 | $8.67 | 49% | Lower |
| $0.35 | $5.95 | $11.05 | 35% | Low |

Lower prices = better math, but orders may not fill. The bot uses GTC (Good-Til-Cancelled) orders and waits up to `FILL_TIMEOUT` seconds.

---

## 🔒 Security Notes

- **Private keys** are loaded from environment variables only — never hardcoded
- All orders use **Gnosis Safe** signatures (SIGNATURE_TYPE=2) — your EOA never holds funds directly
- The bot **never** polls REST API for fill status (WebSocket only) — avoids rate-limit bans
- Telegram bot uses **allowlist** — only responds to your configured `TRADE_CHAT_ID`
- WebSocket connections close gracefully (code 1000) to prevent 429 rate limits on reconnect

---

## 📊 Order Flow

```
1. Strategy detects streak ≥ TRIGGER_N
2. market_resolver.py finds next epoch's tokens via Gamma API
3. order_engine.py places batch GTC orders via POST /orders
4. user_ws.py tracks fills in real-time (MATCHED → MINED → CONFIRMED)
5. On fill: state → observation (waiting for market resolution)
6. verifier.py checks outcome via Gamma API
7. Won? → redeemer.py queues on-chain redemption
8. Back to SCANNING
```

---

## 🏥 Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| "No PRIVATE_KEY configured" | Missing `.env` | Copy `.env.example` → `.env` |
| Orders not filling | Price too aggressive for current market | Adjust `BET_PRICE` closer to 0.50 |
| "429 Too Many Requests" | Rate-limited by Polymarket WS | Bot auto-recovers with backoff; don't restart rapidly |
| "FAILED" order status | On-chain execution failed | Polymarket executor issue; retry logic handles this |
| Zero balance shown | Wrong `POLY_FUNDER_ADDRESS` | Check your Gnosis Safe address |

---

## ⚠️ Disclaimer

**This is experimental trading software. Use at your own risk.**

- Prediction markets are volatile and unpredictable
- Past performance does not guarantee future results
- The "series reversal" strategy has no mathematical guarantee — it's a heuristic
- You can and will lose money
- Never trade with money you can't afford to lose
- This software is provided as-is, with no warranty

---

## 📄 License

MIT License. See the code — it's yours to learn from, modify, and run.

---

*Built with obsessive attention to order lifecycle, WebSocket reliability, and not losing money twice on the same trade.*
