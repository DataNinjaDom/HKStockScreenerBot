# HK Stock Screening & Alert Bot

<p align="center">
  <img src="assets/logo.png" alt="AI Algorithmic Trading Bot Logo" width="160" />
</p>

**Automated stock screening and multi-channel alerts — AI-driven technical analysis with Telegram & WhatsApp delivery.**

---

## What it does

- **Screens** a large universe of stocks (from an AASTOCKS Excel export or built-in list)
- **Downloads** historical OHLCV via yfinance and computes a full set of technical indicators
- **Ranks** by 20-day return and flags alerts (volume spikes, RSI, MACD, GMMA, Bollinger, ADX, golden/death cross, 52W levels)
- **Exports** multi-sheet Excel reports and performance vs Hang Seng Index charts
- **Sends** daily summaries and attachments to **Telegram** and/or **WhatsApp** (Twilio)

Runs as a **daily workflow** from the command line or Jupyter, with optional built-in scheduler, duplicate-report protection, and retries.

---

## Features

| Area | Details |
|------|--------|
| **Data** | yfinance per-ticker download, HSI benchmark, configurable period (e.g. `1y`) |
| **Indicators** | Returns (1/5/20/60d, YTD), GMMA, Z-Score, ADX/DI+/DI-, RSI, MACD, Bollinger %B, volume analysis, 52W high/low, SMA 50/200, golden/death cross |
| **Alerts** | Volume spike, strong trend (ADX), RSI overbought/oversold, MACD cross, MA cross, Z-score extremes, near 52W high/low, Bollinger breaks |
| **Output** | Excel (multiple sheets), normalized performance chart, candlestick charts (top N) |
| **Delivery** | Telegram (HTML summary + chart + candlesticks + Excel), WhatsApp (split messages + chart via file.io) |
| **Reliability** | Per-stock error handling, retries, rate limiting, optional scheduler, same-day send guard |

---

## Requirements

- **Python 3.9+**
- See [requirements.txt](requirements.txt) for packages (yfinance, pandas, numpy, matplotlib, mplfinance, openpyxl, python-telegram-bot, twilio, ta, requests, nest_asyncio, schedule).

---

## Quick start

```bash
# Clone
git clone https://github.com/DataNinjaDom/HKStockScreenerBot.git
cd HKStockScreenerBot

# Install
pip install -r requirements.txt

# Configure (edit CONFIG section in hk_stock_screener_bot.py)
# - INPUT_EXCEL_PATH or use fallback list
# - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and/or Twilio WhatsApp

# Run
python hk_stock_screener_bot.py
```

**Force resend same day:**  
`python hk_stock_screener_bot.py --force`

**Jupyter:** Run the script in a cell or `import hk_stock_screener_bot; hk_stock_screener_bot.main()`

---

## Configuration

All settings live in the **CONFIG** block at the top of `hk_stock_screener_bot.py`:

- **Paths:** input Excel, output Excel/charts, log file, duplicate marker
- **Data:** `DATA_PERIOD`, `TOP_N_STOCKS`, `TOP_N_EXCEL`
- **Indicators:** ADX/RSI periods, Bollinger std, alert thresholds (volume, ADX, RSI, z-score, 52W %, etc.)
- **Rate limiting:** download delay/timeout, Telegram/WhatsApp retries
- **Channels:** `TELEGRAM_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`; Twilio WhatsApp vars
- **Scheduler:** `SCHEDULER_ENABLED`, `SCHEDULER_RUN_TIME` (e.g. `"18:30"` after market close)

---

## Input / output

| Input | Description |
|-------|-------------|
| `aastocks_export.xlsx` | Optional. Column with "Code"/"Ticker"/"代號" etc. → normalized to `XXXX.HK`. If missing, uses built-in blue-chip list. |

| Output | Description |
|-------|-------------|
| `stock_screening_results.xlsx` | Multi-sheet: Full results, Top/Bottom performers, Bullish GMMA, High ADX uptrend, RSI extremes, Volume spikes, MACD crossovers, Alert summary, Market summary |
| `top_performers_chart.png` | Normalized price vs HSI + relative strength |
| `candlestick_charts/*.png` | Last 60 days, volume, SMA, Bollinger (top N stocks) |
| `bot_run_log.txt` | Run log (DEBUG to file, INFO to console) |
| `.last_sent_date.txt` | Prevents duplicate Telegram/WhatsApp send same day (override with `--force` or `FORCE_SEND`) |

---

## Telegram setup

1. Create a bot with [@BotFather](https://t.me/BotFather), get the token.
2. Start a chat with the bot, send any message.
3. Open `https://api.telegram.org/bot<TOKEN>/getUpdates`, find your `chat_id`.
4. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in the CONFIG section.

---

## WhatsApp (Twilio) setup

1. Sign up at [Twilio](https://www.twilio.com/try-twilio), use the WhatsApp sandbox.
2. Join the sandbox from your WhatsApp (e.g. “join &lt;code&gt;”).
3. Copy Account SID and Auth Token; set `TWILIO_WHATSAPP_FROM` (sandbox number) and `TWILIO_WHATSAPP_TO` (your number as `whatsapp:+...`).
4. Chart is sent via a temporary file host (e.g. file.io); for production, use a WhatsApp Business API number and approved templates.

---

## License

Use and modify as you like. No warranty.

---

<p align="center">
  <sub>Built for automated screening and alerts — data-driven, no fluff.</sub>
</p>
