# Nasdaq-100 (`US100Cash`) M15 Auto-Trading Bot (XM / MetaTrader 5)

Automated intraday structural floor/ceiling rejection trading system for XM's **Nasdaq-100 Cash Index CFD (`US100Cash`)** on **15-minute (`M15`)** candles, deployed on Linux (`/opt/nasdaq`) alongside a Wine MetaTrader 5 sidecar stack.

> Start every session at `docs/EXECUTION-PLAN.md`, then check `docs/HANDOFF.md`. For server install, service layout, and troubleshooting, see `docs/NASDAQ-OPS.md`.

---

## Architecture Overview

| Component | Path | Runtime | Purpose |
|---|---|---|---|
| **Trading Engine** | `engine.py` | Linux Python (`/opt/nasdaq`) | Evaluates closed `US100Cash` `M15` bars, applies bidirectional regime/risk gates, executes `FORWARD_TEST` or `LIVE` orders |
| **Risk & Session Filter** | `trade_filter.py` | Linux Python | Escalating SL cooldowns (30m/60m), 3-SL / `$15` daily circuit breaker, adaptive ATR% band (`0.04%–0.80%`), DST-aware `America/New_York` CME hours & macro/open blackouts |
| **MT5 Candle + Warm-Up Sidecar** | `tools/mt5_feed.py` | Wine Python (`mt5feed-nasdaq`) | Publishes closed `US100Cash` `M15` bars (`mt5_last_candle.json`) + 250-bar startup history (`mt5_warmup_candles.json`) so EMA200 is warm immediately |
| **MT5 Quote & Shadow Logger** | `tools/mt5_quotes.py` | Wine Python (`mt5quotes-nasdaq`) | Strictly read-only logger recording live bid/ask spreads, contract specs, and 12,000 backfilled `M15` candles (`xm_data/candles_m15.csv`) |
| **MT5 Order Executor Sidecar** | `tools/mt5_executor.py` | Wine Python (`mt5exec-nasdaq`) | Dormant (`MT5_EXEC_ENABLED=0`) and demo-locked (`MT5_EXEC_ALLOW_REAL=0`) by default; bridges Linux `LIVE` intents to MT5 via `/opt/nasdaq/mt5_bridge/` |
| **Web Dashboard** | `dashboard.py` + `pressure.py` | Linux Python (`:6001`) | Read-only Flask UI showing the `US100Cash` M15 funnel, 60-bar tape pressure, and `/readiness`: current feed/heartbeat, shared data gates and an unsigned manual checklist |
| **Feed Diagnosis** | `feed_health.py` + `tools/feed_doctor.py` | Linux Python | Classifies every "no fresh M15 bar" cause (missing file / unreadable / schema / foreign symbol / frozen candle / clock skew), reads the sidecar heartbeat (`mt5_feed_health.json`), and proves whether MT5 itself is alive via the quote-logger witness |
| **Autosync & Safe Deploy** | `tools/autosync.sh` | Cron (`root`, every 15m) | Commits live data, pulls upstream merges gated by `tools/smoke_test.py`, runs `check_data.py` + `handoff_check.py` + `xm_quote_report.py` |

---

## XM `US100Cash` Contract & Strategy Summary

- **Symbol:** `US100Cash` (Nasdaq-100 Cash Index CFD, no futures expiry roll)
- **Timeframe:** `M15` (`CANDLE_SECONDS = 900`, 4 bars/hour)
- **Contract sizing:** `CONTRACT_SIZE = 1.0`, `LOT_SIZE = 0.10` (`$0.10` per index point; XM min lot `0.10`)
- **Geometry:** `SL = 2.0 * ATR(14)`, `TP = 4.0 * ATR(14)` (`RR = 1:2`, breakeven decisive win rate `33.3%`)
- **Assumed cost:** `$0.25` per round-trip trade at `0.10` lot (~`2.5` index points)
- **Entry gates:** 20-bar (5h) structural floor/ceiling test (`0.12%` buffer) + `>= 15%` rejection wick + close back inside structure + `EMA50 vs EMA200` trend + 12-bar (3h) `EMA50` slope + `0.50 * ATR` proximity to `EMA50` + directional `RSI(14)` (`40–70` BUY / `30–60` SELL)
- **US Market Hours & Blackouts (`America/New_York`, DST-aware):**
  - **Quiet / Closed:** Fri `16:55 ET` – Sun `18:10 ET` + Mon–Thu `16:55–18:10 ET` CME maintenance break
  - **Entry Blackouts:** `08:25–08:40 ET` (US macro releases), `09:25–09:45 ET` (NASDAQ `09:30 ET` cash open whip-saw), `16:45–18:15 ET` (CME close & rollover spread widening)

---

## Verification & Operations Suite

```bash
# 1. Full offline regression suite (engine, filter, dashboard, feed, quotes, executor, autosync)
python3 tools/smoke_test.py

# 2. Offline LIVE execution safety probe (fake MT5 + temporary bridge; no orders)
python3 tools/live_path_probe.py

# 3. Feed triage (reason + sidecar heartbeat + independent quote-logger witness)
python3 tools/feed_doctor.py

# 4. Data integrity, HANDOFF snapshot freshness & engine liveness
python3 tools/check_data.py
python3 tools/handoff_check.py    # also reports "engine lag" when xm_data runs ahead of the engine

# 5. XM US100Cash quote/spread/cost/parity report & 12,000-bar M15 historical backtest
python3 tools/xm_quote_report.py
python3 tools/backtest.py xm_data/candles_m15.csv --spread 0.25

# 6. Readiness regression suite (install Flask to run the dashboard-specific tests)
python3 -m unittest discover -s tests -v
```

The Readiness page and `tools/live_readiness.py` share the same post-gate data calculations. Fewer than ten trades leaves statistical results **UNKNOWN**; passing all six data gates is necessary but not sufficient for real-money trading. Current feed/heartbeat health, recent engine/log snapshots and the manually signed contract, cost, demo-soak and kill-switch checks remain separate requirements.

For a read-only local dashboard using the checked-in snapshot (no MT5 connection or orders):

```bash
python3 -m venv .venv
.venv/bin/pip install flask
NASDAQ_DATA_DIR="$PWD" .venv/bin/python dashboard.py  # then open http://localhost:6001/readiness
```

The local checkout does not contain the live MT5 feed, so a missing-feed verdict here does not describe the VPS. `/healthz` is dashboard-process liveness only, not a trading or feed-health check.

## Server Operations (`/opt/nasdaq`)

See `docs/NASDAQ-OPS.md` for step-by-step commands to install `/opt/nasdaq`, install the `deploy/*.service` systemd units (root uses `WINEPREFIX=/root/.mt5`), install `tzdata` into the Wine interpreter, verify `mt5feed-nasdaq` + `mt5quotes-nasdaq`, and troubleshoot stale feeds — then run Stage B demo validation (`docs/START-LEVER1.md`) before arming `LIVE` mode.
