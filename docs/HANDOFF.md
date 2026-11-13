# HANDOFF — Nasdaq-100 (`US100Cash`) M15 Auto-Trading Bot

<!-- HANDOFF-SNAPSHOT machine-checked by tools/handoff_check.py; refresh with --update -->
| key | value |
|---|---|
| as_of_utc | 2026-11-04 08:15 |
| data_collection | 5239 |
| closed_trades | 2 |
| wins_losses | 1W/1L |
| win_rate_pct | 50.0 |
| engine_ledger_usd | 213.88 |
| true_equity_usd | 213.89 |
| live_era_trades | 2 |
| live_era_net_usd | +13.39 |
| log_covered_trades | 2 |
| log_bars | 529 |
| log_last_bar_utc | 2026-11-04 08:15 |
| skip_rows | 0 |
| open_trade | none |
| spread_usd_per_trade | 0.25 |
<!-- /HANDOFF-SNAPSHOT -->

---

## 1. Executive State (Read First)

This repository auto-trades XM's **Nasdaq-100 Cash Index CFD (`US100Cash`)** on **15-minute (`M15`)** candles under **`/opt/nasdaq`** (cutover date **2026-10-24**; all legacy data discarded).

> **⚠️ 2026-11-13 session — feed outage resolved; evidence base still tiny.** The 2026-11-02 freeze (ledger stuck at 2026-10-30 20:45 UTC while `xm_data/` ran on) was root-caused on the VPS: `mt5feed-nasdaq` lost its MT5 handle (`NO_RATES`, `(-10004, 'No IPC connection')`, 291 reconnects, fresh heartbeat over a frozen feed file) and had no reconnect path for that status. The sidecar was restarted and the engine's first post-outage bar is **2026-11-02 12:45 UTC**; evidence now runs through **2026-11-04 08:15 UTC** (529 bars, data collection 5239). The ledger holds **2 closed trades (1W/1L, gross $213.88)** and `live_readiness.py` is at **2/100 — 0/6 data gates** (feed gate FAIL: worst open-market gap 810 min). **53 open-market bars (2.7 days) are permanently lost: do not backfill the forward ledger**; the first ~14 post-outage bars are indicator-contaminated. Both `handoff_check.py` (engine-lag fail-closed) and the ladder in `docs/EXECUTION-PLAN.md` verify this checkout — the checked-in snapshot is never a live-feed health check, so re-run `tools/feed_doctor.py` + §6 on `/opt/nasdaq` each session (last log bar and last GitHub push both stop at 2026-11-04 08:15 UTC here).

- **Day-0 Nasdaq ledger:** Root `trades.csv`, `forward_test_log.csv`, `skipped_trades.csv`, and `status.json` start clean at **$200.00** equity (`0W/0L`, `symbol: "US100Cash"`, `timeframe: "M15"`).
- **Execution mode:** `TRADING_MODE = "FORWARD_TEST"` and `DATA_SOURCE = "MT5"` by default. Both the direct `MetaTrader5` path and the Linux Wine atomic file-bridge (`tools/mt5_executor.py` via `/opt/nasdaq/mt5_bridge/`) are implemented. `tools/live_path_probe.py` reports `11/11` checks against **fake MT5 in temporary directories**, not a broker demo. `MT5_EXEC_ENABLED=0` and `MT5_EXEC_ALLOW_REAL=0` remain locked off until the preceding phases and demo validation (`docs/START-LEVER1.md`) complete.

---

## 2. Instrument & Strategy Specification (`US100Cash` M15)

| Parameter | Value | Meaning on `US100Cash` (~20,500) |
|---|---|---|
| `SYMBOL_MT5` / `DATA_SOURCE` | `"US100Cash"` / `"MT5"` | XM Nasdaq-100 Cash Index CFD via local Wine MT5 terminal |
| `CANDLE_SECONDS` | `900` (`M15`) | 15-minute bars (4 bars/hour); closed-bar evaluation |
| `CONTRACT_SIZE` / `LOT_SIZE` | `1.0` / `0.10` | `$0.10` per index point at XM's `0.10` min lot (`100` pts = `$10.00`) |
| `ASSUMED_SPREAD_USD` | `0.25` | `2.5` index points x `$0.10/pt` round-trip cost assumption |
| `MAX_SPREAD_POINTS` / `MAX_SPREAD_TO_1R` | `8.0` pts / `0.35` | Hard live spread guard (`$0.80` at `0.10` lot or `35%` of 1R) |
| `LOOKBACK_PERIOD` | `20` | 20 x M15 = **5 hours** structural floor/ceiling |
| `FLOOR_BUFFER_PCT` | `0.0012` | `0.12%` (~24.6 pts at 20,500) touch tolerance |
| `WICK_RATIO_TARGET` | `0.15` | Rejection wick >= `15%` of candle range |
| `EMA_FAST` / `EMA_SLOW` | `50` / `200` | `12.5h` / `50h` (~2.2 trading days); seeded immediately via `mt5_warmup_candles.json` |
| `EMA_SLOPE_LOOKBACK` | `12` | `EMA50` rising/falling vs **3 hours** (12 M15 bars) ago |
| `MAX_BELOW_EMA_ATR` | `0.50` | Adverse entry distance cap from `EMA50` (`0.50 * ATR`) |
| `RSI_MIN` / `RSI_MAX` | `40.0` / `70.0` | BUY `(40, 70)`, mirrored SELL `(30, 60)` |
| `ATR_SL_MULT` / `ATR_TP_MULT` | `2.0` / `4.0` | `1:2` R:R (`33.3%` breakeven decisive WR) |
| `MIN_ATR_PERCENT` / `MAX_ATR_PERCENT` | `0.0004` / `0.0080` | `0.04%–0.80%` (`~8–164` pts on M15 at `20,500`) |
| `SL_COOLDOWN_*` | `30m` / `60m` | 2 M15 bars after 1 SL; 4 M15 bars after 2+ consecutive SLs |
| `MAX_DAILY_LOSSES` / `MAX_DAILY_LOSS_USD` | `3` SLs / `$15.00` | Daily UTC circuit breaker |

---

## 3. Market Hours & Session Blackouts (`America/New_York`)

`US100Cash` follows the CME Globex Nasdaq-100 session. All schedule checks in `trade_filter.py` convert UTC to `America/New_York` (`zoneinfo.ZoneInfo`) so US DST shifts never misalign the cash open or rollover break:

1. **Market Quiet / Closed (`is_market_quiet`):**
   - **Weekend:** Friday `16:55 ET` through Sunday `18:10 ET`
   - **Daily CME maintenance break (Mon–Thu):** `16:55 ET` through `18:10 ET`
   - Suppresses false stale-feed alerts in `engine.py`, `tools/mt5_quotes.py`, `tools/xm_quote_report.py`, and `tools/check_data.py`.
2. **Entry Blackouts (`BLACKOUT_WINDOWS_ET`):**
   - `08:25–08:40 ET` — US `08:30 ET` high-impact macro releases (CPI / NFP / PPI / Retail Sales)
   - `09:25–09:45 ET` — NASDAQ `09:30 ET` cash open whip-saw
   - `16:45–18:15 ET` — CME daily close & rollover spread spike

---

## 4. Server Layout (`/opt/nasdaq`) & Services

| Systemd Unit | Source Template | Runtime | Role |
|---|---|---|---|
| `nasdaq-engine` | `deploy/nasdaq-engine.service` | `/opt/nasdaq/venv/bin/python` | Runs `engine.py` |
| `nasdaq-dashboard` | `deploy/nasdaq-dashboard.service` | `/opt/nasdaq/venv/bin/python` | Runs `dashboard.py` on `0.0.0.0:6001` |
| `mt5feed-nasdaq` | `deploy/mt5feed.service` | Wine Python (`/root/.mt5`) | Runs `tools/mt5_feed.py` (`mt5_last_candle.json` + 250-bar `mt5_warmup_candles.json` + `mt5_feed_health.json` heartbeat) |
| `mt5quotes-nasdaq` | `deploy/mt5quotes.service` | Wine Python (`/root/.mt5`) | Runs read-only `tools/mt5_quotes.py` (`xm_data/` + 12,000-bar `candles_m15.csv` backfill) |
| `mt5exec-nasdaq` | `deploy/mt5exec.service` | Wine Python (`/root/.mt5`) | Runs `tools/mt5_executor.py` (`MT5_EXEC_ENABLED=0` dormant until armed) |

---

## 5. Immediate Next Operational Milestones

Follow `docs/EXECUTION-PLAN.md` in order; do not move to demo execution or real money while Phase 1 is blocked.

1. **Verify the restored pipeline on the live VPS (P0):** the feed sidecar was restarted after the 2026-11-02 12:44 UTC root cause and the engine wrote through **2026-11-04 08:15 UTC**, but this checkout shows no evidence after that timestamp (last GitHub push is the same instant). Run task 1.0 of `docs/EXECUTION-PLAN.md` (`systemctl status nasdaq-engine; journalctl -u nasdaq-engine`, `tools/feed_doctor.py`) and the §6 ladder on `/opt/nasdaq`; confirm bars are still arriving every 15 min, and compare the October 30 outage plus the post-resume 11-02 21:45→23:15 gap against the MT5 feed, engine and journal. The checked-in snapshot is not a live-feed health check — and, as of 2026-11-02, a matching snapshot is not proof of a running engine either (`handoff_check.py` now fails closed on that lag). Never rewrite the forward log to make a gate pass.
2. **Keep accumulating forward-test evidence:** Once Phase 1 is complete, keep `mt5feed-nasdaq` and `nasdaq-engine` healthy and work toward 100 post-gate closed trades. Use the XM historical backtest for context, not as a substitute for forward-test evidence.
3. **Later, perform the demo soak:** Only after the preceding phases, follow `docs/START-LEVER1.md` on an XM **demo** account, keep `MT5_EXEC_ALLOW_REAL=0`, and verify 5–10 actual demo round-trips plus position reconciliation and HALT flatten. The offline fake-MT5 probe is not that soak.

---

## 6. Standard Diagnostic & Replay Commands

```bash
# Always run integrity + freshness + regression checks first:
python3 tools/feed_doctor.py          # feed reason/heartbeat/witness (exit 0 OK, 1 stale, 2 broken)
python3 tools/check_data.py
python3 tools/handoff_check.py
python3 tools/smoke_test.py
python3 tools/live_path_probe.py

# XM US100Cash spread, contract-spec, and M15 candle parity report:
python3 tools/xm_quote_report.py

# Historical / forward-book statistical & replay reports (at $0.25/trade cost):
python3 tools/backtest.py xm_data/candles_m15.csv --spread 0.25
python3 tools/live_readiness.py --spread 0.25
python3 tools/win_rate_report.py --spread 0.25
python3 tools/pathwalk_sims.py --spread 0.25 --census
python3 tools/analyze_losers.py --spread 0.25
python3 tools/phantom_trades.py --spread 0.25
python3 tools/pressure_report.py
```

---

## 7. Go-Live Readiness Gates (`US100Cash`)

Before switching `MT5_EXEC_ALLOW_REAL=1` on a real-money XM account (`docs/START-LEVER1.md`):
1. **Contract & Cost Verification (`tools/xm_quote_report.py`):** `calc_profit_usd_0.10lot_plus_100pts == 10.0`, `volume_min <= 0.10`, median live spread `<= 3.5` pts (`$0.35` at `0.10` lot), and M15 candle parity `OK`.
2. **Statistical Edge (`tools/backtest.py` + `tools/live_readiness.py`):** Day-block bootstrap `P(net/trade > 0) >= 90%` and break-even/assumed spread cover `>= 2.0x` at measured XM spread.
3. **Execution Soak (`tools/live_path_probe.py` + XM Demo):** Zero orphaned positions, zero duplicate orders, verified SL/TP deal reconciliation by `position_id`, and verified `/opt/nasdaq/HALT` emergency flatten.

---

## 8. Key Reference Docs

- `docs/NASDAQ-OPS.md` — `/opt/nasdaq` install, systemd service map, Wine `tzdata` setup & triage runbook
- `docs/STALE-FEED-TRIAGE.md` — STALE_FEED/FEED_* reason taxonomy, sidecar heartbeat, shared-VPS/shared-login checklist
- `docs/START-LEVER1.md` — Wine `mt5_executor.py` Stage A/B/C live execution protocol
- `docs/XM-LOGGER.md` — Read-only `mt5_quotes.py` & `xm_quote_report.py` reference
- `docs/AUTOSYNC.md` — Unattended 15-min data sync & smoke-gated auto-deploy
- `docs/ACCELERATE-TESTING.md` — Fast 6-month XM M15 historical backtest workflow

---

## 9. Data Conventions

- **`trades.csv` `Profit` is GROSS** (`(exit - entry) * LOT_SIZE * CONTRACT_SIZE` in `FORWARD_TEST`; net broker deal P/L including swap/commission in `LIVE`). Spread haircuts (`$0.25` default) are applied at report time via `--spread 0.25` and in `dashboard.py`.
- **`forward_test_log.csv` timestamps** mark the **bar close in UTC** (`YYYY-MM-DD HH:MM:SS`, `:00/:15/:30/:45`), and logged indicators (`EMA_50`, `EMA_200`, `RSI`, `ATR`, `Dynamic_Floor`) are **PRE-update** for that candle.

---

## 10. Session & Push Protocol

1. Run `python3 tools/check_data.py` and `python3 tools/handoff_check.py` before interpreting any numbers.
2. Never edit live code directly on the server — `tools/autosync.sh` skips deploys if `/opt/nasdaq` is dirty.
3. Run `python3 tools/smoke_test.py` and `python3 tools/live_path_probe.py` before every commit.
4. Refresh the snapshot block with `python3 tools/handoff_check.py --update` whenever ledger/config facts change, then commit and push to the active branch.
