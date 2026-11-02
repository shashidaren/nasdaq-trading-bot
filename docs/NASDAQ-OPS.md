# NASDAQ-OPS — `/opt/nasdaq` Operations Runbook (`US100Cash` M15)

Day-to-day install, service, and triage reference for the XM **Nasdaq-100 Cash Index CFD (`US100Cash`)** `M15` bot on the Linux VPS. Start every session at `docs/EXECUTION-PLAN.md` and check `docs/HANDOFF.md`; this page is the operational "how", not the strategy spec.

- **Root:** `/opt/nasdaq` (code, CSVs, `xm_data/`, `mt5_bridge/`, `venv/`)
- **Runtime split:** Linux `venv` python for `engine.py` / `dashboard.py`; **Wine** python (`C:/Python312/python.exe`, `WINEPREFIX=/root/.mt5`) for the three MT5 sidecars
- **Autosync:** `root` cron every 15 min (`tools/autosync.sh`) commits live data and deploys upstream merges gated by `tools/smoke_test.py`

---

## 1. Fresh Install (as `root`)

```bash
# 1. Code + Linux virtualenv (dashboard/engine only - never the Wine sidecars)
sudo mkdir -p /opt/nasdaq
sudo git clone <this-repo-url> /opt/nasdaq
cd /opt/nasdaq && git checkout main
python3 -m venv /opt/nasdaq/venv
/opt/nasdaq/venv/bin/pip install --upgrade pip flask python-dotenv requests

# 2. Day-0 ledger (only on a brand-new install - NEVER on an existing server)
printf 'Trade_Num,Trade_Type,Entry_Time,Exit_Time,Entry_Price,Stop_Loss,Take_Profit,Exit_Price,Exit_Reason,Profit,Balance_After,RSI_At_Entry,ATR_At_Entry,Wick_Ratio_At_Entry,EMA50_At_Entry,EMA200_At_Entry\n' > /opt/nasdaq/trades.csv
printf 'Timestamp,Open,High,Low,Close,Wick_Ratio,Volume,Vol_MA,Vol_Confirmed,Dynamic_Floor,EMA_50,EMA_200,Trend_Confirmed,Tested_Floor,Valid_Rejection,Held_Floor,RSI,ATR\n' > /opt/nasdaq/forward_test_log.csv
printf 'Timestamp,Reason,Price,ATR\n' > /opt/nasdaq/skipped_trades.csv
python3 - <<'PY'
import json
json.dump({
    "symbol": "US100Cash", "mode": "FORWARD_TEST", "data_source": "MT5", "timeframe": "M15",
    "equity": 200.0, "total_trades": 0, "next_trade_num": 1, "wins": 0, "losses": 0,
    "win_rate": 0.0, "daily_losses": 0, "max_daily_losses": 3, "trade_active": False,
    "trade_type": None, "entry_price": None, "stop_loss": None, "take_profit": None,
    "entry_time": None, "current_trade_num": None,
    "last_update": "1970-01-01 00:00:00", "funnel": {},
}, open("/opt/nasdaq/status.json", "w"), indent=2)
PY

# 3. XM MT5 terminal: run it once under the SAME prefix, log in, and enable the symbol
#    WINEPREFIX=/root/.mt5 wine C:/Program\ Files/MetaTrader\ 5/terminal64.exe
#    Right-click Market Watch -> Symbols -> Cash Indices -> enable US100Cash
#    (pick US100Cash, NOT "Nasdaq" = Nasdaq Inc single-stock CFD).

# 4. Wine-side tzdata (Windows/Wine python has no IANA database):
WINEPREFIX=/root/.mt5 wine C:/Python312/python.exe -m pip install tzdata

# 5. Install the 5 systemd units (root: WINEPREFIX=/root/.mt5, never /home/root/.mt5)
for pair in mt5feed:mt5feed-nasdaq mt5quotes:mt5quotes-nasdaq mt5exec:mt5exec-nasdaq \
            nasdaq-engine:nasdaq-engine nasdaq-dashboard:nasdaq-dashboard; do
    src="${pair%%:*}"; dst="${pair##*:}"
    sudo cp "/opt/nasdaq/deploy/${src}.service" "/etc/systemd/system/${dst}.service"
    sudo sed -i 's|YOUR_LINUX_USER|root|g; s|/home/root/.mt5|/root/.mt5|g' "/etc/systemd/system/${dst}.service"
done
sudo systemctl daemon-reload
sudo systemctl enable --now mt5feed-nasdaq mt5quotes-nasdaq mt5exec-nasdaq nasdaq-engine nasdaq-dashboard

# 6. 15-minute autosync cron (root)
( sudo crontab -l 2>/dev/null | grep -v 'autosync\.sh' ; \
  echo '*/15 * * * * /opt/nasdaq/tools/autosync.sh >> /var/log/nasdaq-autosync.log 2>&1' ) | sudo crontab -
```

Non-root operator? Substitute your login (`trader`) for `root` in step 5 and drop the `/home/root/.mt5` fix-up — the prefix then stays `/home/trader/.mt5` and must match the prefix the MT5 terminal was installed under.

**Renamed repo / existing server clone?** GitHub redirects the old URL, so autosync keeps working — but re-point `origin` so the deployment names the current repo and future branch/PR links resolve:
```bash
cd /opt/nasdaq && git remote set-url origin <this-repo-url> && git remote -v
```
Then confirm the bot identity is current (`git config user.name` should print `nasdaq-autosync`; `tools/autosync.sh` re-pins it on its next run).

---

## 2. Service Map

| Systemd Unit | Template | Runtime | Role | Health |
|---|---|---|---|---|
| `nasdaq-engine` | `deploy/nasdaq-engine.service` | `/opt/nasdaq/venv/bin/python` | `engine.py` — closed `M15` evaluation + `FORWARD_TEST`/`LIVE` | `journalctl -u nasdaq-engine -f` |
| `nasdaq-dashboard` | `deploy/nasdaq-dashboard.service` | `/opt/nasdaq/venv/bin/python` | `dashboard.py` on `0.0.0.0:6001` | `curl -s localhost:6001/healthz` (process liveness only); open `/readiness` for evidence and feed status |
| `mt5feed-nasdaq` | `deploy/mt5feed.service` | Wine (`/root/.mt5`) | `tools/mt5_feed.py` → `mt5_last_candle.json` + 250-bar `mt5_warmup_candles.json` + heartbeat `mt5_feed_health.json` | `python3 tools/feed_doctor.py` → `OK` |
| `mt5quotes-nasdaq` | `deploy/mt5quotes.service` | Wine (`/root/.mt5`) | read-only `tools/mt5_quotes.py` → `xm_data/` + 12,000-bar `candles_m15.csv` | `xm_quote_report.py --quiet` |
| `mt5exec-nasdaq` | `deploy/mt5exec.service` | Wine (`/root/.mt5`) | `tools/mt5_executor.py` bridge (`MT5_EXEC_ENABLED=0` dormant) | `mt5_bridge/result.json` |

`/root/.mt5` is the root user's Wine prefix. Root's home is `/root`; `/home/root/.mt5` is a wrong path that silently starts a **second, unlogged-in** prefix (`initialize()` fails, feed goes stale).

---

## 3. Verification Ladder

```bash
cd /opt/nasdaq
python3 tools/feed_doctor.py           # WHY is the M15 feed stale? (reason + heartbeat + witness)
python3 tools/check_data.py            # CSV integrity + freshness (market-hours aware)
python3 tools/handoff_check.py         # docs/HANDOFF.md snapshot vs live data + engine lag vs xm_data
systemctl status nasdaq-engine mt5feed-nasdaq mt5quotes-nasdaq  # is the engine still writing?
python3 tools/smoke_test.py            # offline regression suite (uses venv Flask for dashboard tests if available)
python3 tools/live_path_probe.py       # 11/11 execution-path checks against fake MT5 (no orders sent)
/opt/nasdaq/venv/bin/python -m unittest discover -s tests -v  # shared gates + Flask dashboard
python3 tools/xm_quote_report.py       # spread, contract spec, M15 parity, blackout map
python3 tools/backtest.py xm_data/candles_m15.csv --spread 0.25
```

---

## 4. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `mt5quotes-nasdaq` restart loop, `exit 2` | 15+ min without quotes during open market | `systemctl status mt5quotes-nasdaq`; confirm MT5 terminal logged in under `/root/.mt5`; check `xm_data/quotes-*.csv` flags |
| Feed stale but market open | Terminal not running / wrong prefix / `US100Cash` not in Market Watch | `python3 tools/feed_doctor.py`; then `systemctl show mt5feed-nasdaq -p Environment` → expect `WINEPREFIX=/root/.mt5` |
| `FEED_SYMBOL_MISMATCH` in `journal.csv` | another MT5 stack on the VPS writes the same `MT5_FEED_FILE` | give every stack its own feed/warm-up/order paths — `docs/STALE-FEED-TRIAGE.md` |
| `mt5_feed_health.json` says `TICK_FROZEN` / `OFFSET_UNKNOWN` | terminal has no live ticks (competing login with the same XM account, broker disconnect) | `docs/STALE-FEED-TRIAGE.md` §3, then `systemctl restart mt5feed-nasdaq` |
| `ZoneInfoNotFoundError: America/New_York` | Wine python without `tzdata` | `WINEPREFIX=/root/.mt5 wine C:/Python312/python.exe -m pip install tzdata` (fixed-rule fallback keeps the bot trading meanwhile — see `docs/XM-LOGGER.md`) |
| `LOT_SIZE 0.10 outside broker [...]` | Symbol spec not loaded | enable `US100Cash` in Market Watch, restart `mt5feed-nasdaq` |
| Autosync not deploying code | `/opt/nasdaq` working tree dirty | never edit live code on the server; commit from a dev checkout |
| Dashboard shows `STALE` | Market closed (CME weekend/break) | expected between `Fri 16:55 ET` and `Sun 18:10 ET`, and `16:55–18:10 ET` daily |

**Emergency flatten (LIVE only):** `touch /opt/nasdaq/HALT` — `mt5exec-nasdaq` flattens the open `US100Cash` position and refuses new intents while the file exists. Remove it only after the position is confirmed closed in the terminal.

---

## 5. Change Protocol

1. Edit in a **dev checkout**, never on the VPS working tree (`tools/autosync.sh` skips deploys when `/opt/nasdaq` is dirty).
2. Run `tools/smoke_test.py` + `tools/live_path_probe.py` before every commit.
3. Push to the active development branch, open a PR, merge to `main` — autosync picks up `main` within 15 minutes.
4. Live data (`trades.csv`, `forward_test_log.csv`, `skipped_trades.csv`, `status.json`, `xm_data/`) is written **only** by the running services/cron; never hand-edit it in Git.
