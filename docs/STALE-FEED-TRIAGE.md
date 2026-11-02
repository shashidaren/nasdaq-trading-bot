# STALE_FEED Triage — `US100Cash` M15 feed (`/opt/nasdaq`)

Everything you need when `journal.csv` shows `STALE_FEED` (or `FEED_*`) rows, or when the
dashboard shows an **M15 feed problem** banner. One command answers the question:

```bash
cd /opt/nasdaq && python3 tools/feed_doctor.py            # human report, exit 0/1/2
python3 tools/feed_doctor.py --json                       # machine-readable
python3 tools/feed_doctor.py --assume-open                # triage outside CME hours
```

Exit codes: `0` feed healthy (or market closed), `1` feed stale while open,
`2` feed broken (missing / unreadable / wrong schema / foreign symbol).

---

## 1. Read the alert, not just the number

Every alert now names a **reason** and a **freeze instant**:

| Journal event | Meaning | First command |
|---|---|---|
| `FEED_FILE_MISSING` | `mt5_last_candle.json` was never written (or was deleted) | `systemctl status mt5feed-nasdaq` |
| `FEED_FILE_UNREADABLE` | file exists but is not valid JSON (writer died mid-write, foreign writer) | `tools/feed_doctor.py` |
| `FEED_FILE_SCHEMA` | JSON parsed but the OHLC keys are wrong/absent | check the writer version on both stacks |
| `FEED_SYMBOL_MISMATCH` | the bar is tagged for another instrument — **never traded** | see §3 |
| `STALE_FEED` | a real closed bar exists but is older than 20 min | §2 |
| `FEED_CLOCK_SKEW` | bar timestamp is in the future (bad `server_offset_s` / tzdata) | re-check §3 DST notes |

> **The old trap.** Alerts used to say "no new closed M15 candles for *N* min", where *N* was
> time since **this engine process** last processed a bar. Restarting the engine reset *N*,
> so `890 min → 20 min` looked like the feed recovering when nothing had changed. If a
> historical journal shows the reported minutes **decreasing**, that is a counter reset, not
> a recovery — `tools/feed_doctor.py` flags these explicitly as `counter reset`.

---

## 2. Ladder: is the *sidecar* alive, or is the *terminal* dead?

```bash
# 1. Heartbeat written by tools/mt5_feed.py every poll (status, tick age, broker offset, reconnects)
cat /opt/nasdaq/mt5_feed_health.json

# 2. What the engine last concluded (also rendered in the dashboard banner)
python3 -c "import json;print(json.load(open('/opt/nasdaq/status.json'))['feed'])"

# 3. The sidecar's own log
journalctl -u mt5feed-nasdaq -n 80 --no-pager

# 4. The terminal it is attached to (wrong prefix => second, unlogged-in prefix)
systemctl show mt5feed-nasdaq -p Environment
```

| `mt5_feed_health.json` → `status` | Cause | Fix |
|---|---|---|
| `OK` but the bar is old | sidecar healthy, bars not arriving (market shut / broker gap) | check CME hours; if open, `tools/feed_doctor.py` witness block |
| `init_failed` / `INIT_FAILED` | MT5 not running or not logged in **in the service prefix** | start/login the Wine terminal under the same `WINEPREFIX` |
| `SYMBOL_SELECT_FAILED` | `US100Cash` not in Market Watch | right-click Market Watch → Symbols → Cash Indices → enable `US100Cash` |
| `OFFSET_UNKNOWN` | no fresh tick yet, so UTC conversion is impossible → **nothing is written** | fix the connection; the last good offset is re-read from `mt5_feed_offset.json` on restart |
| `TICK_FROZEN` | `tick_age_s` above `MT5_FEED_FROZEN_S` (300 s) — the terminal has data but it is not moving | §3 (login/session), then `systemctl restart mt5feed-nasdaq` |
| `TERMINAL_DISCONNECTED` | `terminal_info().connected == False` | `systemctl restart mt5feed-nasdaq`; if it repeats, restart the terminal |
| `NO_RATES` | `copy_rates_from_pos` failed | enable the symbol, restart the sidecar, check the terminal's history tab |

`MT5_FEED_FROZEN_S` / `MT5_FEED_RECONNECT_S` control the frozen-tick watchdog (bounded
`shutdown()` + `initialize()` attempts, never a tight loop).

---

## 3. Shared VPS: two MT5 stacks, one XM login

Running this stack beside another MT5 bot on the same box is supported, but three things
must be **per-stack**, and one thing is shared:

| Item | Rule |
|---|---|
| `MT5_FEED_FILE` / `MT5_WARMUP_FILE` / `MT5_FEED_HEALTH_FILE` / `MT5_FEED_OFFSET_FILE` | **Unique per stack.** Defaults are `/opt/nasdaq/...`; the second stack must publish `/opt/<stack>/...`. Bars are now tagged with `symbol`/`timeframe`/`account`, and the engine refuses a bar whose symbol is not `US100Cash` (`FEED_SYMBOL_MISMATCH`) — it will alert instead of trading a foreign price. |
| `MT5_ORDER_DIR` / `mt5_bridge/` | **Unique per stack** — a shared order directory means one executor can fill another stack's intent. |
| `WINEPREFIX` | **Unique per stack** (e.g. `/root/.mt5` for this one). Two Wine prefixes are two independent terminals. |
| XM login | **Shared is accepted, but fragile.** A second terminal logging in with the same credentials can invalidate/kick the first. The symptom here is exactly `TICK_FROZEN`: the surviving terminal's `symbol_info_tick().time` stops advancing while `terminal_info().connected` may still read `True`, so no fresh offset can be learned and **no feed file is written at all**. |

How to tell them apart in one shot:

```bash
python3 tools/feed_doctor.py
```

* **"MT5 itself is fine (the quote logger is still recording)"** → the read-only quote logger
  (`mt5quotes-nasdaq`, same login, different service) is still getting 1-second-fresh ticks,
  so the login/session is healthy and the fault is inside the `mt5feed` process/service.
* **Quote logger also silent** (`xm_data/quotes-YYYY-MM-DD.csv` mtime old) → the whole
  terminal/login is down: a competing login, a broker disconnect, or a dead Wine terminal.

DST note: XM server time moves with the broker's own DST (currently `+2h`, i.e. `7200` in
`server_offset_s`). After a broker DST change the offset changes by exactly one hour; the
sidecar re-learns it from a fresh tick and persists it to `mt5_feed_offset.json`. If you see
`FEED_CLOCK_SKEW` right after a DST switch, `server_offset_s` is stale — restore a fresh
tick, do not hand-edit files.

---

## 4. Fix checklist (copy/paste)

```bash
cd /opt/nasdaq
cat mt5_feed_health.json                     # status / tick_age_s / server_offset_s
python3 tools/feed_doctor.py                 # verdict + witness + journal forensics
systemctl restart mt5feed-nasdaq             # 90% of cases: the sidecar stopped publishing
journalctl -u mt5feed-nasdaq -f              # expect: "publishing closed M15 US100Cash candles"
python3 tools/feed_doctor.py --quiet         # expect: OK - feed healthy
```

Expected healthy output:

```
verdict     : OK - feed healthy - last closed bar 2026-10-26 17:45:00 UTC, 3 min old
  status    : OK  heartbeat 1s  fresh=True
  tick age  : 1s  terminal_connected: True
  offset    : 7200  reconnects: 0
```

If `feed_doctor` says `BROKEN` while the quote logger keeps writing, the engine is fine and
MT5 is fine — only `mt5feed-nasdaq` is not publishing. Never paper over it with a manual
`echo` into `mt5_last_candle.json`: the engine would trade a bar nobody published.

---

## 5. Resume after a multi-day outage (IPC loss)

Real incident, 2026-10-30 20:45 UTC → 2026-11-02: the *engine* was healthy the whole
time and correctly refused to trade; `mt5feed-nasdaq` lost its MT5 handle
(`copy_rates_from_pos failed: (-10004, 'No IPC connection')`, status `NO_RATES`,
`reconnects: 291`) and never recovered, while `mt5quotes-nasdaq` kept writing quotes
and candles from a separate process. Symptoms:

| Signal | Reading |
|---|---|
| `systemctl status nasdaq-engine` → `active (running)`, log repeats `MT5: feed stale [CANDLE_OLD]` | Engine alive, doing the right thing. Not the fault |
| `mt5_feed_health.json` → `status: NO_RATES`, heartbeat fresh, `last_error: -10004 No IPC connection` | Sidecar alive, MT5 handle dead |
| `mt5_last_candle.json` mtime hours/days old | Nothing published since the handle died |
| `xm_data/quotes-*.csv` current, `feed_doctor` witness fresh | Broker + quote logger fine ⇒ fault is isolated to `mt5feed-nasdaq` |

`-10004` means the Wine MT5 terminal the sidecar attached to restarted or exited;
the Python process kept a dead handle. Restarting the sidecar is the whole fix:

```bash
systemctl restart mt5feed-nasdaq
journalctl -u mt5feed-nasdaq -f        # expect: "publishing closed M15 US100Cash candles"
python3 tools/feed_doctor.py --quiet   # expect: OK - feed healthy
```

If `-10004` survives the restart, the terminal in that `WINEPREFIX` is not running or
not logged in — start it before restarting the sidecar again
(`systemctl show mt5feed-nasdaq -p Environment`, `docs/NASDAQ-OPS.md`). Since 2026-11-02
`tools/mt5_feed.py` also reconnects on `NO_RATES`/`TERMINAL_DISCONNECTED`, not only on
`TICK_FROZEN`, so this no longer needs a manual restart to recover — but the *first*
attempt at restart is still the fastest confirmation of the diagnosis.

### Then verify the engine, in this order

1. **Feed first.** Confirm `mt5_last_candle.json` is fresh and `mt5_warmup_candles.json`
   was rewritten (`python3 tools/feed_doctor.py`; warm-up age should be minutes, not days).
2. **Engine second.** Check `forward_test_log.csv` grows every 15 min and
   `status.json` `last_update` tracks the current bar.
3. **Indicators after the gap.** The engine appends the first post-outage bar to its
   in-memory series, so RSI/ATR/EMA are computed across the gap: ATR picks up the whole
   weekend/adverse move in one true range and stays inflated for ~14 bars, RSI similarly,
   EMAs mildly. The engine's gates use ATR (near-EMA distance, min/max ATR), so treat the
   first ~14 M15 bars (3.5 h) after recovery as indicator-contaminated and review any
   trade taken in that window by hand. Restarting the engine does **not** remove the gap
   (`load_history_from_csv()` fills the series from the log, and `load_warmup_from_mt5()`
   only seeds when the log is empty).
4. **Never backfill** `forward_test_log.csv` from `xm_data/candles_m15.csv` to hide the
   gap: that fabricates "the engine evaluated this bar" evidence. The missing bars stay
   missing, and the sample gate counts only what really ran.
5. Record the outage window and root cause in `docs/EXECUTION-PLAN.md` Phase 1 notes, and
   refresh `docs/HANDOFF.md` (`python3 tools/handoff_check.py --update`) — since
   2026-11-02 `handoff_check.py` fails with `engine lag` when `xm_data/` runs ahead of the
   engine's own files, so a silent repeat of this outage can no longer pass as FRESH.
