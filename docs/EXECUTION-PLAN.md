# Execution Plan — Nasdaq-100 (`US100Cash`) M15 Bot

**Branch:** the active development branch (`arena/...`); never work on `main` directly
**Reviewed:** 2026-10-30, **re-reviewed 2026-11-02 12:35 UTC, re-re-reviewed 2026-11-13 (checkout ladder green)**
**Status:** **Phase 1 verification green in this checkout (2026-11-13); VPS-side re-verification and Phase 2 accumulation remain.** The 2026-10-30 feed outage was root-caused on the VPS (2026-11-02 12:44 UTC): `mt5feed-nasdaq`'s MT5 handle died (`status: NO_RATES`, `last_error: copy_rates_from_pos failed: (-10004, 'No IPC connection')`, `reconnects: 291`, feed file unwritten since 2026-10-31 03:01 UTC) and the sidecar's bounded reconnect path only ran for `TICK_FROZEN`, so it kept rewriting a fresh heartbeat over a frozen feed file — while `nasdaq-engine` correctly stayed `active (running)` and refused to trade (`MT5: feed stale [CANDLE_OLD]`), and the separate `mt5quotes-nasdaq` logger kept `xm_data/` alive. The sidecar was restarted and the engine's first post-outage bar is **2026-11-02 12:45 UTC**; evidence now runs through **2026-11-04 08:15 UTC** (529 log bars, data collection 5239, 2 closed trades 1W/1L, gross ledger $213.88). **53 open-market M15 bars (2.7 days, 2026-10-30 20:45 → 2026-11-02 12:45) of forward-test evidence remain permanently lost — do not backfill**; Phase 2 sits at 2/100 post-gate trades (0/6 data gates; feed gate FAIL, worst open-market gap 810 min inside the 14-day window), and the first ~14 post-outage bars are indicator-contaminated. Ladder in this checkout (2026-11-13): `check_data.py` exit 0 (2 warnings investigated: maintenance-adjacent 21:00 UTC slots + the documented outage run), `pytest tests/` 56 passed, `smoke_test.py` scenarios A–S all PASS (Flask installed), `live_path_probe.py` 11/11, `handoff_check.py` FRESH after `--update` **at the time — see the 2026-11-17 re-review: that FRESH was a false green once the whole pipeline froze**, dashboard Readiness tab verified on `:6001` (six gates, stats, verdict, unsigned checklist). `feed_doctor.py` still reports `FILE_MISSING /opt/nasdaq` here — **expected in an isolated checkout, and not evidence about the VPS: re-run the ladder on `/opt/nasdaq` every session**, where the last evidence bar and the last GitHub push both stop at **2026-11-04 08:15 UTC** (confirm the pipeline is still moving). The reconnect fix (`should_reconnect()` on `NO_RATES`/`TERMINAL_DISCONNECTED`) and the safe autosync rollback are both present on `main` (5ff4498) with their regression tests; the controlled VPS autosync hand-deploy procedure (operator pauses cron, updates `tools/autosync.sh` by hand, verifies, resumes cron) still applies before relying on auto-deploy (operator-approved; see the Phase 1 notes below and `docs/AUTOSYNC.md`).

The checked-in snapshot is **not live telemetry**: `status.json` records FORWARD_TEST, two closed trades (1W/1L) and a gross engine ledger of $213.88 — as of **2026-11-04 08:15 UTC**, i.e. frozen at the snapshot's `as_of`. A historical `feed: OK` in that file does not prove the feed is currently healthy, and a matching HANDOFF snapshot does not prove the engine is alive (before 2026-11-02 every snapshot key was derived from the engine's own frozen files, so `handoff_check.py` reported FRESH throughout the outage; it now fails closed on engine lag).

Every prompt in this session should follow this plan. Work is ordered: **stabilize → evidence → soak → gates → live**. Do not skip a phase.

---

## Phase 1 — Stabilize & Verify (this session)

Goal: make the repo self-describing and the dashboard show a trustworthy readiness picture.

| # | Task | Command / Artifact | Acceptance |
|---|------|-------------------|------------|
| 1.0 | **Restore the feed sidecar, then the engine** (P0, 2026-11-02) | `/opt/nasdaq`: `systemctl restart mt5feed-nasdaq`; `journalctl -u mt5feed-nasdaq -f`; `python3 tools/feed_doctor.py`; then confirm the engine resumes (`journalctl -u nasdaq-engine -f`) — runbook: `docs/STALE-FEED-TRIAGE.md` §5 | `feed_doctor.py` → OK; `mt5_warmup_candles.json` refreshed; `status.json` `last_update` within one bar of now; `forward_test_log.csv` grows every 15 min; `python3 tools/handoff_check.py` no longer reports engine lag. Do **not** backfill the ledger, and treat the first ~14 post-outage bars as indicator-contaminated |
| 1.1 | Refresh HANDOFF snapshot | `python3 tools/handoff_check.py --update` | `docs/HANDOFF.md` snapshot block matches live data (no `[ drift ]` / `[ STALE ]`, no engine lag) |
| 1.2 | Run full verification ladder | `tools/feed_doctor.py`, `tools/check_data.py`, `tools/smoke_test.py`, `tools/live_path_probe.py` | 0 failures; all warnings investigated, including any CME maintenance-boundary gaps |
| 1.3 | Verify **Readiness** tab | `dashboard.py` — nav tab + `/readiness` panel | Tab renders the current feed verdict, the same six data gates as `live_readiness.py`, trade stats, readiness verdict and an unsigned non-data checklist |
| 1.4 | Commit + push | `git commit` / `git push origin arena/6cc2c0b1-nasdaq-trading-bot` | Working tree clean; work remains on the active development branch |

**Definition of done for Phase 1:** `python3 tools/check_data.py` exits 0 with its warnings investigated, `tools/handoff_check.py` reports no problems, the offline tests and execution probe pass, the dashboard loads on `:6001` with a working Readiness tab, and `tools/feed_doctor.py` is verified on the actual MT5 host.

### Phase 1 verification notes — checkout snapshot, 2026-10-30

- The HANDOFF snapshot has been refreshed. `check_data.py` exits 0 but reports **18 missing M15 slots**. Four are 21:00 UTC bars adjacent to the daily maintenance break; **14 occurred during open-market hours on October 30** (04:45–05:00, 06:00–07:00, 07:30–08:00 and 08:30–08:45 UTC). All missing slots are present in the checked-in XM candle backfill. Their absence from the engine's forward log is **not yet proven explained**; investigate the engine/feed/journals on the VPS. Do not edit or backfill the forward ledger to hide the gaps.
- **Autosync rollback race (leading hypothesis, not proven):** `tools/autosync.sh` committed live data, merged upstream, smoke-tested, and on failure ran `git reset --hard` to the pre-merge commit — erasing rows the still-running engine/XM logger wrote after the data commit. Data collection 4740 (04:45:01) contains candles only through 04:30; a failed deploy rolled back at 04:45:05 and another at 05:00, while the engine journal/EMAs show it processed candles now missing from the CSV. Timing fits, but precise write times are unverified and no trade loss has been established. **Fix status (2026-10-30, this branch):** rollback rewritten to preserve concurrent writes through merges/conflicts/aborts/smoke failures (data-only commits around every step, out-of-worktree backup + verification, no `reset --hard`, fail-safe no-push + non-zero exit if preservation is unverifiable), with deterministic temp-repo integration tests in `tests/test_autosync.py` (also run by smoke Scenario S). **Not deployed:** merging this to `main` would let the *currently deployed, unsafe* autosync perform the deploy (bash also reads its own script file incrementally, so a mid-run self-update is itself hazardous). Deploy only via the controlled procedure: operator pauses cron, updates `tools/autosync.sh` on the VPS by hand, verifies, resumes cron — awaiting operator approval.
- **`feed_doctor.py` XM witness time bug (fixed on this branch):** the witness treated `candles_m15.csv`'s `bar_close_utc` as an *open* time and added 15 minutes, so the reported bar age was one bar too fresh (a bar closed 21 min ago read as 6 min) and could wrongly prove MT5 alive. It now resolves the timestamp column from the header, reports open/close correctly, and fails closed on unknown schema. Regression: `tests/test_feed_doctor.py` + smoke Scenario R.
- `feed_doctor.py` returns `FILE_MISSING` in this isolated development checkout because there is no local MT5 feed/heartbeat. This does **not** establish the VPS feed's present state; rerun the doctor and verification ladder on `/opt/nasdaq`.
- The offline smoke suite and `live_path_probe.py` exercise temporary files and **fake MT5**, not a real XM demo or live account. Passing them does not complete the execution soak.
- The checked-in ledger has **1/100** post-gate trades. `live_readiness.py --spread 0.25` currently shows **0/6** data gates passing: the sample and the 90-minute open-market gap fail; four statistical gates remain unknown. The dashboard also checks current feed/heartbeat and snapshot freshness independently.

The Readiness page and CLI deliberately fail closed on missing/malformed evidence. Scheduled closures split gap runs, but missing open-market candles remain visible. `UNKNOWN` is not `PASS`; even 6/6 data gates passing does not replace the manual contract/cost, demo-soak and kill-switch sign-offs.

### Re-review notes — 2026-11-02 12:35 UTC (engine freeze)

Re-ran the ladder in this checkout. **The plan was stale in the specific way that matters: it described a working-but-quiet forward test, while the evidence pipeline has actually been dead since Friday.**

| Evidence | Value | Reading |
|---|---|---|
| `xm_data/quotes-2026-11-02.csv` last row | `2026-11-02 12:29:30 UTC`, 0 gaps > 90 s all day, spread 2.90 pts | XM logger + MT5 + Wine bridge are **alive right now** |
| `xm_data/candles_m15.csv` last bar | `2026-11-02 12:30 UTC` (12,514 rows) | Backfill current; the missing 2026-10-30 slots are all present here (21/21 checked) |
| `forward_test_log.csv` last row | `2026-10-30 20:45 UTC` (364 rows) | Engine stopped writing at Friday's 16:45 ET bar |
| `status.json` `last_update` | `2026-10-30 20:45:03` | No status write since then → no candle processed |
| Gap, open-market only | **53 M15 bars / 63.7 h wall clock** | ~2.7 days of forward-test evidence lost; `live_readiness.py` stuck at 1/100 trades |
| `feed_doctor.py` | `BROKEN - FILE_MISSING` | Expected in this checkout (no `/opt/nasdaq`); says nothing about the VPS |
| `check_data.py` | exits 0, 2 warnings (18 missing M15 slots + 90-min gaps) | Unchanged since 2026-10-30; it only inspects the log, so a frozen log passes |
| `live_readiness.py --spread 0.25` | 0/6 data gates; sample 1/100 FAIL, feed gap FAIL | Unchanged verdict |

**What this does *not* establish:** the cause. The checked-in files cannot distinguish (a) `nasdaq-engine` stopped/crash-looping, (b) engine running but unable to read `mt5_last_candle.json` (`mt5feed-nasdaq` dead or pointed at another path — quotes and the feed file come from *different* sidecars), or (c) an operator pause. Only `/opt/nasdaq` can answer that; run task 1.0 there.

**Second finding (fixed on this branch):** `handoff_check.py` could not see this outage. Every snapshot key is derived from the engine's own artifacts, and `as_of_utc` came from `status.json`, so a frozen engine produced a perfectly matching snapshot — `HANDOFF FRESH` for 2.7 days. It now compares the newest `xm_data/` candle with the engine watermark and reports **engine lag** in open-market M15 bars: STALE while the market is open (≥ `ENGINE_LAG_BARS` = 8), informational while the market is quiet (a Friday-close-frozen log is normal on Saturday). Regression: `tests/test_handoff_check.py`.

### Root cause — confirmed on the VPS, 2026-11-02 12:44 UTC

`systemctl status nasdaq-engine` shows `active (running) since 2026-10-26 17:41`, repeating
`MT5: feed stale [CANDLE_OLD] last closed M15 bar ...` every 60 s, and `journal.csv` holds
`STALE_FEED` alerts from 2026-11-02 10:10 onward. The engine is healthy and behaving
correctly. `feed_doctor.py` on `/opt/nasdaq` gives `STALE - CANDLE_OLD`, with:

- `mt5_last_candle.json` — exists, `mtime 2026-10-31 03:01:39`, candle frozen at
  `2026-10-30 20:45 UTC` → nothing published for ~2.7 days
- `mt5_feed_health.json` — `status: NO_RATES`, heartbeat 0 s (process alive),
  `offset 7200`, **`reconnects: 291`**, `last_error: copy_rates_from_pos failed:
  (-10004, 'No IPC connection')`
- witness — `xm_data/quotes-2026-11-02.csv` last quote 21 s old, `candles_m15.csv` last
  bar closed 14 min ago → MT5, Wine and the **quote logger** are all fine

**Diagnosis:** `mt5feed-nasdaq`'s MT5 handle died (the Wine terminal/session it attached to
restarted while the Python process kept running) and the sidecar could not recover, because
its bounded `shutdown()+initialize()` path only ran for `TICK_FROZEN` — never for
`NO_RATES`. So it kept rewriting a fresh heartbeat with a frozen feed file until someone
looked. `mt5quotes-nasdaq` is a separate process with its own handle, which is exactly why
the witness stayed healthy and why this looked like "engine stopped" from the repo.

**Fixed on this branch:** `tools/mt5_feed.py` now reconnects on `NO_RATES` /
`TERMINAL_DISCONNECTED` as well (`should_reconnect()`, still one attempt per
`MT5_FEED_RECONNECT_S`), and `tools/feed_doctor.py` now turns `NO_RATES` + `-10004` into an
explicit "restart mt5feed-nasdaq" step instead of printing `none - feed is healthy`
underneath a STALE verdict. Regression: `tests/test_mt5_feed_reconnect.py`,
`tests/test_feed_doctor.py`.

**Done (operator, on the VPS):** `mt5feed-nasdaq` was restarted after the 2026-11-02 12:44 UTC root-cause confirmation — the engine's first post-outage bar is 2026-11-02 12:45 UTC and evidence runs to 2026-11-04 08:15 UTC. **Still required every session (operator, on the VPS):** re-run `tools/feed_doctor.py` + the §6 ladder on `/opt/nasdaq`, confirm bars are still arriving (both the last log bar and the last GitHub push stop at 2026-11-04 08:15 UTC in this checkout), and investigate the post-resume 90-minute gap (11-02 21:45→23:15) — `docs/STALE-FEED-TRIAGE.md` §5.

**Do not:** backfill `forward_test_log.csv` from `xm_data/` to "recover" the 53 bars (that would fabricate forward-test evidence and silently rewrite the record), or count the XM backfill toward the 100-trade gate. The bars are gone; Phase 2 continues from the 2 post-gate trades already on the ledger.

### Re-review notes — 2026-11-17 (readiness re-verification; false-FRESH fixed)

Re-ran the ladder. It is green as a *code* baseline but the checkout has been frozen for
**13 days**, and the previous "Phase 1 verification green" claim was partly an artifact of a
gate that could not see a fully frozen pipeline.

| Evidence | Value | Reading |
|---|---|---|
| `handoff_check.py` (before) | `HANDOFF FRESH`, `age +0.0 h` | **False green.** Age was computed snapshot-vs-frozen-files, and engine-vs-feed lag was 0 because *both* sides stopped |
| `handoff_check.py` (after) | `HANDOFF STALE`, `wall age 312.2 h`, 819 open-market bars | Correctly fails closed |
| `check_data.py` | exit 0, 2 warnings | Only inspects file contents; a frozen log passes by design |
| `pytest tests/` | 74 passed | Baseline green (was 69; 5 new regressions added) |
| `smoke_test.py` | A–S all PASS | Offline only; not live evidence |
| `live_path_probe.py` | 11/11 | Fake MT5, not a broker demo |
| `live_readiness.py --spread 0.25` | exit 1, **0/6** data gates | Unchanged verdict: 2/100 trades, feed-gap FAIL |
| `feed_doctor.py` | `BROKEN - FILE_MISSING` | Expected in an isolated checkout, not evidence about the VPS |

**Fixed on this branch (tooling correctness — none of it creates trading evidence):**

1. **`handoff_check.py` `engine_lag()` had the same class of blind spot one level up.** The
   2026-11-02 fix compared the engine watermark with the newest XM candle, which catches
   "engine dead, logger alive". It could not catch "everything dead": when `mt5feed-nasdaq`
   *and* `mt5quotes-nasdaq` both stop, watermark and feed match exactly, `age` is `+0.0 h`,
   and the gate printed FRESH for 13 days. It now also compares the newest evidence (the later
   of engine watermark and XM data) with the **wall clock** and distinguishes
   `stale = "engine"` from `stale = "stale"` in the message. A checkout with no `xm_data/` is
   no longer "unknown" when the log itself is days old. Renamed the reporting field to
   `engine lag` and added a `newest evidence` line.
2. **`handoff_check.py --update` exited 0 while stamping a frozen snapshot.** It now prints the
   STALE reason and **exits 1** — the block is still rewritten (it should honestly reflect the
   files), but the run does not report success.
3. **`data_collection` was reported as `0` in any shallow clone** (it scraped `git log` with
   `-n 50`, and agent checkouts are shallow). `--update` then overwrote a real `5239` with `0`,
   destroying evidence. `_data_collection_number()` now widens the search and returns `None`
   when unknown, and `live_facts()` keeps the existing doc value in that case.
4. **`autosync.sh` silently downgraded a STALE handoff to a log line.** It now raises an
   `EXTRA` alert like `check_data.py` does, so a frozen pipeline pages instead of passing quietly.

**Still true and still the operator's job:** the VPS must be re-checked with task 1.0 and the
§6 ladder. Nothing above is evidence that the live pipeline is running; 814 of the 819 missing
open-market bars are *not* explainable by market closure and must be treated as lost
forward-test evidence — do not backfill.

### Re-review notes — 2026-11-18 (Readiness tab unusable: torn write in the evidence log)

The operator report was "it still says readiness cannot be computed". `/readiness` rendered
exactly that, with `Evidence unavailable or invalid: forward_test_log.csv contains unparseable
rows or changed while being read`. Phase 1 task 1.3 (Readiness tab) was therefore failing in
practice even though the code, the gates and the CLI were correct.

**Root cause (data, not code):** `forward_test_log.csv` carried **140 NUL bytes before the
2026-11-04 19:30:00 bar** (file size was exactly 180002 bytes - a partial/sparse append left a
hole, so that row did not start at column 0). `live_readiness.load_readiness_data()` compares
the count of rows `csv.DictReader` can parse with the count of rows in the file, correctly sees
a mismatch, and fails closed. Three things made that hard to see:

| Evidence | Before | Reading |
|---|---|---|
| `/readiness` | `NOT READY — EVIDENCE UNAVAILABLE`, "contains unparseable rows" | Correct fail-closed, zero diagnostic value |
| `tools/check_data.py` | exit **0**, `[WARN] 1 rows with unparsable timestamp/OHLC (skipped)` | Integrity gate passed a corrupted evidence file |
| `tools/live_readiness.py` | exit 1, same opaque message | CLI and tab agree, neither names the line |
| `git status` / data review | clean | The corruption is *inside* a committed row, not a stray line |

**Fixed on this branch (tooling correctness + one data repair; no trading evidence created):**

1. **`tools/repair_log.py` (new).** `--check` names the file, line, column and NUL count and
   exits 1; `--apply` removes **only** NUL bytes (plus a missing trailing newline), writes a
   `<file>.bak-YYYYmmddTHHMMSSZ` backup, re-validates the repaired pair with the strict
   `live_readiness` loader, and restores the backups / exits 2 if anything does not validate.
   It refuses when the padding hid *lost* row bytes: that is a human decision, not a repair.
   Nothing else is touched - no value, no row order, no row count, no backfill.
2. **`live_readiness.py`** detects NUL padding before parsing and raises with
   `forward_test_log.csv has 140 NUL byte(s) starting at line 530, column 0 - a torn or partial
   write ... run python3 tools/repair_log.py --check, then --apply`, plus line numbers for
   invalid/unordered bars. The gate logic is unchanged.
3. **`check_data.py`** fails (exit 1) on NUL bytes and on unparsable rows instead of warning and
   skipping them, so the autosync integrity gate can no longer bless a torn evidence file.
4. **`engine.py`** checks the log tail before appending (`ensure_appendable_log`): it never glues
   a bar onto a partial line, adds a missing newline, and warns with the repair command when the
   tail is NUL-padded. It never edits or truncates evidence.
5. **`forward_test_log.csv`** repaired with the tool: 140 NUL bytes removed, everything else
   byte-identical (1276 bars, same timestamps/OHLC, `check_data.py` exit 0).
6. **Regressions:** `tests/test_repair_log.py` (7 tests: check never writes, apply removes only
   padding, backups, lost-data hole refused, failed validation rolled back, trailing newline,
   engine guard) and a dashboard test asserting `/readiness` names the repair command.

**Result:** the Readiness tab renders again on the checked-in evidence - 6 closed trades (2W/4L),
**0/6 data gates** (sample 6/100, feed gate worst open-market gap 690 min), verdict
`NOT READY — FEED` in this isolated checkout, plus the unsigned non-data checklist.

**Still the operator's job (nothing above changes the verdict):**

- **Run the repair on the VPS if the file is still torn there:**
  `cd /opt/nasdaq && python3 tools/repair_log.py --check` (exit 1 → `--apply`). Do this after the
  deploy lands: autosync merges *data files* with "server wins", so a locally modified
  `forward_test_log.csv` keeps the server copy and the padding survives the merge.
- **Task 1.0 remains P0.** The pipeline is still frozen: newest evidence **2026-11-17 09:30 UTC**,
  `handoff_check.py` STALE (27.7 h), and `status.json` shows the sidecar signature from the
  October incident (`status: NO_RATES`, `last_error: (-10004, 'No IPC connection')`). Confirm the
  autosync cron is running at all - the feed self-heal is in `autosync.sh`, so a frozen pipeline
  with a dead handle means cron did not run or the whole host is down.
- **Do not** treat the repair as evidence: the 140-byte repair restores readability, not trades.

---

## Phase 2 — Statistical Evidence (forward-test accumulation)

Goal: grow the post-gate evidence base so `tools/live_readiness.py` can pass its data gates.

**Evidence timeline:** the engine wrote nothing between 2026-10-30 20:45 and 2026-11-02 12:45 UTC (53 open-market bars lost), then accumulated through **2026-11-04 08:15 UTC** (2 post-gate trades, 1W/1L). No evidence has appeared in this checkout since that timestamp, so before measuring anything, confirm on the VPS that the feed and engine are still moving (task 1.0/§6 ladder). 2 post-gate trades are statistically meaningless — the 100-trade target starts from 2, not from a backfill.

| # | Task | Command / Artifact | Acceptance |
|---|------|-------------------|------------|
| 2.1 | Keep `mt5feed-nasdaq` + `nasdaq-engine` healthy | `docs/NASDAQ-OPS.md` §3 verification ladder | `feed_doctor.py` → OK; no `STALE_FEED` outside market-quiet windows |
| 2.2 | Accumulate ≥ 100 post-gate closed trades | `trades.csv` (entries >= `GATES_DEPLOY`, 2026-10-24) | `live_readiness.py` reports n ≥ 100 post-gate trades |
| 2.3 | Run statistical edge report | `python3 tools/live_readiness.py --spread 0.25` | Gate 1 (sample) + Gate 2 (edge P ≥ 90%) moving toward PASS |
| 2.4 | Run historical backtest for context | `python3 tools/backtest.py xm_data/candles_m15.csv --spread 0.25` | 6-month XM M15 backtest completes; used as prior, not as go/no-go |
| 2.5 | Review losers & pressure | `tools/analyze_losers.py`, `tools/pressure_report.py` | No systematic failure mode hidden in the 1% tail |

**Definition of done for Phase 2:** `live_readiness.py --spread 0.25` data gates trending to 6/6 PASS; no unexplained losing cluster.

---

## Phase 3 — Execution Soak (XM demo, real order path)

Goal: prove the LIVE order path end-to-end without risking real money.

| # | Task | Command / Artifact | Acceptance |
|---|------|-------------------|------------|
| 3.1 | Stage A: arm executor on demo | `docs/START-LEVER1.md` — `MT5_EXEC_ENABLED=1`, `MT5_EXEC_ALLOW_REAL=0`, `TRADING_MODE=LIVE`, XM demo | `tools/live_path_probe.py` 11/11 clean against demo |
| 3.2 | Stage B: 5–10 live demo round-trips | Same demo window | Demo fills match `trades.csv`/`status.json` signal flow; SL/TP reconciliation by `position_id` works |
| 3.3 | Stage C: parity review | Compare simulator vs demo fills (slippage, fill latency) | Parity within assumed spread ($0.25/trade) |
| 3.4 | Test kill switches | `touch /opt/nasdaq/HALT` → emergency flatten | Position flattens, new intents refused, `HALT` removal only after confirmed close |

**Definition of done for Phase 3:** Zero orphaned positions, zero duplicate orders, verified position-id reconciliation, tested HALT flatten.

---

## Phase 4 — Go-Live Gates (before `MT5_EXEC_ALLOW_REAL=1`)

All three must hold (from `docs/HANDOFF.md` §7):

| Gate | Source | Threshold |
|------|--------|-----------|
| Contract & Cost | `tools/xm_quote_report.py` | `calc_profit_usd_0.10lot_plus_100pts == 10.0`, `volume_min <= 0.10`, median spread ≤ 3.5 pts ($0.35 @ 0.10 lot), M15 parity OK |
| Statistical Edge | `tools/backtest.py` + `tools/live_readiness.py` | Day-block bootstrap P(net/trade > 0) ≥ 90%; break-even/assumed spread cover ≥ 2.0x |
| Execution Soak | `tools/live_path_probe.py` + XM Demo | Zero orphans, zero duplicates, SL/TP by position_id, HALT tested |

Plus the non-data checklist from `live_readiness.py` §8 (order path, spread profile, demo parity, kill switches, micro-live caps) — signed off by hand.

**Definition of done for Phase 4:** All 3 data gates PASS + non-data checklist signed → authorized to flip `MT5_EXEC_ALLOW_REAL=1` on a real-money XM account.

---

## Phase 5 — Micro-Live (real money, capped)

Goal: prove live economics at small size before scaling.

| # | Task | Cap |
|---|------|-----|
| 5.1 | Flip `MT5_EXEC_ALLOW_REAL=1` on real XM account | 0.10 lot only, weekday-only, max daily loss $15 / 3 SLs |
| 5.2 | Monitor daily | `dashboard.py` Readiness tab + `feed_doctor.py` each session |
| 5.3 | Review after N trades | Re-run `live_readiness.py` on live ledger; scale only if live P/L matches forward-test within spread |

---

## How to use this plan

- **Each session starts here.** Read Phase 1 status first, then pick the next incomplete task.
- **Do not edit live code on the server** — edit in this checkout, run `tools/smoke_test.py` + `tools/live_path_probe.py`, commit, push, let autosync deploy.
- **Refresh the HANDOFF snapshot** (`tools/handoff_check.py --update`) whenever ledger/config facts change, then commit.
- **Readiness tab** (`dashboard.py` → `/readiness`) is the single source of truth for "are we ready?" during any session.
