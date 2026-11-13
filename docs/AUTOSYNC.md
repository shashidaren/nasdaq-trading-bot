# AUTOSYNC — Unattended Data Sync & Smoke-Gated Auto-Deploy (`/opt/nasdaq`)

`tools/autosync.sh` runs from root's crontab every 15 minutes on the server hosting `/opt/nasdaq`.

## What Every Run Does

1. **Commits live data** (`trades.csv`, `forward_test_log.csv`, `skipped_trades.csv`, `status.json`, `xm_data/`) as `"data collection N"`.
2. **Fetches `origin/main`:** If `origin/main` advanced (e.g. a merged PR) and `/opt/nasdaq` has no hand-edited code (staged or unstaged):
   - Stops `nasdaq-engine` if `engine.py` or `trade_filter.py` changed.
   - Snapshots live data again and backs it up outside the worktree (`.git/autosync-backup-*`), and records the pre-merge commit in `refs/autosync/pre-deploy`. If either fails, **nothing is merged** (fail-safe).
   - Merges `origin/main` (`--ours` on live data files, `--theirs` on code/docs), then folds in any rows written while the merge ran.
   - Runs `python3 tools/smoke_test.py` (`SMOKE_GATE=1`). On failure it rolls back **code only** and preserves live data (see below).
   - On success it commits any rows written during the smoke run, then restarts `nasdaq-engine` / `nasdaq-dashboard` as needed and checks `status.json` freshness.
3. **Pushes** the commit to `origin/main` — skipped if a rollback could not be verified.
4. **Feed sidecar self-heal (phase 3b, `FEED_SELF_HEAL=1`):** runs `tools/feed_doctor.py --json` and restarts `mt5feed-nasdaq` **at most once per run, and only when the doctor says a restart is the fix** (`actions.restart_sidecar`):
   - `dead-handle` — heartbeat present and fresh, but the MT5 handle is dead (`last_error` mentions IPC / `-10004`, or status is `NO_RATES` / `INIT_FAILED` / `SYMBOL_SELECT_FAILED`). This is the 2026-10-30→11-02 incident: the sidecar process stayed alive and heartbeat-fresh over a frozen feed file for 2.7 days because nothing ever restarted it.
   - `not-running` — feed file and heartbeat both missing, **and** systemd does not report `inactive`/`deactivating` (a deliberate operator stop is never overridden).

   The restart only fires against an open-market `STALE`/`BROKEN` verdict: `OK` and `QUIET` (weekends, 16:55–18:10 ET maintenance) are never restarted; a stale heartbeat (the unit may be deliberately stopped) is never restarted; a configuration problem (foreign writer / wrong `MT5_FEED_FILE`) is alerted but not restarted, because a restart would loop without fixing it. After a restart (`FEED_HEAL_WAIT_S`, default 20 s) the doctor runs again: recovery is logged, no recovery adds a `⚠️` digest line with `journalctl` guidance, and the next cron run tries again. If `systemctl` finds no matching unit, autosync logs `feed service NOT FOUND - restart feed sidecar manually` instead of guessing a target. Set `FEED_SELF_HEAL=0` to disable auto-restarts entirely.

## Live-Data Preservation (why there is no `git reset --hard`)

The engine and the XM quote logger keep appending to the tracked data files while autosync merges and smoke-tests. The previous rollback ran `git reset --hard` to the pre-merge commit, silently erasing every row written after the phase-1 data commit — for the whole smoke run. That is the leading (unproven) hypothesis for the missing 2026-10-30 04:45/05:00 M15 candles: failed deploys rolled back at 04:45:05 and 05:00 while the engine kept running.

The current flow makes data loss structurally impossible in the tested paths:

- Data is committed (data paths only, `git commit --only`) in phase 1, again immediately before the merge, after the merge, after a successful smoke run, and around every rollback step.
- Rollback = back up data → `merge --abort` if needed (then restore anything the abort reverted) → commit data → `git reset --soft` to the pre-merge commit → re-commit data on top → restore **code paths one file at a time** (no checkout/reset ever names a data path) → verify.
- Verification compares the out-of-worktree backup against the surviving files: every backed-up CSV/JSONL line must still be present, and `status.json` must not have gone backwards. Conflict-marker lines are never treated as data.
- **Fail-safe:** if backup, snapshot, or verification fails, autosync merges nothing (pre-merge failure) or skips the push, keeps the backup under `.git/autosync-backup-*`, logs `ROLLBACK FAILED`, and exits non-zero for manual intervention. The pre-merge commit is always reachable as `refs/autosync/pre-deploy`.
- Residual risk, accepted and documented: a write landing inside the sub-second-to-seconds window while `git merge` itself rewrites a *conflicted* data file cannot be salvaged without heuristics that could corrupt the ledger. Data-file conflicts require upstream to edit data files, which normal PRs do not; the engine is additionally stopped during core-code deploys.
- Regression coverage: `tests/test_autosync.py` drives the real script against isolated temp git repos (failed smoke with concurrent writes, successful deploy, data/code conflict, merge-commit rejection + abort, dirty-code skip, plain sync) and runs inside `tools/smoke_test.py` Scenario S, so it is exercised on every deploy.
5. **Runs advisory & integrity gates:**
   - `python3 tools/check_data.py` (ledger + M15 bar census + gate conformance)
   - `python3 tools/handoff_check.py --quiet` (`docs/HANDOFF.md` freshness)
   - `python3 tools/xm_quote_report.py --quiet` (`mt5quotes-nasdaq` liveness, market-hours aware)
6. **Optional Telegram Digest** (`NOTIFY=off|quiet|alerts|always`; static alert text on `STALE` so `alert_once` deduplicates cleanly).

## Cron Setup

```bash
*/15 * * * * /opt/nasdaq/tools/autosync.sh >> /var/log/nasdaq-autosync.log 2>&1
```

## Rules for Operators & Agents

- **Never hand-edit `.py` files in `/opt/nasdaq`** — `autosync.sh` skips deploys when local code is dirty.
- **Never force-push `main`** — live data commits advance `origin/main` every 15 minutes; always merge PRs cleanly.
