#!/usr/bin/env bash
# ==============================================================================
# tools/autosync.sh — unattended data sync + safe auto-deploy for /opt/nasdaq
# Runs from cron (root). Full documentation: docs/AUTOSYNC.md
#
# Every run:
#   1. Commits the live data files (incl. xm_data/, the XM quote logger's output) and
#      pushes them to origin/main
#   2. If origin/main advanced (e.g. a merged PR):
#        stops the engine -> snapshots live data -> merges (conflicts: data
#        files = server wins, everything else = remote wins) -> runs the smoke
#        test -> rolls back CODE ONLY on failure -> restarts the engine ->
#        verifies status.json is fresh
#   3. Runs tools/check_data.py (integrity gate), tools/handoff_check.py (docs/HANDOFF.md
#      freshness) and tools/xm_quote_report.py --quiet (XM logger liveness).
#   4. Optionally sends a digest to Telegram (NOTIFY=off by default).
#
# FEED SELF-HEAL (phase 3b, FEED_SELF_HEAL=1 by default):
#   Every run asks tools/feed_doctor.py --json whether the feed sidecar
#   (mt5feed-nasdaq) is in a state that ONLY a restart can fix, and restarts it
#   at most once per run - never blindly:
#     - actions.restart_sidecar == "dead-handle": heartbeat fresh but the MT5
#       handle is dead (IPC / -10004 / NO_RATES / INIT_FAILED /
#       SYMBOL_SELECT_FAILED) - the 2026-10-30..11-02 incident, where the sidecar
#       heartbeat stayed fresh over a frozen feed file for 2.7 days;
#     - actions.restart_sidecar == "not-running": feed file and heartbeat both
#       missing, and systemd does NOT report a deliberate `inactive` stop;
#   and only against an open-market STALE/BROKEN verdict (QUIET/OK never
#   restart - weekends and the 16:55-18:10 ET maintenance break are expected
#   stillness). After a restart it re-checks and alerts if the feed did not
#   recover; if the restart does not help, the next cron run tries again and
#   the alert keeps firing. Set FEED_SELF_HEAL=0 to disable auto-restarts.
#
# DATA-PRESERVATION CONTRACT (the reason for the helpers below):
#   The engine and the XM quote logger keep appending to the tracked data
#   files while this script merges and smoke-tests. The previous rollback ran
#   `git reset --hard` to the pre-merge commit, which silently erased every
#   row written after the phase-1 data commit (the leading hypothesis for the
#   missing 2026-10-30 04:45/05:00 M15 candles). Therefore:
#     - live data is committed (data paths only) immediately before the
#       merge, after the merge, and around every rollback step;
#     - rollback is `git reset --soft` + an `--only` data commit + per-file
#       restore of code paths. There is NO `reset --hard` in this script and
#       no checkout/rm ever names a data path during rollback;
#     - before any git operation that rewrites worktree files (merge,
#       conflict checkout, merge --abort), the data files are copied to
#       .git/autosync-backup-* and later verified line-by-line (append-only
#       evidence must be a subset of what survives; status.json must not go
#       backwards). If preservation cannot be verified the run fails safe:
#       push is skipped, the backup is kept, and the script exits non-zero.
# ==============================================================================
set -uo pipefail

NASDAQ_DIR="${NASDAQ_DIR:-/opt/nasdaq}"
LOCK_FILE="${LOCK_FILE:-/var/lock/nasdaq-autosync.lock}"
REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-main}"
AUTO_DEPLOY="${AUTO_DEPLOY:-1}"
SMOKE_GATE="${SMOKE_GATE:-1}"
ENGINE_SERVICE="${ENGINE_SERVICE:-}"
DASHBOARD_SERVICE="${DASHBOARD_SERVICE:-}"
FEED_SERVICE="${FEED_SERVICE:-}"
FEED_SELF_HEAL="${FEED_SELF_HEAL:-1}"
FEED_HEAL_WAIT_S="${FEED_HEAL_WAIT_S:-20}"
NOTIFY="${NOTIFY:-off}"
DAILY_STAMP="${DAILY_STAMP:-/tmp/nasdaq_autosync_daily_digest}"
ALERT_STAMP="${ALERT_STAMP:-/tmp/nasdaq_autosync_last_alert}"
HEARTBEAT_FILE="${HEARTBEAT_FILE:-$NASDAQ_DIR/autosync_heartbeat.json}"
DATA_FILES=(trades.csv forward_test_log.csv skipped_trades.csv status.json xm_data)
CODE_PATHS=(engine.py trade_filter.py dashboard.py pressure.py feed_health.py tools/)

log() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*"; }

TG_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TG_CHAT="${TELEGRAM_CHAT_ID:-}"
if [ -z "$TG_TOKEN" ] && [ -f "$NASDAQ_DIR/.env" ]; then
    TG_TOKEN=$(sed -n 's/^TELEGRAM_BOT_TOKEN=//p' "$NASDAQ_DIR/.env" | head -n1 | tr -d "\"'")
    TG_CHAT=$(sed -n 's/^TELEGRAM_CHAT_ID=//p' "$NASDAQ_DIR/.env" | head -n1 | tr -d "\"'")
fi

tg() {
    if [ -z "$TG_TOKEN" ] || [ -z "$TG_CHAT" ]; then return 0; fi
    curl -s -m 10 -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${TG_CHAT}" \
        --data-urlencode "text=${1}" >/dev/null 2>&1 || true
}

alert_once() {
    [ "$NOTIFY" = "off" ] && return 0
    if [ "$1" = "$(cat "$ALERT_STAMP" 2>/dev/null)" ]; then return 0; fi
    tg "$2"
    printf '%s' "$1" > "$ALERT_STAMP" 2>/dev/null || true
}

critical_alert() {
    # Repository-integrity failures must never be silently suppressed by
    # NOTIFY=off. Always log loudly; deliver Telegram too when credentials exist.
    log "ALERT: $1"
    tg "🚨 nasdaq autosync: $1"
}

detect_service() {
    if [ -n "${2:-}" ]; then echo "$2"; return 0; fi
    local unit def
    while IFS= read -r unit; do
        [ -z "$unit" ] && continue
        def=$(systemctl cat "$unit" 2>/dev/null) || continue
        if printf '%s' "$def" | grep -q "$1" && printf '%s' "$def" | grep -qF "$NASDAQ_DIR"; then
            echo "$unit"; return 0
        fi
    done < <(systemctl list-units --type=service --no-legend --plain 2>/dev/null | awk '{print $1}')
    return 1
}

# --- live-data preservation helpers -------------------------------------------
# All helpers assume cwd is the repo root (guaranteed before phase 1).

is_data_path() {
    case "$1" in
        trades.csv|forward_test_log.csv|skipped_trades.csv|status.json|xm_data|xm_data/*) return 0 ;;
        *) return 1 ;;
    esac
}

DATA_COMMIT_NOTE=""
GIT_STATE_FATAL=0
commit_live_data() {
    # Commit ONLY the live-data paths (worktree content) as the next
    # "data collection N". Returns 0 when the data paths are committed/clean,
    # 1 on failure. `--only` guarantees staged non-data changes are never
    # swept into a data commit by accident.
    DATA_COMMIT_NOTE="no new data"
    local f diff_rc staged_trades log_subjects new_trades last_n n
    for f in "${DATA_FILES[@]}"; do
        if ! git add -- "$f" 2>/dev/null; then
            DATA_COMMIT_NOTE="GIT ERROR: git add failed for $f"
            log "$DATA_COMMIT_NOTE"
            GIT_STATE_FATAL=1
            return 1
        fi
    done
    git diff --cached --quiet -- "${DATA_FILES[@]}" 2>/dev/null
    diff_rc=$?
    if [ "$diff_rc" -eq 0 ]; then
        return 0
    elif [ "$diff_rc" -ne 1 ]; then
        DATA_COMMIT_NOTE="GIT ERROR: cached diff failed (rc=$diff_rc)"
        log "$DATA_COMMIT_NOTE"
        GIT_STATE_FATAL=1
        return 1
    fi
    staged_trades=$(git diff --cached -- trades.csv 2>/dev/null)
    if [ $? -ne 0 ]; then
        DATA_COMMIT_NOTE="GIT ERROR: could not inspect staged trades.csv"
        log "$DATA_COMMIT_NOTE"
        GIT_STATE_FATAL=1
        return 1
    fi
    new_trades=$(printf '%s\n' "$staged_trades" | grep -c '^+[^+]' || true)
    log_subjects=$(git log --format=%s 2>/dev/null)
    if [ $? -ne 0 ]; then
        DATA_COMMIT_NOTE="GIT ERROR: could not read commit history for data counter"
        log "$DATA_COMMIT_NOTE"
        GIT_STATE_FATAL=1
        return 1
    fi
    last_n=$(printf '%s\n' "$log_subjects" | sed -n 's/^data collection \([0-9][0-9]*\)$/\1/p' | sort -n | tail -n1)
    n=$(( ${last_n:-0} + 1 ))
    if git commit -q --only -m "data collection $n" -- "${DATA_FILES[@]}"; then
        DATA_COMMIT_NOTE="committed ${new_trades} new trade rows -> 'data collection $n'"
        log "data: $DATA_COMMIT_NOTE"
        return 0
    fi
    DATA_COMMIT_NOTE="COMMIT FAILED"
    log "data commit failed"
    GIT_STATE_FATAL=1
    return 1
}

BACKUP_DIR=""
backup_data_files() {
    # Copy the live-data paths under .git/ so no git operation (merge,
    # checkout, abort, reset) can destroy the copy. Repeat calls refresh the
    # same backup dir so it always mirrors the newest worktree data.
    if [ -z "$BACKUP_DIR" ]; then
        BACKUP_DIR=".git/autosync-backup-$(date -u '+%Y%m%dT%H%M%S')-$$"
    fi
    mkdir -p "$BACKUP_DIR" || return 1
    local f
    for f in "${DATA_FILES[@]}"; do
        [ -e "$f" ] || continue
        rm -rf "$BACKUP_DIR/$f" || return 1
        cp -a "$f" "$BACKUP_DIR/" || return 1
    done
    return 0
}

cleanup_backup() {
    # Only called once preservation is proven (successful deploy or verified
    # rollback); on any doubt the backup is kept for manual recovery.
    if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
        rm -rf "$BACKUP_DIR" 2>/dev/null || true
    fi
    BACKUP_DIR=""
}

status_newer() {
    # 0 iff $1's status.json last_update is strictly later than $2's.
    python3 - "$1" "$2" <<'PYEOF'
import json, sys
def lu(p):
    try:
        with open(p) as fh:
            return json.load(fh).get("last_update") or ""
    except Exception:
        return ""
sys.exit(0 if lu(sys.argv[1]) > lu(sys.argv[2]) else 1)
PYEOF
}

restore_richer_data() {
    # Undo clobbering of live-data files by a git operation: wherever the
    # backup copy is strictly richer (more lines, or a newer status.json),
    # restore it over the worktree copy. Never deletes worktree files.
    [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ] || return 0
    local bak rel blines clines
    while IFS= read -r -d '' bak; do
        rel="${bak#$BACKUP_DIR/}"
        # A backup taken over a conflicted tree contains merge markers; such a
        # file is not data and must never be restored over a clean copy.
        if grep -q '^<<<<<<< ' "$bak" 2>/dev/null; then
            log "data-preserve: skipping conflicted backup copy of $rel"
            continue
        fi
        if [ ! -f "$rel" ]; then
            mkdir -p "$(dirname "$rel")" && cp -a "$bak" "$rel" \
                && log "data-preserve: restored missing $rel from backup"
            continue
        fi
        case "$rel" in
            status.json)
                if status_newer "$bak" "$rel"; then
                    cp -a "$bak" "$rel" && log "data-preserve: restored newer status.json from backup"
                fi ;;
            *.json)
                if [ "$bak" -nt "$rel" ]; then
                    cp -a "$bak" "$rel" && log "data-preserve: restored newer $rel from backup"
                fi ;;
            *)
                blines=$(wc -l < "$bak" | tr -d ' ')
                clines=$(wc -l < "$rel" | tr -d ' ')
                if [ "$blines" -gt "$clines" ]; then
                    cp -a "$bak" "$rel" \
                        && log "data-preserve: restored longer $rel ($clines -> $blines lines) from backup"
                fi ;;
        esac
    done < <(find "$BACKUP_DIR" -type f -print0)
    return 0
}

verify_data_preserved() {
    # Every line captured in the backup must still be present in the worktree
    # file (append-only CSV/JSONL evidence), status.json must not have gone
    # backwards, and JSON snapshots must still parse. 1 if anything is lost.
    [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ] || return 1
    local bak rel missing rc=0
    while IFS= read -r -d '' bak; do
        rel="${bak#$BACKUP_DIR/}"
        if [ ! -f "$rel" ]; then
            log "data-preserve: VERIFY FAIL - $rel missing from worktree"
            rc=1
            continue
        fi
        case "$rel" in
            status.json)
                if status_newer "$bak" "$rel"; then
                    log "data-preserve: VERIFY FAIL - status.json went backwards"
                    rc=1
                fi ;;
            *.json)
                if ! python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$rel" 2>/dev/null; then
                    log "data-preserve: VERIFY FAIL - $rel no longer parses as JSON"
                    rc=1
                fi ;;
            *)
                # Lines inside conflict hunks of a backup taken mid-merge are
                # not evidence: one side is upstream's version, which the
                # rollback intentionally does not keep.
                missing=$(awk '/^<<<<<<< /{h=1} !h{print} /^>>>>>>> /{h=0}' "$bak" \
                          | grep -vxFf "$rel" 2>/dev/null | sed '/^$/d' | head -n 3)
                if [ -n "$missing" ]; then
                    log "data-preserve: VERIFY FAIL - $rel lost lines present in backup, e.g.: $missing"
                    rc=1
                fi ;;
        esac
    done < <(find "$BACKUP_DIR" -type f -print0)
    return $rc
}

restore_code_paths_to_head() {
    # Make every non-data path match HEAD exactly, restoring files one by one.
    # Data paths are never named here, so a data write racing the rollback
    # cannot be clobbered (unlike the old `git reset --hard`).
    local f paths=".git/autosync-code-paths-$$" sorted=".git/autosync-code-paths-sorted-$$"
    if ! git diff --name-only -z HEAD > "$paths" 2>/dev/null \
       || ! git diff --cached --name-only -z HEAD >> "$paths" 2>/dev/null \
       || ! sort -z -u "$paths" > "$sorted"; then
        rm -f "$paths" "$sorted"
        log "rollback: git diff failed while enumerating code paths"
        return 1
    fi
    while IFS= read -r -d '' f; do
        is_data_path "$f" && continue
        if git cat-file -e "HEAD:$f" 2>/dev/null; then
            git checkout HEAD -- "$f" >/dev/null 2>&1 || { rm -f "$paths" "$sorted"; return 1; }
        else
            git rm -q -f -- "$f" >/dev/null 2>&1 || { rm -f "$paths" "$sorted"; return 1; }
        fi
    done < "$sorted"
    # Confirm no non-data path still differs from HEAD (staged or unstaged).
    : > "$paths"
    if ! git diff --name-only -z HEAD > "$paths" 2>/dev/null \
       || ! git diff --cached --name-only -z HEAD >> "$paths" 2>/dev/null \
       || ! sort -z -u "$paths" > "$sorted"; then
        rm -f "$paths" "$sorted"
        log "rollback: git diff failed while verifying restored code paths"
        return 1
    fi
    while IFS= read -r -d '' f; do
        is_data_path "$f" && continue
        log "rollback: $f still differs from HEAD after code restore"
        rm -f "$paths" "$sorted"
        return 1
    done < "$sorted"
    rm -f "$paths" "$sorted"
    return 0
}

rollback_deploy() {
    # Roll CODE back to $1 (the pre-merge commit) while preserving every
    # live-data write made up to this moment. Sequence: back up -> abort any
    # in-progress merge -> restore anything the abort clobbered -> commit data
    # -> soft-reset to pre-merge -> re-commit data on top (data only) ->
    # restore code paths to HEAD one by one -> commit stragglers -> verify.
    # Returns 1 (fail-safe) if preservation cannot be guaranteed/verified.
    local pre="$1"
    backup_data_files || { log "rollback: cannot back up live data - refusing to roll back"; return 1; }
    if [ -f .git/MERGE_HEAD ]; then
        git merge --abort >/dev/null 2>&1 || { log "rollback: git merge --abort failed"; return 1; }
        restore_richer_data
    fi
    commit_live_data || { log "rollback: cannot commit live data before rollback"; return 1; }
    backup_data_files || return 1
    git reset --soft "$pre" >/dev/null 2>&1 || { log "rollback: git reset --soft $pre failed"; return 1; }
    commit_live_data || { log "rollback: cannot re-commit live data on pre-merge code"; return 1; }
    restore_code_paths_to_head || { log "rollback: code restore incomplete"; return 1; }
    commit_live_data || return 1
    if ! verify_data_preserved; then
        restore_richer_data
        commit_live_data || return 1
        verify_data_preserved || { log "rollback: data preservation UNVERIFIED after restore"; return 1; }
    fi
    log "rollback: code restored to $pre; live data preserved and verified"
    return 0
}

feed_doctor_line() {
    # "<VERDICT>\t<RESTART_ACTION>\t<CHURN_ADVISORY>" from tools/feed_doctor.py
    # --json, or "ERROR\t-\t-" if the doctor cannot run. The doctor exits 1/2
    # for STALE/BROKEN on purpose, so its rc is never used as a gate here.
    python3 tools/feed_doctor.py --data-dir "$NASDAQ_DIR" --json 2>/dev/null \
        | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)
    churn = r.get("reconnect_churn") or {}
    churn_tag = (
        "{:.2f}/h".format(churn["rate_per_hour"])
        if churn.get("elevated") and churn.get("rate_per_hour") is not None
        else "-"
    )
    print("{}\t{}\t{}".format(
        (r.get("verdict") or ["UNKNOWN"])[0],
        (r.get("actions") or {}).get("restart_sidecar") or "-",
        churn_tag,
    ))
except Exception:
    print("ERROR\t-\t-")
' 2>/dev/null | tail -n 1 || true
}

HEARTBEAT_PHASE="preflight"
write_heartbeat() {
    # Atomic runtime stamp read by dashboard.py /readiness (STALE > 45 min).
    # Gitignored so writing it never dirties the worktree or causes merge conflicts.
    local rc="${1:-0}" status="ok"
    [ -z "$HEARTBEAT_FILE" ] && return 0
    if [ "$rc" -ne 0 ]; then
        status="error"
    elif [ "${PUSH_MSG:-}" != "ok" ] || [ "${CHECK_RC:-0}" != "0" ]; then
        status="warn"
    else
        case "${DEPLOY_MSG:-none}" in
            none|deployed*)
                case "${DEPLOY_MSG:-none}" in
                    *FAILED*|*STALE*|*NOT\ ACTIVE*|*NOT\ FOUND*|*GIT\ ERROR*|*ABORTED*)
                        status="warn" ;;
                esac ;;
            *) status="warn" ;;
        esac
    fi
    HB_STATUS="$status" HB_RC="$rc" HB_PHASE="${HEARTBEAT_PHASE:-done}" \
    HB_DATA="${DATA_MSG:-not run}" HB_PUSH="${PUSH_MSG:-not run}" \
    HB_DEPLOY="${DEPLOY_MSG:-none}" HB_FEED="${FEED_MSG:-not run}" \
    HB_CHECK="${CHECK_MSG:-not run}" HB_NOTIFY="${NOTIFY:-off}" \
    python3 - "$HEARTBEAT_FILE" <<'PYEOF' >/dev/null 2>&1 || true
import datetime, json, os, sys
path = sys.argv[1]
now = datetime.datetime.now(datetime.timezone.utc)
payload = {
    "ts_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
    "ts_epoch": round(now.timestamp(), 3),
    "status": os.environ.get("HB_STATUS", "ok"),
    "exit_code": int(os.environ.get("HB_RC", "0")),
    "phase": os.environ.get("HB_PHASE", "done"),
    "data": os.environ.get("HB_DATA", ""),
    "push": os.environ.get("HB_PUSH", ""),
    "deploy": os.environ.get("HB_DEPLOY", ""),
    "feed": os.environ.get("HB_FEED", ""),
    "check": os.environ.get("HB_CHECK", "").removesuffix(" ==").strip(),
    "notify": os.environ.get("HB_NOTIFY", ""),
}
parent = os.path.dirname(path)
if parent:
    os.makedirs(parent, exist_ok=True)
tmp = f"{path}.tmp-{os.getpid()}"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2)
    fh.write("\n")
    fh.flush()
os.replace(tmp, path)
PYEOF
}

# --- lock ---------------------------------------------------------------------
exec 9>"$LOCK_FILE" || { log "FATAL: cannot open lock file"; exit 1; }
if ! flock -n 9; then log "another autosync run is active - skipping"; exit 0; fi

cd "$NASDAQ_DIR" || { log "FATAL: cannot cd to $NASDAQ_DIR"; alert_once "fatal-cd|$NASDAQ_DIR" "nasdaq autosync: FATAL - cannot cd to $NASDAQ_DIR"; exit 1; }
trap 'write_heartbeat $?' EXIT

# Cheap fail-closed preflight: Git can misleadingly return an empty status and
# a successful "no diff" result when its object database is damaged. Never
# interpret those outputs until repository connectivity has been verified.
if ! git fsck --connectivity-only >/tmp/nasdaq_autosync_fsck.log 2>&1; then
    FSCK_DETAIL=$(tail -n 20 /tmp/nasdaq_autosync_fsck.log | tr '\n' ' ')
    critical_alert "repository integrity check FAILED in $NASDAQ_DIR; autosync aborted without touching live data: $FSCK_DETAIL"
    exit 1
fi

if [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ] || [ -f .git/MERGE_HEAD ]; then
    log "repo is mid rebase/merge - manual attention required"
    alert_once "mid-rebase-merge" "nasdaq autosync: $NASDAQ_DIR is mid rebase/merge. Fix manually; autosync paused."
    exit 1
fi

# Commit identity is brand-pinned to this deployment. A clone made before the repo was
# renamed can still carry the retired bot identity in .git/config, and an "only if unset"
# default would then preserve it forever - so any other name is corrected here.
if [ "$(git config user.name)" != "nasdaq-autosync" ]; then
    log "git user.name '$(git config user.name)' -> 'nasdaq-autosync'"
    git config user.name "nasdaq-autosync"
fi
git config user.email >/dev/null 2>&1 || git config user.email "autosync@$(hostname)"
export GIT_TERMINAL_PROMPT=0

DATA_MSG="no new data"
DEPLOY_MSG="none"
DEPLOY_FATAL=0
EXTRA=""

# --- phase 1: commit live data -------------------------------------------------
HEARTBEAT_PHASE="commit"
if commit_live_data; then
    DATA_MSG="$DATA_COMMIT_NOTE"
    [ "$DATA_MSG" = "no new data" ] && log "no data changes"
else
    DATA_MSG="COMMIT FAILED (will retry next run)"
fi
if [ "$GIT_STATE_FATAL" = "1" ]; then
    critical_alert "Git could not safely inspect or commit live data in $NASDAQ_DIR; autosync stopped before fetch/push"
    exit 1
fi

# --- phase 2: fetch & (maybe) deploy ------------------------------------------
HEARTBEAT_PHASE="deploy"
git fetch "$REMOTE" "$BRANCH" >/dev/null 2>&1 || log "WARN: git fetch failed (offline/credentials?)"

LOCAL_SHA=$(git rev-parse --verify HEAD 2>/dev/null)
if [ $? -ne 0 ] || [ -z "$LOCAL_SHA" ]; then
    critical_alert "git rev-parse could not resolve HEAD in $NASDAQ_DIR; autosync aborted"
    exit 1
fi
REMOTE_SHA=$(git rev-parse --verify "$REMOTE/$BRANCH" 2>/dev/null)
if [ $? -ne 0 ] || [ -z "$REMOTE_SHA" ]; then
    critical_alert "git rev-parse could not resolve $REMOTE/$BRANCH after fetch; autosync aborted"
    exit 1
fi

NEED_MERGE=0
if [ "$LOCAL_SHA" != "$REMOTE_SHA" ]; then
    git merge-base --is-ancestor "$REMOTE_SHA" HEAD 2>/dev/null
    MB_RC=$?
    if [ "$MB_RC" -eq 0 ]; then
        :
    elif [ "$MB_RC" -eq 1 ]; then
        NEED_MERGE=1
    else
        critical_alert "git merge-base failed (rc=$MB_RC); autosync aborted"
        exit 1
    fi
fi

if [ "$NEED_MERGE" = "1" ]; then
    # Staged counts as a hand-edit too: merging over staged code could sweep
    # it into the merge commit or conflict unpredictably. Data paths are not
    # checked here - they are snapshotted by commit_live_data below.
    CODE_STATUS=$(git status --porcelain --untracked-files=no -- "${CODE_PATHS[@]}" 2>/dev/null)
    STATUS_RC=$?
    if [ "$STATUS_RC" -ne 0 ]; then
        GIT_STATE_FATAL=1
        DEPLOY_MSG="GIT ERROR - git status failed (rc=$STATUS_RC); deploy skipped"
        EXTRA+=$'\n🚨 '"$DEPLOY_MSG"
        log "$DEPLOY_MSG"
    elif [ -n "$CODE_STATUS" ]; then
        DEPLOY_MSG="SKIPPED - local code edits detected on server (hand-edits are not supported); manual pull needed"
        log "$DEPLOY_MSG"
    elif [ "$GIT_STATE_FATAL" = "1" ]; then
        : # state was not trustworthy; fail closed and leave the tree untouched
    elif [ "$AUTO_DEPLOY" != "1" ]; then
        DEPLOY_MSG="SKIPPED (AUTO_DEPLOY=0) - origin/main is ahead; manual pull needed"
        log "$DEPLOY_MSG"
    else
        CODE_DIFF=$(git diff --name-only HEAD "$REMOTE_SHA" -- '*.py' 2>/dev/null)
        [ $? -eq 0 ] || { critical_alert "git diff failed while inspecting deploy paths"; exit 1; }
        CORE_DIFF=$(git diff --name-only HEAD "$REMOTE_SHA" -- engine.py trade_filter.py 2>/dev/null)
        [ $? -eq 0 ] || { critical_alert "git diff failed while inspecting core deploy paths"; exit 1; }
        DASH_DIFF=$(git diff --name-only HEAD "$REMOTE_SHA" -- dashboard.py pressure.py feed_health.py trade_filter.py tools/live_readiness.py tools/replay_lib.py 2>/dev/null)
        [ $? -eq 0 ] || { critical_alert "git diff failed while inspecting dashboard deploy paths"; exit 1; }
        CODE_CHANGED=$(printf '%s\n' "$CODE_DIFF" | grep -c . || true)
        CORE_CHANGED=$(printf '%s\n' "$CORE_DIFF" | grep -c . || true)
        DASH_CHANGED=$(printf '%s\n' "$DASH_DIFF" | grep -c . || true)
        SVC=$(detect_service 'engine\.py' "$ENGINE_SERVICE" || true)

        if [ -n "$SVC" ] && [ "$CORE_CHANGED" -gt 0 ]; then
            log "stopping engine ($SVC)"
            systemctl stop "$SVC" >/dev/null 2>&1 || log "WARN: stop $SVC failed"
        fi

        # Snapshot live data immediately before merging: writers may have
        # appended since phase 1, and merging on top of uncommitted data is
        # what made the old hard-reset rollback destructive. If the snapshot
        # or the out-of-worktree backup fails, do not merge at all.
        DEPLOY_OK=1
        if ! commit_live_data; then
            DEPLOY_OK=0
            DEPLOY_MSG="ABORTED - could not snapshot live data before merge (fail-safe; nothing merged)"
            log "$DEPLOY_MSG"
        fi
        PRE_MERGE=$(git rev-parse --verify HEAD 2>/dev/null)
        if [ $? -ne 0 ] || [ -z "$PRE_MERGE" ]; then
            critical_alert "git rev-parse failed before deploy snapshot; autosync aborted"
            exit 1
        fi
        if [ "$DEPLOY_OK" = "1" ]; then
            git update-ref refs/autosync/pre-deploy "$PRE_MERGE" >/dev/null 2>&1 || true
            if ! backup_data_files; then
                DEPLOY_OK=0
                DEPLOY_MSG="ABORTED - could not back up live data before merge (fail-safe; nothing merged)"
                log "$DEPLOY_MSG"
            fi
        fi

        MERGE_RC=0
        SMOKE="not run (no code change)"
        if [ "$DEPLOY_OK" = "1" ]; then
            if ! git merge "$REMOTE/$BRANCH" -m "autosync: merge upstream (live data kept local)" >/dev/null 2>&1; then
                UNMERGED=$(git diff --name-only --diff-filter=U 2>/dev/null)
                UNMERGED_RC=$?
                if [ "$UNMERGED_RC" -ne 0 ]; then
                    log "git diff failed while checking merge conflicts"
                    MERGE_RC=1
                elif [ ! -f .git/MERGE_HEAD ] && [ -z "$UNMERGED" ]; then
                    # e.g. a writer dirtied a data file after the snapshot and
                    # git refused to start the merge. Fail safe into rollback,
                    # which commits that data and leaves code untouched.
                    log "merge did not start (data changed under us?) - treating as failed deploy"
                    MERGE_RC=1
                else
                    log "merge conflict - resolving: data files=ours, everything else=theirs"
                    # Do NOT refresh the backup here: the worktree data files
                    # currently contain conflict markers and must never be
                    # captured as "truth" (a marker file has MORE lines than
                    # the clean resolution and would be "restored" over it).
                    # The pristine pre-merge backup remains authoritative.
                    while IFS= read -r f; do
                        [ -z "$f" ] && continue
                        if is_data_path "$f"; then
                            git checkout --ours -- "$f" >/dev/null 2>&1 || MERGE_RC=1
                        else
                            git checkout --theirs -- "$f" >/dev/null 2>&1 || MERGE_RC=1
                        fi
                        git add -- "$f" >/dev/null 2>&1 || MERGE_RC=1
                    done <<< "$UNMERGED"
                    UNMERGED_AFTER=$(git diff --name-only --diff-filter=U 2>/dev/null)
                    UNMERGED_AFTER_RC=$?
                    if [ "$UNMERGED_AFTER_RC" -ne 0 ] || [ -n "$UNMERGED_AFTER" ]; then
                        log "merge conflicts could not be fully resolved (or git diff failed)"
                        MERGE_RC=1
                    elif [ "$MERGE_RC" = "0" ]; then
                        # Recover any pre-merge-snapshot rows the conflicted
                        # merge dropped. (Rows appended to a conflicted file
                        # during the seconds the merge sat unresolved cannot
                        # be salvaged safely; see docs/AUTOSYNC.md residuals.)
                        restore_richer_data
                        for f in "${DATA_FILES[@]}"; do git add -- "$f" 2>/dev/null || MERGE_RC=1; done
                        git commit -q -m "autosync: merge upstream (live data kept local)" >/dev/null 2>&1 || MERGE_RC=1
                    fi
                fi
            fi

            if [ "$MERGE_RC" = "0" ]; then
                # Fold in anything written while the merge ran, and undo any
                # data rows the merge itself dropped.
                restore_richer_data
                commit_live_data || log "WARN: post-merge data commit failed (data still in worktree; next run retries)"
            fi

            if [ "$MERGE_RC" = "0" ] && [ "$SMOKE_GATE" = "1" ] && [ "$CODE_CHANGED" -gt 0 ]; then
                if python3 tools/smoke_test.py >/tmp/nasdaq_autosync_smoke.log 2>&1; then
                    SMOKE="ok"
                else
                    SMOKE="FAIL"
                fi
            fi

            if [ "$MERGE_RC" != "0" ] || [ "$SMOKE" = "FAIL" ]; then
                log "deploy failed (merge_rc=$MERGE_RC smoke=$SMOKE) - rolling back code to $PRE_MERGE (live data preserved)"
                if rollback_deploy "$PRE_MERGE"; then
                    DEPLOY_MSG="ROLLED BACK (smoke=$SMOKE) - code restored, live data preserved & verified; needs a manual look"
                    cleanup_backup
                else
                    DEPLOY_FATAL=1
                    DEPLOY_MSG="ROLLBACK FAILED - data preservation could not be verified; push disabled. Manual intervention required (data backup: $BACKUP_DIR, pre-merge ref: refs/autosync/pre-deploy)"
                fi
                EXTRA="$EXTRA
🚨 deploy rolled back - start a review session"
            else
                # Capture rows written during the smoke run before pushing.
                commit_live_data || log "WARN: post-deploy data commit failed (data still in worktree; next run retries)"
                DEPLOY_SHORT=$(git rev-parse --short --verify HEAD 2>/dev/null)
                if [ $? -ne 0 ] || [ -z "$DEPLOY_SHORT" ]; then
                    critical_alert "git rev-parse failed after deploy; autosync aborted"
                    exit 1
                fi
                DEPLOY_MSG="deployed $DEPLOY_SHORT (code files: $CODE_CHANGED, smoke: $SMOKE)"
                cleanup_backup
            fi
        fi

        if [ "$CORE_CHANGED" -gt 0 ]; then
            if [ -n "$SVC" ]; then
                log "starting engine ($SVC)"
                systemctl start "$SVC" >/dev/null 2>&1 || DEPLOY_MSG="$DEPLOY_MSG; FAILED to start $SVC"
                sleep 20
                if systemctl is-active --quiet "$SVC" 2>/dev/null; then
                    if [ -z "$(find status.json -mmin -20 2>/dev/null)" ]; then
                        DEPLOY_MSG="$DEPLOY_MSG; engine active but status.json looks STALE"
                    fi
                else
                    DEPLOY_MSG="$DEPLOY_MSG; ENGINE SERVICE NOT ACTIVE"
                fi
            else
                DEPLOY_MSG="$DEPLOY_MSG; engine service NOT FOUND - restart engine manually"
                EXTRA="$EXTRA
⚠️ engine service not detected - manual restart needed"
            fi
        fi
        if [ "$DASH_CHANGED" -gt 0 ]; then
            DSVC=$(detect_service 'dashboard\.py' "$DASHBOARD_SERVICE" || true)
            if [ -n "$DSVC" ]; then
                log "restarting dashboard ($DSVC)"
                systemctl restart "$DSVC" >/dev/null 2>&1 \
                    || DEPLOY_MSG="$DEPLOY_MSG; FAILED to restart $DSVC"
            else
                DEPLOY_MSG="$DEPLOY_MSG; dashboard service NOT FOUND - restart dashboard manually"
            fi
        fi
    fi
fi

# Make every deploy failure visible in the existing digest/alert channel. The
# detailed text remains in DEPLOY_MSG; the EXTRA line ensures ALERTS policy
# notices skips and failures even when no service/feed warning was emitted.
case "$DEPLOY_MSG" in
    none|deployed*)
        case "$DEPLOY_MSG" in
            *FAILED*|*STALE*|*NOT\ ACTIVE*|*NOT\ FOUND*|*GIT\ ERROR*|*ABORTED*)
                EXTRA+=$'\n🚨 deploy: '"$DEPLOY_MSG" ;;
        esac ;;
    *) EXTRA+=$'\n⚠️ deploy: '"$DEPLOY_MSG" ;;
esac

# --- phase 3: push -------------------------------------------------------------
HEARTBEAT_PHASE="push"
PUSH_MSG="ok"
if [ "$GIT_STATE_FATAL" = "1" ]; then
    PUSH_MSG="SKIPPED - Git state was not trustworthy; manual intervention required"
    EXTRA+=$'\n🚨 push skipped after Git state error'
elif [ "$DEPLOY_FATAL" = "1" ]; then
    PUSH_MSG="SKIPPED - rollback could not be verified (fail-safe); manual intervention required"
    EXTRA="$EXTRA
🚨 push skipped after unverified rollback - inspect $NASDAQ_DIR manually (data backup kept under .git/autosync-backup-*)"
elif ! git push "$REMOTE" "$BRANCH" >/dev/null 2>&1; then
    PUSH_MSG="FAILED (data is safe in a local commit; retries next run)"
    EXTRA="$EXTRA
⚠️ push to $REMOTE failed"
fi
log "push: $PUSH_MSG"

# --- phase 3b: feed sidecar self-heal (doctor-gated, never blind) --------------
# See the FEED SELF-HEAL note at the top of this file and
# docs/STALE-FEED-TRIAGE.md §5. One restart attempt at most per run, only for
# a state that restart is the documented fix for, only while the market is open.
FEED_MSG="not run"
if [ "$FEED_SELF_HEAL" != "1" ]; then
    FEED_MSG="disabled (FEED_SELF_HEAL=0)"
elif [ ! -f tools/feed_doctor.py ]; then
    FEED_MSG="not run (tools/feed_doctor.py missing)"
else
    FEED_LINE=$(feed_doctor_line)
    FEED_LEVEL=""
    FEED_ACTION="-"
    FEED_CHURN="-"
    if [ -n "$FEED_LINE" ]; then
        IFS=$'\t' read -r FEED_LEVEL FEED_ACTION FEED_CHURN <<< "$FEED_LINE"
        [ -z "${FEED_CHURN:-}" ] && FEED_CHURN="-"
    fi
    case "$FEED_LEVEL" in
        OK|QUIET)
            if [ "$FEED_CHURN" != "-" ]; then
                FEED_MSG="$FEED_LEVEL (reconnect churn advisory: $FEED_CHURN)"
            else
                FEED_MSG="$FEED_LEVEL"
            fi ;;
        STALE|BROKEN)
            if [ -z "$FEED_ACTION" ] || [ "$FEED_ACTION" = "-" ]; then
                # Unhealthy, but not in a state automation may restart (e.g.
                # stale heartbeat, foreign writer, wrong path): alert only.
                FEED_MSG="$FEED_LEVEL (no safe auto-restart)"
                EXTRA="$EXTRA
⚠️ feed sidecar: $FEED_LEVEL - no safe auto-restart; run python3 tools/feed_doctor.py"
            else
                FSVC=$(detect_service 'mt5_feed\.py' "$FEED_SERVICE" || true)
                DO_RESTART=1
                if [ "$FEED_ACTION" = "not-running" ]; then
                    # Distinguish a crashed unit from a deliberate stop:
                    # is-active prints the state even on a non-zero rc.
                    FSTATE=$(printf '%s' "$(systemctl is-active "$FSVC" 2>/dev/null || true)" \
                             | tr -d '[:space:]')
                    case "$FSTATE" in
                        inactive|deactivating)
                            DO_RESTART=0
                            FEED_MSG="unit is $FSTATE while feed files are missing - not restarting (deliberate stop?)"
                            EXTRA="$EXTRA
⚠️ feed sidecar unit is stopped while feed files are missing - start it manually if unintended" ;;
                    esac
                fi
                if [ "$DO_RESTART" = "1" ]; then
                    if [ -z "$FSVC" ]; then
                        FEED_MSG="$FEED_LEVEL ($FEED_ACTION) - feed service NOT FOUND - restart feed sidecar manually"
                        EXTRA="$EXTRA
⚠️ feed sidecar service not detected - restart mt5feed-nasdaq manually"
                    else
                        log "feed self-heal: $FEED_LEVEL/$FEED_ACTION - restarting sidecar ($FSVC)"
                        if systemctl restart "$FSVC" >/dev/null 2>&1; then
                            sleep "$FEED_HEAL_WAIT_S"
                            FEED2=$(feed_doctor_line)
                            FEED_LEVEL2=""
                            if [ -n "$FEED2" ]; then
                                IFS=$'\t' read -r FEED_LEVEL2 _ _ <<< "$FEED2"
                            fi
                            case "$FEED_LEVEL2" in
                                OK|QUIET)
                                    FEED_MSG="restarted $FSVC; feed is $FEED_LEVEL2" ;;
                                *)
                                    FEED_MSG="restarted $FSVC; feed still ${FEED_LEVEL2:-unknown}"
                                    EXTRA="$EXTRA
⚠️ feed sidecar restarted but feed still ${FEED_LEVEL2:-unknown} - journalctl -u $FSVC -n 50 (if -10004 persists: the Wine terminal is not running/logged in)" ;;
                            esac
                        else
                            FEED_MSG="FAILED to restart $FSVC"
                            EXTRA="$EXTRA
⚠️ feed sidecar restart FAILED ($FSVC) - systemctl status $FSVC"
                        fi
                    fi
                fi
            fi
            ;;
        *)
            FEED_MSG="feed_doctor failed to run"
            EXTRA="$EXTRA
⚠️ feed_doctor failed during feed self-heal - run python3 tools/feed_doctor.py by hand" ;;
    esac
    log "feed: $FEED_LEVEL/$FEED_ACTION -> $FEED_MSG"
fi

# --- phase 4: integrity + stats ------------------------------------------------
CHECK_MSG="not run"
python3 tools/check_data.py >/tmp/nasdaq_autosync_check.log 2>&1
CHECK_RC=$?
CHECK_MSG=$(grep -E '^== result' /tmp/nasdaq_autosync_check.log | tail -n1 | sed 's/^== result: //')
if [ -z "$CHECK_MSG" ]; then
    CHECK_MSG="check_data FAILED to run (rc=$CHECK_RC)"
fi
if [ "$CHECK_RC" != "0" ]; then
    EXTRA="$EXTRA
🚨 check_data: $CHECK_MSG - start a review session"
fi

# --- phase 4b: handoff freshness (ADVISORY) ------------------------------------
HANDOFF_MSG="not run"
if [ -f tools/handoff_check.py ] && [ -f docs/HANDOFF.md ]; then
    if python3 tools/handoff_check.py --quiet >/tmp/nasdaq_autosync_handoff.log 2>&1; then
        HANDOFF_MSG=$(tail -n1 /tmp/nasdaq_autosync_handoff.log)
    else
        HANDOFF_MSG=$(tail -n1 /tmp/nasdaq_autosync_handoff.log)
        [ -z "$HANDOFF_MSG" ] && HANDOFF_MSG="handoff_check failed to run"
    fi
    log "handoff: $HANDOFF_MSG"
    # A STALE handoff means the snapshot no longer describes the present: the
    # pipeline is frozen or the ledger moved far past the doc. That is a
    # readiness problem, not just a reminder to run --update.
    case "$HANDOFF_MSG" in
        "STALE"*)
            EXTRA="$EXTRA
⚠️ handoff STALE: $HANDOFF_MSG - the snapshot is not a current readiness picture" ;;
    esac
fi

# --- phase 4c: XM logger liveness (ADVISORY) -----------------------------------
XM_MSG="not run"
if [ -f tools/xm_quote_report.py ]; then
    XM_MSG=$(python3 tools/xm_quote_report.py --quiet 2>/dev/null | tail -n1)
    [ -z "$XM_MSG" ] && XM_MSG="xm_quote_report failed to run"
    log "xm: $XM_MSG"
    case "$XM_MSG" in
        "xm logger: STALE"*)
            EXTRA="$EXTRA
⚠️ XM quote logger stopped writing - check: systemctl status mt5quotes-nasdaq" ;;
    esac
fi

STATS=$(python3 - <<'PYEOF' 2>/dev/null
import json
try:
    s = json.load(open("status.json"))
    print("equity {eq} | {w}W/{l}L | daily SLs {d}/{m} | open trade: {a} | updated {u} UTC".format(
        eq=s.get("equity"), w=s.get("wins"), l=s.get("losses"),
        d=s.get("daily_losses"), m=s.get("max_daily_losses"),
        a="yes" if s.get("trade_active") else "no", u=s.get("last_update")))
except Exception as e:
    print("status.json unreadable: %s" % e)
PYEOF
)

HEARTBEAT_PHASE="done"
log "done. data=[$DATA_MSG] deploy=[$DEPLOY_MSG] feed=[$FEED_MSG] check=[$CHECK_MSG] handoff=[$HANDOFF_MSG] xm=[$XM_MSG]"

# --- phase 5: notification policy ----------------------------------------------
SEND=1
DAILY_TAG=""
if [ "$NOTIFY" = "off" ]; then
    SEND=0
elif [ "$NOTIFY" = "quiet" ]; then
    if [ "$DATA_MSG" = "no new data" ] && [ "$DEPLOY_MSG" = "none" ] \
       && [ "$PUSH_MSG" = "ok" ] && [ -z "$EXTRA" ]; then
        SEND=0
    fi
elif [ "$NOTIFY" = "alerts" ]; then
    SEND=0
    if [ "$DEPLOY_MSG" != "none" ] || [ -n "$EXTRA" ]; then
        CUR_STATE="${DEPLOY_MSG}|${EXTRA}"
        if [ "$CUR_STATE" != "$(cat "$ALERT_STAMP" 2>/dev/null)" ]; then
            SEND=1
            printf '%s' "$CUR_STATE" > "$ALERT_STAMP" 2>/dev/null || true
        fi
    else
        rm -f "$ALERT_STAMP" 2>/dev/null || true
    fi
    if [ "$SEND" = "0" ] && [ "$(cat "$DAILY_STAMP" 2>/dev/null)" != "$(date -u '+%Y-%m-%d')" ]; then
        SEND=1
        DAILY_TAG=" (daily)"
        date -u '+%Y-%m-%d' > "$DAILY_STAMP" 2>/dev/null || true
    fi
fi
log "notify: policy=$NOTIFY send=$SEND"

if [ "$SEND" = "1" ]; then
    tg "🤖 nasdaq autosync${DAILY_TAG} — $(date -u '+%m-%d %H:%M') UTC
📦 data: $DATA_MSG (push: $PUSH_MSG)
🚀 deploy: $DEPLOY_MSG
📡 feed: $FEED_MSG
🩺 integrity: $CHECK_MSG
📝 handoff: $HANDOFF_MSG
📈 $XM_MSG
💰 $STATS$EXTRA"
fi

if [ "$GIT_STATE_FATAL" = "1" ]; then
    critical_alert "autosync stopped because a Git state/commit operation failed; inspect $NASDAQ_DIR and the autosync log"
    exit 1
fi
if [ "$DEPLOY_FATAL" = "1" ]; then
    log "FATAL: deploy rollback could not be verified - manual intervention required (see deploy message above)"
    exit 1
fi
exit 0
