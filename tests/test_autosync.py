"""Deterministic integration tests for tools/autosync.sh data preservation.

Each test builds an isolated trio of git repositories in a temp dir:

  origin.git (bare)  <- what GitHub would hold
  upstream/          <- a clone used to land "merged PR" commits
  work/              <- a clone acting as /opt/nasdaq on the VPS

The real tools/autosync.sh is copied into the fixture, but tools/smoke_test.py
and tools/check_data.py are stubs. The smoke stub simulates the engine and the
XM quote logger writing candle/trade/quote/status rows DURING the smoke run -
i.e. after autosync's data commits but before any rollback - which is exactly
the window in which the old `git reset --hard` rollback erased live rows
(leading hypothesis for the missing 2026-10-30 04:45/05:00 M15 candles).

A fake `systemctl` on PATH keeps the run hermetic on any host (including the
VPS, where this suite runs inside tools/smoke_test.py Scenario S).

Run with ``python3 -m unittest discover -s tests -v``.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUTOSYNC = ROOT / "tools" / "autosync.sh"

STUB_SMOKE = '''\
import json, os, sys
if os.environ.get("FAKE_SMOKE_WRITE") == "1":
    # Engine + XM logger writing while the smoke test runs (post-merge,
    # pre-rollback): one new row in every live-data file.
    with open("forward_test_log.csv", "a") as fh:
        fh.write("2026-10-30 04:45:00,20000,20010,19990,20005\\n")
    with open("trades.csv", "a") as fh:
        fh.write("99,BUY,2026-10-30 04:45:00\\n")
    with open("skipped_trades.csv", "a") as fh:
        fh.write("2026-10-30 04:45:00,SKIP_DURING_SMOKE\\n")
    with open("xm_data/candles_m15.csv", "a") as fh:
        fh.write("2026-10-30 04:45:00,20000,20010,19990,20005,100,290,1793361600,7200\\n")
    with open("xm_data/quotes-2026-10-30.csv", "a") as fh:
        fh.write("2026-10-30 04:45:05,20000,20002.9,2.9,2.9,2.9,6,0,1.0,7200,\\n")
    with open("status.json", "w") as fh:
        json.dump({"last_update": "2026-10-30 04:45:05", "equity": 222.7}, fh)
sys.exit(1 if "BAD" in open("engine.py").read() else 0)
'''

STUB_CHECK = "print('== result: 0 fail, 0 warn ==')\n"

SEED_FILES = {
    "engine.py": "ENGINE_V1\n",
    "trade_filter.py": "TF_V1\n",
    "dashboard.py": "DASH_V1\n",
    "pressure.py": "P_V1\n",
    # feed_health.py + tools/feed_doctor.py are copied from the real repo in
    # setUp: autosync's feed self-heal (phase 3b) gates its sidecar restart on
    # the doctor's machine-readable `actions` block, so the fixture must run
    # the REAL doctor against the fixture's feed files.
    ".gitignore": ("__pycache__/\n*.py[cod]\n"
                   "mt5_last_candle.json\nmt5_feed_health.json\n"
                   "mt5_warmup_candles.json\nmt5_feed_offset.json\njournal.csv\n"),
    "tools/smoke_test.py": STUB_SMOKE,
    "tools/check_data.py": STUB_CHECK,
    "trades.csv": "Trade_Num,Trade_Type,Entry_Time\n1,BUY,2026-10-30 04:00:00\n",
    "forward_test_log.csv": ("Timestamp,Open,High,Low,Close\n"
                             "2026-10-30 04:15:00,1,1,1,1\n"
                             "2026-10-30 04:30:00,1,1,1,1\n"),
    "skipped_trades.csv": "Timestamp,Reason\n",
    "status.json": '{"last_update": "2026-10-30 04:30:01", "equity": 200}\n',
    "xm_data/candles_m15.csv": ("bar_close_utc,open,high,low,close,tick_volume,spread_pts,time_srv,offset_s\n"
                                "2026-10-30 04:30:00,1,1,1,1,1,290,1,7200\n"),
    "xm_data/quotes-2026-10-30.csv": ("ts_utc,bid,ask,spread,spread_min,spread_max,n_ok,n_bad,tick_age_s,srv_offset_s,flags\n"
                                      "2026-10-30 04:30:00,1,1,0,0,0,6,0,1.0,7200,\n"),
    "xm_data/symbol_spec.json": '{"symbol": "US100Cash"}\n',
}

LOCAL_ROW = "2026-10-30 04:40:00,1,1,1,1\n"          # written before the run
SMOKE_CANDLE = "2026-10-30 04:45:00,20000,20010,19990,20005"
SMOKE_TRADE = "99,BUY,2026-10-30 04:45:00"


@unittest.skipUnless(all(shutil.which(t) for t in ("git", "bash", "flock")),
                     "needs git, bash and flock")
class AutosyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        top = Path(self.tmp.name)
        self.home = top / "home"
        self.home.mkdir()
        self.env = {"PATH": os.environ.get("PATH", ""), "HOME": str(self.home),
                    "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"}
        self.origin = top / "origin.git"
        self.git_at(top, "init", "--bare", str(self.origin))
        self.git_at(self.origin, "symbolic-ref", "HEAD", "refs/heads/main",
                    git_dir=True)
        seed = top / "seed"
        self.git_at(top, "init", str(seed))
        self.git(seed, "symbolic-ref", "HEAD", "refs/heads/main")
        for rel, text in SEED_FILES.items():
            self.write(seed, rel, text)
        # Real feed doctor + its dependency, so phase 3b exercises the actual
        # doctor-gated restart decision (stub feed_health would break --json).
        shutil.copy(ROOT / "feed_health.py", seed / "feed_health.py")
        shutil.copy(ROOT / "tools" / "feed_doctor.py", seed / "tools" / "feed_doctor.py")
        shutil.copy(AUTOSYNC, seed / "tools" / "autosync.sh")
        os.chmod(seed / "tools" / "autosync.sh", 0o755)
        self.git(seed, "add", "-A")
        self.git(seed, "commit", "-q", "-m", "seed")
        self.git(seed, "push", "-q", str(self.origin), "main")
        self.work = top / "work"
        self.upstream = top / "upstream"
        self.git_at(top, "clone", "-q", str(self.origin), str(self.work))
        self.git_at(top, "clone", "-q", str(self.origin), str(self.upstream))
        # Healthy feed baseline in the "VPS" clone (gitignored runtime files,
        # so clones never carry them): every test starts from an OK verdict;
        # the self-heal tests overwrite these two.
        now = int(time.time())
        self.write(self.work, "mt5_last_candle.json", json.dumps({
            "symbol": "US100Cash", "timeframe": "M15", "account": "1",
            "ts": now - 1000, "server_offset_s": 7200,
            "open": 20000.0, "high": 20010.0, "low": 19990.0,
            "close": 20005.0, "tick_volume": 100}))
        self.write(self.work, "mt5_feed_health.json", json.dumps({
            "status": "OK", "symbol": "US100Cash", "timeframe": "M15",
            "account": "1", "server_offset_s": 7200, "tick_age_s": 1.0,
            "last_error": None}))
        bindir = top / "bin"
        bindir.mkdir()
        # Hermetic systemctl: logs every call, succeeds only for `restart`
        # (so the feed self-heal's post-restart re-check runs), reports state
        # via $SYSTEMCTL_STATE for `is-active`, fails everything else.
        stub = bindir / "systemctl"
        stub.write_text(
            "#!/bin/sh\n"
            '[ -n "${SYSTEMCTL_LOG:-}" ] && echo "$@" >> "$SYSTEMCTL_LOG"\n'
            "case \"$1\" in\n"
            "  restart) exit 0 ;;\n"
            "  is-active) if [ -n \"${SYSTEMCTL_STATE:-}\" ] && [ -f \"$SYSTEMCTL_STATE\" ]; then\n"
            "               cat \"$SYSTEMCTL_STATE\"; exit 0\n"
            "             fi\n"
            "             exit 3 ;;\n"
            "esac\n"
            "exit 1\n", encoding="utf-8")
        os.chmod(stub, 0o755)
        self.bindir = bindir

    # -- helpers ---------------------------------------------------------------
    def git_at(self, cwd, *args, git_dir=False):
        cmd = ["git"] + (["--git-dir", str(cwd)] if git_dir else []) + list(args)
        proc = subprocess.run(cmd, cwd=None if git_dir else cwd, env=self.env,
                              capture_output=True, text=True, timeout=60)
        if proc.returncode != 0:
            raise AssertionError(f"git {args} failed: {proc.stderr[-400:]}")
        return proc.stdout

    def git(self, repo, *args):
        cmd = ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
               "-c", "commit.gpgsign=false", *args]
        proc = subprocess.run(cmd, cwd=repo, env=self.env,
                              capture_output=True, text=True, timeout=60)
        if proc.returncode != 0:
            raise AssertionError(f"git {args} failed in {repo}: {proc.stderr[-400:]}")
        return proc.stdout

    def write(self, repo, rel, text, append=False):
        path = Path(repo) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a" if append else "w", encoding="utf-8") as fh:
            fh.write(text)

    def upstream_change(self, rel, text, msg="upstream", append=False):
        self.write(self.upstream, rel, text, append=append)
        self.git(self.upstream, "add", "-A")
        self.git(self.upstream, "commit", "-q", "-m", msg)
        self.git(self.upstream, "push", "-q", "origin", "main")

    def run_autosync(self, **extra_env):
        env = dict(self.env)
        env.update({
            "PATH": f"{self.bindir}:{env['PATH']}",
            "NASDAQ_DIR": str(self.work),
            "LOCK_FILE": str(Path(self.tmp.name) / "autosync.lock"),
            "DAILY_STAMP": str(Path(self.tmp.name) / "daily.stamp"),
            "ALERT_STAMP": str(Path(self.tmp.name) / "alert.stamp"),
            "NOTIFY": "off", "TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": "",
            # Feed self-heal defaults for the fixture: unit pre-detected
            # (no systemd here) and zero wait so the suite stays fast.
            "FEED_SERVICE": "mt5feed-nasdaq",
            "FEED_HEAL_WAIT_S": "0",
            "SYSTEMCTL_LOG": str(Path(self.tmp.name) / "systemctl.log"),
            "SYSTEMCTL_STATE": str(Path(self.tmp.name) / "systemctl.state"),
        })
        env.update(extra_env)
        return subprocess.run(["bash", "tools/autosync.sh"], cwd=self.work, env=env,
                              capture_output=True, text=True, timeout=180)

    def systemctl_calls(self):
        path = Path(self.tmp.name) / "systemctl.log"
        if not path.exists():
            return []
        return path.read_text(encoding="utf-8").splitlines()

    def write_dead_handle_feed(self):
        """Feed frozen + fresh heartbeat in NO_RATES/-10004: the 2026-11-02
        incident state where restart is the documented fix (doctor action
        'dead-handle')."""
        self.write(self.work, "mt5_last_candle.json", json.dumps({
            "symbol": "US100Cash", "timeframe": "M15", "account": "1",
            "ts": 1793361600, "server_offset_s": 7200,
            "open": 28403.06, "high": 28413.84, "low": 28378.65,
            "close": 28405.03, "tick_volume": 2773}))
        self.write(self.work, "mt5_feed_health.json", json.dumps({
            "status": "NO_RATES", "symbol": "US100Cash", "timeframe": "M15",
            "account": "1", "server_offset_s": 7200, "tick_age_s": None,
            "reconnects": 291,
            "last_error": "copy_rates_from_pos failed: (-10004, 'No IPC connection')"}))

    def head(self, rel):
        return self.git(self.work, "show", f"HEAD:{rel}")

    def origin_head(self, rel):
        return self.git_at(self.origin, "show", f"main:{rel}", git_dir=True)

    def porcelain(self):
        return self.git(self.work, "status", "--porcelain")

    def assert_repo_healthy(self):
        self.assertEqual(self.porcelain(), "", "worktree/index must be clean")
        self.assertFalse((self.work / ".git" / "MERGE_HEAD").exists())
        self.assertEqual(list((self.work / ".git").glob("autosync-backup-*")), [],
                         "backup dir must be cleaned up once preservation is verified")

    def assert_smoke_rows_preserved(self):
        self.assertIn(SMOKE_CANDLE, self.head("forward_test_log.csv"))
        self.assertIn(SMOKE_TRADE, self.head("trades.csv"))
        self.assertIn("SKIP_DURING_SMOKE", self.head("skipped_trades.csv"))
        self.assertIn(SMOKE_CANDLE, self.head("xm_data/candles_m15.csv"))
        self.assertIn("2026-10-30 04:45:05", self.head("xm_data/quotes-2026-10-30.csv"))
        self.assertIn('"equity": 222.7', self.head("status.json"))

    # -- tests -------------------------------------------------------------------
    def test_failed_smoke_preserves_concurrent_writes_then_recovers(self):
        self.upstream_change("engine.py", "ENGINE_BAD_V2\n", "bad code")
        self.upstream_change("tools/bad_new.py", "BAD_NEW\n", "bad new file")
        self.git(self.upstream, "rm", "-q", "pressure.py")
        self.git(self.upstream, "commit", "-q", "-m", "bad deletion")
        self.git(self.upstream, "push", "-q", "origin", "main")
        self.write(self.work, "forward_test_log.csv", LOCAL_ROW, append=True)
        proc = self.run_autosync(FAKE_SMOKE_WRITE="1")
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("ROLLED BACK", out)
        # Code is back on the old version, committed and in the worktree:
        # modified file reverted, upstream-added file removed, upstream-
        # deleted file restored.
        self.assertEqual((self.work / "engine.py").read_text(), "ENGINE_V1\n")
        self.assertEqual(self.head("engine.py"), "ENGINE_V1\n")
        self.assertFalse((self.work / "tools" / "bad_new.py").exists())
        self.assertNotIn("bad_new.py", self.git(self.work, "ls-files"))
        self.assertEqual((self.work / "pressure.py").read_text(), "P_V1\n")
        # Rows written DURING the smoke run survived the rollback, in HEAD.
        self.assert_smoke_rows_preserved()
        self.assertIn(LOCAL_ROW.strip(), self.head("forward_test_log.csv"))
        self.assert_repo_healthy()
        # Pre-deploy ref was recorded for forensics/manual recovery.
        self.git(self.work, "rev-parse", "refs/autosync/pre-deploy")
        # Local data commits diverge from origin (which carries the bad code
        # commit), so this run's push is rejected - nothing reaches origin yet.
        self.assertEqual(self.origin_head("engine.py"), "ENGINE_BAD_V2\n")
        self.assertIn("push: FAILED", out)
        # A later, fixed upstream deploys cleanly and finally pushes the data.
        self.upstream_change("engine.py", "ENGINE_V3_GOOD\n", "fix code")
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("deployed", out)
        self.assertEqual((self.work / "engine.py").read_text(), "ENGINE_V3_GOOD\n")
        self.assertEqual(self.origin_head("engine.py"), "ENGINE_V3_GOOD\n")
        self.assertIn(SMOKE_TRADE, self.origin_head("trades.csv"))
        self.assertIn(SMOKE_CANDLE, self.origin_head("forward_test_log.csv"))
        self.assert_repo_healthy()

    def test_successful_deploy_preserves_concurrent_writes_and_pushes(self):
        self.upstream_change("engine.py", "ENGINE_V2\n", "good code")
        self.upstream_change("tools/new_tool.py", "NEW\n", "new tool")
        self.write(self.work, "forward_test_log.csv", LOCAL_ROW, append=True)
        proc = self.run_autosync(FAKE_SMOKE_WRITE="1")
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("deployed", out)
        self.assertEqual((self.work / "engine.py").read_text(), "ENGINE_V2\n")
        self.assertTrue((self.work / "tools" / "new_tool.py").exists())
        self.assert_smoke_rows_preserved()
        self.assertIn(SMOKE_TRADE, self.origin_head("trades.csv"))
        self.assertIn(SMOKE_CANDLE, self.origin_head("forward_test_log.csv"))
        self.assertEqual(self.origin_head("engine.py"), "ENGINE_V2\n")
        self.assert_repo_healthy()

    def test_merge_conflict_data_ours_code_theirs(self):
        self.upstream_change("trades.csv",
                             "Trade_Num,Trade_Type,Entry_Time\n1,SELL,UPSTREAM_EDIT\n",
                             "upstream touches data")
        self.upstream_change("engine.py", "ENGINE_V2\n", "good code")
        # Local edit of the SAME data line -> guaranteed merge conflict.
        self.write(self.work, "trades.csv",
                   "Trade_Num,Trade_Type,Entry_Time\n1,BUY,LOCAL_EDIT\n")
        proc = self.run_autosync(FAKE_SMOKE_WRITE="1")
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("merge conflict - resolving", out)
        self.assertIn("deployed", out)
        trades = self.head("trades.csv")
        self.assertIn("LOCAL_EDIT", trades)          # server data wins
        self.assertNotIn("UPSTREAM_EDIT", trades)
        self.assertIn(SMOKE_TRADE, trades)           # smoke-time row kept too
        self.assertNotIn("<<<<<<<", trades)          # no conflict markers
        self.assertEqual(self.head("engine.py"), "ENGINE_V2\n")  # code = theirs
        self.assert_repo_healthy()

    def test_merge_commit_rejection_aborts_and_preserves(self):
        # A commit-msg hook rejects the merge commit AND simulates a candle
        # landing mid-merge. `git merge --abort` would revert that candle;
        # autosync must restore it from the out-of-worktree backup.
        hook = self.work / ".git" / "hooks" / "commit-msg"
        hook.write_text(
            "#!/bin/sh\n"
            "if grep -q 'merge upstream' \"$1\"; then\n"
            "  grep -q HOOK_CANDLE forward_test_log.csv || "
            "echo '2026-10-30 04:45:00,HOOK_CANDLE,1,1,1' >> forward_test_log.csv\n"
            "  exit 1\n"
            "fi\n"
            "exit 0\n", encoding="utf-8")
        os.chmod(hook, 0o755)
        self.upstream_change("engine.py", "ENGINE_V2\n", "good code")
        # A local data row forces a phase-1 commit, so the deploy is a true
        # merge (not a fast-forward) and the commit-msg hook actually fires.
        self.write(self.work, "forward_test_log.csv", LOCAL_ROW, append=True)
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("ROLLED BACK", out)
        self.assertEqual(self.head("engine.py"), "ENGINE_V1\n")
        self.assertIn("HOOK_CANDLE", self.head("forward_test_log.csv"))
        self.assertIn(LOCAL_ROW.strip(), self.head("forward_test_log.csv"))
        self.assert_repo_healthy()
        # Without the hook, the next run deploys and the candle reaches origin.
        hook.unlink()
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("deployed", out)
        self.assertEqual(self.head("engine.py"), "ENGINE_V2\n")
        self.assertIn("HOOK_CANDLE", self.origin_head("forward_test_log.csv"))
        self.assert_repo_healthy()

    def test_dirty_code_skips_deploy_but_still_syncs_data(self):
        self.upstream_change("engine.py", "ENGINE_V2\n", "good code")
        self.write(self.work, "engine.py", "ENGINE_HAND_EDIT\n")
        self.write(self.work, "forward_test_log.csv", LOCAL_ROW, append=True)
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("SKIPPED - local code edits", out)
        # Hand-edit untouched, data still committed locally, nothing deployed.
        self.assertEqual((self.work / "engine.py").read_text(), "ENGINE_HAND_EDIT\n")
        self.assertIn(LOCAL_ROW.strip(), self.head("forward_test_log.csv"))
        self.assertEqual(self.porcelain(), " M engine.py\n")
        self.assertEqual(self.origin_head("engine.py"), "ENGINE_V2\n")

    def test_plain_data_sync_without_upstream_change(self):
        self.write(self.work, "forward_test_log.csv", LOCAL_ROW, append=True)
        self.write(self.work, "trades.csv", "2,SELL,2026-10-30 04:40:00\n", append=True)
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("data collection 1", out)
        self.assertIn(LOCAL_ROW.strip(), self.origin_head("forward_test_log.csv"))
        self.assertIn("2,SELL,2026-10-30 04:40:00", self.origin_head("trades.csv"))
        self.assert_repo_healthy()

    def test_no_hard_reset_anywhere(self):
        lines = AUTOSYNC.read_text(encoding="utf-8").splitlines()
        offenders = [l for l in lines
                     if "reset --hard" in l and not l.lstrip().startswith("#")]
        self.assertEqual(offenders, [])
        text = "\n".join(lines)
        self.assertIn("git reset --soft", text)
        self.assertIn("refs/autosync/pre-deploy", text)
        self.assertIn("commit_live_data", text)
        self.assertIn("verify_data_preserved", text)

    # -- feed sidecar self-heal (phase 3b) -------------------------------------
    def test_healthy_feed_never_touches_the_sidecar(self):
        self.write(self.work, "forward_test_log.csv", LOCAL_ROW, append=True)
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("feed: OK/- -> OK", out)
        self.assertFalse(any(c.startswith("restart ") for c in self.systemctl_calls()),
                         f"systemctl must not restart anything on an OK feed: {self.systemctl_calls()}")
        self.assertNotIn("feed sidecar", out)
        self.assert_repo_healthy()

    def test_dead_handle_restarts_sidecar_once_and_verifies(self):
        # 2026-11-02 incident state: fresh heartbeat, dead MT5 handle, frozen
        # feed file, market assumed open -> doctor says 'dead-handle', autosync
        # restarts exactly once and re-checks afterwards.
        self.write_dead_handle_feed()
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("feed self-heal: STALE/dead-handle - restarting sidecar (mt5feed-nasdaq)", out)
        # Post-restart re-check ran and still sees the broken state (the stub
        # cannot actually heal anything) - logged for the operator, plus an
        # EXTRA digest alert with journalctl guidance (digest-only by design).
        self.assertIn("feed: STALE/dead-handle -> restarted mt5feed-nasdaq; feed still STALE", out)
        restarts = [c for c in self.systemctl_calls() if c.startswith("restart ")]
        self.assertEqual(restarts, ["restart mt5feed-nasdaq"],
                         "exactly one doctor-gated restart per run")
        self.assert_repo_healthy()

    def test_feed_self_heal_can_be_disabled(self):
        self.write_dead_handle_feed()
        proc = self.run_autosync(FEED_SELF_HEAL="0")
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("disabled (FEED_SELF_HEAL=0)", out)
        self.assertFalse(any(c.startswith("restart ") for c in self.systemctl_calls()),
                         "FEED_SELF_HEAL=0 must never restart the sidecar")

    def test_not_running_unit_restarts_unless_deliberately_stopped(self):
        # Feed file + heartbeat both missing -> doctor action 'not-running'.
        # Default stub: is-active reports nothing (failed/unknown) -> restart.
        (self.work / "mt5_last_candle.json").unlink()
        (self.work / "mt5_feed_health.json").unlink()
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("feed self-heal: BROKEN/not-running - restarting sidecar (mt5feed-nasdaq)", out)
        self.assertTrue(any(c.startswith("restart ") for c in self.systemctl_calls()))
        self.assert_repo_healthy()
        # Deliberate stop: is-active says 'inactive' -> autosync must not fight it.
        Path(self.tmp.name, "systemctl.state").write_text("inactive\n", encoding="utf-8")
        before = len([c for c in self.systemctl_calls() if c.startswith("restart ")])
        proc = self.run_autosync()
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("not restarting (deliberate stop?)", out)
        after = len([c for c in self.systemctl_calls() if c.startswith("restart ")])
        self.assertEqual(before, after, "an `inactive` unit must not be auto-restarted")

    def test_feed_service_not_detected_alerts_instead_of_guessing(self):
        self.write_dead_handle_feed()
        proc = self.run_autosync(FEED_SERVICE="")
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1500:])
        self.assertIn("feed service NOT FOUND - restart feed sidecar manually", out)
        self.assertFalse(any(c.startswith("restart ") for c in self.systemctl_calls()),
                         "without a detected unit autosync must not guess a restart target")


if __name__ == "__main__":
    unittest.main()
