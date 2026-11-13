"""Regression tests for tools/feed_doctor.py's XM witness.

The quote logger (tools/mt5_quotes.py) keys candles_m15.csv rows by
``bar_close_utc``. The witness used to treat that value as an OPEN time and
add another 15 minutes, so the reported bar age was one full bar too fresh:
a bar that closed 21 minutes ago was reported as "closed 6 min ago" and
could wrongly prove MT5 alive (witness threshold: 20 min) while the feed
was actually stale.

Run with ``python3 -m unittest discover -s tests -v``.
"""
import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "feed_doctor", ROOT / "tools" / "feed_doctor.py")
feed_doctor = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(feed_doctor)

NOW = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)
NOW_EPOCH = NOW.timestamp()
CANDLE_HEADER = "bar_close_utc,open,high,low,close,tick_volume,spread_pts,time_srv,offset_s"
QUOTES_HEADER = "ts_utc,bid,ask,spread,spread_min,spread_max,n_ok,n_bad,tick_age_s,srv_offset_s,flags"


def candle_row(bar_close_utc):
    return f"{bar_close_utc},20000,20010,19990,20005,100,290,1793361600,7200"


class WitnessCandleAgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.xm = Path(self.tmp.name) / "xm_data"
        self.xm.mkdir()

    def write_candles(self, text):
        (self.xm / "candles_m15.csv").write_text(text, encoding="utf-8")

    def test_bar_close_utc_is_a_close_time_not_an_open(self):
        # Bar closed at 11:39 -> exactly 21 minutes before NOW (12:00).
        self.write_candles(CANDLE_HEADER + "\n" + candle_row("2026-10-30 11:39:00") + "\n")
        w = feed_doctor.witness_quotes(str(self.xm), NOW_EPOCH)
        # The buggy code reported 21*60 - 900 = 360 s here.
        self.assertEqual(w["last_bar_close_age_s"], 21 * 60)
        self.assertEqual(w["last_bar_close_utc"], "2026-10-30 11:39:00")
        self.assertEqual(w["last_bar_open_utc"], "2026-10-30 11:24:00")

    def test_fresh_bar_age_is_exact(self):
        self.write_candles(CANDLE_HEADER + "\n" + candle_row("2026-10-30 11:55:00") + "\n")
        w = feed_doctor.witness_quotes(str(self.xm), NOW_EPOCH)
        self.assertEqual(w["last_bar_close_age_s"], 5 * 60)

    def test_header_only_file_reports_no_bar(self):
        self.write_candles(CANDLE_HEADER + "\n")
        w = feed_doctor.witness_quotes(str(self.xm), NOW_EPOCH)
        self.assertIsNone(w["last_bar_close_age_s"])
        self.assertIsNone(w["last_bar_close_utc"])
        self.assertIsNone(w["last_bar_open_utc"])

    def test_unknown_schema_fails_closed(self):
        self.write_candles("when,open\n2026-10-30 11:55:00,20000\n")
        w = feed_doctor.witness_quotes(str(self.xm), NOW_EPOCH)
        self.assertIsNone(w["last_bar_close_age_s"])
        self.assertIsNone(w["last_bar_close_utc"])

    def test_stale_witness_does_not_prove_mt5_alive(self):
        # Feed file missing; quote file present but an hour old (mtime), and the
        # newest witness bar closed 21 min ago: beyond the 20-min witness
        # threshold, so the verdict must NOT claim "MT5 itself is fine". With
        # the old +900 s bug this exact fixture produced that claim.
        root = Path(self.tmp.name)
        self.write_candles(CANDLE_HEADER + "\n" + candle_row("2026-10-30 11:39:00") + "\n")
        qpath = self.xm / "quotes-2026-10-30.csv"
        qpath.write_text(QUOTES_HEADER + "\n"
                         "2026-10-30 11:00:00,20000,20002.9,2.9,2.9,2.9,6,0,1.0,7200,\n",
                         encoding="utf-8")
        old = NOW_EPOCH - 3600
        os.utime(qpath, (old, old))
        report = feed_doctor.build_report(str(root), None, now_epoch=NOW_EPOCH,
                                          assume_open=True)
        self.assertEqual(report["witness"]["last_bar_close_age_s"], 21 * 60)
        steps = " ".join(report["verdict"][2])
        self.assertNotIn("MT5 itself is fine", steps)

    def test_fresh_witness_still_proves_mt5_alive(self):
        root = Path(self.tmp.name)
        self.write_candles(CANDLE_HEADER + "\n" + candle_row("2026-10-30 11:55:00") + "\n")
        qpath = self.xm / "quotes-2026-10-30.csv"
        qpath.write_text(QUOTES_HEADER + "\n"
                         "2026-10-30 11:59:00,20000,20002.9,2.9,2.9,2.9,6,0,1.0,7200,\n",
                         encoding="utf-8")
        os.utime(qpath, (NOW_EPOCH - 60, NOW_EPOCH - 60))
        report = feed_doctor.build_report(str(root), None, now_epoch=NOW_EPOCH,
                                          assume_open=True)
        steps = " ".join(report["verdict"][2])
        self.assertIn("MT5 itself is fine", steps)


class IpcFailureGuidanceTests(unittest.TestCase):
    """Regression: a sidecar whose MT5 handle died (error -10004 "No IPC connection").

    Observed on the live VPS 2026-11-02: mt5feed-nasdaq stayed alive and kept
    heartbeating with status NO_RATES / last_error -10004 while the feed file
    stayed frozen at 2026-10-30 20:30, and mt5quotes-nasdaq (separate process)
    was still recording. feed_doctor printed "next steps: - none - feed is
    healthy" for that STALE verdict because NO_RATES was not handled and the
    witness rescue only fired for structural reasons.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.xm = self.root / "xm_data"
        self.xm.mkdir()
        # frozen feed file: last closed bar opened 20:30, closed 20:45 (Fri)
        frozen = {"symbol": "US100Cash", "timeframe": "M15", "account": "420568040",
                  "ts": int(datetime(2026, 10, 30, 20, 30, tzinfo=timezone.utc).timestamp()),
                  "server_offset_s": 7200, "open": 28403.06, "high": 28413.84,
                  "low": 28378.65, "close": 28405.03, "tick_volume": 2773}
        (self.root / "mt5_last_candle.json").write_text(json.dumps(frozen), encoding="utf-8")
        (self.root / "mt5_feed_health.json").write_text(json.dumps({
            "status": "NO_RATES", "symbol": "US100Cash", "timeframe": "M15",
            "account": "420568040", "heartbeat_epoch": NOW_EPOCH,
            "server_offset_s": 7200, "tick_age_s": None, "reconnects": 291,
            "last_error": "copy_rates_from_pos failed: (-10004, 'No IPC connection')",
        }), encoding="utf-8")
        # live witness: the quote logger is healthy
        (self.xm / "quotes-2026-10-30.csv").write_text(
            QUOTES_HEADER + "\n"
            "2026-10-30 11:59:00,20000,20002.9,2.9,2.9,2.9,6,0,1.0,7200,\n", encoding="utf-8")
        (self.xm / "candles_m15.csv").write_text(
            CANDLE_HEADER + "\n" + candle_row("2026-10-30 11:55:00") + "\n", encoding="utf-8")
        for name in ("quotes-2026-10-30.csv", "candles_m15.csv"):
            os.utime(self.xm / name, (NOW_EPOCH - 60, NOW_EPOCH - 60))

    def tearDown(self):
        self.tmp.cleanup()

    def report(self):
        return feed_doctor.build_report(str(self.root), None, now_epoch=NOW_EPOCH,
                                        assume_open=True)

    def test_stale_verdict_names_the_ipc_failure_and_the_restart(self):
        level, _headline, steps = self.report()["verdict"]
        text = " ".join(steps)
        self.assertEqual(level, "STALE")
        self.assertIn("No IPC connection", text)
        self.assertIn("restart mt5feed-nasdaq", text)

    def test_no_rates_status_itself_is_reported(self):
        text = " ".join(self.report()["verdict"][2])
        self.assertIn("NO_RATES", text)

    def test_never_claims_healthy_on_a_stale_verdict(self):
        level, _headline, steps = self.report()["verdict"]
        self.assertNotEqual(level, "OK")
        self.assertTrue(steps)
        self.assertNotIn("none - feed is healthy", " ".join(steps))

    def test_cli_never_prints_none_feed_is_healthy_for_a_stale_feed(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = feed_doctor.main(["--data-dir", str(self.root), "--assume-open"])
        self.assertEqual(rc, 1)
        self.assertNotIn("none - feed is healthy", buf.getvalue())

    def test_ok_feed_still_says_healthy(self):
        # the CLI uses the wall clock, so this fixture must be fresh in real time
        live_now = datetime.now(timezone.utc)
        fresh = {"symbol": "US100Cash", "timeframe": "M15", "account": "420568040",
                 "ts": int((live_now - timedelta(minutes=30)).timestamp()),
                 "server_offset_s": 7200, "open": 20000.0, "high": 20010.0,
                 "low": 19990.0, "close": 20005.0, "tick_volume": 100}
        (self.root / "mt5_last_candle.json").write_text(json.dumps(fresh), encoding="utf-8")
        (self.root / "mt5_feed_health.json").unlink()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = feed_doctor.main(["--data-dir", str(self.root), "--assume-open"])
        self.assertEqual(rc, 0)
        self.assertIn("none - feed is healthy", buf.getvalue())


class RestartActionTests(unittest.TestCase):
    """actions.restart_sidecar: the machine-readable gate tools/autosync.sh
    consumes so it never restarts the feed sidecar blindly (phase 3b)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.write_dead_handle()

    def write_dead_handle(self):
        frozen = {"symbol": "US100Cash", "timeframe": "M15", "account": "1",
                  "ts": int(datetime(2026, 10, 30, 20, 30, tzinfo=timezone.utc).timestamp()),
                  "server_offset_s": 7200, "open": 28403.06, "high": 28413.84,
                  "low": 28378.65, "close": 28405.03, "tick_volume": 2773}
        (self.root / "mt5_last_candle.json").write_text(json.dumps(frozen), encoding="utf-8")
        (self.root / "mt5_feed_health.json").write_text(json.dumps({
            "status": "NO_RATES", "symbol": "US100Cash", "timeframe": "M15",
            "account": "1", "server_offset_s": 7200, "tick_age_s": None,
            "reconnects": 291,
            "last_error": "copy_rates_from_pos failed: (-10004, 'No IPC connection')",
        }), encoding="utf-8")

    def report(self, now_epoch=NOW_EPOCH, assume_open=True):
        return feed_doctor.build_report(str(self.root), None, now_epoch=now_epoch,
                                        assume_open=assume_open)

    def test_fresh_heartbeat_with_dead_handle_recommends_restart(self):
        # The 2026-11-02 incident exactly: process alive (fresh heartbeat),
        # MT5 handle dead, feed file frozen -> only a restart fixes this.
        self.assertEqual(self.report()["actions"]["restart_sidecar"], "dead-handle")

    def test_no_rates_status_alone_is_enough_even_without_ipc_text(self):
        health = json.loads((self.root / "mt5_feed_health.json").read_text(encoding="utf-8"))
        health["last_error"] = None
        (self.root / "mt5_feed_health.json").write_text(json.dumps(health), encoding="utf-8")
        self.assertEqual(self.report()["actions"]["restart_sidecar"], "dead-handle")

    def test_stale_heartbeat_never_recommends_restart(self):
        # A heartbeat older than 120 s may be a deliberately stopped process:
        # automation must not fight an operator stop, so no restart is offered.
        old = NOW_EPOCH - 3600
        os.utime(self.root / "mt5_feed_health.json", (old, old))
        self.assertIsNone(self.report()["actions"]["restart_sidecar"])

    def test_file_missing_with_no_heartbeat_is_not_running(self):
        (self.root / "mt5_last_candle.json").unlink()
        (self.root / "mt5_feed_health.json").unlink()
        rep = self.report()
        self.assertEqual(rep["feed"]["reason"], "FILE_MISSING")
        self.assertEqual(rep["actions"]["restart_sidecar"], "not-running")

    def test_file_missing_with_live_heartbeat_is_a_path_problem_not_a_restart(self):
        # The heartbeat proves the process runs: the feed path is misconfigured
        # (different MT5_FEED_FILE). Restarting would loop without fixing it.
        (self.root / "mt5_last_candle.json").unlink()
        health = json.loads((self.root / "mt5_feed_health.json").read_text(encoding="utf-8"))
        health["status"] = "OK"
        health["last_error"] = None
        (self.root / "mt5_feed_health.json").write_text(json.dumps(health), encoding="utf-8")
        rep = self.report()
        self.assertEqual(rep["feed"]["reason"], "FILE_MISSING")
        self.assertIsNone(rep["actions"]["restart_sidecar"])

    def test_healthy_sidecar_with_old_bar_does_not_recommend_restart(self):
        # Feed stale but the sidecar reports status OK + fresh heartbeat:
        # not the dead-handle state; a restart is not the documented fix.
        health = json.loads((self.root / "mt5_feed_health.json").read_text(encoding="utf-8"))
        health["status"] = "OK"
        health["last_error"] = None
        (self.root / "mt5_feed_health.json").write_text(json.dumps(health), encoding="utf-8")
        rep = self.report()
        self.assertIn(rep["verdict"][0], ("STALE", "BROKEN"))
        self.assertIsNone(rep["actions"]["restart_sidecar"])

    def test_actions_are_reported_even_when_market_quiet(self):
        # Saturday: verdict is QUIET (autosync applies the open-market gate on
        # its side), but the dead-handle action itself is still visible.
        saturday = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc).timestamp()
        rep = self.report(now_epoch=saturday, assume_open=False)
        self.assertEqual(rep["verdict"][0], "QUIET")
        self.assertEqual(rep["actions"]["restart_sidecar"], "dead-handle")

    def test_ok_feed_reports_no_action(self):
        live_now = datetime.now(timezone.utc)
        fresh = {"symbol": "US100Cash", "timeframe": "M15", "account": "1",
                 "ts": int((live_now - timedelta(seconds=1000)).timestamp()),
                 "server_offset_s": 7200, "open": 20000.0, "high": 20010.0,
                 "low": 19990.0, "close": 20005.0, "tick_volume": 100}
        (self.root / "mt5_last_candle.json").write_text(json.dumps(fresh), encoding="utf-8")
        health = json.loads((self.root / "mt5_feed_health.json").read_text(encoding="utf-8"))
        health["status"] = "OK"
        health["last_error"] = None
        (self.root / "mt5_feed_health.json").write_text(json.dumps(health), encoding="utf-8")
        rep = self.report(now_epoch=live_now.timestamp(), assume_open=True)
        self.assertEqual(rep["verdict"][0], "OK")
        self.assertIsNone(rep["actions"]["restart_sidecar"])


if __name__ == "__main__":
    unittest.main()
