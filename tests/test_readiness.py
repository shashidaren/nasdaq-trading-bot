"""Read-only regression tests for the readiness report and dashboard.

Run with ``python3 -m unittest discover -s tests -v``. Dashboard tests require
Flask; the statistical and data-integrity tests use only the standard library.
"""
import csv
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools import live_readiness as LR  # noqa: E402
from trade_filter import is_market_quiet  # noqa: E402

try:
    import dashboard  # noqa: E402
except ModuleNotFoundError as exc:
    if exc.name != "flask":
        raise
    dashboard = None

TRADE_FIELDS = [
    "Trade_Num", "Trade_Type", "Entry_Time", "Exit_Time", "Entry_Price", "Stop_Loss",
    "Take_Profit", "Exit_Price", "Exit_Reason", "Profit", "Balance_After", "RSI_At_Entry",
    "ATR_At_Entry", "Wick_Ratio_At_Entry", "EMA50_At_Entry", "EMA200_At_Entry",
]
BAR_FIELDS = ["Timestamp", "Open", "High", "Low", "Close"]
FMT = "%Y-%m-%d %H:%M:%S"
NOW = datetime(2026, 10, 30, 14, 35, tzinfo=timezone.utc)


def bar(dt):
    return {"dt": dt, "o": 20000.0, "h": 20010.0, "l": 19990.0, "c": 20005.0}


def trade(dt, profit=2.0):
    return {"et": dt, "xt": dt + timedelta(minutes=5), "profit": profit}


def gate(evidence, key):
    return next(g for g in evidence["gates"] if g["key"] == key)


def write_csv(path, fields, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fixture(root, count=1, profit=2.0):
    """A contiguous open-market log and a synthetic ledger, never real orders."""
    root = Path(root)
    end = NOW.replace(tzinfo=None, minute=30)
    t = datetime(2026, 10, 26, 14, 0)
    slots = []
    while t <= end:
        midpoint = (t - timedelta(minutes=7)).replace(tzinfo=timezone.utc)
        if not is_market_quiet(midpoint):
            slots.append(t)
        t += timedelta(minutes=15)
    trade_rows = []
    for i, dt in enumerate(slots[:count]):
        trade_rows.append({
            "Trade_Num": i + 1, "Trade_Type": "BUY", "Entry_Time": dt.strftime(FMT),
            "Exit_Time": (dt + timedelta(minutes=5)).strftime(FMT),
            "Entry_Price": 20000, "Stop_Loss": 19980, "Take_Profit": 20040,
            "Exit_Price": 20020, "Exit_Reason": "TP" if profit > 0 else "SL",
            "Profit": profit, "Balance_After": 200 + (i + 1) * profit, "ATR_At_Entry": 10,
        })
    log_rows = [{"Timestamp": dt.strftime(FMT), "Open": 20000, "High": 20010,
                 "Low": 19990, "Close": 20005} for dt in slots]
    write_csv(root / "trades.csv", TRADE_FIELDS, trade_rows)
    write_csv(root / "forward_test_log.csv", BAR_FIELDS, log_rows)
    status = {"symbol": "US100Cash", "equity": 200 + count * profit,
              "total_trades": count, "wins": count if profit > 0 else 0,
              "losses": 0 if profit > 0 else count, "win_rate": 100 if profit > 0 else 0,
              "daily_losses": 0, "max_daily_losses": 3,
              "last_update": "2026-10-30 14:30:01", "funnel": {}}
    (root / "status.json").write_text(json.dumps(status), encoding="utf-8")
    feed = {"ts": int(NOW.timestamp()) - 1200, "symbol": "US100Cash", "timeframe": "M15",
            "open": 20000, "high": 20010, "low": 19990, "close": 20005, "tick_volume": 100}
    (root / "mt5_last_candle.json").write_text(json.dumps(feed), encoding="utf-8")
    health = {"status": "OK", "symbol": "US100Cash", "terminal_connected": True,
              "server_offset_s": 7200, "tick_age_s": 1}
    health_path = root / "mt5_feed_health.json"
    health_path.write_text(json.dumps(health), encoding="utf-8")
    os.utime(health_path, (NOW.timestamp(), NOW.timestamp()))
    return status, trade_rows, log_rows


class GateTests(unittest.TestCase):
    def test_sample_is_post_gate_and_small_samples_are_unknown(self):
        prior = datetime(2026, 10, 23, 10)
        current = datetime(2026, 10, 26, 14)
        trades = [trade(prior + timedelta(minutes=i)) for i in range(100)]
        trades += [trade(current + timedelta(minutes=i)) for i in range(9)]
        bars = [bar(current), bar(current + timedelta(minutes=15))]
        evidence = LR.evaluate_data_gates(trades, bars)
        self.assertEqual(gate(evidence, "sample")["value"], "9")
        self.assertFalse(gate(evidence, "sample")["ok"])
        for key in ("edge", "cost", "fragility", "recent"):
            self.assertIsNone(gate(evidence, key)["ok"], key)

    def test_exact_100_and_all_shared_gates_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture(tmp, count=100)
            trades, bars = LR.load_readiness_data(Path(tmp) / "trades.csv", Path(tmp) / "forward_test_log.csv")
            evidence = LR.evaluate_data_gates(trades, bars)
            self.assertTrue(all(g["ok"] is True for g in evidence["gates"]))
            self.assertEqual(gate(evidence, "sample")["value"], "100")
            self.assertEqual(gate(evidence, "edge")["value"], "100.0%")
            self.assertEqual(gate(evidence, "cost")["value"], "8.00x")

    def test_negative_edge_and_zero_cost_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture(tmp, count=100, profit=-2.0)
            trades, bars = LR.load_readiness_data(Path(tmp) / "trades.csv", Path(tmp) / "forward_test_log.csv")
            ev = LR.evaluate_data_gates(trades, bars, boot=100)
            self.assertTrue(gate(ev, "sample")["ok"])
            self.assertFalse(gate(ev, "edge")["ok"])
            self.assertFalse(gate(ev, "cost")["ok"])
            self.assertFalse(gate(ev, "recent")["ok"])
            self.assertFalse(gate(LR.evaluate_data_gates(trades, bars, spread=0, boot=100), "cost")["ok"])
            for invalid in (-0.1, math.nan, math.inf):
                with self.assertRaises(ValueError):
                    LR.evaluate_data_gates(trades, bars, spread=invalid, boot=100)

    def test_scheduled_closures_do_not_mask_open_market_outages(self):
        # Monday's 75-minute CME break and the DST-transition weekend are not
        # 90-minute / 50-hour feed outages. Missing open slots remain counted.
        monday = [bar(datetime(2026, 10, 26, 20, 45)), bar(datetime(2026, 10, 26, 22, 15)),
                  bar(datetime(2026, 10, 26, 22, 30))]
        weekend = [bar(datetime(2026, 10, 30, 20, 45)), bar(datetime(2026, 11, 1, 23, 15)),
                   bar(datetime(2026, 11, 1, 23, 30))]
        for bars in (monday, weekend):
            gaps, _, _, missing = LR.feed_report(bars)
            self.assertLessEqual(max((g[2] for g in gaps), default=0), 30)
            self.assertGreaterEqual(missing, 1)  # 21:00 close is actually absent
            self.assertTrue(gate(LR.evaluate_data_gates([], bars), "feed")["ok"])
        # Five missing slots before the daily break are a 90-minute outage,
        # even though the surrounding wall-clock interval includes the closure.
        outage = [bar(datetime(2026, 10, 26, 19, 45)), bar(datetime(2026, 10, 26, 22, 15))]
        self.assertEqual(LR.feed_report(outage)[0][0][2], 90)
        self.assertFalse(gate(LR.evaluate_data_gates([], outage), "feed")["ok"])
        open_gap = [bar(datetime(2026, 10, 30, 5, 45)), bar(datetime(2026, 10, 30, 7, 15))]
        self.assertFalse(gate(LR.evaluate_data_gates([], open_gap), "feed")["ok"])

    def test_missing_or_incomplete_log_is_not_a_feed_pass(self):
        dt = datetime(2026, 10, 26, 14)
        self.assertIsNone(gate(LR.evaluate_data_gates([trade(dt)], []), "feed")["ok"])
        late_bars = [bar(dt + timedelta(hours=1)), bar(dt + timedelta(hours=1, minutes=15))]
        result = gate(LR.evaluate_data_gates([trade(dt)], late_bars), "feed")
        self.assertIsNone(result["ok"])
        self.assertIn("coverage is incomplete", result["note"])

    def test_loader_rejects_skipped_corrupt_and_duplicate_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, trades, bars = fixture(tmp, count=2)
            path = Path(tmp) / "trades.csv"
            trades[1]["Trade_Num"] = trades[0]["Trade_Num"]
            write_csv(path, TRADE_FIELDS, trades)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                LR.load_readiness_data(path, Path(tmp) / "forward_test_log.csv")
            trades[1]["Trade_Num"] = 2
            trades[1]["Profit"] = "nan"
            write_csv(path, TRADE_FIELDS, trades)
            with self.assertRaisesRegex(ValueError, "invalid values"):
                LR.load_readiness_data(path, Path(tmp) / "forward_test_log.csv")
            trades[1]["Profit"] = 2
            trades[1]["Trade_Type"] = "INVALID"
            write_csv(path, TRADE_FIELDS, trades)
            with self.assertRaisesRegex(ValueError, "unparseable"):
                LR.load_readiness_data(path, Path(tmp) / "forward_test_log.csv")
            trades[1]["Trade_Type"] = "BUY"
            write_csv(path, TRADE_FIELDS, trades)
            bars[1]["Timestamp"] = bars[0]["Timestamp"]
            write_csv(Path(tmp) / "forward_test_log.csv", BAR_FIELDS, bars)
            with self.assertRaisesRegex(ValueError, "duplicate, or unordered"):
                LR.load_readiness_data(path, Path(tmp) / "forward_test_log.csv")
            write_csv(path, ["Trade_Num"], [{"Trade_Num": 1}])
            with self.assertRaisesRegex(ValueError, "header"):
                LR.load_readiness_data(path, Path(tmp) / "forward_test_log.csv")

    def test_cli_uses_same_gates_and_fails_closed_on_no_price_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture(tmp, count=100)
            trades, bars = LR.load_readiness_data(Path(tmp) / "trades.csv", Path(tmp) / "forward_test_log.csv")
            expected = LR.evaluate_data_gates(trades, bars, boot=100)
            command = [sys.executable, str(ROOT / "tools/live_readiness.py"), "--root", tmp,
                       "--spread", "0.25", "--boot", "100", "--horizon", "5"]
            proc = subprocess.run(command, text=True, capture_output=True, timeout=30)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            for item in expected["gates"]:
                result = "PASS" if item["ok"] is True else "FAIL" if item["ok"] is False else "UNKNOWN"
                self.assertIn(f"[{result}] {item['label']}", proc.stdout)
                self.assertIn(item["value"], proc.stdout)
            write_csv(Path(tmp) / "forward_test_log.csv", BAR_FIELDS, [])
            proc = subprocess.run(command, text=True, capture_output=True, timeout=30)
            self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
            self.assertIn("[UNKNOWN] feed:", proc.stdout)
            self.assertIn("net/trade n/a", proc.stdout)
            self.assertNotIn("Traceback", proc.stderr + proc.stdout)


@unittest.skipIf(dashboard is None, "Flask not installed")
class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.status, self.trade_rows, self.bar_rows = fixture(self.root, count=1)
        self.patches = []
        for name, file in (("TRADES_FILE_PATH", "trades.csv"),
                           ("LOG_FILE_PATH", "forward_test_log.csv"),
                           ("STATUS_FILE_PATH", "status.json"),
                           ("JOURNAL_FILE_PATH", "journal.csv"),
                           ("MT5_FEED_FILE", "mt5_last_candle.json"),
                           ("MT5_FEED_HEALTH_FILE", "mt5_feed_health.json"),
                           ("AUTOSYNC_HEARTBEAT_FILE", "autosync_heartbeat.json")):
            patcher = mock.patch.object(dashboard, name, str(self.root / file))
            patcher.start()
            self.addCleanup(patcher.stop)
        dashboard._cached_evidence.cache_clear()
        self.addCleanup(dashboard._cached_evidence.cache_clear)
        self.clock = mock.patch.object(dashboard.time, "time", return_value=NOW.timestamp())
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.client = dashboard.app.test_client()

    def test_deep_links_render_active_panel_without_javascript(self):
        for url, selected in (("/", "dashboard"), ("/journal", "journal"), ("/readiness", "readiness")):
            response = self.client.get(url)
            text = response.get_data(as_text=True)
            self.assertEqual(response.status_code, 200)
            self.assertIn(f'id="panel-{selected}" class="tab-panel"', text)
            self.assertIn('href="/readiness"', text)
            self.assertIn(f'id="tab-{selected}"', text)
        text = self.client.get("/readiness").get_data(as_text=True)
        self.assertIn("NOT READY — DATA GATES", text)
        self.assertIn("UNKNOWN", text)
        self.assertIn("1/6 data gates pass", text)
        self.assertIn("100 post-gate trades", text)
        self.assertIn("Non-Data Gates (sign off by hand)", text)
        self.assertEqual(self.client.get("/healthz").get_json()["scope"], "dashboard process only")

    def test_missing_feed_cannot_be_overridden_by_old_status_ok(self):
        self.status["feed"] = {"ok": True, "reason": "OK", "checked_utc": "2026-10-26 14:00:00"}
        (self.root / "status.json").write_text(json.dumps(self.status), encoding="utf-8")
        (self.root / "mt5_last_candle.json").unlink()
        response = self.client.get("/readiness")
        text = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("NOT READY — FEED", text)
        self.assertIn("FILE_MISSING", text)
        self.assertIn("BROKEN", text)

    def test_bad_or_missing_heartbeat_degrades_fresh_feed(self):
        self.assertTrue(dashboard.feed_banner(self.status)["ok"])
        (self.root / "mt5_feed_health.json").unlink()
        feed = dashboard.feed_banner(self.status)
        self.assertFalse(feed["ok"])
        self.assertEqual(feed["level"], "DEGRADED")
        (self.root / "mt5_feed_health.json").write_text(json.dumps({"status": "TICK_FROZEN"}))
        os.utime(self.root / "mt5_feed_health.json", (NOW.timestamp(), NOW.timestamp()))
        feed = dashboard.feed_banner(self.status)
        self.assertEqual(feed["level"], "DEGRADED")
        self.assertIn("TICK_FROZEN", " ".join(feed["lines"]))

    def test_market_closure_is_never_a_healthy_feed(self):
        sat = datetime(2026, 10, 31, 14, tzinfo=timezone.utc).timestamp()
        feed = dashboard.feed_banner(self.status, now_epoch=sat)
        self.assertFalse(feed["ok"])
        self.assertEqual(feed["level"], "MARKET QUIET")
        self.assertIn("not proof", " ".join(feed["lines"]))

    def test_malformed_heartbeat_and_invalid_feed_never_show_healthy(self):
        health_path = self.root / "mt5_feed_health.json"
        health_path.write_text(json.dumps(["status"]))
        text = self.client.get("/readiness").get_data(as_text=True)
        self.assertIn("NOT READY — FEED", text)
        self.assertIn("heartbeat data is invalid", text)
        health_path.write_text(json.dumps({"status": "OK", "symbol": "US100Cash",
                                           "terminal_connected": True, "tick_age_s": 1}))
        os.utime(health_path, (NOW.timestamp(), NOW.timestamp()))
        feed_path = self.root / "mt5_last_candle.json"
        payload = json.loads(feed_path.read_text())
        payload["close"] = "NaN"
        feed_path.write_text(json.dumps(payload))
        feed = dashboard.feed_banner(self.status)
        self.assertFalse(feed["ok"])
        self.assertEqual(feed["reason"], "SCHEMA_INVALID")
        payload["close"] = 20005
        payload.pop("timeframe")
        feed_path.write_text(json.dumps(payload))
        feed = dashboard.feed_banner(self.status)
        self.assertFalse(feed["ok"])
        self.assertIn("explicitly tagged", feed["detail"])

    def test_forming_bar_and_future_heartbeat_fail_closed(self):
        feed_path = self.root / "mt5_last_candle.json"
        payload = json.loads(feed_path.read_text())
        payload["ts"] = int(NOW.timestamp()) - 100  # open, not yet closed
        feed_path.write_text(json.dumps(payload))
        feed = dashboard.feed_banner(self.status)
        self.assertFalse(feed["ok"])
        self.assertIn("has not closed", feed["detail"])
        payload["ts"] = int(NOW.timestamp()) - 1200
        feed_path.write_text(json.dumps(payload))
        health_path = self.root / "mt5_feed_health.json"
        os.utime(health_path, (NOW.timestamp() + 3600, NOW.timestamp() + 3600))
        feed = dashboard.feed_banner(self.status)
        self.assertEqual(feed["level"], "DEGRADED")
        self.assertIn("in the future", " ".join(feed["lines"]))

    def test_all_data_gates_pass_is_not_live_authorization(self):
        self.status, _, _ = fixture(self.root, count=100)
        text = self.client.get("/readiness").get_data(as_text=True)
        self.assertIn("6/6 data gates pass", text)
        self.assertIn("DATA GATES PASS — MANUAL REVIEW REQUIRED", text)
        self.assertIn("never authorizes real-money trading", text)
        self.assertIn("not automatically checked or signed off", text)
        self.assertNotIn("NOT READY", text)

    def test_cli_and_dashboard_agree_on_mixed_trade_gate_values(self):
        self.status, rows, _ = fixture(self.root, count=100)
        for row in rows[::3]:
            row["Profit"], row["Exit_Reason"] = -4, "SL"
        write_csv(self.root / "trades.csv", TRADE_FIELDS, rows)
        self.status.update(wins=66, losses=34)
        (self.root / "status.json").write_text(json.dumps(self.status), encoding="utf-8")
        trades, bars = LR.load_readiness_data(self.root / "trades.csv", self.root / "forward_test_log.csv")
        expected = LR.evaluate_data_gates(trades, bars)
        cli = subprocess.run([sys.executable, str(ROOT / "tools/live_readiness.py"),
                              "--root", str(self.root), "--horizon", "5"],
                             capture_output=True, text=True, timeout=30)
        text = unescape(self.client.get("/readiness").get_data(as_text=True))
        self.assertEqual(cli.returncode, 1)
        for item in expected["gates"]:
            result = "PASS" if item["ok"] is True else "FAIL" if item["ok"] is False else "UNKNOWN"
            self.assertIn(f"[{result}] {item['label']}", cli.stdout)
            row = re.search(rf'<tr data-gate="{item["key"]}".*?</tr>', text, re.DOTALL)
            self.assertIsNotNone(row, item["key"])
            self.assertIn(result, row.group())
            self.assertIn(item["value"], row.group())

    def test_status_drift_and_daily_halt_block_otherwise_passing_data(self):
        self.status, _, _ = fixture(self.root, count=100)
        self.status["total_trades"] = 99
        self.status["daily_losses"] = 3
        (self.root / "status.json").write_text(json.dumps(self.status), encoding="utf-8")
        text = self.client.get("/readiness").get_data(as_text=True)
        self.assertIn("6/6 data gates pass", text)
        self.assertIn("NOT READY — SNAPSHOT", text)
        self.assertIn("trade counters do not match", text)
        self.assertIn("circuit breaker is halted", text)

    def test_unreadable_evidence_and_status_fail_closed_without_a_500(self):
        (self.root / "trades.csv").unlink()
        (self.root / "status.json").write_text("{bad json", encoding="utf-8")
        response = self.client.get("/readiness")
        text = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("NOT READY — EVIDENCE UNAVAILABLE", text)
        self.assertIn("Readiness could not be computed", text)
        self.assertIn("Gate results unavailable", text)
        self.assertIn("Unavailable", text)

    def test_torn_log_row_fails_closed_and_names_the_repair_command(self):
        """2026-11-18: NUL padding made the tab useless without saying why or how to fix it."""
        path = self.root / "forward_test_log.csv"
        lines = path.read_bytes().split(b"\n")
        lines[2] = b"\x00" * 140 + lines[2]
        path.write_bytes(b"\n".join(lines))
        text = self.client.get("/readiness").get_data(as_text=True)
        self.assertIn("NOT READY — EVIDENCE UNAVAILABLE", text)
        self.assertIn("Readiness could not be computed", text)
        self.assertIn("140 NUL byte(s) starting at line 3", text)
        self.assertIn("tools/repair_log.py --apply", text)

    def test_cache_invalidates_when_evidence_changes_but_feed_is_always_live(self):
        with mock.patch.object(LR, "evaluate_data_gates", wraps=LR.evaluate_data_gates) as evaluate:
            self.client.get("/readiness")
            self.client.get("/readiness")
            self.assertEqual(evaluate.call_count, 1)
            (self.root / "mt5_last_candle.json").unlink()
            text = self.client.get("/readiness").get_data(as_text=True)
            self.assertIn("NOT READY — FEED", text)
            self.assertEqual(evaluate.call_count, 1)
            self.trade_rows.append(dict(self.trade_rows[0], Trade_Num=2,
                                        Entry_Time="2026-10-26 14:15:00", Exit_Time="2026-10-26 14:20:00"))
            write_csv(self.root / "trades.csv", TRADE_FIELDS, self.trade_rows)
            self.client.get("/readiness")
            self.assertEqual(evaluate.call_count, 2)

    def test_autosync_last_run_age_renders_read_only_and_flags_stale_over_45m(self):
        """2026-11-17->18: commented-out cron + NOTIFY=off hid a stopped autosync."""
        hb_path = self.root / "autosync_heartbeat.json"

        def card_html():
            resp = self.client.get("/readiness")
            self.assertEqual(resp.status_code, 200)
            body = unescape(resp.get_data(as_text=True))
            match = re.search(r'<div data-card="autosync".*?</div>', body, re.DOTALL)
            self.assertIsNotNone(match, "autosync card missing from /readiness")
            return match.group()

        # 1. Missing stamp -> NO HEARTBEAT, read-only (never created).
        card = card_html()
        self.assertIn("Autosync last run age", card)
        self.assertIn("NO HEARTBEAT", card)
        self.assertIn("stale > 45 min", card)
        self.assertFalse(hb_path.exists())

        def write_stamp(age_s, status="ok", exit_code=0, **extra):
            ts = NOW.timestamp() - age_s
            dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            payload = {
                "ts_utc": dt_str, "ts_epoch": ts, "status": status, "exit_code": exit_code,
                "phase": "done", "push": "ok", "deploy": "none", "notify": "off",
            }
            payload.update(extra)
            raw = json.dumps(payload, indent=2) + "\n"
            hb_path.write_text(raw, encoding="utf-8")
            return raw

        # 2. Fresh stamp (12 min old) and exact 45-min boundary -> OK, surfaces notify: off.
        raw_fresh = write_stamp(12 * 60)
        card = card_html()
        self.assertIn("OK · 12 min ago", card)
        self.assertIn("push: ok", card)
        self.assertIn("notify: off", card)
        self.assertEqual(hb_path.read_text(encoding="utf-8"), raw_fresh)

        write_stamp(45 * 60)
        self.assertIn("OK · 45 min ago", card_html())

        # 3. Over 45 min (45m 01s, 46 min, and 14 h) -> STALE, read-only.
        write_stamp(45 * 60 + 1)
        card = card_html()
        self.assertIn("STALE · 45m 01s ago", card)
        self.assertIn("Older than 45 min", card)

        raw_stale = write_stamp(46 * 60)
        card = card_html()
        self.assertIn("STALE · 46 min ago", card)
        self.assertIn("crontab", card)
        self.assertEqual(hb_path.read_text(encoding="utf-8"), raw_stale)

        write_stamp(14 * 3600)
        self.assertIn("STALE · 14.0 h ago", card_html())

        # 4. Fresh warn / error / future / corrupt stamps never masquerade as OK.
        write_stamp(5 * 60, status="warn", push="FAILED")
        card = card_html()
        self.assertIn("WARN · 5 min ago", card)
        self.assertIn("push: FAILED", card)

        write_stamp(3 * 60, status="error", exit_code=1, phase="preflight")
        card = card_html()
        self.assertIn("ERROR · 3 min ago", card)
        self.assertIn("exit 1 (preflight)", card)

        write_stamp(-600)
        card = card_html()
        self.assertIn("STALE · clock skew", card)
        self.assertIn("in the future", card)

        hb_path.write_text("{truncated", encoding="utf-8")
        card = card_html()
        self.assertIn("UNREADABLE", card)
        self.assertEqual(hb_path.read_text(encoding="utf-8"), "{truncated")


if __name__ == "__main__":
    unittest.main()
