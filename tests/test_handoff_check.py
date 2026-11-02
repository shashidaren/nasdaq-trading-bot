"""Regression tests for tools/handoff_check.py's engine-lag check.

Motivation: every documented key in the HANDOFF snapshot is derived from the
engine's own artifacts (`forward_test_log.csv`, `trades.csv`, `status.json`).
When `nasdaq-engine` stops writing, those keys keep matching the snapshot and
`handoff_check` reported HANDOFF FRESH while the forward test had produced no
new bar for days - even though the read-only XM logger kept filling `xm_data/`.

`newest_xm_bar()` + `engine_lag()` close that blind spot. These tests pin:

  * tail-reading of `xm_data/candles_m15.csv` (bar_close_utc) and
    `xm_data/quotes-*.csv` (ts_utc), and the newer of the two wins;
  * open-market bar counting, so a weekend/maintenance break does not inflate
    the lag and a weekend-frozen log is not reported as a problem;
  * the engine problem is only raised while the market is open;
  * a checkout without `xm_data/` degrades to "unknown" instead of crashing.

Run with ``python3 -m unittest discover -s tests -v``.
"""
import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "handoff_check", ROOT / "tools" / "handoff_check.py")
handoff_check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(handoff_check)

HC = handoff_check

CANDLE_HEADER = "bar_close_utc,open,high,low,close,tick_volume,spread_pts,time_srv,offset_s"
QUOTE_HEADER = ("ts_utc,bid,ask,spread,spread_min,spread_max,n_ok,n_bad,"
                "tick_age_s,srv_offset_s,flags")

# Monday 2026-11-02 12:30 UTC - US cash session open.
MON_NOW = datetime(2026, 11, 2, 12, 30)
FRI_CLOSE = datetime(2026, 10, 30, 20, 45)   # engine's last 2026-10-30 bar
SAT_NOON = datetime(2026, 10, 31, 12, 0)


def candle_line(dt, close=28600.0):
    return f"{dt:%Y-%m-%d %H:%M:%S},{close},{close + 10},{close - 10},{close},2000,290,0,7200"


def quote_line(dt, bid=28599.0):
    return (f"{dt:%Y-%m-%d %H:%M:%S},{bid},{bid + 2.9},2.90,2.90,2.90,"
            "6,0,1.0,7200,")


class LagRepo:
    """Minimal repo: engine artifacts frozen, XM logger still writing."""

    def __init__(self, candles_until=MON_NOW, quotes_until=MON_NOW,
                 watermark=FRI_CLOSE):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "xm_data").mkdir()
        if candles_until is not None:
            self.write_candles(candles_until)
        if quotes_until is not None:
            self.write_quotes(quotes_until)
        self.facts = {
            "log_last_bar_utc": f"{watermark:%Y-%m-%d %H:%M}",
            "_as_of_dt": watermark,
            "as_of_utc": f"{watermark:%Y-%m-%d %H:%M}",
        }

    def write_candles(self, until, start=None):
        start = start or (until - HC.timedelta(minutes=900 * 40))
        lines = [CANDLE_HEADER]
        t = start
        while t <= until:
            lines.append(candle_line(t))
            t += HC.timedelta(minutes=15)
        (self.root / "xm_data" / "candles_m15.csv").write_text("\n".join(lines) + "\n")

    def write_quotes(self, until, per_day=200):
        day = f"{until:%Y-%m-%d}"
        lines = [QUOTE_HEADER]
        t = until - HC.timedelta(seconds=30 * (per_day - 1))
        while t <= until:
            lines.append(quote_line(t))
            t += HC.timedelta(seconds=30)
        (self.root / "xm_data" / f"quotes-{day}.csv").write_text("\n".join(lines) + "\n")

    def lag(self, now=MON_NOW):
        return HC.engine_lag(str(self.root), self.facts, now=now)

    def cleanup(self):
        self.tmp.cleanup()


class NewestXmBarTests(unittest.TestCase):
    def test_reads_bar_close_utc_from_candles(self):
        repo = LagRepo(quotes_until=None)
        try:
            dt, src = HC.newest_xm_bar(str(repo.root))
            self.assertEqual(dt, MON_NOW)
            self.assertEqual(src, "candles_m15.csv")
        finally:
            repo.cleanup()

    def test_reads_ts_utc_from_quotes(self):
        repo = LagRepo(candles_until=None)
        try:
            dt, src = HC.newest_xm_bar(str(repo.root))
            self.assertEqual(dt, MON_NOW)
            self.assertEqual(src, f"quotes-{MON_NOW:%Y-%m-%d}.csv")
        finally:
            repo.cleanup()

    def test_newest_of_the_two_wins(self):
        repo = LagRepo(candles_until=MON_NOW)
        try:
            later = MON_NOW + HC.timedelta(minutes=15)
            repo.write_quotes(later, per_day=5)
            dt, src = HC.newest_xm_bar(str(repo.root))
            self.assertEqual(dt, later)
            self.assertTrue(src.startswith("quotes-"))
        finally:
            repo.cleanup()

    def test_missing_xm_data_is_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(HC.newest_xm_bar(tmp), (None, None))


class EngineWatermarkTests(unittest.TestCase):
    def test_latest_of_last_bar_and_status(self):
        facts = {"log_last_bar_utc": "2026-10-30 20:30",
                 "_as_of_dt": datetime(2026, 10, 30, 20, 45, 3)}
        dt, src = HC.engine_watermark(facts)
        self.assertEqual(dt, datetime(2026, 10, 30, 20, 45, 3))
        self.assertIn("status.json", src)

    def test_log_bar_wins_when_newer(self):
        facts = {"log_last_bar_utc": "2026-10-30 21:00",
                 "_as_of_dt": datetime(2026, 10, 30, 20, 45, 3)}
        dt, src = HC.engine_watermark(facts)
        self.assertEqual(dt, datetime(2026, 10, 30, 21, 0))
        self.assertIn("forward_test_log.csv", src)

    def test_no_facts_is_none(self):
        self.assertEqual(HC.engine_watermark({}), (None, None))


class EngineLagTests(unittest.TestCase):
    def test_frozen_engine_behind_live_feed_is_lagging(self):
        repo = LagRepo()
        try:
            lag = repo.lag()
            self.assertIsNotNone(lag)
            self.assertTrue(lag["lagging"])
            self.assertTrue(lag["market_open"])
            self.assertGreaterEqual(lag["bars"], HC.ENGINE_LAG_BARS)
            self.assertIsNotNone(HC.engine_problem(lag))
        finally:
            repo.cleanup()

    def test_weekend_is_not_counted_as_open_market_bars(self):
        """Fri 20:45 -> Mon 12:30 is 64 h wall clock but ~53 trading bars."""
        repo = LagRepo()
        try:
            lag = repo.lag()
            wall_bars = lag["lag_s"] / HC.CANDLE_SECONDS
            self.assertGreater(wall_bars, 250)
            self.assertLess(lag["bars"], 60)
        finally:
            repo.cleanup()

    def test_quiet_market_is_informational_only(self):
        """A log frozen at Friday's close is expected on Saturday."""
        repo = LagRepo(candles_until=SAT_NOON, quotes_until=SAT_NOON)
        try:
            lag = repo.lag(now=SAT_NOON)
            self.assertFalse(lag["market_open"])
            self.assertTrue(lag["lagging"])
            self.assertIsNone(HC.engine_problem(lag))
        finally:
            repo.cleanup()

    def test_small_lag_is_not_lagging(self):
        repo = LagRepo(watermark=MON_NOW - HC.timedelta(minutes=15 * 4))
        try:
            lag = repo.lag()
            self.assertFalse(lag["lagging"])
            self.assertIsNone(HC.engine_problem(lag))
        finally:
            repo.cleanup()

    def test_threshold_is_inclusive(self):
        repo = LagRepo(watermark=MON_NOW - HC.timedelta(
            minutes=15 * HC.ENGINE_LAG_BARS))
        try:
            lag = repo.lag()
            self.assertEqual(lag["bars"], HC.ENGINE_LAG_BARS)
            self.assertTrue(lag["lagging"])
        finally:
            repo.cleanup()

    def test_missing_xm_data_is_unknown_not_a_problem(self):
        with tempfile.TemporaryDirectory() as tmp:
            facts = {"log_last_bar_utc": "2026-10-30 20:45",
                     "_as_of_dt": datetime(2026, 10, 30, 20, 45)}
            self.assertIsNone(HC.engine_lag(tmp, facts, now=MON_NOW))
            self.assertIsNone(HC.engine_problem(None))

    def test_engine_ahead_of_feed_is_not_lagging(self):
        repo = LagRepo(candles_until=MON_NOW, quotes_until=MON_NOW,
                       watermark=MON_NOW + HC.timedelta(minutes=15))
        try:
            lag = repo.lag()
            self.assertEqual(lag["bars"], 0)
            self.assertFalse(lag["lagging"])
        finally:
            repo.cleanup()


if __name__ == "__main__":
    unittest.main()
