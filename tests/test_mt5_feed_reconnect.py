"""Regression tests for tools/mt5_feed.py's reconnect policy.

Live incident 2026-11-02: `mt5feed-nasdaq` stayed alive and heartbeating with
status ``NO_RATES`` and ``last_error: copy_rates_from_pos failed: (-10004,
'No IPC connection')`` while its feed file sat frozen at 2026-10-30 20:30.
The bounded ``shutdown() + initialize()`` recovery only ran for
``TICK_FROZEN``, so a sidecar that lost its MT5 IPC handle could never heal
itself - it just kept rewriting the heartbeat. ``should_reconnect()`` now
covers every "handle looks dead" status.

Run with ``python3 -m unittest discover -s tests -v``.
"""
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "mt5_feed", ROOT / "tools" / "mt5_feed.py")
mt5_feed = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mt5_feed)

F = mt5_feed


class ReconnectPolicyTests(unittest.TestCase):
    def test_no_rates_triggers_reconnect(self):
        """The 2026-11-02 outage: -10004 No IPC connection => status NO_RATES."""
        self.assertIn(F.ST_NO_RATES, F.RECONNECT_STATUSES)
        self.assertTrue(F.should_reconnect(F.ST_NO_RATES, 1000.0, 0.0))

    def test_every_dead_handle_status_triggers_reconnect(self):
        for status in (F.ST_TICK_FROZEN, F.ST_NO_RATES, F.ST_TERMINAL_DOWN):
            with self.subTest(status=status):
                self.assertTrue(F.should_reconnect(status, 1000.0, 0.0))

    def test_offset_unknown_reconnects(self):
        """The offset is learned from a tick, so a dead handle blocks it forever."""
        self.assertIn(F.ST_OFFSET_UNKNOWN, F.RECONNECT_STATUSES)
        self.assertTrue(F.should_reconnect(F.ST_OFFSET_UNKNOWN, 1000.0, 0.0))

    def test_healthy_states_never_reconnect(self):
        for status in (F.ST_OK, F.ST_INIT_FAILED, F.ST_SYMBOL_FAILED, None):
            with self.subTest(status=status):
                self.assertFalse(F.should_reconnect(status, 1000.0, 0.0))

    def test_cooldown_bounds_the_attempts(self):
        every = F.RECONNECT_EVERY_S
        self.assertFalse(F.should_reconnect(F.ST_NO_RATES, 1000.0, 1000.0))
        self.assertFalse(F.should_reconnect(F.ST_NO_RATES, 1000.0, 1000.0 - every + 1))
        self.assertTrue(F.should_reconnect(F.ST_NO_RATES, 1000.0, 1000.0 - every))

    def test_missing_last_attempt_still_reconnects(self):
        self.assertTrue(F.should_reconnect(F.ST_NO_RATES, 1000.0, None))


if __name__ == "__main__":
    unittest.main()
