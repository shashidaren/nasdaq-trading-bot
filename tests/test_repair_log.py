"""Regression tests for tools/repair_log.py (torn-write repair of the evidence CSVs).

The 2026-11-18 incident: `forward_test_log.csv` carried 140 NUL bytes before the
2026-11-04 19:30 bar, so every strict reader failed closed and the dashboard
Readiness tab said "Readiness could not be computed." These tests pin down that
the repair removes ONLY NUL padding (values, order and row count survive), that
--check never writes, and that anything that is real corruption is refused and
restored rather than guessed at.

Run with ``python3 -m unittest discover -s tests -v``. Standard library only.
"""
import csv
import hashlib
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools import live_readiness as LR  # noqa: E402
from tools import repair_log  # noqa: E402

TRADE_FIELDS = [
    "Trade_Num", "Trade_Type", "Entry_Time", "Exit_Time", "Entry_Price", "Stop_Loss",
    "Take_Profit", "Exit_Price", "Exit_Reason", "Profit", "Balance_After", "RSI_At_Entry",
    "ATR_At_Entry", "Wick_Ratio_At_Entry", "EMA50_At_Entry", "EMA200_At_Entry",
]
BAR_FIELDS = ["Timestamp", "Open", "High", "Low", "Close"]
FMT = "%Y-%m-%d %H:%M:%S"
SCRIPT = ROOT / "tools" / "repair_log.py"


def write_csv(path, fields, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fixture(root, count=4):
    """A small but strictly valid ledger + contiguous M15 log (no real orders)."""
    root = Path(root)
    start = datetime(2026, 10, 26, 14, 0)
    slots = [start + timedelta(minutes=15 * i) for i in range(count)]
    trades = []
    for i, dt in enumerate(slots):
        trades.append({
            "Trade_Num": i + 1, "Trade_Type": "BUY", "Entry_Time": dt.strftime(FMT),
            "Exit_Time": (dt + timedelta(minutes=5)).strftime(FMT),
            "Entry_Price": 20000, "Stop_Loss": 19980, "Take_Profit": 20040,
            "Exit_Price": 20020, "Exit_Reason": "TP", "Profit": 2, "Balance_After": 200 + 2 * (i + 1),
            "ATR_At_Entry": 10,
        })
    bars = [{"Timestamp": dt.strftime(FMT), "Open": 20000, "High": 20010,
             "Low": 19990, "Close": 20005} for dt in slots]
    write_csv(root / "trades.csv", TRADE_FIELDS, trades)
    write_csv(root / "forward_test_log.csv", BAR_FIELDS, bars)
    return trades, bars


def run(root, *args):
    proc = subprocess.run([sys.executable, str(SCRIPT), "--root", str(root), *args],
                          text=True, capture_output=True, timeout=60)
    return proc.returncode, proc.stdout + proc.stderr


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def backups(root):
    return sorted(Path(root).glob("*.bak-*"))


class RepairLogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.trades, self.bars = fixture(self.root)

    def tear_a_log_row(self, line_no, pad=140):
        """Pad one log row with NULs, exactly like the 2026-11-04 torn write."""
        path = self.root / "forward_test_log.csv"
        lines = path.read_bytes().split(b"\n")
        lines[line_no] = b"\x00" * pad + lines[line_no]
        path.write_bytes(b"\n".join(lines))

    def test_clean_evidence_is_reported_clean_and_never_written(self):
        before = digest(self.root / "forward_test_log.csv")
        code, out = run(self.root)
        self.assertEqual(code, 0, out)
        self.assertIn("CLEAN", out)
        self.assertEqual(before, digest(self.root / "forward_test_log.csv"))
        self.assertEqual([], backups(self.root))

    def test_check_names_the_torn_line_and_leaves_the_file_byte_identical(self):
        self.tear_a_log_row(3)
        before = digest(self.root / "forward_test_log.csv")
        self.assertIsNotNone(LR.torn_write_report(self.root / "forward_test_log.csv"))
        with self.assertRaisesRegex(ValueError, r"NUL byte.*line 4.*repair_log.py"):
            LR.load_readiness_data(self.root / "trades.csv", self.root / "forward_test_log.csv")
        code, out = run(self.root, "--check")
        self.assertEqual(code, 1, out)
        self.assertIn("line 4", out)
        self.assertIn("repair_log.py --apply", out)
        self.assertEqual(before, digest(self.root / "forward_test_log.csv"))
        self.assertEqual([], backups(self.root))

    def test_apply_strips_only_the_padding_and_keeps_every_value(self):
        path = self.root / "forward_test_log.csv"
        original = path.read_bytes()
        self.tear_a_log_row(3)
        torn = path.read_bytes()
        code, out = run(self.root, "--apply")
        self.assertEqual(code, 0, out)
        self.assertIn("REPAIRED", out)
        # Byte-for-byte the original file: only the 140 NULs were removed.
        self.assertEqual(original, path.read_bytes())
        saved = backups(self.root)
        self.assertEqual(torn, saved[0].read_bytes())
        self.assertEqual("forward_test_log.csv", saved[0].name.split(".bak-")[0])
        # The strict loader accepts it again and the row count is unchanged.
        trades, bars = LR.load_readiness_data(self.root / "trades.csv", path)
        self.assertEqual(len(trades), len(self.trades))
        self.assertEqual(len(bars), len(self.bars))
        self.assertIsNone(LR.torn_write_report(path))
        # Values survive: the repaired bar is still the same bar.
        self.assertEqual(self.bars[2]["Timestamp"], bars[2]["dt"].strftime(FMT))
        self.assertEqual(self.bars[2]["Close"], bars[2]["c"])
        # Idempotent.
        code, out = run(self.root, "--apply")
        self.assertEqual(code, 0, out)
        self.assertIn("CLEAN", out)

    def test_a_hole_with_no_row_bytes_is_refused_not_guessed(self):
        path = self.root / "forward_test_log.csv"
        lines = path.read_bytes().split(b"\n")
        lines.insert(3, b"\x00" * 40)          # the row itself is gone - padding only
        path.write_bytes(b"\n".join(lines))
        before = digest(path)
        code, out = run(self.root, "--apply")
        self.assertEqual(code, 2, out)
        self.assertIn("no row bytes", out)
        self.assertEqual(before, digest(path))
        self.assertEqual([], backups(self.root))
        self.assertEqual([], sorted(Path(self.root).glob("*.repair-*")))

    def test_repair_that_does_not_validate_is_rolled_back(self):
        path = self.root / "forward_test_log.csv"
        lines = path.read_bytes().split(b"\n")
        # Padded row whose own timestamp is also nonsense: padding + real corruption.
        lines[3] = b"\x00" * 20 + lines[3].replace(b"2026-10-26 14:30:00", b"2026-13-26 14:30:00")
        path.write_bytes(b"\n".join(lines))
        before = digest(path)
        code, out = run(self.root, "--apply")
        self.assertEqual(code, 2, out)
        self.assertIn("still fails the strict loader", out)
        self.assertEqual(before, digest(path))
        self.assertEqual([], sorted(Path(self.root).glob("*.repair-*")))

    def test_missing_trailing_newline_is_added_without_touching_values(self):
        path = self.root / "forward_test_log.csv"
        path.write_bytes(path.read_bytes().rstrip(b"\n"))
        code, out = run(self.root, "--check")
        self.assertEqual(code, 1, out)
        self.assertIn("missing its trailing newline", out)
        code, out = run(self.root, "--apply")
        self.assertEqual(code, 0, out)
        self.assertTrue(path.read_bytes().endswith(b"\n"))
        self.assertEqual(len(self.bars),
                         len(LR.load_readiness_data(self.root / "trades.csv", path)[1]))

    def test_engine_append_guard_repairs_a_torn_tail_without_erasing_it(self):
        import engine
        path = self.root / "forward_test_log.csv"
        path.write_bytes(path.read_bytes().rstrip(b"\n"))
        problem = engine.ensure_appendable_log(path)
        self.assertIn("unterminated", problem)
        self.assertTrue(path.read_bytes().endswith(b"\n"))
        self.assertIn(b"2026-10-26 14:45:00", path.read_bytes())
        # A NUL-padded tail is reported, never rewritten by the engine.
        path.write_bytes(path.read_bytes() + b"\x00" * 5 + b"2026-10-26 15:00:00,x,y,z,w\n")
        before = digest(path)
        self.assertIn("NUL", engine.ensure_appendable_log(path))
        self.assertEqual(before, digest(path))
        self.assertEqual("", engine.ensure_appendable_log(self.root / "does-not-exist.csv"))


if __name__ == "__main__":
    unittest.main()
