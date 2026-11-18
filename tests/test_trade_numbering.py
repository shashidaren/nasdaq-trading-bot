"""Regression tests for Trade_Num uniqueness, restart persistence, and check_data FAIL gates.

Covers the 2026-11-17 VPS duplicate Trade_Num #7 incident:
  load_trade_stats() set next_trade_num = 7 from rows 1..6,
  restore_open_trade_from_status() restored current_trade_num = 7 without
  advancing next_trade_num, trade #7 closed leaving next_trade_num = 7, and
  the next entry opened a second trade #7.

Invariant: Trade_Num is unique, strictly increasing, never reused across
restarts, and log_trade() never appends a duplicate or rewrites history.
"""
import contextlib
import csv
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine as sim_engine  # noqa: E402
import trade_filter as TF  # noqa: E402

CHECK_DATA = ROOT / "tools" / "check_data.py"

LOG_HEADER = [
    "Timestamp", "Open", "High", "Low", "Close", "Wick_Ratio", "Volume", "Vol_MA",
    "Vol_Confirmed", "Dynamic_Floor", "EMA_50", "EMA_200", "Trend_Confirmed",
    "Tested_Floor", "Valid_Rejection", "Held_Floor", "RSI", "ATR",
]


def _quiet(fn, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args, **kwargs)


def _seed_closed_trades(path: Path, count: int) -> list:
    """Write closed trades #1..#count with continuous Balance_After starting from $200."""
    rows = []
    bal = 200.00
    for idx in range(1, count + 1):
        profit = 20.00 if idx % 3 == 1 else -10.00
        reason = "TP" if profit > 0 else "SL"
        bal = round(bal + profit, 2)
        hour = 9 + idx
        rows.append({
            "Trade_Num": str(idx),
            "Trade_Type": "SELL",
            "Entry_Time": f"2026-11-16 {hour:02d}:15:00",
            "Exit_Time": f"2026-11-16 {hour:02d}:45:05",
            "Entry_Price": "27500.00",
            "Stop_Loss": "27600.00",
            "Take_Profit": "27300.00",
            "Exit_Price": "27300.00" if reason == "TP" else "27600.00",
            "Exit_Reason": reason,
            "Profit": f"{profit:.2f}",
            "Balance_After": f"{bal:.2f}",
            "RSI_At_Entry": "50.0",
            "ATR_At_Entry": "50.00",
            "Wick_Ratio_At_Entry": "60.0%",
            "EMA50_At_Entry": "27510.00",
            "EMA200_At_Entry": "27700.00",
        })
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=sim_engine.TRADES_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def _read_trade_rows(path: Path) -> list:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _read_journal_rows(path: Path) -> list:
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class _FakeMT5(types.ModuleType):
    TRADE_ACTION_DEAL = 1
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TIME_GTC = 0
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    SYMBOL_FILLING_IOC = 2
    TRADE_RETCODE_DONE = 10009
    DEAL_ENTRY_OUT = 1
    DEAL_REASON_TP = 5

    def __init__(self):
        super().__init__("MetaTrader5")
        self.positions = []
        self.deals = []
        self.balance = 200.00
        self.send_returns_none = False
        self.spawn_on_none = False
        self._next_ticket = 5001

    def initialize(self, **_kw):
        return True

    def shutdown(self):
        return None

    def last_error(self):
        return (1, "Success")

    def account_info(self):
        return types.SimpleNamespace(login=1, balance=self.balance, currency="USD")

    def symbol_select(self, _sym, _enable=True):
        return True

    def symbol_info(self, sym):
        return types.SimpleNamespace(
            name=sym, filling_mode=self.SYMBOL_FILLING_IOC, point=0.01,
            volume_min=0.10, volume_max=500.0,
        )

    def symbol_info_tick(self, _sym):
        return types.SimpleNamespace(bid=27292.40, ask=27294.90, time=int(time.time()))

    def positions_get(self, **kw):
        ps = list(self.positions)
        if "symbol" in kw:
            ps = [p for p in ps if p.symbol == kw["symbol"]]
        return tuple(ps)

    def history_deals_get(self, _from_dt=None, _to_dt=None, **kw):
        ds = list(self.deals)
        if "position" in kw:
            ds = [d for d in ds if getattr(d, "position_id", None) == kw["position"]]
        return tuple(ds)

    def order_send(self, request):
        t = self._next_ticket
        self._next_ticket += 1
        pos = types.SimpleNamespace(
            ticket=t, identifier=t, symbol=request["symbol"], type=request["type"],
            volume=request["volume"], price_open=request["price"],
            sl=request["sl"], tp=request["tp"], magic=request["magic"],
            time=int(time.time()),
        )
        if self.send_returns_none:
            if self.spawn_on_none:
                self.positions.append(pos)
            return None
        self.positions.append(pos)
        return types.SimpleNamespace(
            retcode=self.TRADE_RETCODE_DONE, order=t, deal=t + 100,
            price=request["price"], volume=request["volume"], comment="filled",
        )


def _load_live_engine(fake_mt5):
    src = (ROOT / "engine.py").read_text(encoding="utf-8")
    spec = importlib.util.spec_from_loader("engine_live_numbering_test", loader=None)
    mod = importlib.util.module_from_spec(spec)
    saved_mt5 = sys.modules.get("MetaTrader5")
    sys.modules["MetaTrader5"] = fake_mt5
    try:
        exec(
            compile(
                src.replace('TRADING_MODE = "FORWARD_TEST"', 'TRADING_MODE = "LIVE"', 1),
                str(ROOT / "engine.py"),
                "exec",
            ),
            mod.__dict__,
        )
    finally:
        if saved_mt5 is None:
            sys.modules.pop("MetaTrader5", None)
        else:
            sys.modules["MetaTrader5"] = saved_mt5
    return mod


class SimulatedTradeNumberingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self._saved = {
            "TRADES_LOG_PATH": sim_engine.TRADES_LOG_PATH,
            "LOG_FILE_PATH": sim_engine.LOG_FILE_PATH,
            "STATUS_FILE_PATH": sim_engine.STATUS_FILE_PATH,
            "MT5_FEED_FILE": sim_engine.MT5_FEED_FILE,
            "MT5_WARMUP_FILE": sim_engine.MT5_WARMUP_FILE,
            "MT5_FEED_HEALTH_FILE": sim_engine.MT5_FEED_HEALTH_FILE,
            "MT5_ORDER_DIR": sim_engine.MT5_ORDER_DIR,
            "JOURNAL_FILE_PATH": sim_engine.JOURNAL_FILE_PATH,
            "TF_TRADES": TF.TRADES_LOG,
            "TF_SKIPS": TF.SKIP_LOG,
        }
        self.addCleanup(self._restore_paths)
        sim_engine.TRADES_LOG_PATH = str(self.root / "trades.csv")
        sim_engine.LOG_FILE_PATH = str(self.root / "forward_test_log.csv")
        sim_engine.STATUS_FILE_PATH = str(self.root / "status.json")
        sim_engine.MT5_FEED_FILE = str(self.root / "mt5_last_candle.json")
        sim_engine.MT5_WARMUP_FILE = str(self.root / "mt5_warmup_candles.json")
        sim_engine.MT5_FEED_HEALTH_FILE = str(self.root / "mt5_feed_health.json")
        sim_engine.MT5_ORDER_DIR = str(self.root / "mt5_bridge")
        sim_engine.JOURNAL_FILE_PATH = str(self.root / "journal.csv")
        TF.TRADES_LOG = sim_engine.TRADES_LOG_PATH
        TF.SKIP_LOG = str(self.root / "skipped_trades.csv")

    def _restore_paths(self):
        sim_engine.TRADES_LOG_PATH = self._saved["TRADES_LOG_PATH"]
        sim_engine.LOG_FILE_PATH = self._saved["LOG_FILE_PATH"]
        sim_engine.STATUS_FILE_PATH = self._saved["STATUS_FILE_PATH"]
        sim_engine.MT5_FEED_FILE = self._saved["MT5_FEED_FILE"]
        sim_engine.MT5_WARMUP_FILE = self._saved["MT5_WARMUP_FILE"]
        sim_engine.MT5_FEED_HEALTH_FILE = self._saved["MT5_FEED_HEALTH_FILE"]
        sim_engine.MT5_ORDER_DIR = self._saved["MT5_ORDER_DIR"]
        sim_engine.JOURNAL_FILE_PATH = self._saved["JOURNAL_FILE_PATH"]
        TF.TRADES_LOG = self._saved["TF_TRADES"]
        TF.SKIP_LOG = self._saved["TF_SKIPS"]

    def _boot(self):
        eng = _quiet(sim_engine.NasdaqEngine)
        eng.atr = 78.91
        eng.rsi = 56.2
        eng.ema_fast = 27349.43
        eng.ema_slow = 27714.81
        return eng

    def test_restart_mid_trade_restore_close_then_next_entry_gets_max_plus_one(self):
        """Exact 2026-11-17 incident: 6 closed -> open #7 -> restart -> close #7 -> open #8."""
        _seed_closed_trades(self.root / "trades.csv", 6)

        eng1 = self._boot()
        self.assertEqual(eng1.next_trade_num, 7)
        _quiet(eng1.execute_simulated_trade, "SELL", 27292.40, 0.656, "2026-11-17 02:15:00")
        self.assertEqual(eng1.current_trade_num, 7)
        self.assertEqual(eng1.next_trade_num, 8)

        # Simulate the pre-fix status.json case too (where next_trade_num was left at 7)
        # to prove restore_open_trade_from_status() itself advances next_trade_num to 8.
        st = json.loads((self.root / "status.json").read_text(encoding="utf-8"))
        st["next_trade_num"] = 7
        (self.root / "status.json").write_text(json.dumps(st), encoding="utf-8")

        # Restart mid-trade (2026-11-17 09:41 UTC)
        eng2 = self._boot()
        self.assertTrue(eng2.trade_active)
        self.assertEqual(eng2.current_trade_num, 7)
        self.assertEqual(eng2.next_trade_num, 8)

        # Close restored trade #7 at TP (15:45 UTC)
        self.assertTrue(_quiet(eng2.close_simulated_trade, eng2.take_profit, "TP"))
        self.assertEqual(eng2.next_trade_num, 8)
        self.assertIsNone(eng2.current_trade_num)

        # Open subsequent trade (21:15 UTC) -> MUST be #8, never a second #7
        _quiet(eng2.execute_simulated_trade, "SELL", 27009.81, 0.741, "2026-11-17 21:15:00")
        self.assertEqual(eng2.current_trade_num, 8)
        self.assertEqual(eng2.next_trade_num, 9)

        # Restart mid-trade again while #8 is open, close at SL, then restart flat and open #9
        eng3 = self._boot()
        self.assertEqual(eng3.current_trade_num, 8)
        self.assertEqual(eng3.next_trade_num, 9)
        self.assertTrue(_quiet(eng3.close_simulated_trade, eng3.stop_loss, "SL"))

        eng4 = self._boot()
        self.assertEqual(eng4.next_trade_num, 9)
        _quiet(eng4.execute_simulated_trade, "SELL", 27100.00, 0.50, "2026-11-18 10:15:00")
        self.assertEqual(eng4.current_trade_num, 9)
        self.assertEqual(eng4.next_trade_num, 10)
        self.assertTrue(_quiet(eng4.close_simulated_trade, eng4.take_profit, "TP"))

        nums = [int(r["Trade_Num"]) for r in _read_trade_rows(self.root / "trades.csv")]
        self.assertEqual(nums, [1, 2, 3, 4, 5, 6, 7, 8, 9])

    def test_restore_with_missing_current_trade_num_allocates_max_plus_one(self):
        """Legacy status.json without current_trade_num must not reuse max_closed (#6)."""
        _seed_closed_trades(self.root / "trades.csv", 6)
        (self.root / "status.json").write_text(json.dumps({
            "trade_active": True,
            "trade_type": "SELL",
            "entry_price": 27300.0,
            "stop_loss": 27450.0,
            "take_profit": 27000.0,
            "entry_time": "2026-11-17 02:15:00",
            "current_trade_num": None,
        }), encoding="utf-8")

        eng = self._boot()
        self.assertEqual(eng.current_trade_num, 7)
        self.assertEqual(eng.next_trade_num, 8)
        self.assertTrue(_quiet(eng.close_simulated_trade, 27000.0, "TP"))
        nums = [int(r["Trade_Num"]) for r in _read_trade_rows(self.root / "trades.csv")]
        self.assertEqual(nums, [1, 2, 3, 4, 5, 6, 7])

    def test_allocator_uses_ledger_max_when_in_memory_counter_is_stale(self):
        _seed_closed_trades(self.root / "trades.csv", 7)
        eng = self._boot()
        eng.next_trade_num = 2  # artificially stale in-memory counter
        _quiet(eng.execute_simulated_trade, "BUY", 27200.0, 0.40, "2026-11-17 21:15:00")
        self.assertEqual(eng.current_trade_num, 8)
        self.assertEqual(eng.next_trade_num, 9)

    def test_close_that_would_duplicate_existing_trade_num_fails_closed(self):
        """log_trade() refuses a duplicate Trade_Num, writes no second row, and journals loudly."""
        _seed_closed_trades(self.root / "trades.csv", 7)
        before_bytes = (self.root / "trades.csv").read_bytes()

        # Simulate crash between log_trade(#7) and save_status(): #7 is already in trades.csv
        # while status.json still claims trade #7 is open.
        (self.root / "status.json").write_text(json.dumps({
            "trade_active": True,
            "trade_type": "SELL",
            "entry_price": 27009.81,
            "stop_loss": 27204.98,
            "take_profit": 26619.47,
            "entry_time": "2026-11-17 21:15:00",
            "current_trade_num": 7,
        }), encoding="utf-8")

        eng = self._boot()
        bal_before = eng.balance
        wins_before, losses_before = eng.wins, eng.losses

        closed = _quiet(eng.close_simulated_trade, 27204.98, "SL")
        self.assertFalse(closed)
        self.assertEqual((self.root / "trades.csv").read_bytes(), before_bytes)
        self.assertEqual(len(_read_trade_rows(self.root / "trades.csv")), 7)
        self.assertEqual(eng.balance, bal_before)
        self.assertEqual((eng.wins, eng.losses), (wins_before, losses_before))
        self.assertFalse(eng.trade_active)
        self.assertEqual(eng.next_trade_num, 8)

        journal = _read_journal_rows(self.root / "journal.csv")
        dup_events = [j for j in journal if j["Event"] == "TRADE_NUM_DUPLICATE"]
        self.assertEqual(len(dup_events), 1)
        self.assertIn("#7", dup_events[0]["Detail"])

        # Subsequent genuine trade allocates #8 cleanly and appends row #8.
        _quiet(eng.execute_simulated_trade, "SELL", 27100.00, 0.50, "2026-11-17 22:00:00")
        self.assertEqual(eng.current_trade_num, 8)
        self.assertTrue(_quiet(eng.close_simulated_trade, eng.take_profit, "TP"))
        nums = [int(r["Trade_Num"]) for r in _read_trade_rows(self.root / "trades.csv")]
        self.assertEqual(nums, [1, 2, 3, 4, 5, 6, 7, 8])

    def test_log_trade_refuses_non_increasing_invalid_and_torn_ledger(self):
        _seed_closed_trades(self.root / "trades.csv", 3)
        # Create a gap [1, 3] to verify strictly-increasing refusal of #2 as well.
        rows = [r for r in _read_trade_rows(self.root / "trades.csv") if r["Trade_Num"] != "2"]
        with open(self.root / "trades.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=sim_engine.TRADES_FIELDNAMES)
            w.writeheader()
            w.writerows(rows)
        snapshot = (self.root / "trades.csv").read_bytes()

        eng = self._boot()
        eng.trade_type = "BUY"
        eng.entry_time = "2026-11-17 12:00:00"
        eng.entry_price, eng.stop_loss, eng.take_profit = 27000.0, 26900.0, 27200.0

        eng.current_trade_num = 2
        self.assertFalse(_quiet(eng.log_trade, 27200.0, "TP", 20.0))
        self.assertEqual((self.root / "trades.csv").read_bytes(), snapshot)
        self.assertEqual(eng.next_trade_num, 4)

        eng.current_trade_num = None
        self.assertFalse(_quiet(eng.log_trade, 27200.0, "TP", 20.0))
        self.assertEqual((self.root / "trades.csv").read_bytes(), snapshot)

        # Torn NUL byte in trades.csv also fails closed without appending.
        torn = b"\x00" + snapshot
        (self.root / "trades.csv").write_bytes(torn)
        eng.current_trade_num = 4
        self.assertFalse(_quiet(eng.log_trade, 27200.0, "TP", 20.0))
        self.assertEqual((self.root / "trades.csv").read_bytes(), torn)

        events = [j["Event"] for j in _read_journal_rows(self.root / "journal.csv")]
        self.assertIn("TRADE_NUM_ORDER_ERROR", events)
        self.assertEqual(events.count("TRADE_NUM_ERROR"), 2)


class LivePathsTradeNumberingTests(unittest.TestCase):
    """Verify direct MT5, None-reconcile, Wine bridge, and startup broker reconcile use the allocator."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fake = _FakeMT5()
        self.live = _load_live_engine(self.fake)
        self.live.MT5_BRIDGE_TIMEOUT_S = 1.5
        self._saved_tf = (TF.TRADES_LOG, TF.SKIP_LOG)
        self.addCleanup(self._restore_tf)

    def _restore_tf(self):
        TF.TRADES_LOG, TF.SKIP_LOG = self._saved_tf

    def _boot(self, subdir: str, use_bridge: bool = False):
        d = self.root / subdir
        d.mkdir(parents=True, exist_ok=True)
        self.live.TRADES_LOG_PATH = str(d / "trades.csv")
        self.live.LOG_FILE_PATH = str(d / "forward_test_log.csv")
        self.live.STATUS_FILE_PATH = str(d / "status.json")
        self.live.MT5_FEED_FILE = str(d / "mt5_last_candle.json")
        self.live.MT5_WARMUP_FILE = str(d / "mt5_warmup_candles.json")
        self.live.MT5_FEED_HEALTH_FILE = str(d / "mt5_feed_health.json")
        self.live.MT5_ORDER_DIR = str(d / "mt5_bridge")
        self.live.JOURNAL_FILE_PATH = str(d / "journal.csv")
        TF.TRADES_LOG = self.live.TRADES_LOG_PATH
        TF.SKIP_LOG = str(d / "skipped_trades.csv")
        self.live.mt5 = None if use_bridge else self.fake
        eng = _quiet(self.live.NasdaqEngine)
        eng.atr = 50.0
        eng.rsi = 52.0
        eng.ema_fast = 27300.0
        eng.ema_slow = 27400.0
        return eng, d

    def test_direct_mt5_and_none_reconcile_and_orphan_startup(self):
        d = self.root / "direct"
        d.mkdir()
        _seed_closed_trades(d / "trades.csv", 6)

        eng1, _ = self._boot("direct", use_bridge=False)
        eng1.next_trade_num = 1  # stale counter must still allocate #7 from ledger
        _quiet(eng1.execute_live_trade, "SELL", 27292.40, 0.5, "2026-11-17 02:15:00")
        self.assertEqual(eng1.current_trade_num, 7)
        self.assertEqual(eng1.next_trade_num, 8)

        # Restart mid-trade while broker holds position -> close -> open #8 via None-reconcile
        eng2, _ = self._boot("direct", use_bridge=False)
        self.assertEqual(eng2.current_trade_num, 7)
        self.assertEqual(eng2.next_trade_num, 8)
        pos7 = self.fake.positions.pop()
        self.fake.deals.append(types.SimpleNamespace(
            position_id=pos7.ticket, entry=self.fake.DEAL_ENTRY_OUT,
            price=pos7.tp, profit=20.0, commission=0.0, swap=0.0, fee=0.0,
        ))
        _quiet(eng2.check_live_exits)
        self.assertEqual(eng2.next_trade_num, 8)

        # order_send returns None, but position appeared on broker -> allocates #8
        self.fake.send_returns_none = True
        self.fake.spawn_on_none = True
        _quiet(eng2.execute_live_trade, "SELL", 27292.40, 0.5, "2026-11-17 21:15:00")
        self.assertEqual(eng2.current_trade_num, 8)
        self.assertEqual(eng2.next_trade_num, 9)
        pos8 = self.fake.positions.pop()
        self.fake.deals.append(types.SimpleNamespace(
            position_id=pos8.ticket, entry=self.fake.DEAL_ENTRY_OUT,
            price=pos8.tp, profit=20.0, commission=0.0, swap=0.0, fee=0.0,
        ))
        _quiet(eng2.check_live_exits)

        # Orphan broker position on startup (no status.json) -> allocates #9, sets next=10
        (d / "status.json").unlink()
        self.fake.positions.append(types.SimpleNamespace(
            ticket=9900, magic=self.live.MAGIC_NUMBER, symbol="US100Cash",
            type=1, price_open=27300.0, sl=27400.0, tp=27100.0,
        ))
        eng3, _ = self._boot("direct", use_bridge=False)
        self.assertEqual(eng3.current_trade_num, 9)
        self.assertEqual(eng3.next_trade_num, 10)
        pos9 = self.fake.positions.pop()
        self.fake.deals.append(types.SimpleNamespace(
            position_id=pos9.ticket, entry=self.fake.DEAL_ENTRY_OUT,
            price=27100.0, profit=20.0, commission=0.0, swap=0.0, fee=0.0,
        ))
        _quiet(eng3.check_live_exits)

        # Duplicate live exit refuses to append a second #9
        eng3.current_trade_num = 9
        eng3.trade_active_live = True
        self.assertFalse(_quiet(eng3._record_live_exit, 27100.0, 20.0))
        self.assertEqual(eng3.next_trade_num, 10)

        nums = [int(r["Trade_Num"]) for r in _read_trade_rows(d / "trades.csv")]
        self.assertEqual(nums, [1, 2, 3, 4, 5, 6, 7, 8, 9])

    def test_wine_bridge_allocates_without_burning_rejected_intents(self):
        d = self.root / "bridge"
        bridge_dir = d / "mt5_bridge"
        bridge_dir.mkdir(parents=True)
        _seed_closed_trades(d / "trades.csv", 6)
        (bridge_dir / "positions.json").write_text(
            json.dumps({"positions": [], "closed_recent": [], "balance": 220.0}),
            encoding="utf-8",
        )

        eng1, _ = self._boot("bridge", use_bridge=True)

        def respond(status: str, position_id: int = 7001):
            intent_path = bridge_dir / "intent.json"
            result_path = bridge_dir / "result.json"
            seen = {}

            def worker():
                deadline = time.monotonic() + 1.2
                while time.monotonic() < deadline:
                    if intent_path.is_file():
                        data = json.loads(intent_path.read_text(encoding="utf-8"))
                        seen.update(data)
                        intent_path.unlink()
                        result_path.write_text(json.dumps({
                            "intent_id": data["intent_id"],
                            "status": status,
                            "position_id": position_id,
                            "fill_price": 27292.40,
                            "sl": data["sl"],
                            "tp": data["tp"],
                            "message": status,
                        }), encoding="utf-8")
                        return
                    time.sleep(0.02)

            th = threading.Thread(target=worker, daemon=True)
            th.start()
            return th, seen

        # Rejected intent previews #7 and does NOT burn #7.
        th, seen_rej = respond("REJECTED")
        _quiet(eng1.execute_live_trade, "SELL", 27292.40, 0.5, "2026-11-17 02:00:00")
        th.join(timeout=1.0)
        self.assertEqual(seen_rej.get("trade_num"), 7)
        self.assertFalse(eng1.trade_active_live)
        self.assertEqual(eng1.next_trade_num, 7)

        # Filled intent allocates #7 and advances next_trade_num to 8.
        eng1.next_trade_num = 2  # stale in-memory value still resolves to #7
        th, seen_fill = respond("FILLED", position_id=7001)
        _quiet(eng1.execute_live_trade, "SELL", 27292.40, 0.5, "2026-11-17 02:15:00")
        th.join(timeout=1.0)
        self.assertEqual(seen_fill.get("trade_num"), 7)
        self.assertEqual(eng1.current_trade_num, 7)
        self.assertEqual(eng1.next_trade_num, 8)

        # Restart mid-trade while bridge position 7001 is open -> close -> open #8
        (bridge_dir / "positions.json").write_text(json.dumps({
            "positions": [{
                "position_id": 7001, "magic": self.live.MAGIC_NUMBER, "symbol": "US100Cash",
                "side": "SELL", "price_open": 27292.40, "sl": 27392.40, "tp": 27092.40,
                "time_utc": "2026-11-17 02:15:00",
            }],
            "closed_recent": [],
            "balance": 220.0,
        }), encoding="utf-8")
        eng2, _ = self._boot("bridge", use_bridge=True)
        self.assertEqual(eng2.current_trade_num, 7)
        self.assertEqual(eng2.next_trade_num, 8)

        (bridge_dir / "positions.json").write_text(json.dumps({
            "positions": [],
            "closed_recent": [{
                "position_id": 7001, "price": 27092.40, "profit": 20.0,
                "time_utc": "2026-11-17 15:45:06",
            }],
            "balance": 240.0,
        }), encoding="utf-8")
        _quiet(eng2.check_live_exits)
        self.assertEqual(eng2.next_trade_num, 8)

        th, seen_next = respond("FILLED", position_id=7002)
        _quiet(eng2.execute_live_trade, "SELL", 27009.81, 0.5, "2026-11-17 21:15:00")
        th.join(timeout=1.0)
        self.assertEqual(seen_next.get("trade_num"), 8)
        self.assertEqual(eng2.current_trade_num, 8)
        self.assertEqual(eng2.next_trade_num, 9)


class CheckDataTradeNumberingTests(unittest.TestCase):
    """tools/check_data.py must FAIL (exit 1) and name duplicate or missing Trade_Num values."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        with open(self.root / "forward_test_log.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(LOG_HEADER)
            w.writerow([
                "2026-11-16 10:15:00", "27500", "27520", "27490", "27500", "50.0%", "100",
                "100.0", "True", "27480.00", "27510.00", "27700.00", "True", "True", "True",
                "True", "50.0", "50.00",
            ])
        (self.root / "skipped_trades.csv").write_text("Timestamp,Reason,Price,ATR\n", encoding="utf-8")

    def _run_check(self) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(CHECK_DATA), str(self.root)],
            capture_output=True,
            text=True,
            timeout=30,
        )

    def _write_rows(self, rows: list):
        with open(self.root / "trades.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=sim_engine.TRADES_FIELDNAMES)
            w.writeheader()
            w.writerows(rows)

    def test_clean_and_empty_ledgers_pass(self):
        self._write_rows([])
        proc = self._run_check()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("0 fail", proc.stdout)

        _seed_closed_trades(self.root / "trades.csv", 8)
        proc = self._run_check()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("#1..#8", proc.stdout)

    def test_duplicate_trade_num_fails_and_names_the_number(self):
        rows = _seed_closed_trades(self.root / "trades.csv", 8)
        rows[-1]["Trade_Num"] = "7"  # 2026-11-17 duplicate #7
        self._write_rows(rows)

        proc = self._run_check()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("[FAIL]", proc.stdout)
        self.assertIn("duplicate Trade_Num", proc.stdout)
        self.assertIn("#7 (x2)", proc.stdout)

    def test_missing_trade_num_fails_and_names_the_numbers(self):
        rows = _seed_closed_trades(self.root / "trades.csv", 7)
        rows = [r for r in rows if r["Trade_Num"] not in ("4", "6")]
        self._write_rows(rows)

        proc = self._run_check()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("[FAIL]", proc.stdout)
        self.assertIn("missing Trade_Num", proc.stdout)
        self.assertIn("#4", proc.stdout)
        self.assertIn("#6", proc.stdout)

    def test_missing_trade_num_one_and_blank_and_out_of_order_fail(self):
        rows = _seed_closed_trades(self.root / "trades.csv", 3)
        self._write_rows(rows[1:])  # starts at #2 -> missing #1
        proc = self._run_check()
        self.assertEqual(proc.returncode, 1, proc.stdout)
        self.assertIn("missing Trade_Num", proc.stdout)
        self.assertIn("#1", proc.stdout)

        rows_blank = _seed_closed_trades(self.root / "trades.csv", 2)
        rows_blank[1]["Trade_Num"] = ""
        self._write_rows(rows_blank)
        proc = self._run_check()
        self.assertEqual(proc.returncode, 1, proc.stdout)
        self.assertIn("missing/blank Trade_Num at line(s) 3", proc.stdout)

        rows_order = _seed_closed_trades(self.root / "trades.csv", 3)
        rows_order[1]["Trade_Num"], rows_order[2]["Trade_Num"] = "3", "2"
        self._write_rows(rows_order)
        proc = self._run_check()
        self.assertEqual(proc.returncode, 1, proc.stdout)
        self.assertIn("out-of-order Trade_Num", proc.stdout)
        self.assertIn("#3->#2", proc.stdout)


if __name__ == "__main__":
    unittest.main()
