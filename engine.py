#!/usr/bin/env python3
"""
Nasdaq-100 Cash Index CFD (US100Cash) M15 Auto-Trading Engine for XM / MetaTrader 5.

Deployed on /opt/nasdaq to trade XM's US100Cash on 15-minute (M15) bars:
  - DATA_SOURCE="MT5" default (reads closed M15 bars from tools/mt5_feed.py +
    seeds 250 historical M15 bars on startup from mt5_warmup_candles.json so
    EMA200/EMA50/RSI/ATR are warm immediately without waiting 50 hours).
  - DST-aware America/New_York market hours (suppresses stale-feed alerts during
    CME weekend closure and the daily 17:00-18:00 ET maintenance break).
  - Hardened LIVE execution supporting BOTH:
      1) Direct MetaTrader5 Python API (Windows / Wine / tools/live_path_probe.py)
      2) Linux -> Wine atomic JSON bridge (/opt/nasdaq/mt5_bridge via tools/mt5_executor.py)
"""
import csv
import json
import math
import os
import shutil
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):
        return False
import feed_health
from trade_filter import (
    MAX_DAILY_LOSSES,
    get_daily_sl_count,
    is_market_quiet,
    load_recent_trades,
    should_take_trade,
)

# ==========================================
# CONFIGURATION SWITCH
# Set to "FORWARD_TEST" for simulated fills on live XM M15 candles
# Set to "LIVE" for real MT5 order execution (direct or Wine mt5_bridge)
# ==========================================
TRADING_MODE = "FORWARD_TEST"
if os.getenv("TRADING_MODE"):
    TRADING_MODE = os.getenv("TRADING_MODE").strip().upper()

DATA_DIR = os.getenv("NASDAQ_DATA_DIR", "/opt/nasdaq")
load_dotenv(dotenv_path=os.path.join(DATA_DIR, ".env"))

# Data source for FORWARD_TEST mode: "MT5" (default: local Wine MT5 terminal
# publishing XM US100Cash M15 bars) or "TWELVEDATA" (WebSocket fallback).
DATA_SOURCE = (os.getenv("DATA_SOURCE") or "MT5").strip().upper()

TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- INSTRUMENT (XM Nasdaq-100 Cash Index CFD) ---
SYMBOL_MT5 = os.getenv("SYMBOL_MT5", "US100Cash")
SYMBOL_TD = os.getenv("SYMBOL_TD", "NDX")

# --- STRATEGY PARAMETERS (US100Cash intraday swing, M15 candles) ---
LOOKBACK_PERIOD = 20            # 20 x M15 = 5 trading hours structural floor/ceiling
WICK_RATIO_TARGET = 0.15        # rejection wick >= 15% of bar range
EMA_FAST = 50                   # 50 x M15 = 12.5 hours
EMA_SLOW = 200                  # 200 x M15 = 50 trading hours (~2.2 trading days)
RSI_PERIOD = 14
ATR_PERIOD = 14
FLOOR_BUFFER_PCT = 0.0012       # 0.12% (~24 pts on US100Cash @ 20,000)

# Risk geometry: RR 1:2 (breakeven WR ~33.3%). SL = 2x M15 ATR, TP = 4x M15 ATR.
ATR_SL_MULT = 2.0
ATR_TP_MULT = 4.0

# Filters
REQUIRE_VOLUME_CONFIRM = False
VOLUME_SPIKE_MULTIPLIER = 0.9
REQUIRE_TREND_CONFIRM = True
RSI_MIN = 40.0
RSI_MAX = 70.0
MIN_ATR = 0.0  # trade_filter.py enforces percentage-based MIN_ATR_PERCENT / MAX_ATR_PERCENT

# --- Stale feed guard (M15 cadence = 900s; alert after 20m during open market) ---
CANDLE_SECONDS = 900                    # 15-minute bars (M15)
STALE_FEED_SECONDS = 20 * 60            # 20 min (> 1 M15 bar)
STALE_ALERT_COOLDOWN_SECONDS = 30 * 60  # journal alert at most every 30 min

# --- Regime gates (EMA Slope & Distance from Mean) ---
REQUIRE_EMA_SLOPE = True
EMA_SLOPE_LOOKBACK = 12        # Compare EMA50 vs 12 M15 bars ago (3 hours)
MAX_BELOW_EMA_ATR = 0.50       # Entry distance buffer from EMA50

# --- Position sizing & XM US100Cash contract economics ---
# XM US100Cash: 1.00 lot = $1.00 per index point (CONTRACT_SIZE = 1.0).
# At LOT_SIZE = 0.10: $0.10 per index point -> 70-pt SL (2x 35-pt ATR) risks $7.00,
# 140-pt TP pays $14.00, and typical ~2.5-pt spread costs ~$0.25/trade.
LOT_SIZE = float(os.getenv("LOT_SIZE", "0.10"))
CONTRACT_SIZE = float(os.getenv("CONTRACT_SIZE", "1.0"))
ASSUMED_SPREAD_USD = float(os.getenv("ASSUMED_SPREAD_USD", "0.25"))
MAGIC_NUMBER = int(os.getenv("MAGIC_NUMBER", "987656"))

# Live execution guards
MAX_SPREAD_POINTS = float(os.getenv("MAX_SPREAD_POINTS", "8.0"))
MAX_SPREAD_TO_1R = float(os.getenv("MAX_SPREAD_TO_1R", "0.35"))
LIVE_DEVIATION_POINTS = int(os.getenv("LIVE_DEVIATION_POINTS", "500"))  # $5.00 on 0.01-pt symbol
MT5_BRIDGE_TIMEOUT_S = float(os.getenv("MT5_BRIDGE_TIMEOUT_S", "12.0"))

STATS_EVERY_N_CANDLES = 30
STARTING_BALANCE = 200.00

LOG_FILE_PATH = os.getenv("NASDAQ_LOG_FILE", os.path.join(DATA_DIR, "forward_test_log.csv"))
TRADES_LOG_PATH = os.getenv("NASDAQ_TRADES_FILE", os.path.join(DATA_DIR, "trades.csv"))
STATUS_FILE_PATH = os.getenv("NASDAQ_STATUS_FILE", os.path.join(DATA_DIR, "status.json"))
MT5_FEED_FILE = os.getenv("MT5_FEED_FILE", os.path.join(DATA_DIR, "mt5_last_candle.json"))
MT5_WARMUP_FILE = os.getenv("MT5_WARMUP_FILE", os.path.join(DATA_DIR, "mt5_warmup_candles.json"))
MT5_ORDER_DIR = os.getenv("MT5_ORDER_DIR", os.path.join(DATA_DIR, "mt5_bridge"))
# Heartbeat written by tools/mt5_feed.py every poll (status, tick age, broker offset).
MT5_FEED_HEALTH_FILE = os.getenv("MT5_FEED_HEALTH_FILE", os.path.join(DATA_DIR, "mt5_feed_health.json"))
JOURNAL_FILE_PATH = os.getenv("NASDAQ_JOURNAL_FILE", os.path.join(DATA_DIR, "journal.csv"))

SYMBOL_FILLING_FOK = 1
SYMBOL_FILLING_IOC = 2

TRADES_FIELDNAMES = [
    "Trade_Num", "Trade_Type", "Entry_Time", "Exit_Time", "Entry_Price", "Stop_Loss", "Take_Profit",
    "Exit_Price", "Exit_Reason", "Profit", "Balance_After", "RSI_At_Entry",
    "ATR_At_Entry", "Wick_Ratio_At_Entry", "EMA50_At_Entry", "EMA200_At_Entry",
]


def migrate_trades_csv(path: str = None) -> bool:
    """Self-heal trades.csv schema drift (15-field pre-SELL -> 16-field schema)."""
    path = TRADES_LOG_PATH if path is None else path
    if not os.path.isfile(path):
        return False
    try:
        with open(path, newline="") as f:
            raw = [r for r in csv.reader(f) if r]
    except Exception as e:
        print(f"WARNING: could not read {path} for schema check: {e}")
        return False
    if not raw:
        return False

    header = [h.strip() for h in raw[0]]
    if header == TRADES_FIELDNAMES:
        return False

    migrated, odd = [], 0
    for r in raw[1:]:
        if len(r) == len(TRADES_FIELDNAMES):
            migrated.append(r)
        elif len(r) == len(TRADES_FIELDNAMES) - 1:
            migrated.append([r[0], "BUY"] + r[1:])
        else:
            migrated.append(r)
            odd += 1

    backup = path + ".bak-pre-migration"
    tmp = path + ".tmp-migration"
    try:
        shutil.copy2(path, backup)
        with open(tmp, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(TRADES_FIELDNAMES)
            w.writerows(migrated)
        os.replace(tmp, path)
    except Exception as e:
        print(f"WARNING: trades.csv schema migration FAILED ({e})")
        return False

    print(f"WARNING: migrated {path} to 16-field schema "
          f"({len(raw) - 1} rows rewritten, {odd} odd). Backup: {backup}.")
    return True


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def latest_closed_candle_ts(rates):
    """MT5's `time` for position 1: the open timestamp of its latest closed bar."""
    if rates is None or len(rates) == 0:
        return None
    return int(rates[0]["time"])


def ensure_appendable_log(path):
    """Check the tail before appending: never glue a row onto a partial line.

    A torn/sparse write leaves NUL padding inside the CSV, which every strict
    reader then rejects ("has NUL byte(s)"). The engine cannot repair that
    safely, but it can stop making it worse: a torn final row would otherwise
    swallow the next append. Returns a short description of what was found
    ("" when the tail is clean) so the caller can journal it.
    """
    try:
        size = os.path.getsize(path)
        if not size:
            return ""
        with open(path, "rb") as fh:
            fh.seek(max(0, size - 4096))
            tail = fh.read()
    except OSError:
        return ""
    problems = []
    if b"\x00" in tail:
        problems.append("NUL padding (torn write)")
    if not tail.endswith(b"\n"):
        try:
            with open(path, "ab") as fh:
                fh.write(b"\n")
        except OSError:
            return "unterminated final row (could not add a newline)"
        problems.append("unterminated final row (newline added)")
    return "; ".join(problems)


def last_logged_candle_close_ts(path=None):
    """Read the newest log Timestamp as a UTC candle-close watermark."""
    path = LOG_FILE_PATH if path is None else path
    if not path or not os.path.isfile(path):
        return 0
    latest = None
    try:
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                value = (row.get("Timestamp") or "").strip()
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                    try:
                        latest = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        continue
    except (OSError, csv.Error, UnicodeError):
        return 0
    return int(latest.timestamp()) if latest is not None else 0


def mt5_closed_candle_age_seconds(candle_open_ts, now_epoch=None):
    """Age of the latest closed MT5 candle, whose `time` is its open time."""
    if candle_open_ts is None:
        return None
    now = time.time() if now_epoch is None else float(now_epoch)
    closed_at = int(candle_open_ts) + CANDLE_SECONDS
    return max(0.0, now - closed_at)


def _select_mt5_filling(symbol_info, mt5_mod) -> int:
    """Decode MT5 SYMBOL_FILLING_* bitmask (1=FOK, 2=IOC) into ORDER_FILLING_* enum."""
    fok_enum = getattr(mt5_mod, "ORDER_FILLING_FOK", 0)
    ioc_enum = getattr(mt5_mod, "ORDER_FILLING_IOC", 1)
    ret_enum = getattr(mt5_mod, "ORDER_FILLING_RETURN", 2)
    flags = int(getattr(symbol_info, "filling_mode", 0) or 0) if symbol_info is not None else 0
    if flags & SYMBOL_FILLING_IOC:
        return ioc_enum
    if flags & SYMBOL_FILLING_FOK:
        return fok_enum
    return ret_enum


def _deal_net_profit(deal) -> float:
    total = 0.0
    for attr in ("profit", "commission", "swap", "fee"):
        try:
            v = float(getattr(deal, attr, 0.0) or 0.0)
            if math.isfinite(v):
                total += v
        except (TypeError, ValueError):
            continue
    return total


def _atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.flush()
    os.replace(tmp, path)


if TRADING_MODE == "LIVE":
    try:
        import MetaTrader5 as mt5
    except ImportError:
        mt5 = None  # Linux host: orders route via Wine tools/mt5_executor.py bridge
else:
    mt5 = None
    try:
        from twelvedata import TDClient
    except ImportError:
        TDClient = None


class NasdaqEngine:
    def __init__(self):
        self.lows = deque(maxlen=EMA_SLOW)
        self.highs = deque(maxlen=EMA_SLOW)
        self.closes = deque(maxlen=EMA_SLOW)
        self.volumes = deque(maxlen=EMA_SLOW)
        self.tr_list = deque(maxlen=ATR_PERIOD)
        self.rsi_gains = deque(maxlen=RSI_PERIOD)
        self.rsi_losses = deque(maxlen=RSI_PERIOD)
        self.ema50_history = deque(maxlen=EMA_SLOW)

        self.prev_close = None
        self.rsi = None
        self.ema_fast = None
        self.ema_slow = None
        self.atr = None

        self.k_fast = 2 / (EMA_FAST + 1)
        self.k_slow = 2 / (EMA_SLOW + 1)

        self.current_minute = None
        self.tick_pool = []

        self.balance = STARTING_BALANCE
        self.trade_active = False
        self.trade_type = "BUY"
        self.entry_price = 0.0
        self.stop_loss = 0.0
        self.take_profit = 0.0
        self.current_trade_num = None

        self.next_trade_num = 1
        self.wins = 0
        self.losses = 0

        self.entry_time = None
        self.entry_rsi = None
        self.entry_atr = None
        self.entry_wick_ratio = None
        self.entry_ema_fast = None
        self.entry_ema_slow = None

        # Live mode tracking
        self.active_ticket = None
        self.active_position_id = None
        self.trade_active_live = False
        self.live_unknown_order = False
        self._last_live_candle_ts = 0
        self._warmup_close_ts = 0

        self.warmup_logged = False
        self.candles_evaluated = 0
        self.candles_warm = 0

        self._last_price_mono = None
        self._last_stale_alert_mono = None
        self._feed_diag = None          # feed_health.diagnose_feed_file() result of the last poll
        self._feed_health = None        # feed_health.diagnose_sidecar_health() result
        self._last_feed_log_mono = None  # throttle for the "no feed" stdout line
        self._start_mono = time.monotonic()

        # Buy Funnel Counters
        self.hit_tested_floor = 0
        self.hit_valid_rejection = 0
        self.hit_held_support = 0
        self.hit_volume_confirmed = 0
        self.hit_trend_confirmed = 0
        self.hit_slope_confirmed = 0
        self.hit_price_near_ema = 0
        self.hit_all = 0

        # Sell Funnel Counters
        self.hit_sell_tested_ceiling = 0
        self.hit_sell_valid_rejection = 0
        self.hit_sell_held_resistance = 0
        self.hit_sell_trend_confirmed = 0
        self.hit_sell_slope_confirmed = 0
        self.hit_sell_price_near_ema = 0
        self.hit_sell_all = 0

        self.load_trade_stats()
        self.restore_open_trade_from_status()

        if TRADING_MODE == "LIVE":
            if mt5 is not None:
                if not mt5.initialize():
                    print(f"MT5 Initialize failed: {mt5.last_error()}")
                    sys.exit(1)
                account = mt5.account_info()
                if account is not None:
                    print(f"Connected to MT5 | Account: {account.login} | Balance: {account.balance} {account.currency}")
                mt5.symbol_select(SYMBOL_MT5, True)
            else:
                print(f"LIVE mode via Wine MT5 bridge ({MT5_ORDER_DIR})")
            self.load_history_from_csv()
            self.load_warmup_from_mt5()
            self._reconcile_live_on_startup()
            self.save_status()
        elif DATA_SOURCE == "MT5":
            self._report_feed_at_boot()
            self.load_history_from_csv()
            self.load_warmup_from_mt5()
            self.save_status()
        else:
            self.load_history_from_csv()
            self.save_status()

    def load_trade_stats(self):
        """Reload wins/losses/balance/next-trade-num from trades.csv."""
        self.next_trade_num = 1
        self.wins = 0
        self.losses = 0
        self.balance = STARTING_BALANCE

        if not os.path.isfile(TRADES_LOG_PATH):
            prior_closed = 0
            if os.path.isfile(STATUS_FILE_PATH):
                try:
                    with open(STATUS_FILE_PATH) as f:
                        prior_closed = json.load(f).get("total_trades", 0) or 0
                except Exception:
                    pass
            if prior_closed:
                print(f"WARNING: trades.csv is MISSING but status.json shows {prior_closed} "
                      f"closed trades - balance/numbering will RESET to ${STARTING_BALANCE:.2f}/#1!")
            else:
                print(f"No trades.csv found - starting fresh (balance ${STARTING_BALANCE:.2f})")
            return

        migrate_trades_csv(TRADES_LOG_PATH)

        try:
            with open(TRADES_LOG_PATH, newline="") as f:
                reader = csv.DictReader(f)
                max_num = 0
                last_balance = None
                for row in reader:
                    try:
                        num = int(str(row.get("Trade_Num", 0) or 0).replace(",", ""))
                        if num > max_num:
                            max_num = num
                    except (ValueError, TypeError):
                        pass

                    reason = (row.get("Exit_Reason") or "").strip().upper()
                    if reason == "TP":
                        self.wins += 1
                    elif reason == "SL":
                        self.losses += 1

                    try:
                        bal = float(str(row.get("Balance_After", 0) or 0).replace(",", ""))
                        if bal > 0:
                            last_balance = bal
                    except (ValueError, TypeError):
                        pass

                self.next_trade_num = max_num + 1
                if last_balance is not None:
                    self.balance = last_balance

            closed = self.wins + self.losses
            print(f"Loaded trade stats -> next_trade=#{self.next_trade_num} | "
                  f"closed={closed} ({self.wins}W/{self.losses}L) | balance=${self.balance:.2f}")
        except Exception as e:
            print(f"Failed to load trade stats: {e}")
            self.next_trade_num = 1
            self.wins = 0
            self.losses = 0
            self.balance = STARTING_BALANCE

    def restore_open_trade_from_status(self):
        """Restore open trade metadata from status.json after crash/restart."""
        if not os.path.isfile(STATUS_FILE_PATH):
            return
        try:
            with open(STATUS_FILE_PATH) as f:
                data = json.load(f)
        except Exception as e:
            print(f"Could not read status.json for open-trade restore: {e}")
            return

        if not data.get("trade_active"):
            return

        entry = data.get("entry_price")
        sl = data.get("stop_loss")
        tp = data.get("take_profit")
        if entry is None or sl is None or tp is None:
            print("status.json marks trade_active but missing entry/SL/TP - ignoring")
            return

        self.trade_type = data.get("trade_type", "BUY")
        self.entry_price = float(entry)
        self.stop_loss = float(sl)
        self.take_profit = float(tp)
        self.entry_time = data.get("entry_time") or utc_now_str()
        self.current_trade_num = data.get("current_trade_num")
        if self.current_trade_num is None:
            self.current_trade_num = max(1, self.next_trade_num - 1)
        self.entry_rsi = data.get("entry_rsi")
        self.entry_atr = data.get("entry_atr")
        self.entry_wick_ratio = data.get("entry_wick_ratio")
        self.entry_ema_fast = data.get("entry_ema_fast")
        self.entry_ema_slow = data.get("entry_ema_slow")

        if TRADING_MODE == "LIVE":
            self.trade_active = False
            self.trade_active_live = True
            saved_pos = data.get("active_position_id") or data.get("active_ticket")
            self.active_ticket = int(saved_pos) if saved_pos is not None else -1
            self.active_position_id = self.active_ticket if self.active_ticket > 0 else None
        else:
            self.trade_active = True

        print(f"Restored OPEN {self.trade_type} trade #{self.current_trade_num} | "
              f"Entry=${self.entry_price:,.2f} SL=${self.stop_loss:,.2f} TP=${self.take_profit:,.2f}")

    def _live_magic_positions(self) -> list:
        """Return currently open broker positions matching SYMBOL_MT5 and MAGIC_NUMBER."""
        if mt5 is not None:
            try:
                raw = mt5.positions_get(symbol=SYMBOL_MT5)
            except TypeError:
                raw = mt5.positions_get()
            if not raw:
                return []
            return [
                p for p in raw
                if int(getattr(p, "magic", 0) or 0) == MAGIC_NUMBER
                and str(getattr(p, "symbol", SYMBOL_MT5)) == SYMBOL_MT5
            ]

        pos_file = os.path.join(MT5_ORDER_DIR, "positions.json")
        if not os.path.isfile(pos_file):
            return []
        try:
            with open(pos_file, encoding="utf-8") as fh:
                snap = json.load(fh)
            return [
                p for p in (snap.get("positions") or [])
                if int(p.get("magic", MAGIC_NUMBER)) == MAGIC_NUMBER
                and str(p.get("symbol", SYMBOL_MT5)) == SYMBOL_MT5
            ]
        except Exception:
            return []

    def _reconcile_live_on_startup(self):
        """Attach to an open broker position on restart or reconcile an offline close."""
        open_pos = self._live_magic_positions()
        if open_pos:
            p0 = open_pos[0]
            if isinstance(p0, dict):
                pid = int(p0.get("position_id") or 0)
                side = str(p0.get("side") or "BUY")
                px = float(p0.get("price_open") or self.entry_price or 0.0)
                sl = float(p0.get("sl") or self.stop_loss or 0.0)
                tp = float(p0.get("tp") or self.take_profit or 0.0)
                ts = str(p0.get("time_utc") or self.entry_time or utc_now_str())
            else:
                pid = int(getattr(p0, "ticket", 0) or 0)
                side = "BUY" if int(getattr(p0, "type", 0) or 0) == 0 else "SELL"
                px = float(getattr(p0, "price_open", 0.0) or self.entry_price or 0.0)
                sl = float(getattr(p0, "sl", 0.0) or self.stop_loss or 0.0)
                tp = float(getattr(p0, "tp", 0.0) or self.take_profit or 0.0)
                ts = self.entry_time or utc_now_str()

            self.trade_active = False
            self.trade_active_live = True
            self.active_ticket = pid
            self.active_position_id = pid
            self.trade_type = side
            if px > 0:
                self.entry_price = px
            if sl > 0:
                self.stop_loss = sl
            if tp > 0:
                self.take_profit = tp
            self.entry_time = ts
            if self.current_trade_num is None:
                self.current_trade_num = self.next_trade_num
                self.next_trade_num += 1
            return

        if self.trade_active_live:
            self.check_live_exits()

    def _recompute_indicators_from_series(self):
        closes_list = list(self.closes)
        highs_list = list(self.highs)
        lows_list = list(self.lows)
        self.ema_fast = None
        self.ema_slow = None
        self.ema50_history.clear()
        self.tr_list.clear()
        self.rsi_gains.clear()
        self.rsi_losses.clear()
        self.prev_close = None

        for idx, close in enumerate(closes_list):
            high = highs_list[idx]
            low = lows_list[idx]
            if idx + 1 == EMA_FAST:
                self.ema_fast = sum(closes_list[:EMA_FAST]) / EMA_FAST
                self.ema50_history.append(self.ema_fast)
            elif idx + 1 > EMA_FAST and self.ema_fast is not None:
                self.ema_fast = (close * self.k_fast) + (self.ema_fast * (1 - self.k_fast))
                self.ema50_history.append(self.ema_fast)

            if idx + 1 == EMA_SLOW:
                self.ema_slow = sum(closes_list[:EMA_SLOW]) / EMA_SLOW
            elif idx + 1 > EMA_SLOW and self.ema_slow is not None:
                self.ema_slow = (close * self.k_slow) + (self.ema_slow * (1 - self.k_slow))

            if self.prev_close is not None:
                change = close - self.prev_close
                self.rsi_gains.append(max(change, 0))
                self.rsi_losses.append(max(-change, 0))
                tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
                self.tr_list.append(tr)
            self.prev_close = close

        self.calculate_rsi()
        self.calculate_atr()

    def load_history_from_csv(self):
        if not os.path.isfile(LOG_FILE_PATH):
            print("No existing log found. Starting fresh.")
            return
        print(f"Loading history from {LOG_FILE_PATH}...")
        try:
            rows = []
            with open(LOG_FILE_PATH, mode="r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            if not rows:
                return
            recent = rows[-EMA_SLOW:] if len(rows) > EMA_SLOW else rows
            print(f"Found {len(rows)} total rows -> using last {len(recent)}")
            for row in recent:
                try:
                    low = float(str(row["Low"]).replace(",", ""))
                    high = float(str(row.get("High", low)).replace(",", ""))
                    close = float(str(row["Close"]).replace(",", ""))
                    vol_str = row.get("Volume", "1")
                    volume = float(vol_str) if vol_str not in ("Calculating", "", None) else 1.0
                    self.lows.append(low)
                    self.highs.append(high)
                    self.closes.append(close)
                    self.volumes.append(volume)
                    ema50_str = row.get("EMA_50")
                    try:
                        if ema50_str not in ("Calculating", "", None):
                            self.ema50_history.append(float(str(ema50_str).replace(",", "")))
                    except (TypeError, ValueError):
                        pass
                    if self.prev_close is not None:
                        change = close - self.prev_close
                        self.rsi_gains.append(max(change, 0))
                        self.rsi_losses.append(max(-change, 0))
                        tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
                        self.tr_list.append(tr)
                    self.prev_close = close
                except Exception:
                    continue
            closes_list = list(self.closes)
            if len(closes_list) >= EMA_FAST:
                self.ema_fast = sum(closes_list[:EMA_FAST]) / EMA_FAST
                for price in closes_list[EMA_FAST:]:
                    self.ema_fast = (price * self.k_fast) + (self.ema_fast * (1 - self.k_fast))
            if len(closes_list) >= EMA_SLOW:
                self.ema_slow = sum(closes_list[:EMA_SLOW]) / EMA_SLOW
                for price in closes_list[EMA_SLOW:]:
                    self.ema_slow = (price * self.k_slow) + (self.ema_slow * (1 - self.k_slow))
            self.calculate_rsi()
            self.calculate_atr()
            print(f"Loaded {len(self.closes)} candles")
        except Exception as e:
            print(f"Failed to load history: {e}")

    def load_warmup_from_mt5(self):
        """Instant Day-0 M15 warm-up: seed indicators from mt5_warmup_candles.json.

        On M15, waiting 200 live bars takes 50 trading hours (>2 days). Instead,
        tools/mt5_feed.py publishes the last 250 closed M15 bars from XM to
        mt5_warmup_candles.json on startup. Only used when forward_test_log.csv
        has fewer than EMA_SLOW bars AND MT5_WARMUP_FILE lives alongside
        LOG_FILE_PATH / MT5_FEED_FILE (so isolated temp-dir tests never read a
        production warm-up file).
        """
        if len(self.closes) >= EMA_SLOW:
            return
        log_dir = os.path.dirname(os.path.abspath(LOG_FILE_PATH))
        warm_dir = os.path.dirname(os.path.abspath(MT5_WARMUP_FILE))
        if warm_dir != log_dir or not os.path.isfile(MT5_WARMUP_FILE):
            return
        try:
            with open(MT5_WARMUP_FILE, encoding="utf-8") as fh:
                payload = json.load(fh)
            candles = payload.get("candles") if isinstance(payload, dict) else payload
            if not candles or not isinstance(candles, list):
                return
            candles = sorted(candles, key=lambda r: int(r["ts"]))
            # Only prepend/seed if forward_test_log.csv is currently empty
            if len(self.closes) == 0:
                for bar in candles[-EMA_SLOW:]:
                    self.lows.append(float(bar["low"]))
                    self.highs.append(float(bar["high"]))
                    self.closes.append(float(bar["close"]))
                    self.volumes.append(float(bar.get("tick_volume", 1.0)))
                self._recompute_indicators_from_series()
                self._warmup_close_ts = int(candles[-1]["ts"]) + CANDLE_SECONDS
                print(f"Seeded {len(self.closes)} M15 warm-up bars from {MT5_WARMUP_FILE} "
                      f"(EMA200={'ready' if self.ema_slow else 'warming'})")
        except Exception as e:
            print(f"Failed to load MT5 warm-up bars: {e}")

    def calculate_rsi(self):
        if len(self.rsi_gains) < RSI_PERIOD:
            self.rsi = None
            return
        avg_gain = sum(self.rsi_gains) / RSI_PERIOD
        avg_loss = sum(self.rsi_losses) / RSI_PERIOD
        self.rsi = 100.0 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))

    def calculate_atr(self):
        if len(self.tr_list) < ATR_PERIOD:
            self.atr = None
            return
        self.atr = sum(self.tr_list) / ATR_PERIOD

    def update_indicators(self, high, low, close):
        if self.ema_fast is None:
            if len(self.closes) >= EMA_FAST:
                self.ema_fast = sum(list(self.closes)[-EMA_FAST:]) / EMA_FAST
        else:
            self.ema_fast = (close * self.k_fast) + (self.ema_fast * (1 - self.k_fast))
        if self.ema_slow is None:
            if len(self.closes) >= EMA_SLOW:
                self.ema_slow = sum(list(self.closes)[-EMA_SLOW:]) / EMA_SLOW
        else:
            self.ema_slow = (close * self.k_slow) + (self.ema_slow * (1 - self.k_slow))
        if self.prev_close is not None:
            change = close - self.prev_close
            self.rsi_gains.append(max(change, 0))
            self.rsi_losses.append(max(-change, 0))
            self.calculate_rsi()
            tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
            self.tr_list.append(tr)
            self.calculate_atr()
        self.prev_close = close

    def save_status(self):
        closed = self.wins + self.losses
        win_rate = (self.wins / closed * 100) if closed > 0 else 0.0
        active_trade = self.trade_active or self.trade_active_live

        recent = load_recent_trades(50)
        daily_losses = get_daily_sl_count(recent)
        symbol_label = SYMBOL_MT5 if (TRADING_MODE == "LIVE" or DATA_SOURCE == "MT5") else SYMBOL_TD

        data = {
            "symbol": symbol_label,
            "equity": round(self.balance, 2),
            "total_trades": closed,
            "next_trade_num": self.next_trade_num,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(win_rate, 1),
            "trade_active": active_trade,
            "trade_type": self.trade_type if active_trade else None,
            "active_ticket": self.active_ticket if active_trade else None,
            "active_position_id": self.active_position_id if active_trade else None,
            "daily_losses": daily_losses,
            "max_daily_losses": MAX_DAILY_LOSSES,
            "last_update": utc_now_str(),
            "feed": self.feed_status(),
            "rsi": round(self.rsi, 1) if self.rsi else None,
            "ema_fast": round(self.ema_fast, 2) if self.ema_fast else None,
            "ema_slow": round(self.ema_slow, 2) if self.ema_slow else None,
            "atr": round(self.atr, 2) if self.atr else None,
            "entry_price": round(self.entry_price, 2) if active_trade and self.entry_price else None,
            "stop_loss": round(self.stop_loss, 2) if active_trade and self.stop_loss else None,
            "take_profit": round(self.take_profit, 2) if active_trade and self.take_profit else None,
            "entry_time": self.entry_time if active_trade else None,
            "current_trade_num": self.current_trade_num if active_trade else None,
            "entry_rsi": round(self.entry_rsi, 1) if active_trade and self.entry_rsi is not None else None,
            "entry_atr": round(self.entry_atr, 2) if active_trade and self.entry_atr is not None else None,
            "entry_wick_ratio": self.entry_wick_ratio if active_trade else None,
            "entry_ema_fast": round(self.entry_ema_fast, 2) if active_trade and self.entry_ema_fast is not None else None,
            "entry_ema_slow": round(self.entry_ema_slow, 2) if active_trade and self.entry_ema_slow is not None else None,
            "funnel": {
                "candles_evaluated": self.candles_evaluated,
                "tested_floor": self.hit_tested_floor,
                "valid_rejection": self.hit_valid_rejection,
                "held_support": self.hit_held_support,
                "volume_confirmed": self.hit_volume_confirmed,
                "trend_confirmed": self.hit_trend_confirmed,
                "slope_confirmed": self.hit_slope_confirmed,
                "price_near_ema": self.hit_price_near_ema,
                "all_confirmed": self.hit_all,
                "sell_tested_ceiling": self.hit_sell_tested_ceiling,
                "sell_valid_rejection": self.hit_sell_valid_rejection,
                "sell_held_resistance": self.hit_sell_held_resistance,
                "sell_trend_confirmed": self.hit_sell_trend_confirmed,
                "sell_slope_confirmed": self.hit_sell_slope_confirmed,
                "sell_price_near_ema": self.hit_sell_price_near_ema,
                "sell_all_confirmed": self.hit_sell_all,
            },
        }
        try:
            _atomic_write_json(STATUS_FILE_PATH, data)
        except Exception as e:
            print(f"Failed to write status.json: {e}")

    def journal(self, event: str, detail: str):
        """Append an event to the local journal."""
        try:
            file_exists = os.path.isfile(JOURNAL_FILE_PATH)
            os.makedirs(os.path.dirname(JOURNAL_FILE_PATH) or ".", exist_ok=True)
            with open(JOURNAL_FILE_PATH, mode="a", newline="") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Timestamp", "Event", "Detail"])
                writer.writerow([utc_now_str(), event, detail])
        except Exception as e:
            print(f"\nJournal failed: {e}")

    def feed_stale_seconds(self, now_mono: float = None) -> float:
        if self._last_price_mono is None:
            return 0.0
        if now_mono is None:
            now_mono = time.monotonic()
        return max(0.0, now_mono - self._last_price_mono)

    def maybe_alert_stale_feed(
        self,
        stale_seconds: float,
        source: str = "price ticks",
        recovery: str = "check the feed",
        event: str = "STALE_FEED",
        detail: str = None,
    ) -> bool:
        if self._last_stale_alert_mono is not None and \
                time.monotonic() - self._last_stale_alert_mono < STALE_ALERT_COOLDOWN_SECONDS:
            return False
        self._last_stale_alert_mono = time.monotonic()
        if detail is None:
            detail = f"no {source} for {stale_seconds / 60:.0f} min - {recovery}"
        self.journal(event, detail)
        return True

    def feed_status(self) -> dict:
        """Compact feed diagnosis for status.json (reason, freeze instant, sidecar)."""
        diag = self._feed_diag
        if not diag:
            return {"ok": None, "reason": "NOT_CHECKED", "path": MT5_FEED_FILE,
                    "checked_utc": utc_now_str()}
        health = self._feed_health or {}
        return {
            "ok": bool(diag.get("ok")),
            "reason": diag.get("reason"),
            "detail": diag.get("detail"),
            "path": diag.get("path"),
            "symbol": diag.get("symbol"),
            "candle_open_utc": diag.get("candle_open_utc"),
            "candle_age_s": round(diag["candle_age_s"], 1) if diag.get("candle_age_s") is not None else None,
            "frozen_since_utc": diag.get("frozen_since_utc"),
            "file_age_s": round(diag["file_age_s"], 1) if diag.get("file_age_s") is not None else None,
            "sidecar": {
                "status": health.get("status"),
                "heartbeat_age_s": round(health["age_s"], 1) if health.get("age_s") is not None else None,
                "fresh": health.get("fresh"),
                "tick_age_s": health.get("tick_age_s"),
                "server_offset_s": health.get("server_offset_s"),
                "last_error": health.get("last_error"),
            } if health.get("present") else None,
            "checked_utc": utc_now_str(),
        }

    def mt5_feed_is_stale(self, rates, now_mono=None, now_epoch=None) -> bool:
        """Alert and return True when the MT5 M15 feed is unusable during open market hours.

        "Stale" now means one of: the sidecar's feed file is missing/unreadable, it
        carries a foreign symbol, its bar is in the future, or the bar is older than
        STALE_FEED_SECONDS. The reported age is always the *feed's* age (bar close or
        file mtime), never this process's uptime, so an engine restart can no longer
        turn a 890-minute outage into a fresh-looking "20 min" alert.
        """
        diag = self._feed_diag or feed_health.diagnose_feed_file(
            MT5_FEED_FILE,
            expected_symbol=SYMBOL_MT5,
            now_epoch=now_epoch,
            candle_seconds=CANDLE_SECONDS,
            max_age_s=STALE_FEED_SECONDS,
        )
        if diag.get("ok"):
            return False

        now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc) if now_epoch is not None else None
        if is_market_quiet(now_dt):
            return True

        self._feed_health = feed_health.diagnose_sidecar_health(MT5_FEED_HEALTH_FILE, now_epoch)
        event, detail = feed_health.format_feed_problem(
            diag,
            self._feed_health,
            engine_uptime_s=self._engine_uptime_seconds(),
        )
        if self.maybe_alert_stale_feed(
            diag.get("candle_age_s") or 0.0,
            event=event,
            detail=detail,
        ):
            print(f"\nFeed problem ({event}): {detail}", flush=True)
        return True

    def _engine_uptime_seconds(self) -> float:
        return max(0.0, time.monotonic() - self._start_mono)

    def log_candle(self, timestamp, o, h, l, c, ratio, tick_count, vol_ma, dynamic_floor, ema_f, ema_s, tested, rejected, held, vol_conf, trend_conf, rsi_val, atr_val):
        file_exists = os.path.isfile(LOG_FILE_PATH)
        torn = ensure_appendable_log(LOG_FILE_PATH)
        if torn:
            print(f"WARNING: {LOG_FILE_PATH}: {torn} - run "
                  f"`python3 tools/repair_log.py --check` (readiness fails closed on it)",
                  flush=True)
        os.makedirs(os.path.dirname(LOG_FILE_PATH) or ".", exist_ok=True)
        with open(LOG_FILE_PATH, mode="a", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "Timestamp", "Open", "High", "Low", "Close", "Wick_Ratio", "Volume", "Vol_MA",
                    "Vol_Confirmed", "Dynamic_Floor", "EMA_50", "EMA_200", "Trend_Confirmed",
                    "Tested_Floor", "Valid_Rejection", "Held_Floor", "RSI", "ATR",
                ],
            )
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "Timestamp": timestamp,
                "Open": f"{o:.2f}",
                "High": f"{h:.2f}",
                "Low": f"{l:.2f}",
                "Close": f"{c:.2f}",
                "Wick_Ratio": f"{ratio:.1%}",
                "Volume": f"{tick_count:.0f}",
                "Vol_MA": f"{vol_ma:.1f}" if vol_ma else "Calculating",
                "Vol_Confirmed": str(vol_conf),
                "Dynamic_Floor": f"{dynamic_floor:.2f}" if dynamic_floor else "Calculating",
                "EMA_50": f"{ema_f:.2f}" if ema_f else "Calculating",
                "EMA_200": f"{ema_s:.2f}" if ema_s else "Calculating",
                "Trend_Confirmed": str(trend_conf),
                "Tested_Floor": str(tested),
                "Valid_Rejection": str(rejected),
                "Held_Floor": str(held),
                "RSI": f"{rsi_val:.1f}" if rsi_val else "Calculating",
                "ATR": f"{atr_val:.2f}" if atr_val else "Calculating",
            })

    def log_trade(self, exit_price, exit_reason, profit, exit_time=None):
        file_exists = os.path.isfile(TRADES_LOG_PATH)
        if file_exists:
            migrate_trades_csv(TRADES_LOG_PATH)
        os.makedirs(os.path.dirname(TRADES_LOG_PATH) or ".", exist_ok=True)
        with open(TRADES_LOG_PATH, mode="a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TRADES_FIELDNAMES)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "Trade_Num": self.current_trade_num,
                "Trade_Type": self.trade_type,
                "Entry_Time": self.entry_time,
                "Exit_Time": exit_time or utc_now_str(),
                "Entry_Price": f"{self.entry_price:.2f}",
                "Stop_Loss": f"{self.stop_loss:.2f}",
                "Take_Profit": f"{self.take_profit:.2f}",
                "Exit_Price": f"{exit_price:.2f}",
                "Exit_Reason": exit_reason,
                "Profit": f"{profit:.2f}",
                "Balance_After": f"{self.balance:.2f}",
                "RSI_At_Entry": f"{self.entry_rsi:.1f}" if self.entry_rsi is not None else "",
                "ATR_At_Entry": f"{self.entry_atr:.2f}" if self.entry_atr is not None else "",
                "Wick_Ratio_At_Entry": f"{self.entry_wick_ratio:.1%}" if self.entry_wick_ratio is not None else "",
                "EMA50_At_Entry": f"{self.entry_ema_fast:.2f}" if self.entry_ema_fast is not None else "",
                "EMA200_At_Entry": f"{self.entry_ema_slow:.2f}" if self.entry_ema_slow is not None else "",
            })

    def close_simulated_trade(self, exit_price: float, exit_reason: str) -> bool:
        """Finalize one simulated exit and write the ledger/status exactly once."""
        if not self.trade_active:
            return False
        if exit_reason not in ("TP", "SL"):
            raise ValueError(f"Unsupported exit reason: {exit_reason}")

        if self.trade_type == "BUY":
            profit = (exit_price - self.entry_price) * LOT_SIZE * CONTRACT_SIZE
        else:
            profit = (self.entry_price - exit_price) * LOT_SIZE * CONTRACT_SIZE

        self.balance += profit
        if exit_reason == "TP":
            self.wins += 1
        else:
            self.losses += 1

        self.trade_active = False
        self.log_trade(exit_price=exit_price, exit_reason=exit_reason, profit=profit)

        pnl_text = f"+${profit:,.2f}" if profit >= 0 else f"-${abs(profit):,.2f}"
        self.journal(
            "TRADE_EXIT",
            f"{exit_reason} HIT ({self.trade_type} #{self.current_trade_num}) | "
            f"Exit: {exit_price:,.2f} ({pnl_text}) | Equity: ${self.balance:,.2f}",
        )
        self.save_status()
        return True

    def check_position_on_closed_candle(self, o: float, h: float, l: float, c: float) -> bool:
        """Evaluate ONE closed M15 candle against an already-open simulated trade."""
        if not self.trade_active:
            return False

        if self.trade_type == "BUY":
            stop_hit = l <= self.stop_loss
            target_hit = h >= self.take_profit
            if stop_hit:
                exit_price = o if o <= self.stop_loss else self.stop_loss
                return self.close_simulated_trade(exit_price=exit_price, exit_reason="SL")
            if target_hit:
                return self.close_simulated_trade(exit_price=self.take_profit, exit_reason="TP")

        elif self.trade_type == "SELL":
            stop_hit = h >= self.stop_loss
            target_hit = l <= self.take_profit
            if stop_hit:
                exit_price = o if o >= self.stop_loss else self.stop_loss
                return self.close_simulated_trade(exit_price=exit_price, exit_reason="SL")
            if target_hit:
                return self.close_simulated_trade(exit_price=self.take_profit, exit_reason="TP")

        return False

    def process_closed_candle(self, o, h, l, c, tick_count, candle_ts=None):
        """Exit old trades on the bar first, then evaluate the close for a new signal."""
        if TRADING_MODE == "LIVE":
            self.check_live_exits()
        else:
            self.check_position_on_closed_candle(o, h, l, c)
        timestamp = None
        if candle_ts is not None:
            close_ts = int(candle_ts) + CANDLE_SECONDS
            timestamp = datetime.fromtimestamp(close_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.evaluate_candle(o, h, l, c, tick_count, timestamp=timestamp)

    def evaluate_candle(self, o, h, l, c, tick_count, timestamp=None):
        candle_range = h - l
        if candle_range <= 0:
            return

        self.candles_evaluated += 1

        body_bottom = min(o, c)
        body_top = max(o, c)
        lower_wick = body_bottom - l
        upper_wick = h - body_top

        lower_wick_ratio = lower_wick / candle_range
        upper_wick_ratio = upper_wick / candle_range

        valid_buy_rejection = lower_wick_ratio >= WICK_RATIO_TARGET
        valid_sell_rejection = upper_wick_ratio >= WICK_RATIO_TARGET

        dynamic_floor, dynamic_ceiling, volume_ma = None, None, None
        tested_floor, held_support = False, False
        tested_ceiling, held_resistance = False, False
        volume_confirmed = False

        if len(self.closes) >= LOOKBACK_PERIOD:
            dynamic_floor = min(list(self.lows)[-LOOKBACK_PERIOD:])
            dynamic_ceiling = max(list(self.highs)[-LOOKBACK_PERIOD:])
            volume_ma = sum(list(self.volumes)[-LOOKBACK_PERIOD:]) / LOOKBACK_PERIOD

            tested_floor = l <= (dynamic_floor * (1 + FLOOR_BUFFER_PCT))
            held_support = c > dynamic_floor

            tested_ceiling = h >= (dynamic_ceiling * (1 - FLOOR_BUFFER_PCT))
            held_resistance = c < dynamic_ceiling

            volume_confirmed = tick_count >= (volume_ma * VOLUME_SPIKE_MULTIPLIER) if volume_ma else True

        if self.ema_fast is not None and self.ema_slow is not None:
            buy_trend_confirmed = self.ema_fast > self.ema_slow
            sell_trend_confirmed = self.ema_fast < self.ema_slow
        elif self.ema_fast is not None:
            buy_trend_confirmed = c > self.ema_fast
            sell_trend_confirmed = c < self.ema_fast
        else:
            buy_trend_confirmed = False
            sell_trend_confirmed = False

        if len(self.closes) < EMA_SLOW:
            if not self.warmup_logged or len(self.closes) % 30 == 0:
                print(f"Warming up... {len(self.closes)}/{EMA_SLOW}")
                self.warmup_logged = True
        else:
            print(
                f"Floor:{dynamic_floor:,.2f} Ceil:{dynamic_ceiling:,.2f} | "
                f"EMA50:{self.ema_fast:,.2f} EMA200:{self.ema_slow:,.2f} "
                f"| RSI:{self.rsi} ATR:{self.atr:,.2f} | C:{c:,.2f}"
            )
            print(f"   [BUY] Tested:{tested_floor} | Rej:{valid_buy_rejection} ({lower_wick_ratio:.0%}) | Held:{held_support} | Trend:{buy_trend_confirmed}")
            print(f"   [SELL] Tested:{tested_ceiling} | Rej:{valid_sell_rejection} ({upper_wick_ratio:.0%}) | Held:{held_resistance} | Trend:{sell_trend_confirmed}")

        ts = timestamp or utc_now_str()
        self.log_candle(
            ts, o, h, l, c, lower_wick_ratio, tick_count, volume_ma, dynamic_floor,
            self.ema_fast, self.ema_slow, tested_floor, valid_buy_rejection, held_support,
            volume_confirmed, buy_trend_confirmed, self.rsi, self.atr,
        )
        self.lows.append(l)
        self.highs.append(h)
        self.volumes.append(tick_count)
        self.closes.append(c)
        self.update_indicators(h, l, c)
        self.save_status()

        buy_slope_confirmed = False
        sell_slope_confirmed = False
        if self.ema_fast is not None and len(self.ema50_history) >= EMA_SLOPE_LOOKBACK:
            buy_slope_confirmed = self.ema_fast > self.ema50_history[-EMA_SLOPE_LOOKBACK]
            sell_slope_confirmed = self.ema_fast < self.ema50_history[-EMA_SLOPE_LOOKBACK]
        if self.ema_fast is not None:
            self.ema50_history.append(self.ema_fast)

        buy_price_near_ema = False
        sell_price_near_ema = False
        if self.ema_fast is not None and self.atr is not None:
            buy_price_near_ema = c >= (self.ema_fast - (self.atr * MAX_BELOW_EMA_ATR))
            sell_price_near_ema = c <= (self.ema_fast + (self.atr * MAX_BELOW_EMA_ATR))

        vol_ok = volume_confirmed if REQUIRE_VOLUME_CONFIRM else True
        atr_ok = (self.atr is not None and self.atr > MIN_ATR)

        buy_trend_ok = buy_trend_confirmed if REQUIRE_TREND_CONFIRM else True
        buy_slope_ok = buy_slope_confirmed if REQUIRE_EMA_SLOPE else True
        buy_rsi_ok = (self.rsi is not None and RSI_MIN < self.rsi < RSI_MAX)

        sell_trend_ok = sell_trend_confirmed if REQUIRE_TREND_CONFIRM else True
        sell_slope_ok = sell_slope_confirmed if REQUIRE_EMA_SLOPE else True
        sell_rsi_ok = (self.rsi is not None and (100.0 - RSI_MAX) < self.rsi < (100.0 - RSI_MIN))

        in_trade = self.trade_active or self.trade_active_live

        if dynamic_floor is not None:
            if tested_floor:
                self.hit_tested_floor += 1
            if valid_buy_rejection:
                self.hit_valid_rejection += 1
            if held_support:
                self.hit_held_support += 1
            if volume_confirmed:
                self.hit_volume_confirmed += 1
            if buy_trend_confirmed:
                self.hit_trend_confirmed += 1
            if buy_slope_ok:
                self.hit_slope_confirmed += 1
            if buy_price_near_ema:
                self.hit_price_near_ema += 1

            if tested_ceiling:
                self.hit_sell_tested_ceiling += 1
            if valid_sell_rejection:
                self.hit_sell_valid_rejection += 1
            if held_resistance:
                self.hit_sell_held_resistance += 1
            if sell_trend_confirmed:
                self.hit_sell_trend_confirmed += 1
            if sell_slope_ok:
                self.hit_sell_slope_confirmed += 1
            if sell_price_near_ema:
                self.hit_sell_price_near_ema += 1

        buy_signal = (
            dynamic_floor is not None
            and self.ema_slow is not None
            and self.atr is not None
            and tested_floor
            and valid_buy_rejection
            and held_support
            and vol_ok
            and buy_trend_ok
            and buy_slope_ok
            and buy_price_near_ema
            and buy_rsi_ok
            and atr_ok
            and not in_trade
        )

        sell_signal = (
            dynamic_ceiling is not None
            and self.ema_slow is not None
            and self.atr is not None
            and tested_ceiling
            and valid_sell_rejection
            and held_resistance
            and vol_ok
            and sell_trend_ok
            and sell_slope_ok
            and sell_price_near_ema
            and sell_rsi_ok
            and atr_ok
            and not in_trade
        )

        if buy_signal:
            self.hit_all += 1
            allow, reason = should_take_trade(
                current_atr=self.atr,
                current_price=c,
                ema_fast=self.ema_fast,
                ema_slow=self.ema_slow,
            )

            if not allow:
                msg = (f"US100Cash BUY SETUP SKIPPED\nReason: `{reason}`\n"
                       f"Price: `{c:,.2f}` | RSI: `{self.rsi:.1f}` | ATR: `{self.atr:,.2f}`")
                self.journal("BUY_SKIP", msg)
                print(f"\nUS100Cash Trade skipped -> {reason}\n")
                return

            if TRADING_MODE == "LIVE":
                self.execute_live_trade("BUY", c, lower_wick_ratio, ts)
            else:
                self.execute_simulated_trade("BUY", c, lower_wick_ratio, ts)

        elif sell_signal:
            self.hit_sell_all += 1
            allow, reason = should_take_trade(
                current_atr=self.atr,
                current_price=c,
                ema_fast=self.ema_fast,
                ema_slow=self.ema_slow,
            )

            if not allow:
                msg = (f"US100Cash SELL SETUP SKIPPED\nReason: `{reason}`\n"
                       f"Price: `{c:,.2f}` | RSI: `{self.rsi:.1f}` | ATR: `{self.atr:,.2f}`")
                self.journal("SELL_SKIP", msg)
                print(f"\nUS100Cash Trade skipped -> {reason}\n")
                return

            if TRADING_MODE == "LIVE":
                self.execute_live_trade("SELL", c, upper_wick_ratio, ts)
            else:
                self.execute_simulated_trade("SELL", c, upper_wick_ratio, ts)

    def _execute_live_via_bridge(self, direction: str, c: float, wick_ratio: float, ts: str):
        """Linux host -> Wine tools/mt5_executor.py atomic file-bridge execution."""
        if self._live_magic_positions():
            print("Bridge LIVE guard: broker already holds an open position for our magic; skipping duplicate entry")
            self.journal("LIVE_DUPLICATE_BLOCKED", f"{direction} skipped - position already open")
            return

        sl_dist = self.atr * ATR_SL_MULT
        tp_dist = self.atr * ATR_TP_MULT
        sl_est = (c - sl_dist) if direction == "BUY" else (c + sl_dist)
        tp_est = (c + tp_dist) if direction == "BUY" else (c - tp_dist)
        iid = uuid.uuid4().hex
        intent = {
            "intent_id": iid,
            "ts_utc": utc_now_str(),
            "ts_epoch": time.time(),
            "action": "OPEN",
            "side": direction,
            "symbol": SYMBOL_MT5,
            "volume": LOT_SIZE,
            "sl": round(sl_est, 2),
            "tp": round(tp_est, 2),
            "atr": round(self.atr, 4),
            "sl_dist": round(sl_dist, 4),
            "tp_dist": round(tp_dist, 4),
            "magic": MAGIC_NUMBER,
            "trade_num": self.next_trade_num,
            "comment": f"Nasdaq Live {direction}",
        }
        intent_path = os.path.join(MT5_ORDER_DIR, "intent.json")
        result_path = os.path.join(MT5_ORDER_DIR, "result.json")
        _atomic_write_json(intent_path, intent)

        deadline = time.monotonic() + MT5_BRIDGE_TIMEOUT_S
        res = None
        while time.monotonic() < deadline:
            if os.path.isfile(result_path):
                try:
                    with open(result_path, encoding="utf-8") as fh:
                        cand = json.load(fh)
                    if isinstance(cand, dict) and cand.get("intent_id") == iid:
                        res = cand
                        break
                except Exception:
                    pass
            time.sleep(0.2)

        if res is None:
            self.journal("ORDER_BRIDGE_TIMEOUT", f"intent_id={iid} timed out after {MT5_BRIDGE_TIMEOUT_S:.1f}s")
            print(f"Bridge order timeout waiting for result.json (intent_id={iid})")
            return

        status = str(res.get("status") or "").upper()
        if status != "FILLED":
            self.journal("ORDER_FAILED", f"Bridge status={status} | Msg: {res.get('message')}")
            print(f"Bridge order not filled ({status}): {res.get('message')}")
            return

        pos_id = int(res.get("position_id") or res.get("order") or 0)
        fill_px = float(res.get("fill_price") or c)
        self.current_trade_num = self.next_trade_num
        self.next_trade_num += 1
        self.active_ticket = pos_id
        self.active_position_id = pos_id
        self.trade_active = False
        self.trade_active_live = True
        self.trade_type = direction
        self.entry_price = fill_px
        self.stop_loss = float(res.get("sl") or sl_est)
        self.take_profit = float(res.get("tp") or tp_est)
        self.entry_time = ts
        self.entry_rsi = self.rsi
        self.entry_atr = self.atr
        self.entry_wick_ratio = wick_ratio
        self.entry_ema_fast = self.ema_fast
        self.entry_ema_slow = self.ema_slow

        self.journal(
            "LIVE_ENTRY",
            f"{direction} EXECUTED (bridge) | Pos: {pos_id} | Trade #{self.current_trade_num} | "
            f"Entry: {self.entry_price:,.2f} | SL: {self.stop_loss:,.2f} | TP: {self.take_profit:,.2f}",
        )
        self.save_status()

    def execute_live_trade(self, direction: str, c: float, wick_ratio: float, ts: str):
        print(f"\nALL CONDITIONS MET! PREPARING LIVE {direction} ORDER...")
        if self.trade_active_live or self.trade_active:
            print("Already in trade; skipping duplicate live order.")
            return
        if self.live_unknown_order:
            print("Prior order_send returned None (unknown broker state); refusing retry until reconciled.")
            return

        if mt5 is None:
            self._execute_live_via_bridge(direction, c, wick_ratio, ts)
            return

        # 1. Duplicate-exposure check on the broker
        if self._live_magic_positions():
            print(f"Broker already holds an open position for magic {MAGIC_NUMBER}; refusing duplicate order")
            self.journal("LIVE_DUPLICATE_BLOCKED", f"{direction} blocked - position already open on MT5")
            return

        symbol_info = mt5.symbol_info(SYMBOL_MT5)
        if symbol_info is None:
            print(f"symbol_info({SYMBOL_MT5}) returned None")
            return
        point = float(getattr(symbol_info, "point", 0.01) or 0.01)

        # 2. Volume check (never silently upsize)
        vmin = float(getattr(symbol_info, "volume_min", 0.01) or 0.01)
        vmax = float(getattr(symbol_info, "volume_max", 100.0) or 100.0)
        if LOT_SIZE + 1e-9 < vmin or LOT_SIZE - 1e-9 > vmax:
            msg = f"LOT_SIZE {LOT_SIZE} outside broker [{vmin}, {vmax}]"
            print(msg)
            self.journal("ORDER_FAILED", msg)
            return

        # 3. Live quote & spread guard
        tick = mt5.symbol_info_tick(SYMBOL_MT5)
        if tick is None or float(getattr(tick, "bid", 0) or 0) <= 0 or float(getattr(tick, "ask", 0) or 0) < float(tick.bid):
            print("Failed to get valid tick data")
            return

        spread = float(tick.ask) - float(tick.bid)
        sl_dist = self.atr * ATR_SL_MULT
        tp_dist = self.atr * ATR_TP_MULT
        if spread > MAX_SPREAD_POINTS or (sl_dist > 0 and spread > MAX_SPREAD_TO_1R * sl_dist):
            msg = f"Spread {spread:.2f} exceeds guard (max {MAX_SPREAD_POINTS:.2f} pts / {MAX_SPREAD_TO_1R:.0%} of 1R)"
            print(f"Live order skipped: {msg}")
            self.journal("LIVE_SPREAD_SKIP", msg)
            return

        # 4. Recompute SL/TP from live bid/ask so 1:2 R:R holds on actual entry quote
        if direction == "BUY":
            price = float(tick.ask)
            order_type = mt5.ORDER_TYPE_BUY
            sl_price = round((price - sl_dist) / point) * point
            tp_price = round((price + tp_dist) / point) * point
        else:
            price = float(tick.bid)
            order_type = mt5.ORDER_TYPE_SELL
            sl_price = round((price + sl_dist) / point) * point
            tp_price = round((price - tp_dist) / point) * point

        # 5. Decode symbol_info().filling_mode bitmask -> ORDER_FILLING_* enum
        filling_type = _select_mt5_filling(symbol_info, mt5)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": SYMBOL_MT5,
            "volume": LOT_SIZE,
            "type": order_type,
            "price": price,
            "sl": sl_price,
            "tp": tp_price,
            "deviation": LIVE_DEVIATION_POINTS,
            "magic": MAGIC_NUMBER,
            "comment": f"Nasdaq Live {direction}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }
        print(f"   Sending: {direction} {LOT_SIZE} {SYMBOL_MT5} @ {price:,.2f} | SL: {sl_price:,.2f} | TP: {tp_price:,.2f}")

        result = mt5.order_send(request)
        if result is None:
            print("order_send returned None (terminal timeout/disconnect) - checking broker positions before continuing")
            self.journal("ORDER_UNKNOWN", "order_send returned None; reconciling via positions_get")
            after_pos = self._live_magic_positions()
            if after_pos:
                pid = int(getattr(after_pos[0], "ticket", 0) or 0)
                self.current_trade_num = self.next_trade_num
                self.next_trade_num += 1
                self.active_ticket = pid
                self.active_position_id = pid
                self.trade_active_live = True
                self.trade_type = direction
                self.entry_price = float(getattr(after_pos[0], "price_open", price) or price)
                self.stop_loss = sl_price
                self.take_profit = tp_price
                self.entry_time = ts
                self.entry_rsi = self.rsi
                self.entry_atr = self.atr
                self.entry_wick_ratio = wick_ratio
                self.entry_ema_fast = self.ema_fast
                self.entry_ema_slow = self.ema_slow
                self.save_status()
            else:
                self.live_unknown_order = True
            return

        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Order failed: {result.retcode} - {result.comment}")
            self.journal("ORDER_FAILED", f"Code: {result.retcode} | Msg: {result.comment}")
            return

        pos_id = int(result.order)
        fill_px = float(getattr(result, "price", 0.0) or price)
        after_pos = self._live_magic_positions()
        if after_pos:
            pos_id = int(getattr(after_pos[-1], "ticket", pos_id) or pos_id)
            fill_px = float(getattr(after_pos[-1], "price_open", fill_px) or fill_px)

        self.current_trade_num = self.next_trade_num
        self.next_trade_num += 1
        self.active_ticket = pos_id
        self.active_position_id = pos_id
        self.trade_active = False
        self.trade_active_live = True
        self.trade_type = direction
        self.entry_price = fill_px
        self.stop_loss = sl_price
        self.take_profit = tp_price
        self.entry_time = ts
        self.entry_rsi = self.rsi
        self.entry_atr = self.atr
        self.entry_wick_ratio = wick_ratio
        self.entry_ema_fast = self.ema_fast
        self.entry_ema_slow = self.ema_slow

        print(f"ORDER SUCCESS! Position: {self.active_ticket} | Trade #{self.current_trade_num}")
        self.journal(
            "LIVE_ENTRY",
            f"{direction} EXECUTED | Ticket: {self.active_ticket} | Trade #{self.current_trade_num} | "
            f"Entry: {fill_px:,.2f} | SL: {sl_price:,.2f} | TP: {tp_price:,.2f}",
        )
        self.save_status()

    def execute_simulated_trade(self, direction: str, c: float, wick_ratio: float, ts: str):
        self.trade_active = True
        self.trade_type = direction
        self.current_trade_num = self.next_trade_num
        self.next_trade_num += 1

        self.entry_price = c
        if direction == "BUY":
            self.stop_loss = c - (self.atr * ATR_SL_MULT)
            self.take_profit = c + (self.atr * ATR_TP_MULT)
        else:
            self.stop_loss = c + (self.atr * ATR_SL_MULT)
            self.take_profit = c - (self.atr * ATR_TP_MULT)

        self.entry_time = ts
        self.entry_rsi = self.rsi
        self.entry_atr = self.atr
        self.entry_wick_ratio = wick_ratio
        self.entry_ema_fast = self.ema_fast
        self.entry_ema_slow = self.ema_slow

        self.journal(
            "TRADE_ENTRY",
            f"{direction} SETUP #{self.current_trade_num} | Entry: {self.entry_price:,.2f} | "
            f"RSI: {self.rsi:.1f} | ATR: {self.atr:,.2f} | SL: {self.stop_loss:,.2f} | "
            f"TP: {self.take_profit:,.2f}",
        )
        print(f"\nAlert sent -> {direction} Trade #{self.current_trade_num}\n")
        self.save_status()

    def check_position(self, price: float):
        if not self.trade_active:
            return

        if self.trade_type == "BUY":
            if price >= self.take_profit:
                self.close_simulated_trade(exit_price=price, exit_reason="TP")
            elif price <= self.stop_loss:
                self.close_simulated_trade(exit_price=price, exit_reason="SL")

        elif self.trade_type == "SELL":
            if price <= self.take_profit:
                self.close_simulated_trade(exit_price=price, exit_reason="TP")
            elif price >= self.stop_loss:
                self.close_simulated_trade(exit_price=price, exit_reason="SL")

    def _record_live_exit(self, exit_price: float, net_profit: float, exit_time: str = None, broker_balance: float = None):
        reason = "TP" if net_profit > 0 else "SL"
        if net_profit > 0:
            self.wins += 1
        else:
            self.losses += 1

        if broker_balance is not None and broker_balance > 0:
            self.balance = float(broker_balance)
        else:
            self.balance += net_profit

        self.trade_active = False
        self.trade_active_live = False
        self.active_ticket = None
        self.active_position_id = None

        self.log_trade(exit_price=exit_price, exit_reason=reason, profit=net_profit, exit_time=exit_time)
        self.journal(
            "LIVE_EXIT",
            f"{reason} HIT ({self.trade_type} #{self.current_trade_num}) | "
            f"Exit: {exit_price:,.2f} | Net Profit: ${net_profit:,.2f} | Balance: ${self.balance:,.2f}",
        )
        self.save_status()

    def check_live_exits(self):
        """Reconcile broker-side SL/TP/manual exits by position_id (not entry order ticket)."""
        if not (self.trade_active_live or self.trade_active):
            return

        target_pos = self.active_position_id or (self.active_ticket if (self.active_ticket and self.active_ticket > 0) else None)

        # Bridge path (Linux host -> Wine tools/mt5_executor.py)
        if mt5 is None:
            pos_file = os.path.join(MT5_ORDER_DIR, "positions.json")
            if not os.path.isfile(pos_file):
                return
            try:
                with open(pos_file, encoding="utf-8") as fh:
                    snap = json.load(fh)
            except Exception:
                return
            open_ids = {int(p.get("position_id") or 0) for p in (snap.get("positions") or [])}
            if target_pos and target_pos in open_ids:
                return
            for c in reversed(snap.get("closed_recent") or []):
                cid = int(c.get("position_id") or 0)
                if target_pos is None or cid == target_pos:
                    self._record_live_exit(
                        exit_price=float(c.get("price") or 0.0),
                        net_profit=float(c.get("profit") or 0.0),
                        exit_time=c.get("time_utc"),
                        broker_balance=snap.get("balance"),
                    )
                    return
            return

        # Direct MetaTrader5 path
        open_pos = self._live_magic_positions()
        if target_pos is not None and any(int(getattr(p, "ticket", 0) or 0) == target_pos for p in open_pos):
            return
        if target_pos is None and open_pos:
            pid = int(getattr(open_pos[0], "ticket", 0) or 0)
            self.active_ticket = pid
            self.active_position_id = pid
            return

        deals = ()
        if target_pos is not None:
            try:
                deals = mt5.history_deals_get(position=target_pos) or ()
            except TypeError:
                deals = ()
        if not deals:
            dt_from = datetime.now(timezone.utc) - timedelta(days=30)
            dt_to = datetime.now(timezone.utc) + timedelta(days=1)
            try:
                all_deals = mt5.history_deals_get(dt_from, dt_to) or ()
            except TypeError:
                all_deals = mt5.history_deals_get() or ()
            if target_pos is not None:
                deals = tuple(d for d in all_deals if int(getattr(d, "position_id", 0) or 0) == target_pos)
            else:
                # Legacy status.json without active_ticket: attach to the newest closed position
                out_deals = [d for d in all_deals if int(getattr(d, "entry", -1)) == mt5.DEAL_ENTRY_OUT]
                if out_deals:
                    inferred_pos = int(getattr(out_deals[-1], "position_id", 0) or 0)
                    deals = tuple(d for d in all_deals if int(getattr(d, "position_id", 0) or 0) == inferred_pos)

        if not deals:
            return

        closing = [d for d in deals if int(getattr(d, "entry", -1)) in (mt5.DEAL_ENTRY_OUT, getattr(mt5, "DEAL_ENTRY_INOUT", 2))]
        if not closing:
            return

        last_out = closing[-1]
        net_profit = sum(_deal_net_profit(d) for d in deals)
        exit_price = float(getattr(last_out, "price", 0.0) or 0.0)
        acct = mt5.account_info()
        # Only override balance from account_info if broker tracks it dynamically;
        # otherwise apply net_profit delta cleanly.
        new_bal = self.balance + net_profit
        if acct is not None and hasattr(acct, "balance"):
            acct_bal = float(acct.balance)
            if abs(acct_bal - self.balance) > 1e-6 and acct_bal != STARTING_BALANCE:
                new_bal = acct_bal
        self._record_live_exit(exit_price=exit_price, net_profit=net_profit, broker_balance=new_bal)

    def aggregate_tick(self, price: float):
        bucket_now = int(datetime.now(timezone.utc).timestamp() // CANDLE_SECONDS)
        if self.current_minute is None:
            self.current_minute = bucket_now
        if bucket_now != self.current_minute:
            if self.tick_pool:
                o, h, l, c = self.tick_pool[0], max(self.tick_pool), min(self.tick_pool), self.tick_pool[-1]
                self.evaluate_candle(o, h, l, c, len(self.tick_pool))
            self.tick_pool.clear()
            self.current_minute = bucket_now
        self.tick_pool.append(price)

    def on_event(self, event):
        if event.get("event") != "price":
            return
        try:
            self._last_price_mono = time.monotonic()
            price = float(event["price"])
            self.check_position(price)
            self.aggregate_tick(price)
            print(f"{price:,.2f} | ticks: {len(self.tick_pool)}   ", end="\r")
            sys.stdout.flush()
        except Exception:
            return

    def run_live(self):
        """LIVE runner: supports direct MetaTrader5 OR Linux Wine bridge (mt5feed + mt5_executor)."""
        if mt5 is None:
            print(f"Nasdaq Engine LIVE (Wine bridge) starting for {SYMBOL_MT5} (M15)...")
            self._run_sidecar_candle_loop(live_bridge=True)
            return

        print(f"Nasdaq Engine LIVE (direct MT5) starting for {SYMBOL_MT5} (M15)...")
        tf_m15 = getattr(mt5, "TIMEFRAME_M15", 15)
        candles_fetched = 0
        while True:
            try:
                self.check_live_exits()
                rates = mt5.copy_rates_from_pos(SYMBOL_MT5, tf_m15, 1, 1)
                if rates is None or len(rates) == 0:
                    time.sleep(5)
                    continue
                candle_ts = int(rates[0]["time"])
                if candle_ts == self._last_live_candle_ts:
                    time.sleep(5)
                    continue
                self._last_live_candle_ts = candle_ts
                last_candle = rates[0]
                o, h, l, c, vol = (
                    float(last_candle["open"]),
                    float(last_candle["high"]),
                    float(last_candle["low"]),
                    float(last_candle["close"]),
                    int(last_candle["tick_volume"]),
                )
                self.evaluate_candle(o, h, l, c, vol)
                self.check_live_exits()
                if candles_fetched % 4 == 0:
                    print(f"Heartbeat: Processed M15 candle @ "
                          f"{datetime.fromtimestamp(candle_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC "
                          f"| Close: {c:,.2f}")
                candles_fetched += 1
                time.sleep(5)
            except KeyboardInterrupt:
                print("\nShutting down...")
                mt5.shutdown()
                break
            except Exception as e:
                print(f"\nError: {e}")
                time.sleep(10)

    def read_mt5_feed(self, now_epoch=None):
        """Read + validate the latest closed M15 candle published by tools/mt5_feed.py.

        Records a full diagnosis on the instance (`self._feed_diag`) so the stale
        guard can report *why* a bar is missing instead of only that none arrived.
        Returns None when the file is missing, unreadable, has a foreign schema or
        carries a different symbol - a foreign bar must never reach the engine.
        """
        self._feed_diag = feed_health.diagnose_feed_file(
            MT5_FEED_FILE,
            expected_symbol=SYMBOL_MT5,
            now_epoch=now_epoch,
            candle_seconds=CANDLE_SECONDS,
            max_age_s=STALE_FEED_SECONDS,
        )
        if not self._feed_diag.get("usable"):
            return None
        return self._feed_diag.get("rates")

    def _feed_log(self, message: str, every_s: float = 60.0):
        """Print a feed problem at most once per `every_s` (journal has its own cooldown)."""
        now_mono = time.monotonic()
        if self._last_feed_log_mono is not None and now_mono - self._last_feed_log_mono < every_s:
            return
        self._last_feed_log_mono = now_mono
        print(message, flush=True)

    def mt5_next_candle(self, rates, last_processed_close_ts):
        """Return (open_ts, o, h, l, c, tick_volume) for a new closed M15 bar."""
        ts = latest_closed_candle_ts(rates)
        watermark = max(int(last_processed_close_ts or 0), int(self._warmup_close_ts or 0))
        if ts is None or ts + CANDLE_SECONDS <= watermark:
            return None
        r = rates[0]
        return (ts, float(r["open"]), float(r["high"]), float(r["low"]),
                float(r["close"]), int(r["tick_volume"]))

    def _run_sidecar_candle_loop(self, live_bridge: bool = False):
        mode_label = "LIVE (MT5 bridge)" if live_bridge else "FORWARD TEST (MT5 M15 feed)"
        print(f"Nasdaq Engine {mode_label} starting for {SYMBOL_MT5}...")
        last_processed_close_ts = max(last_logged_candle_close_ts(LOG_FILE_PATH), self._warmup_close_ts)
        self._last_price_mono = time.monotonic()
        self._report_feed_at_boot()
        try:
            while True:
                if live_bridge:
                    self.check_live_exits()
                rates = self.read_mt5_feed()
                stale = self.mt5_feed_is_stale(rates)

                if rates is None or len(rates) == 0:
                    if not is_market_quiet():
                        diag = self._feed_diag or {}
                        self._feed_log(
                            f"MT5: feed unusable [{diag.get('reason')}] {diag.get('detail') or ''} "
                            f"(file: {MT5_FEED_FILE}) - retrying in 5s; "
                            "diagnose with: python3 tools/feed_doctor.py",
                        )
                elif stale:
                    diag = self._feed_diag or {}
                    self._feed_log(f"MT5: feed stale [{diag.get('reason')}] {diag.get('detail') or ''} "
                                   "- not evaluating; diagnose with: python3 tools/feed_doctor.py")
                else:
                    candle = self.mt5_next_candle(rates, last_processed_close_ts)
                    if candle is not None:
                        ts, o, h, l, c, vol = candle
                        try:
                            self.process_closed_candle(o, h, l, c, vol, candle_ts=ts)
                            last_processed_close_ts = ts + CANDLE_SECONDS
                            self._last_price_mono = time.monotonic()
                        except Exception as e:
                            print(f"\nError evaluating candle @ {ts}: {e}", flush=True)
                        if ts % 3600 == 0:
                            print(f"Heartbeat: M15 candle @ "
                                  f"{datetime.fromtimestamp(ts, tz=timezone.utc):%Y-%m-%d %H:%M} UTC "
                                  f"| close {c:,.2f}", flush=True)

                time.sleep(5)
        except KeyboardInterrupt:
            print("\nShutting down...")

    def _report_feed_at_boot(self):
        """Print one honest feed summary at startup - never just "file found"."""
        diag = feed_health.diagnose_feed_file(
            MT5_FEED_FILE,
            expected_symbol=SYMBOL_MT5,
            candle_seconds=CANDLE_SECONDS,
            max_age_s=STALE_FEED_SECONDS,
        )
        self._feed_diag = diag
        self._feed_health = feed_health.diagnose_sidecar_health(MT5_FEED_HEALTH_FILE)
        if not diag.get("exists"):
            print(f"NOTE: {MT5_FEED_FILE} not found yet - is the mt5feed-nasdaq sidecar running?",
                  flush=True)
            return
        sidecar = feed_health.summarize_sidecar(self._feed_health)
        print(f"MT5 feed: {MT5_FEED_FILE} [{diag.get('reason')}] {diag.get('detail')}"
              + (f" | {sidecar}" if sidecar else ""), flush=True)

    def run_mt5_test(self):
        self._run_sidecar_candle_loop(live_bridge=False)

    def run_forward_test(self):
        print(f"Nasdaq Engine FORWARD TEST (Twelve Data {SYMBOL_TD}) starting...")
        if not TWELVE_DATA_KEY or TDClient is None:
            print("TWELVE_DATA_API_KEY or twelvedata package missing")
            sys.exit(1)
        while True:
            ws = None
            try:
                td = TDClient(apikey=TWELVE_DATA_KEY)
                ws = td.websocket(on_event=self.on_event)
                ws.subscribe([SYMBOL_TD])
                ws.connect()
                print(f"Connected to Twelve Data ({SYMBOL_TD})\n")
                self._last_price_mono = time.monotonic()
                while True:
                    try:
                        ws.heartbeat()
                        time.sleep(15)
                        stale = self.feed_stale_seconds()
                        if stale > STALE_FEED_SECONDS:
                            if not is_market_quiet():
                                print(f"\nStale feed: no price events for {stale:.0f}s - forcing reconnect")
                                self.maybe_alert_stale_feed(
                                    stale,
                                    source="price ticks",
                                    recovery="forcing a Twelve Data WebSocket reconnect",
                                )
                                break
                    except Exception as e:
                        print(f"\nConnection issue: {e}")
                        print("Reconnecting in 10s...")
                        time.sleep(10)
                        break
            except KeyboardInterrupt:
                print("\nShutting down...")
                break
            except Exception as e:
                print(f"\nError: {e}")
                print("Restarting in 15s...")
                time.sleep(15)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def run(self):
        if TRADING_MODE == "LIVE":
            self.run_live()
        elif DATA_SOURCE == "MT5":
            self.run_mt5_test()
        else:
            self.run_forward_test()


if __name__ == "__main__":
    engine = NasdaqEngine()
    engine.run()
