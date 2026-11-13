#!/usr/bin/env python3
"""
End-to-end offline regression & smoke test suite for the Nasdaq-100 (US100Cash)
M15 Auto-Trading Bot (/opt/nasdaq).

Runs entirely in temporary directories without network access, Telegram, or a
live MetaTrader 5 terminal.

Scenarios:
  A) Uptrend + floor rejection on US100Cash M15  -> BUY triggers & closes at TP ($0.10/pt)
  B) Decline + floor rejection                   -> BUY blocked by regime gates
  C) Downtrend + ceiling rejection               -> SELL triggers & closes at TP
  D) Daily loss circuit breaker                  -> 3 SLs or -$15 realized daily loss halts day
  E) Restart state persistence                   -> EMA50 history, open trade & ledger stats restore
  F) trades.csv schema drift                     -> auto-migrates 15-field -> 16-field & keeps backup
  G) Stale-feed guard + America/New_York schedule -> DST-aware CME quiet hours + macro/open blackouts
  H) MT5 M15 feed + 250-bar startup warm-up      -> DATA_SOURCE="MT5" default, mt5_warmup_candles.json
                                                    seeds EMA200 immediately, closed-bar deduplication
  I) replay_lib + history_to_log + backtest.py   -> SELL direction lock, ratchet, candles_m15.csv replay
  J) handoff_check tolerance policy              -> 5 trades / 48 h (192 M15 bars), --spread 0.25 rule
  K) tools/mt5_quotes.py read-only XM logger     -> AST trading-API ban, PII-free spec, M15 candles_m15.csv,
                                                    server DST healing, CME quiet-hours guard, exit 2 watchdog
  L) tools/xm_quote_report.py                    -> US100Cash 0.10-lot cost/parity/spec, market-closed vs
                                                    mt5quotes-nasdaq STALE line, no --spread CLI flag
  M) MT5 closed-candle simulated exits           -> BUY/SELL SL & TP, tie->SL, gap-open fill, startup reconcile
  N) tools/reconcile_stale_trade.py              -> report-only vs --apply backup & 0.10-lot reconciliation
  O) pressure.py + dashboard.py + autosync.sh    -> 60-bar M15 tape pressure, Flask read-only UI, static STALE
  P) tools/mt5_executor.py + LIVE bridge         -> dormant/demo locks, idempotency, None quarantine, HALT flatten,
                                                    credential-free deploy/*.service units
  Q) Day-0 ledger guards (tools/win_rate_report.py) -> §6 SIGNAL CENSUS and §8 cost grid never divide by
                                                    zero on empty / signal-free logs (0 takeable early-out)
  R) Feed diagnosis (feed_health.py + tools/feed_doctor.py) -> reason taxonomy (missing/unreadable/schema/
                                                    foreign-symbol/future/frozen), freeze-instant alerts that
                                                    survive an engine restart, counter-reset forensics
  S) Readiness report + dashboard              -> shared gates, fail-closed evidence and feed, deep links
                                                    (Flask-specific tests require Flask to be installed)

Usage: python3 tools/smoke_test.py
"""
import ast
import collections
import csv
import importlib.util
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

# Stub optional external network/env packages when running outside venv
for _mod_name in ("requests", "dotenv", "twelvedata"):
    if _mod_name not in sys.modules:
        _mod = types.ModuleType(_mod_name)
        if _mod_name == "requests":
            _mod.post = lambda *_a, **_k: None
        elif _mod_name == "dotenv":
            _mod.load_dotenv = lambda *_a, **_k: False
        elif _mod_name == "twelvedata":
            _mod.TDClient = object
        sys.modules[_mod_name] = _mod

import engine        # noqa: E402
import trade_filter  # noqa: E402

FAILURES = []


def check(label, cond, extra=""):
    print(f"  [{'OK' if cond else 'FAIL'}] {label}" + (f"  ({extra})" if extra else ""))
    if not cond:
        FAILURES.append(label)


def configure_tmp(tmp):
    engine.LOG_FILE_PATH = os.path.join(tmp, "forward_test_log.csv")
    engine.STATUS_FILE_PATH = os.path.join(tmp, "status.json")
    engine.TRADES_LOG_PATH = os.path.join(tmp, "trades.csv")
    engine.MT5_FEED_FILE = os.path.join(tmp, "mt5_last_candle.json")
    engine.MT5_WARMUP_FILE = os.path.join(tmp, "mt5_warmup_candles.json")
    engine.MT5_ORDER_DIR = os.path.join(tmp, "mt5_bridge")
    engine.JOURNAL_FILE_PATH = os.path.join(tmp, "journal.csv")
    trade_filter.TRADES_LOG = engine.TRADES_LOG_PATH
    trade_filter.SKIP_LOG = os.path.join(tmp, "skipped_trades.csv")


def candles_uptrend(n, start=20000.0):
    """Oscillating US100Cash M15 uptrend: RSI ~62, ATR ~38 pts (~0.19%), EMA50 rising."""
    out, p = [], start
    for i in range(n):
        d = (18.0, 18.0, -16.0, 18.0, -16.0)[i % 5]
        o = p
        c = p + d
        out.append((o, max(o, c) + 8.0, min(o, c) - 8.0, c))
        p = c
    return out


def candles_decline(n, start=21000.0):
    """Oscillating US100Cash M15 decline: RSI ~38, ATR ~38 pts (~0.18%), EMA50 falling."""
    out, p = [], start
    for i in range(n):
        d = (-18.0, -18.0, 16.0, -18.0, 16.0)[i % 5]
        o = p
        c = p + d
        out.append((o, max(o, c) + 8.0, min(o, c) - 8.0, c))
        p = c
    return out


def run_candles(eng, candles, ts_start):
    ts = ts_start
    with redirect_stdout(io.StringIO()):
        for (o, h, l, c) in candles:
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            eng.evaluate_candle(o, h, l, c, tick_count=300, timestamp=ts_str)
            eng.check_position(c)
            ts += timedelta(minutes=15)


def rejection_dip_buy(eng):
    """Tests 20-bar M15 floor (within 0.12% buffer), closes above floor, >=15% lower wick."""
    floor = min(list(eng.lows)[-engine.LOOKBACK_PERIOD:])
    p = list(eng.closes)[-1]
    o = p
    c = max(floor + 4.0, p - 6.0)
    low = floor - 10.0
    high = max(o, c) + 5.0
    return (o, high, low, c)


def rejection_dip_sell(eng):
    """Tests 20-bar M15 ceiling, closes below ceiling, >=15% upper wick."""
    ceil = max(list(eng.highs)[-engine.LOOKBACK_PERIOD:])
    p = list(eng.closes)[-1]
    o = p
    c = min(ceil - 4.0, p + 6.0)
    high = ceil + 10.0
    low = min(o, c) - 5.0
    return (o, high, low, c)


_real_is_in_blackout = trade_filter.is_in_blackout

# ==============================================================================
# Scenario A: Uptrend + floor rejection -> BUY trade + TP at $0.10/pt
# ==============================================================================
print("Scenario A: US100Cash M15 uptrend + floor rejection -> expect BUY trade & TP")
tmp_a = tempfile.mkdtemp(prefix="nasdaq_smoke_a_")
configure_tmp(tmp_a)
trade_filter.is_in_blackout = lambda now=None: (False, "")

with redirect_stdout(io.StringIO()):
    eng_a = engine.NasdaqEngine()
check("A: US100Cash M15 defaults set (900s, 0.10 lot, $0.10/pt)",
      engine.SYMBOL_MT5 == "US100Cash" and engine.CANDLE_SECONDS == 900
      and abs(engine.LOT_SIZE - 0.10) < 1e-9 and abs(engine.CONTRACT_SIZE - 1.0) < 1e-9)

t0_a = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
run_candles(eng_a, candles_uptrend(240), t0_a)
dip_a = rejection_dip_buy(eng_a)
run_candles(eng_a, [dip_a], t0_a + timedelta(minutes=15 * 240))
check("A: BUY trade triggered on US100Cash", eng_a.trade_active and eng_a.trade_type == "BUY",
      f"trade_active={eng_a.trade_active} type={eng_a.trade_type} entry={eng_a.entry_price}")

p_now = list(eng_a.closes)[-1]
up = [(p_now + i * 25, p_now + i * 25 + 15, p_now + i * 25 - 5, p_now + i * 25 + 20) for i in range(1, 12)]
run_candles(eng_a, up, t0_a + timedelta(minutes=15 * 241))
check("A: BUY trade closed with TP and positive $0.10/pt profit",
      not eng_a.trade_active and eng_a.wins == 1 and eng_a.balance > 200.0,
      f"wins={eng_a.wins} bal={eng_a.balance:.2f}")
shutil.rmtree(tmp_a, ignore_errors=True)

# ==============================================================================
# Scenario B: Decline + floor rejection -> BUY blocked
# ==============================================================================
print("\nScenario B: US100Cash decline + floor rejection -> expect NO BUY trade")
tmp_b = tempfile.mkdtemp(prefix="nasdaq_smoke_b_")
configure_tmp(tmp_b)
with redirect_stdout(io.StringIO()):
    eng_b = engine.NasdaqEngine()
run_candles(eng_b, candles_decline(240), datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc))
dip_b = rejection_dip_buy(eng_b)
run_candles(eng_b, [dip_b], datetime(2026, 10, 22, 0, 0, tzinfo=timezone.utc))
check("B: no BUY trade in established decline", not eng_b.trade_active)
shutil.rmtree(tmp_b, ignore_errors=True)

# ==============================================================================
# Scenario C: Downtrend + ceiling rejection -> SELL trade & TP
# ==============================================================================
print("\nScenario C: US100Cash downtrend + ceiling rejection -> expect SELL trade & TP")
tmp_c = tempfile.mkdtemp(prefix="nasdaq_smoke_c_")
configure_tmp(tmp_c)
with redirect_stdout(io.StringIO()):
    eng_c = engine.NasdaqEngine()
t0_c = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
run_candles(eng_c, candles_decline(240, 21000.0), t0_c)
dip_c = rejection_dip_sell(eng_c)
run_candles(eng_c, [dip_c], t0_c + timedelta(minutes=15 * 240))
check("C: SELL trade triggered", eng_c.trade_active and eng_c.trade_type == "SELL",
      f"trade_active={eng_c.trade_active} type={eng_c.trade_type}")

p_now = list(eng_c.closes)[-1]
down = [(p_now - i * 25, p_now - i * 25 + 5, p_now - i * 25 - 20, p_now - i * 25 - 15) for i in range(1, 12)]
run_candles(eng_c, down, t0_c + timedelta(minutes=15 * 241))
check("C: SELL trade closed with TP", not eng_c.trade_active and eng_c.wins == 1, f"wins={eng_c.wins}")

# ==============================================================================
# Scenario D: Daily loss circuit breaker (3 SLs OR -$15 realized daily loss)
# ==============================================================================
print("\nScenario D: daily loss circuit breaker (3 SLs or -$15.00 daily cap)")
trade_filter.is_in_blackout = _real_is_in_blackout
trades_sim = [
    {"Trade_Num": "1", "Trade_Type": "BUY", "Entry_Time": "2026-10-20 13:00:00", "Exit_Time": "2026-10-20 13:15:00", "Exit_Reason": "SL", "Profit": "-4.00"},
    {"Trade_Num": "2", "Trade_Type": "BUY", "Entry_Time": "2026-10-20 14:00:00", "Exit_Time": "2026-10-20 14:15:00", "Exit_Reason": "SL", "Profit": "-4.00"},
    {"Trade_Num": "3", "Trade_Type": "SELL", "Entry_Time": "2026-10-20 15:30:00", "Exit_Time": "2026-10-20 15:45:00", "Exit_Reason": "SL", "Profit": "-4.00"},
]
now_d = datetime(2026, 10, 20, 16, 0, tzinfo=timezone.utc)
sl_count = trade_filter.get_daily_sl_count(trades_sim, now_d)
halted, halt_reason = trade_filter.check_daily_loss_limit(trades_sim, now_d)
check("D: daily SL count equals 3", sl_count == 3, f"count={sl_count}")
check("D: 3-SL circuit breaker triggered", halted and "Daily Loss Limit Reached" in halt_reason, halt_reason)

big_loss_day = [
    {"Trade_Num": "1", "Trade_Type": "BUY", "Entry_Time": "2026-10-20 13:00:00", "Exit_Time": "2026-10-20 13:15:00", "Exit_Reason": "SL", "Profit": "-9.00"},
    {"Trade_Num": "2", "Trade_Type": "SELL", "Entry_Time": "2026-10-20 14:00:00", "Exit_Time": "2026-10-20 14:15:00", "Exit_Reason": "SL", "Profit": "-7.50"},
]
halted_usd, usd_reason = trade_filter.check_daily_loss_limit(big_loss_day, now_d)
check("D: -$15.00 daily dollar cap halts even with <3 SLs", halted_usd and "$15" in usd_reason, usd_reason)
check("D: busy-day window >= 100 & LOOKBACK == 30",
      trade_filter.DAY_WINDOW >= 100 and trade_filter.LOOKBACK == 30)

# ==============================================================================
# Scenario E: Restart state persistence (reuses tmp_c)
# ==============================================================================
print("\nScenario E: restart state persistence")
with redirect_stdout(io.StringIO()):
    eng_e = engine.NasdaqEngine()
check("E: EMA50 history seeded from M15 CSV",
      len(eng_e.ema50_history) >= engine.EMA_SLOPE_LOOKBACK, f"{len(eng_e.ema50_history)} loaded")
check("E: trade stats loaded from trades.csv", eng_e.wins == 1 and eng_e.losses == 0)
shutil.rmtree(tmp_c, ignore_errors=True)

# ==============================================================================
# Scenario F: trades.csv schema drift auto-migration
# ==============================================================================
print("\nScenario F: trades.csv schema drift (15-field -> 16-field auto-migration)")
OLD_HEADER = ["Trade_Num", "Entry_Time", "Exit_Time", "Entry_Price", "Stop_Loss", "Take_Profit",
              "Exit_Price", "Exit_Reason", "Profit", "Balance_After", "RSI_At_Entry",
              "ATR_At_Entry", "Wick_Ratio_At_Entry", "EMA50_At_Entry", "EMA200_At_Entry"]
tmp_f = tempfile.mkdtemp(prefix="nasdaq_smoke_f_")
configure_tmp(tmp_f)
with open(engine.TRADES_LOG_PATH, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(OLD_HEADER)
    w.writerow(["1", "2026-10-20 14:00:00", "2026-10-20 14:30:00", "20,500.00", "20,420.00", "20,660.00",
                "20,660.00", "TP", "16.00", "216.00", "55.0", "40.00", "50.0%", "20,480.00", "20,400.00"])
    w.writerow(["2", "SELL", "2026-10-20 15:00:00", "2026-10-20 15:15:00", "20450.00", "20530.00",
                "20290.00", "20530.00", "SL", "-8.00", "208.00", "45.0", "40.00", "55.6%", "20470.00", "20510.00"])

with redirect_stdout(io.StringIO()):
    eng_f = engine.NasdaqEngine()
with open(engine.TRADES_LOG_PATH, newline="") as f:
    rdr = csv.DictReader(f)
    rows_f = list(rdr)
    hdr_f = rdr.fieldnames
check("F: header migrated to 16-field schema", hdr_f == engine.TRADES_FIELDNAMES)
check("F: old 15-field row backfilled as BUY and SELL row aligned",
      rows_f[0]["Trade_Type"] == "BUY" and rows_f[1]["Trade_Type"] == "SELL" and rows_f[1]["Exit_Reason"] == "SL")
check("F: backup created and re-migration is idempotent",
      os.path.exists(engine.TRADES_LOG_PATH + ".bak-pre-migration")
      and engine.migrate_trades_csv(engine.TRADES_LOG_PATH) is False)
shutil.rmtree(tmp_f, ignore_errors=True)

# ==============================================================================
# Scenario G: Stale-feed guard + America/New_York market hours & blackouts
# ==============================================================================
print("\nScenario G: stale-feed guard + America/New_York DST-aware market hours & blackouts")
tmp_g = tempfile.mkdtemp(prefix="nasdaq_smoke_g_")
configure_tmp(tmp_g)
with redirect_stdout(io.StringIO()):
    eng_g = engine.NasdaqEngine()

eng_g._last_price_mono = time.monotonic() - (engine.STALE_FEED_SECONDS + 60)
stale = eng_g.feed_stale_seconds()
check("G: >20-min gap detected as stale on M15", stale > engine.STALE_FEED_SECONDS, f"{stale:.0f}s")
check("G: stale alert fires once then rate-limits",
      eng_g.maybe_alert_stale_feed(stale) is True and eng_g.maybe_alert_stale_feed(stale) is False)

# CME schedule checks across EDT (Oct 2026, UTC-4) and EST (Nov 2026, UTC-5)
check("G: Saturday 12:00 UTC is QUIET (CME closed weekend)",
      engine.is_market_quiet(datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)))
check("G: Tuesday 21:30 UTC (17:30 EDT) is QUIET (daily CME maintenance break)",
      engine.is_market_quiet(datetime(2026, 10, 20, 21, 30, tzinfo=timezone.utc)))
check("G: Tuesday 14:30 UTC (10:30 EDT RTH) is OPEN",
      not engine.is_market_quiet(datetime(2026, 10, 20, 14, 30, tzinfo=timezone.utc)))
# After US DST ends (2026-11-10 = EST = UTC-5): 17:30 EST = 22:30 UTC -> QUIET; 21:30 UTC = 16:30 EST -> OPEN!
check("G: DST shift handled: 2026-11-10 22:30 UTC (17:30 EST) is QUIET, 20:30 UTC (15:30 EST) is OPEN",
      engine.is_market_quiet(datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc))
      and not engine.is_market_quiet(datetime(2026, 11, 10, 20, 30, tzinfo=timezone.utc)))

# Session blackouts (08:30 ET macro, 09:30 ET cash open, 17:00 ET CME rollover)
b_macro, lbl_macro = trade_filter.is_in_blackout(datetime(2026, 10, 20, 12, 30, tzinfo=timezone.utc))  # 08:30 EDT
b_open, lbl_open = trade_filter.is_in_blackout(datetime(2026, 10, 20, 13, 32, tzinfo=timezone.utc))    # 09:32 EDT
b_clear, _ = trade_filter.is_in_blackout(datetime(2026, 10, 20, 15, 0, tzinfo=timezone.utc))           # 11:00 EDT
check("G: 08:30 ET macro & 09:30 ET cash open blackouts block entries; 11:00 ET is clear",
      b_macro and "08:30" in lbl_macro and b_open and "09:30" in lbl_open and not b_clear)

# Wine/Windows Python ships no IANA database -> ZoneInfo("America/New_York") raises
# ZoneInfoNotFoundError. trade_filter.py must fall back to the stdlib fixed US DST
# rule (2nd Sunday of March / 1st Sunday of November) and keep identical verdicts.
class GNoTzdata:
    def __new__(cls, *_a, **_k):
        raise trade_filter.ZoneInfoNotFoundError("no tzdata in the Wine python")


_saved_zi = trade_filter.ZoneInfo
trade_filter.ZoneInfo = GNoTzdata
try:
    g_fb_tz = trade_filter.load_ny_tz()
finally:
    trade_filter.ZoneInfo = _saved_zi
_saved_ny = trade_filter.NY_TZ
trade_filter.NY_TZ = g_fb_tz
try:
    fb_quiet = (trade_filter.is_market_quiet(datetime(2026, 10, 20, 21, 30, tzinfo=timezone.utc))
                and trade_filter.is_market_quiet(datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc))
                and not trade_filter.is_market_quiet(datetime(2026, 11, 10, 20, 30, tzinfo=timezone.utc)))
    fb_lbl = trade_filter.blackout_label_for_utc(datetime(2026, 11, 10, 14, 32, tzinfo=timezone.utc))  # 09:32 EST
finally:
    trade_filter.NY_TZ = _saved_ny
check("G: ZoneInfoNotFoundError (Wine python without tzdata) -> stdlib fixed-rule US Eastern fallback",
      isinstance(g_fb_tz, trade_filter.USEasternFallback)
      and datetime(2026, 10, 20, 21, 30, tzinfo=timezone.utc).astimezone(g_fb_tz).strftime("%H:%M %Z") == "17:30 EDT"
      and datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc).astimezone(g_fb_tz).strftime("%H:%M %Z") == "17:30 EST")
check("G: fixed-rule fallback keeps CME quiet hours & 09:30 ET blackout identical to zoneinfo",
      bool(fb_quiet) and fb_lbl is not None and "09:30" in fb_lbl, str(fb_lbl))
shutil.rmtree(tmp_g, ignore_errors=True)

# ==============================================================================
# Scenario H: MT5 M15 sidecar feed + 250-bar startup warm-up seeding
# ==============================================================================
print("\nScenario H: MT5 M15 sidecar feed + 250-bar mt5_warmup_candles.json seeding")
tmp_h = tempfile.mkdtemp(prefix="nasdaq_smoke_h_")
configure_tmp(tmp_h)

check("H: default DATA_SOURCE is MT5", engine.DATA_SOURCE == "MT5", f"DATA_SOURCE={engine.DATA_SOURCE}")

fake_mt5_h = types.ModuleType("MetaTrader5")
fake_mt5_h.TIMEFRAME_M1 = 1
fake_mt5_h.TIMEFRAME_M5 = 5
fake_mt5_h.TIMEFRAME_M15 = 15
sys.modules["MetaTrader5"] = fake_mt5_h
feed_spec = importlib.util.spec_from_file_location("mt5_feed_under_test", os.path.join(HERE, "mt5_feed.py"))
feed_mod = importlib.util.module_from_spec(feed_spec)
feed_spec.loader.exec_module(feed_mod)

check("H: mt5_feed defaults to US100Cash on M15",
      feed_mod.SYMBOL == "US100Cash" and feed_mod.TIMEFRAME_NAME == "M15")
check("H: fresh +3h tick estimates UTC offset",
      feed_mod.estimate_server_offset(100000 + 10800 - 1, 100000) == 10800)

# Build a 250-bar warm-up payload and verify engine boots warm immediately!
base_srv = 1792497600 + 10800  # 2026-10-20 12:00 UTC in +3h server time
warm_rates = [
    {"time": base_srv + i * 900, "open": 20200.0 + i, "high": 20215.0 + i,
     "low": 20190.0 + i, "close": 20208.0 + i, "tick_volume": 450}
    for i in range(250)
]


class AmbiguousTruthRates(list):
    """Match NumPy arrays returned by MT5: iterable, but not truth-testable."""

    def __bool__(self):
        raise ValueError("The truth value of an array with more than one element is ambiguous")


w_payload = feed_mod.build_warmup_payload(AmbiguousTruthRates(warm_rates), 10800,
                                          symbol="US100Cash", timeframe="M15")
check("H: MT5 NumPy-style rate arrays do not trigger ambiguous truth-value errors",
      len(w_payload["candles"]) == 250)
with open(engine.MT5_WARMUP_FILE, "w") as f:
    json.dump(w_payload, f)

with redirect_stdout(io.StringIO()):
    eng_h = engine.NasdaqEngine()
check("H: 250-bar mt5_warmup_candles.json warms EMA200, EMA50, RSI & ATR immediately on boot",
      len(eng_h.closes) == 200 and eng_h.ema_slow is not None and eng_h.atr is not None,
      f"closes={len(eng_h.closes)} ema200={eng_h.ema_slow} atr={eng_h.atr}")

# Publish next M15 closed bar via mt5_last_candle.json
next_open_utc = (base_srv - 10800) + 250 * 900
with open(engine.MT5_FEED_FILE, "w") as f:
    json.dump({
        "symbol": "US100Cash", "timeframe": "M15", "ts": next_open_utc,
        "open": 20460.0, "high": 20480.0, "low": 20450.0, "close": 20472.0,
        "tick_volume": 510, "updated_at": "2026-10-23 02:45:02",
    }, f)
r1 = eng_h.read_mt5_feed()
c1 = eng_h.mt5_next_candle(r1, eng_h._warmup_close_ts)
check("H: new M15 closed candle accepted after warm-up watermark",
      c1 is not None and c1[0] == next_open_utc and c1[5] == 510)
with redirect_stdout(io.StringIO()):
    eng_h.process_closed_candle(*c1[1:], candle_ts=c1[0])
last_close_ts = engine.last_logged_candle_close_ts(engine.LOG_FILE_PATH)
check("H: logged close timestamp is open_ts + 900s and bar is not re-evaluated",
      last_close_ts == next_open_utc + 900 and eng_h.mt5_next_candle(r1, last_close_ts) is None)
shutil.rmtree(tmp_h, ignore_errors=True)

# ==============================================================================
# Scenario I: replay_lib + history_to_log (candles_m15.csv) + backtest.py
# ==============================================================================
print("\nScenario I: replay_lib walk direction + history_to_log candles_m15.csv + backtest.py")
import replay_lib as R       # noqa: E402
import history_to_log as HL  # noqa: E402
import backtest as BT        # noqa: E402

side_dt = datetime(2026, 10, 20, 14, 0)
ENTRY_I, ATR_I = 20500.0, 40.0   # 1R = 80 pts ($8.00 at 0.10 lot), TP = 160 pts ($16.00)


def bar_i(i, o, h, l, c):
    return dict(dt=side_dt + timedelta(minutes=15 * i), o=o, h=h, l=l, c=c)


out_stp, r_stp, _, _ = R.walk(ENTRY_I, "SELL", ATR_I, [bar_i(1, 20400, 20410, 20330, 20335)])
out_ssl, r_ssl, _, _ = R.walk(ENTRY_I, "SELL", ATR_I, [bar_i(1, 20520, 20595, 20510, 20590)])
out_tie, _, _, _ = R.walk(ENTRY_I, "BUY", ATR_I, [bar_i(1, 20500, 20680, 20400, 20600)])
check("I: SELL TP (+2R), SELL SL (-1R), and ambiguous-bar SL-first tie-break work on M15",
      out_stp == "TP" and abs(r_stp - 2.0) < 1e-9 and out_ssl == "SL" and abs(r_ssl + 1.0) < 1e-9
      and out_tie == "SL")
check("I: r_money scales by LOT_SIZE=0.10 ($8.00 at 40-pt ATR)",
      abs(R.r_money(40.0) - 8.0) < 1e-9)

tmp_i = tempfile.mkdtemp(prefix="nasdaq_smoke_i_")
shadow_csv = os.path.join(tmp_i, "candles_m15.csv")
with open(shadow_csv, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["bar_close_utc", "open", "high", "low", "close", "tick_volume", "spread_pts", "time_srv", "offset_s"])
    pts = candles_uptrend(260, 20000.0)
    t_bar = datetime(2026, 10, 19, 0, 15)
    for (o, h, l, c) in pts:
        w.writerow([t_bar.strftime("%Y-%m-%d %H:%M:%S"), f"{o:.2f}", f"{h:.2f}", f"{l:.2f}", f"{c:.2f}",
                    320, 250, 0, 10800])
        t_bar += timedelta(minutes=15)
bt_log = os.path.join(tmp_i, "bt_log.csv")
with redirect_stdout(io.StringIO()):
    rc_bt = BT.main([shadow_csv, "--spread", "0.25", "--boot", "200", "--save-log", bt_log])
    rc_sc = HL.selfcheck(bt_log)
check("I: backtest.py ingests xm_data/candles_m15.csv directly & history_to_log --selfcheck is FAITHFUL",
      rc_bt == 0 and rc_sc == 0)
shutil.rmtree(tmp_i, ignore_errors=True)

# ==============================================================================
# Scenario J: handoff_check tolerance policy (5 trades / 48 h = 192 M15 bars)
# ==============================================================================
print("\nScenario J: handoff_check tolerance policy (M15 BAR_TOLERANCE = 192, $0.25 spread)")
import handoff_check as HC  # noqa: E402

J_DOC = {
    "as_of_utc": "2026-10-26 14:00", "data_collection": "10",
    "closed_trades": "20", "wins_losses": "9W/11L", "win_rate_pct": "45.0",
    "engine_ledger_usd": "242.00", "true_equity_usd": "242.00",
    "live_era_trades": "20", "live_era_net_usd": "+37.00",
    "log_covered_trades": "20", "log_bars": "400",
    "log_last_bar_utc": "2026-10-26 14:00", "skip_rows": "12",
    "open_trade": "BUY #21 @ 20510.0", "spread_usd_per_trade": "0.25",
}


def j_stale(doc=None, **live_over):
    live = dict(J_DOC)
    live.update(live_over)
    rows = HC.compare(doc or J_DOC, live)
    return sorted(k for k, _d, _l, verdict, _n in rows if verdict == "STALE")


check("J: BAR_TOLERANCE == MAX_AGE_HOURS * 4 == 192 M15 bars",
      HC.TRADE_TOLERANCE == 5 and HC.MAX_AGE_HOURS == 48 and HC.BAR_TOLERANCE == 192)
check("J: +1 M15 bar and 47 h of M15 bars are normal drift",
      j_stale(log_bars="401", log_last_bar_utc="2026-10-26 14:15") == []
      and j_stale(log_bars=str(400 + 47 * 4), log_last_bar_utc="2026-10-28 13:00") == [])
check("J: 49 h of M15 bars or 6 trades behind or mismatched spread is STALE",
      "log_last_bar_utc" in j_stale(log_bars=str(400 + 49 * 4), log_last_bar_utc="2026-10-28 15:00")
      and "closed_trades" in j_stale(closed_trades="26", wins_losses="12W/14L", win_rate_pct="46.2",
                                     live_era_trades="26", log_covered_trades="26")
      and j_stale(spread_usd_per_trade="0.40") == ["spread_usd_per_trade"])

# ==============================================================================
# Scenario K: tools/mt5_quotes.py read-only XM US100Cash logger
# ==============================================================================
print("\nScenario K: tools/mt5_quotes.py read-only XM US100Cash M15 logger")
import mt5_quotes as Q  # noqa: E402

K_T0 = 1792504800  # Tuesday 2026-10-20 14:00:00 UTC (10:00 EDT - open market)
K_SECRETS = ("123456789", "Jane Doe", "XMGlobal-MT5 7", "99999.5", "C:\\Users\\jane")


class KClock:
    def __init__(self, t):
        self.t = float(t)

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


class KMT5(types.ModuleType):
    TIMEFRAME_M15 = 15
    ORDER_TYPE_BUY = 0

    def __init__(self, clock, offset=10800, history_start=K_T0 - 900 * 100):
        super().__init__("MetaTrader5")
        self.clock, self.offset, self.history_start = clock, offset, history_start
        self.flip_utc = self.offset_after = None
        self.spread, self.bid, self.mode, self.frozen = 2.50, 20500.0, "ok", None
        self.trading_calls = []
        for name in Q.FORBIDDEN_API:
            setattr(self, name, (lambda n: lambda *a, **k: self.trading_calls.append(n))(name))

    def _off_at(self, utc):
        return self.offset_after if (self.flip_utc is not None and utc >= self.flip_utc) else self.offset

    def initialize(self):
        return True

    def shutdown(self):
        return None

    def last_error(self):
        return (1, "Success")

    def symbol_select(self, _sym, _en=True):
        return True

    def terminal_info(self):
        T = collections.namedtuple("Term", "build connected trade_allowed maxbars path data_path commondata_path name company")
        return T(4410, True, True, 100000, "C:\\Users\\jane\\MT5", "C:\\Users\\jane\\Data", "Common", "MetaTrader 5", "MetaQuotes")

    def account_info(self):
        A = collections.namedtuple("Acct", "login name server balance equity margin margin_free profit credit trade_mode margin_so_mode leverage currency")
        return A(123456789, "Jane Doe", "XMGlobal-MT5 7", 99999.5, 99999.5, 0.0, 99999.5, 0.0, 0.0, 0, 0, 500, "USD")

    def symbol_info(self, symbol):
        S = collections.namedtuple("S", "name description path digits point spread spread_float "
                                   "trade_calc_mode trade_mode trade_exemode filling_mode order_mode "
                                   "expiration_mode trade_stops_level trade_freeze_level "
                                   "trade_contract_size volume_min volume_max volume_step swap_mode "
                                   "swap_long swap_short swap_rollover3days session_deals bidhigh")
        return S(symbol, "US Tech 100 Cash", "CashIndices\\US100Cash", 2, 0.01, 250, True, 2, 4, 2, 2, 127,
                 15, 0, 0, 1.0, 0.10, 500.0, 0.01, 1, -3.2, 1.1, 4, 99, 21000.0)

    def symbol_info_tick(self, _symbol):
        T = collections.namedtuple("Tick", "time bid ask last volume time_msc flags volume_real")
        if self.mode == "none":
            return None
        if self.mode == "stale":
            if self.frozen is None:
                self.frozen = int(self.clock.t + self._off_at(self.clock.t) - 5)
            return T(self.frozen, self.bid, self.bid + self.spread, 0, 0, 0, 0, 0)
        self.frozen = None
        return T(int(self.clock.t + self._off_at(self.clock.t) - 0.4), self.bid, self.bid + self.spread,
                 0, 0, 0, 0, 0)

    def order_calc_profit(self, _order_type, _symbol, volume, price_open, price_close):
        return round((price_close - price_open) * volume * 1.0, 2)

    def order_calc_margin(self, _order_type, _symbol, volume, price):
        return round(volume * price / 500.0, 2)

    def copy_rates_from_pos(self, _symbol, _tf, start_pos, count):
        cur_open = (int(self.clock.t) // 900) * 900
        bars = []
        for k in range(int(count), 0, -1):
            u = cur_open - (start_pos - 1 + k) * 900
            if u < self.history_start:
                continue
            bars.append({
                "time": u + self._off_at(u),
                "open": 20480.0, "high": 20510.0, "low": 20470.0, "close": 20500.0,
                "tick_volume": 420, "spread": 250,
            })
        return bars


q_src = open(os.path.join(HERE, "mt5_quotes.py"), encoding="utf-8").read()
q_tree = ast.parse(q_src)
q_attrs = {n.attr for n in ast.walk(q_tree) if isinstance(n, ast.Attribute)}
check("K: AST confirms mt5_quotes.py calls ZERO trading APIs",
      not (q_attrs & set(Q.FORBIDDEN_API)), f"overlap={q_attrs & set(Q.FORBIDDEN_API)}")
check("K: mt5_quotes defaults to Z:/opt/nasdaq/xm_data, M15_SECONDS=900, candles_m15.csv",
      Q.DEFAULT_DIR == "Z:/opt/nasdaq/xm_data" and Q.M15_SECONDS == 900
      and Q.CANDLES_FILE == "candles_m15.csv")

class KNoTzdata:
    def __new__(cls, *_a, **_k):
        raise Q.ZoneInfoNotFoundError("no tzdata in the Wine python")


_k_saved_zi = Q.ZoneInfo
Q.ZoneInfo = KNoTzdata
try:
    k_fb_tz = Q.load_ny_tz()
finally:
    Q.ZoneInfo = _k_saved_zi
check("K: mt5_quotes falls back to fixed-rule US Eastern when the Wine python lacks tzdata",
      isinstance(k_fb_tz, Q.USEasternFallback)
      and datetime(2026, 10, 20, 21, 30, tzinfo=timezone.utc).astimezone(k_fb_tz).strftime("%H:%M %Z") == "17:30 EDT"
      and datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc).astimezone(k_fb_tz).strftime("%H:%M %Z") == "17:30 EST"
      and datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc).astimezone(k_fb_tz).weekday() == 1)

tmp_k = tempfile.mkdtemp(prefix="nasdaq_smoke_k_")
kclock = KClock(K_T0)
kmt5 = KMT5(kclock)
klog = Q.XmLogger(kmt5, "US100Cash", tmp_k, interval=30, poll=5, backfill_bars=50,
                  sleep=kclock.sleep, log=lambda _m: None)
klog.start(kclock.t, clock=kclock)
for _ in range(12):
    klog.step(kclock.t)
    kclock.sleep(5)
klog.flush_window(kclock.t)

spec_txt = open(os.path.join(tmp_k, Q.SPEC_FILE), encoding="utf-8").read()
spec_obj = json.loads(spec_txt)
check("K: spec_latest.json leaks ZERO account PII/paths & confirms 0.10 lot x 100 pts = $10.00",
      not any(s in spec_txt for s in K_SECRETS)
      and spec_obj["probes"]["calc_profit_usd_0.10lot_plus_100pts"] == 10.0
      and not kmt5.trading_calls)
check("K: candles_m15.csv written with 15-min UTC closes",
      os.path.isfile(os.path.join(tmp_k, "candles_m15.csv"))
      and sum(1 for _ in open(os.path.join(tmp_k, "candles_m15.csv"))) > 40)

# Watchdog: 16 minutes of no quotes -> SystemExit(2) when respect_market_hours=False
kmt5.mode = "none"
exited_2 = False
for _ in range(210):
    kclock.sleep(5)
    try:
        klog.step(kclock.t)
    except SystemExit as ex:
        exited_2 = (ex.code == 2)
        break
check("K: 15+ quiet minutes during open market exits with code 2 for systemd restart", exited_2)

# Quiet-hours guard: Saturday 12:00 UTC does NOT exit(2) when respect_market_hours=True
sat_t = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc).timestamp()
kclock2 = KClock(sat_t)
kmt5_2 = KMT5(kclock2)
kmt5_2.mode = "none"
klog2 = Q.XmLogger(kmt5_2, "US100Cash", os.path.join(tmp_k, "sat"),
                   respect_market_hours=True, sleep=kclock2.sleep, log=lambda _m: None)
klog2.started_at = sat_t
sat_survived = True
for _ in range(220):
    kclock2.sleep(5)
    try:
        klog2.step(kclock2.t)
    except SystemExit:
        sat_survived = False
        break
check("K: CME weekend closure suppresses exit-2 restart loop when respect_market_hours=True", sat_survived)

# ==============================================================================
# Scenario L: tools/xm_quote_report.py (US100Cash M15 cost/parity/spec/quiet)
# ==============================================================================
print("\nScenario L: tools/xm_quote_report.py (US100Cash 0.10-lot cost, M15 parity, quiet line)")
import xm_quote_report as XQR  # noqa: E402

buf_l = io.StringIO()
with redirect_stdout(buf_l):
    rc_l = XQR.main(["--root", ROOT, "--dir", tmp_k, "--now", str(K_T0 + 60)])
out_l = buf_l.getvalue()
check("L: xm_quote_report runs cleanly on US100Cash logger output & verifies $10.00 probe",
      rc_l == 0 and "0.10 lot x 100 pts = $10.00 OK" in out_l
      and "== 5. SPEC: contract facts for US100Cash ==" in out_l
      and abs(XQR.HARD_CAP_PTS - 8.0) < 1e-9 and abs(XQR.ASSUMED_USD_PER_TRADE - 0.25) < 1e-9)

# Quiet-line: open-market stale vs weekend market-closed OK
q_stale_buf = io.StringIO()
with redirect_stdout(q_stale_buf):
    XQR.main(["--root", ROOT, "--dir", tmp_k, "--quiet", "--now", str(K_T0 + 4 * 3600)])
q_closed_buf = io.StringIO()
with redirect_stdout(q_closed_buf):
    XQR.main(["--root", ROOT, "--dir", tmp_k, "--quiet", "--now", str(sat_t)])
check("L: --quiet reports STALE with mt5quotes-nasdaq during open hours & OK (market closed) on Saturday",
      "xm logger: STALE" in q_stale_buf.getvalue()
      and "mt5quotes-nasdaq" in q_stale_buf.getvalue()
      and "xm logger: OK (market closed" in q_closed_buf.getvalue(),
      q_closed_buf.getvalue().strip())

# Ensure xm_quote_report does NOT accept a --spread flag (its purpose is measuring spread!)
xqr_src = open(os.path.join(HERE, "xm_quote_report.py"), encoding="utf-8").read()
check("L: xm_quote_report has no --spread CLI flag", '"--spread"' not in xqr_src)
shutil.rmtree(tmp_k, ignore_errors=True)

# ==============================================================================
# Scenario M: MT5 closed-candle simulated exits (SL/TP, tie->SL, gap-open fill)
# ==============================================================================
print("\nScenario M: MT5 closed-candle simulated exits (US100Cash $0.10/pt)")
tmp_m = tempfile.mkdtemp(prefix="nasdaq_smoke_m_")
configure_tmp(tmp_m)
with redirect_stdout(io.StringIO()):
    eng_m = engine.NasdaqEngine()
    eng_m.atr, eng_m.rsi, eng_m.ema_fast, eng_m.ema_slow = 40.0, 55.0, 20510.0, 20420.0
    eng_m.execute_simulated_trade("BUY", 20500.0, 0.30, "2026-10-20 14:00:00")
# Gap-down open below SL (SL = 20420.0, open = 20400.0) -> fills at open 20400.0 (-$10.00 at 0.10 lot)
with redirect_stdout(io.StringIO()):
    eng_m.check_position_on_closed_candle(20400.0, 20430.0, 20390.0, 20410.0)
check("M: gap-through BUY stop fills at bar OPEN (-100 pts x $0.10 = -$10.00)",
      not eng_m.trade_active and abs(eng_m.balance - 190.0) < 1e-6, f"bal={eng_m.balance}")

# Ambiguous bar spanning both SL and TP -> resolves conservatively to SL
with redirect_stdout(io.StringIO()):
    eng_m.execute_simulated_trade("SELL", 20500.0, 0.30, "2026-10-20 14:15:00")  # SL=20580, TP=20340
    eng_m.check_position_on_closed_candle(20500.0, 20600.0, 20300.0, 20400.0)
check("M: single M15 candle touching both SL & TP resolves conservatively to SL",
      eng_m.losses == 2 and eng_m.wins == 0)
shutil.rmtree(tmp_m, ignore_errors=True)

# ==============================================================================
# Scenario N: tools/reconcile_stale_trade.py (0.10 lot US100Cash)
# ==============================================================================
print("\nScenario N: tools/reconcile_stale_trade.py (LOT_SIZE=0.10 reconciliation)")
import reconcile_stale_trade as RST  # noqa: E402

check("N: reconcile_stale_trade reads LOT_SIZE == 0.10 from engine.py",
      abs(RST.LOT_SIZE - 0.10) < 1e-9, f"LOT_SIZE={RST.LOT_SIZE}")
rec_res = RST.reconcile(
    {"Trade_Type": "BUY", "Entry_Time": "2026-10-20 14:00:00",
     "Entry_Price": "20500.00", "Stop_Loss": "20420.00", "Take_Profit": "20660.00"},
    [
        {"ts": "2026-10-20 14:00:00", "dt": datetime(2026, 10, 20, 14, 0),
         "o": 20490.0, "h": 20700.0, "l": 20300.0, "c": 20500.0},
        {"ts": "2026-10-20 14:15:00", "dt": datetime(2026, 10, 20, 14, 15),
         "o": 20495.0, "h": 20510.0, "l": 20410.0, "c": 20430.0},
    ],
)
prof_n, bal_n = RST.compute_money("BUY", 20500.0, rec_res["exit_price"], 200.0)
check("N: entry candle ignored; next bar SL reconciled at -$8.00 -> $192.00",
      rec_res["ok"] and rec_res["reason"] == "SL" and abs(prof_n + 8.0) < 1e-6 and abs(bal_n - 192.0) < 1e-6)

# ==============================================================================
# Scenario O: pressure.py + dashboard.py + autosync.sh static alert audit
# ==============================================================================
print("\nScenario O: pressure.py (60-bar M15) + dashboard.py + autosync.sh static STALE")
import pressure  # noqa: E402

p_src = open(os.path.join(ROOT, "pressure.py"), encoding="utf-8").read()
p_tree = ast.parse(p_src)
p_writes = [n.attr for n in ast.walk(p_tree) if isinstance(n, ast.Attribute)
            and n.attr in ("write", "writerow", "writerows", "unlink", "remove")]
check("O: pressure.py is read-only and mirrors engine US100Cash M15 constants",
      not p_writes
      and pressure.PRESSURE_WINDOW == 60
      and abs(pressure.FLOOR_BUFFER_PCT - engine.FLOOR_BUFFER_PCT) < 1e-9
      and pressure.EMA_SLOPE_LOOKBACK == engine.EMA_SLOPE_LOOKBACK
      and abs(pressure.MAX_BELOW_EMA_ATR - engine.MAX_BELOW_EMA_ATR) < 1e-9)

dash_src = open(os.path.join(ROOT, "dashboard.py"), encoding="utf-8").read()
check("O: dashboard.py serves US100Cash M15 on /opt/nasdaq (port 6001) with a readiness deep link",
      "US100Cash" in dash_src and '@app.route("/readiness")' in dash_src
      and 'href="/readiness"' in dash_src and "/opt/nasdaq" in dash_src and "port=6001" in dash_src)

check("O: dashboard.py reads the feed verdict + sidecar heartbeat and renders a reason banner",
      "feed_health" in dash_src and "def feed_banner(" in dash_src
      and "MT5_FEED_HEALTH_FILE" in dash_src
      and '{% if feed and not feed.ok %}' in dash_src
      and "tools/feed_doctor.py" in dash_src)

auto_src = open(os.path.join(HERE, "autosync.sh"), encoding="utf-8").read()
check("O: autosync.sh targets /opt/nasdaq and uses static mt5quotes-nasdaq STALE alert for alert_once dedupe",
      "/opt/nasdaq" in auto_src
      and "systemctl status mt5quotes-nasdaq" in auto_src
      and "XM quote logger stopped writing - check: systemctl status mt5quotes-nasdaq" in auto_src)
_dash_change_line = next((line for line in auto_src.splitlines() if "DASH_CHANGED=$(git diff" in line), "")
check("O: autosync restarts the dashboard when imported readiness/feed modules change",
      all(path in _dash_change_line for path in ("dashboard.py", "pressure.py", "feed_health.py",
                                                  "trade_filter.py", "tools/live_readiness.py",
                                                  "tools/replay_lib.py")))
check("O: autosync.sh pins git user.name to 'nasdaq-autosync' (a pre-rename clone cannot leak the old identity)",
      'git config user.name "nasdaq-autosync"' in auto_src
      and "git config user.name  >/dev/null 2>&1 ||" not in auto_src)
check("O: autosync feed self-heal is doctor-gated - never a blind sidecar restart",
      "FEED_SELF_HEAL" in auto_src
      and "restart_sidecar" in auto_src
      and "feed self-heal:" in auto_src
      and "not restarting (deliberate stop?)" in auto_src
      and "FEED_HEAL_WAIT_S" in auto_src
      and auto_src.count("systemctl restart") == 2  # dashboard + gated feed only
      and 'systemctl restart "$FSVC"' in auto_src)
_auto_code_lines = [line for line in auto_src.splitlines() if not line.lstrip().startswith("#")]
check("O: autosync rollback preserves live data - no git reset --hard, soft-reset + verified data commits",
      not any("reset --hard" in line for line in _auto_code_lines)
      and "git reset --soft" in auto_src
      and "commit_live_data" in auto_src
      and "verify_data_preserved" in auto_src
      and "refs/autosync/pre-deploy" in auto_src)

# ==============================================================================
# Scenario P: tools/mt5_executor.py + LIVE bridge + deploy/*.service audit
# ==============================================================================
print("\nScenario P: tools/mt5_executor.py safety locks + live_path_probe + deploy/*.service")
import mt5_executor as MTE  # noqa: E402


class PExecMT5(types.ModuleType):
    TRADE_ACTION_DEAL = 1
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TIME_GTC = 0
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    TRADE_RETCODE_DONE = 10009
    DEAL_ENTRY_OUT = 1

    def __init__(self, trade_mode=0):
        super().__init__("MetaTrader5")
        self.trade_mode = trade_mode
        self.sent = []
        self.positions = []
        self.send_none = False

    def initialize(self):
        return True

    def terminal_info(self):
        return types.SimpleNamespace(connected=True, trade_allowed=True)

    def account_info(self):
        return types.SimpleNamespace(login=111, server="XM", trade_mode=self.trade_mode,
                                     trade_allowed=True, balance=200.0, equity=200.0, currency="USD")

    def symbol_select(self, _s, _e=True):
        return True

    def symbol_info(self, s):
        return types.SimpleNamespace(name=s, visible=True, filling_mode=2, trade_mode=4,
                                     digits=2, point=0.01, trade_contract_size=1.0,
                                     trade_stops_level=0, volume_min=0.10, volume_max=500.0, volume_step=0.01)

    def symbol_info_tick(self, _s):
        return types.SimpleNamespace(bid=20500.0, ask=20502.5, last=20500.0, time=int(time.time()))

    def positions_get(self, **_kw):
        return tuple(self.positions)

    def history_deals_get(self, *_a, **_kw):
        return ()

    def order_check(self, _req):
        return types.SimpleNamespace(retcode=0, comment="Done")

    def order_send(self, req):
        self.sent.append(dict(req))
        if self.send_none:
            return None
        self.positions.append(types.SimpleNamespace(
            ticket=7001, identifier=7001, symbol="US100Cash", type=0, volume=0.10,
            price_open=20502.5, sl=req.get("sl", 0.0), tp=req.get("tp", 0.0),
            profit=0.0, swap=0.0, magic=987656, time=int(time.time()), comment="",
        ))
        return types.SimpleNamespace(retcode=10009, order=7001, deal=7101, price=20502.5,
                                     volume=0.10, comment="done")


tmp_p = tempfile.mkdtemp(prefix="nasdaq_smoke_p_")
pmt5 = PExecMT5(trade_mode=MTE.ACCOUNT_TRADE_MODE_REAL)  # REAL account!
p_intent = {
    "intent_id": "idemp-001", "ts_epoch": time.time(), "action": "OPEN", "side": "BUY",
    "symbol": "US100Cash", "volume": 0.10, "atr": 40.0,
    "sl": 20420.0, "tp": 20660.0, "sl_dist": 80.0, "tp_dist": 160.0, "magic": 987656,
}

# 1. Dormant gate (enabled=False) -> REJECTED (disabled), 0 orders sent
ex_dormant = MTE.Mt5Executor(pmt5, bridge_dir=os.path.join(tmp_p, "b1"), enabled=False, log=lambda _m: None)
MTE.atomic_write_json(ex_dormant.intent_path, p_intent)
r_dorm = ex_dormant.step()["result"]
check("P: Mt5Executor dormant by default (MT5_EXEC_ENABLED=0 -> REJECTED disabled, 0 orders)",
      r_dorm["status"] == "REJECTED" and "disabled" in r_dorm.get("message", "") and len(pmt5.sent) == 0)

# 2. Real-account lock (enabled=True, allow_real=False on REAL account) -> REJECTED, 0 orders sent
ex_real_lock = MTE.Mt5Executor(pmt5, bridge_dir=os.path.join(tmp_p, "b2"), enabled=True, allow_real=False, log=lambda _m: None)
MTE.atomic_write_json(ex_real_lock.intent_path, dict(p_intent, intent_id="idemp-002"))
r_real = ex_real_lock.step()["result"]
check("P: Mt5Executor blocks REAL account when MT5_EXEC_ALLOW_REAL=0",
      r_real["status"] == "REJECTED" and "REAL account blocked" in r_real.get("message", "") and len(pmt5.sent) == 0,
      r_real.get("message", ""))

# 3. Demo account + idempotency: same intent_id submitted twice -> order_send called ONCE
pmt5_demo = PExecMT5(trade_mode=MTE.ACCOUNT_TRADE_MODE_DEMO)
ex_demo = MTE.Mt5Executor(pmt5_demo, bridge_dir=os.path.join(tmp_p, "b3"), enabled=True, allow_real=False, log=lambda _m: None)
MTE.atomic_write_json(ex_demo.intent_path, dict(p_intent, intent_id="idemp-003"))
r_fill1 = ex_demo.step()["result"]
MTE.atomic_write_json(ex_demo.intent_path, dict(p_intent, intent_id="idemp-003"))
r_fill2 = ex_demo.step()["result"]
check("P: duplicate intent_id returns cached FILLED result without calling order_send twice",
      r_fill1["status"] == "FILLED" and r_fill2["status"] == "FILLED" and len(pmt5_demo.sent) == 1)
shutil.rmtree(tmp_p, ignore_errors=True)

# 4. Run tools/live_path_probe.py end-to-end
probe_proc = subprocess.run([sys.executable, os.path.join(HERE, "live_path_probe.py")],
                            cwd=ROOT, capture_output=True, text=True, timeout=30)
check("P: tools/live_path_probe.py passes all 11/11 execution checks",
      probe_proc.returncode == 0 and "11/11 checks clean" in probe_proc.stdout)

# 5. Audit deploy/*.service files for /opt/nasdaq, US100Cash, and zero embedded credentials
svc_files = ("mt5feed.service", "mt5quotes.service", "mt5exec.service",
             "nasdaq-engine.service", "nasdaq-dashboard.service")
svc_ok = True
for sf in svc_files:
    sp = os.path.join(ROOT, "deploy", sf)
    if not os.path.isfile(sp):
        svc_ok = False
        break
    stxt = open(sp, encoding="utf-8").read()
    # Strip comment lines before checking for credential keywords
    non_comment = "\n".join(ln for ln in stxt.splitlines() if not ln.lstrip().startswith("#"))
    if "/opt/nasdaq" not in stxt or re.search(r"(password|secret)\s*=", non_comment, re.I):
        svc_ok = False
check("P: all 5 deploy/*.service units exist, target /opt/nasdaq, and contain zero credentials", svc_ok)

wine_units = ("mt5feed.service", "mt5quotes.service", "mt5exec.service")
wine_txt = {sf: open(os.path.join(ROOT, "deploy", sf), encoding="utf-8").read() for sf in wine_units}
check("P: mt5*.service install notes pin WINEPREFIX=/root/.mt5 for root (never /home/root/.mt5)",
      all("/root/.mt5" in t and "/home/root/.mt5|" in t for t in wine_txt.values()))
check("P: mt5*.service install notes document 'wine C:/Python312/python.exe -m pip install tzdata'",
      all("m pip install tzdata" in t for t in wine_txt.values()))
check("P: mt5feed.service publishes a heartbeat/offset file and documents per-stack path namespacing",
      all(k in wine_txt["mt5feed.service"] for k in
          ("MT5_FEED_HEALTH_FILE", "MT5_FEED_OFFSET_FILE", "MT5_FEED_FROZEN_S",
           "MT5_FEED_RECONNECT_S", "SHARED VPS", "SYMBOL_MISMATCH")))
check("P: nasdaq-engine.service points at the same feed + heartbeat paths",
      "Environment=MT5_FEED_FILE=/opt/nasdaq/mt5_last_candle.json" in
      open(os.path.join(ROOT, "deploy", "nasdaq-engine.service"), encoding="utf-8").read()
      and "Environment=MT5_FEED_HEALTH_FILE=/opt/nasdaq/mt5_feed_health.json" in
      open(os.path.join(ROOT, "deploy", "nasdaq-engine.service"), encoding="utf-8").read())
check("P: docs/STALE-FEED-TRIAGE.md exists and covers the shared-login failure mode",
      os.path.isfile(os.path.join(ROOT, "docs", "STALE-FEED-TRIAGE.md"))
      and "TICK_FROZEN" in open(os.path.join(ROOT, "docs", "STALE-FEED-TRIAGE.md"),
                               encoding="utf-8").read())
check("P: mt5quotes.service logs the non-demo label 'MT5_QUOTES_LABEL=XM US100Cash'",
      'Environment="MT5_QUOTES_LABEL=XM US100Cash"' in wine_txt["mt5quotes.service"]
      and "US100Cash demo" not in wine_txt["mt5quotes.service"])

# Retired-instrument tripwire: no print/comment/doc may name the legacy crypto symbol.
# The pattern is assembled from fragments so this scanner never matches itself.
_TICKER = "b" + "tc"          # split so this scanner never matches its own source
LEGACY_RE = re.compile("|".join(("bit" + "coin", r"\b" + _TICKER + r"\b",
                                 _TICKER + "usd", "/opt/" + "bit" + "coin")), re.I)
legacy_hits = []
for base, dirs, files in os.walk(ROOT):
    dirs[:] = [d for d in dirs if d not in (".git", "node_modules", "__pycache__", "venv", ".venv")]
    for fn in files:
        if not fn.endswith((".py", ".md", ".sh", ".service", ".json", ".txt", ".csv", ".yml", ".yaml")):
            continue
        fp = os.path.join(base, fn)
        try:
            txt = open(fp, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        if LEGACY_RE.search(txt):
            legacy_hits.append(os.path.relpath(fp, ROOT))
check("P: zero retired-instrument strings (legacy crypto ticker/paths) anywhere in the repo",
      not legacy_hits, f"hits={legacy_hits}")
# Legacy-instrument WORDING tripwire (separate from the ticker scan above): no .py or
# .md may still describe the pre-Nasdaq book. Both patterns are assembled from fragments
# so this scanner never matches its own source, and both are anchored so the NUMERIC
# symbol-spec values that are still correct on US100Cash survive untouched:
# point=0.01, volume_step=0.01, volume_min=0.01, trade_tick_size, and the live "0.10 lot".
_METAL = "g" + "old"                                    # retired metal, never literal here
_LOT_WORD = r"0\." + "0" + r"1\s*[- ]?\s*lot\b"          # retired micro-lot phrase
_LOT_CONST = r"LOT_SIZE\s*[=:]?\s*0\." + "01" + r"\b"     # retired micro-lot constant
LEGACY_WORD_RE = re.compile("|".join((
    r"\b" + _METAL + r"\b",
    _LOT_WORD,
    _LOT_CONST,
)), re.I)

# Self-test first: a tripwire that cannot fire is noise, and one that fires on the live
# US100Cash symbol spec would block every deploy. Both directions are asserted.
_should_hit = [_METAL.upper() + " 09-15 review",
               "sized at " + "0." + "01 lot",
               "0." + "01" + "-lot account",
               "LOT_SIZE = " + "0." + "01"]
_should_miss = ["point=0.01", "volume_step=0.01", "volume_min=0.01",
                "trade_tick_size=0.01", "0.10 lot", "0.10-lot account",
                "LOT_SIZE = 0.10", "volume_min <= 0.10"]
check("P: legacy-wording tripwire self-test (fires on retired wording, ignores 0.01 symbol-spec values)",
      all(LEGACY_WORD_RE.search(x) for x in _should_hit)
      and not any(LEGACY_WORD_RE.search(x) for x in _should_miss),
      f"missed={[x for x in _should_hit if not LEGACY_WORD_RE.search(x)]} "
      f"false_pos={[x for x in _should_miss if LEGACY_WORD_RE.search(x)]}")

wording_hits = []
for base, dirs, files in os.walk(ROOT):
    dirs[:] = [d for d in dirs if d not in (".git", "node_modules", "__pycache__", "venv", ".venv")]
    for fn in files:
        if not fn.endswith((".py", ".md")):
            continue
        fp = os.path.join(base, fn)
        try:
            txt = open(fp, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        for ln, line in enumerate(txt.splitlines(), 1):
            if LEGACY_WORD_RE.search(line):
                wording_hits.append(f"{os.path.relpath(fp, ROOT)}:{ln}")
check("P: zero legacy instrument wording (retired metal / micro-lot sizing) in any .py/.md",
      not wording_hits, f"hits={wording_hits[:8]}")

check("P: docs/NASDAQ-OPS.md exists and the one-time restructure runbook is gone",
      os.path.isfile(os.path.join(ROOT, "docs", "NASDAQ-OPS.md"))
      and not os.path.exists(os.path.join(ROOT, "docs", "NASDAQ-RESTRUCTURE-2026-10-24.md")))

# ==============================================================================
# Scenario Q: Day-0 ledger guards in tools/win_rate_report.py
# ==============================================================================
print("\nScenario Q: tools/win_rate_report.py Day-0 / no-signal census guards")

_Q_HDR = ["Timestamp", "Open", "High", "Low", "Close", "Wick_Ratio", "Volume", "Vol_MA",
          "Vol_Confirmed", "Dynamic_Floor", "EMA_50", "EMA_200", "Trend_Confirmed",
          "Tested_Floor", "Valid_Rejection", "Held_Floor", "RSI", "ATR"]


def _q_log(path, rows):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(_Q_HDR)
        w.writerows(rows)


def _q_report(root):
    return subprocess.run([sys.executable, os.path.join(HERE, "win_rate_report.py"),
                           "--root", root, "--spread", "0.25"],
                          cwd=ROOT, capture_output=True, text=True, timeout=60)


_q_t0 = datetime(2026, 10, 24, 0, 0)
# (a) 300 rising M15 bars that never test the 20-bar floor -> zero full signals
_q_quiet = []
_q_p = 29000.0
for _i in range(300):
    _o, _c = _q_p, _q_p + 2.0
    _q_quiet.append([(_q_t0 + timedelta(minutes=15 * _i)).strftime("%Y-%m-%d %H:%M:%S"),
                     f"{_o:.2f}", f"{_c + 1.0:.2f}", f"{_o - 1.0:.2f}", f"{_c:.2f}",
                     "0.5", "100", "100", "True", f"{_o - 40:.2f}",
                     f"{28900 + 1.5 * _i:.2f}", f"{28800 + _i:.2f}", "True",
                     "False", "False", "False", "55.0", "30.0"])
    _q_p = _c
# (b) 200 floor-touching bars -> many full signals, but all walk to TIME (0 decisive)
_q_signals = []
for _i in range(200):
    _o = 28999.0 if _i % 2 == 0 else 29005.0
    _c = 29005.0 if _i % 2 == 0 else 28999.0
    _q_signals.append([(_q_t0 + timedelta(minutes=15 * _i)).strftime("%Y-%m-%d %H:%M:%S"),
                       f"{_o:.2f}", "29010.00", "28990.00", f"{_c:.2f}",
                       "0.75", "100", "100", "True", "28990.00",
                       f"{29005 + 0.02 * _i:.2f}", "28960.00", "True",
                       "True", "True", "True", "55.0", "30.0"])

_q_tmp = tempfile.mkdtemp(prefix="nasdaq_smoke_q_")
try:
    # (a) both files header-only: the existing Day-0 early-out, no traceback
    open(os.path.join(_q_tmp, "trades.csv"), "w").close()
    _q_log(os.path.join(_q_tmp, "forward_test_log.csv"), [])
    _q_empty = _q_report(_q_tmp)
    check("Q: win_rate_report exits 0 on empty trades.csv + forward_test_log.csv (Day-0 early-out)",
          _q_empty.returncode == 0 and "0 trades and 0 bars yet" in _q_empty.stdout
          and "Traceback" not in _q_empty.stderr,
          (_q_empty.stderr or _q_empty.stdout).strip()[-160:])

    # (b) bars but zero full signals: §6 must print the 0-takeable early-out, not divide by zero
    open(os.path.join(_q_tmp, "trades.csv"), "w").close()
    _q_log(os.path.join(_q_tmp, "forward_test_log.csv"), _q_quiet)
    _q_nosig = _q_report(_q_tmp)
    check("Q: win_rate_report §6 prints '0 takeable signals yet (Day-0 ledger)' on a signal-free log",
          _q_nosig.returncode == 0
          and "0 takeable signals yet (Day-0 ledger)" in _q_nosig.stdout
          and "Traceback" not in _q_nosig.stderr,
          (_q_nosig.stderr or _q_nosig.stdout).strip()[-160:])

    # (c) signals but zero decisive outcomes AND an empty ledger: §6 WR + §8 book WR are n/a
    _q_log(os.path.join(_q_tmp, "forward_test_log.csv"), _q_signals)
    _q_nodec = _q_report(_q_tmp)
    check("Q: win_rate_report prints n/a (not ZeroDivisionError) for 0 decisive outcomes on an empty ledger",
          _q_nodec.returncode == 0 and "0 decisive" in _q_nodec.stdout
          and "no decisive SL/TP rows yet" in _q_nodec.stdout
          and "Traceback" not in _q_nodec.stderr,
          (_q_nodec.stderr or _q_nodec.stdout).strip()[-160:])
finally:
    shutil.rmtree(_q_tmp, ignore_errors=True)

# ==============================================================================
# Scenario R: feed diagnosis (feed_health.py) - "why" not just "stale"
# ==============================================================================
print("\nScenario R: feed diagnosis (feed_health.py) + tools/feed_doctor.py triage")
import feed_health as FH   # noqa: E402

tmp_r = tempfile.mkdtemp(prefix="nasdaq_smoke_r_")
configure_tmp(tmp_r)
_now_r = 1793040000.0   # fixed epoch: 2026-10-26 18:40:00 UTC
_candle_r = int(_now_r) - FH.CANDLE_SECONDS - 60   # last bar closed 60s ago


def _r_feed(payload=None, raw=None, path=None):
    path = path or engine.MT5_FEED_FILE
    if raw is not None:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(raw)
    elif payload is not None:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
    return path


def _r_good_payload(ts=None, symbol="US100Cash"):
    return {"symbol": symbol, "timeframe": "M15", "account": "12345678",
            "ts": _candle_r if ts is None else ts, "open": 29300.0, "high": 29330.0,
            "low": 29290.0, "close": 29320.0, "tick_volume": 450, "server_offset_s": 7200,
            "updated_at": "2026-10-26 15:15:00"}


# 1. Reason taxonomy: every "no bar" cause gets its own diagnosis
d = FH.diagnose_feed_file(os.path.join(tmp_r, "nope.json"), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: missing feed file -> FILE_MISSING / not usable", d["reason"] == FH.REASON_MISSING and not d["usable"],
      d["reason"])
d = FH.diagnose_feed_file(_r_feed(raw="{not json"), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: truncated/foreign JSON -> FILE_UNREADABLE (not a silent None)",
      d["reason"] == FH.REASON_UNREADABLE, d["detail"][:60])
d = FH.diagnose_feed_file(_r_feed({"hello": "world"}), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: wrong keys -> SCHEMA_INVALID", d["reason"] == FH.REASON_SCHEMA, d["detail"][:70])
d = FH.diagnose_feed_file(_r_feed(_r_good_payload(symbol="EURUSD")), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: foreign instrument in the same feed path -> SYMBOL_MISMATCH / not usable",
      d["reason"] == FH.REASON_SYMBOL and not d["usable"], d["detail"][:80])

d = FH.diagnose_feed_file(_r_feed(_r_good_payload(ts=int(_now_r) + 7200)), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: bar dated in the future -> CANDLE_IN_FUTURE / not usable",
      d["reason"] == FH.REASON_FUTURE and not d["usable"], d["detail"][:70])

d = FH.diagnose_feed_file(_r_feed(_r_good_payload(ts=int(_now_r) - 5400)), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: frozen bar -> CANDLE_OLD with the true freeze instant and age",
      d["reason"] == FH.REASON_OLD and d["usable"] and d["candle_age_s"] > 3600
      and d["frozen_since_utc"].startswith("2026-10-26 1"), f"{d['reason']} {d['frozen_since_utc']}")

d = FH.diagnose_feed_file(_r_feed(_r_good_payload()), expected_symbol="US100Cash", now_epoch=_now_r)
check("R: fresh, tagged, matching bar -> OK and usable", d["reason"] == FH.REASON_OK and d["ok"] and d["usable"],
      d["detail"])

# 2. The 890 -> 20 min trap: an engine restart must not reset the reported age
with redirect_stdout(io.StringIO()):
    eng_r = engine.NasdaqEngine()
engine.is_market_quiet = lambda *_a, **_k: False        # force "market open" for determinism
_r_feed(_r_good_payload(ts=int(_now_r) - 5400))
_r_now_backup = FH._now
FH._now = lambda epoch=None: _now_r
try:
    rates_r = eng_r.read_mt5_feed(now_epoch=_now_r)
    check("R: read_mt5_feed recovers a usable-but-stale bar for age reporting", rates_r is not None)
    stale_r = eng_r.mt5_feed_is_stale(rates_r, now_epoch=_now_r)
finally:
    FH._now = _r_now_backup
detail_r = ""
with open(engine.JOURNAL_FILE_PATH, newline="") as fh:
    for row in csv.DictReader(fh):
        detail_r = row["Detail"]
check("R: stale alert fires for an old bar even though the engine just started (no uptime reset)",
      stale_r is True)
check("R: alert detail names the freeze instant + 'frozen', not a bogus 0/20-min uptime",
      "frozen" in detail_r and "closed 2026-10-26" in detail_r and " min ago" in detail_r,
      detail_r[:120])
check("R: alert detail points at tools/feed_doctor.py", "tools/feed_doctor.py" in detail_r, detail_r[-60:])

# Same feed, one hour later, counter must NOT be able to drop: repeat alert keeps the same freeze instant
eng_r._last_stale_alert_mono = None
_first = detail_r
_backup = FH._now
FH._now = lambda epoch=None: _now_r + 3600
try:
    eng_r.read_mt5_feed(now_epoch=_now_r + 3600)
    eng_r.mt5_feed_is_stale(rates_r, now_epoch=_now_r + 3600)
finally:
    FH._now = _backup
with open(engine.JOURNAL_FILE_PATH, newline="") as fh:
    rows_r = [r for r in csv.DictReader(fh)]
check("R: repeat alert reports the same freeze instant (no reset-style '20 min' regression)",
      len(rows_r) == 2 and "frozen" in rows_r[-1]["Detail"], rows_r[-1]["Detail"][:90])
check("R: cooldown still rate-limits a third alert inside 30 min",
      eng_r.maybe_alert_stale_feed(600, event="STALE_FEED", detail="dup") is False)

# 3. Journal forensics: the reported 890 -> 20 min drop is a counter reset, not a recovery
ev_hist = [
    {"timestamp": "2026-10-26 15:39:34", "event": "STALE_FEED", "minutes": 890},
    {"timestamp": "2026-10-26 16:00:39", "event": "STALE_FEED", "minutes": 20},
    {"timestamp": "2026-10-26 16:30:39", "event": "STALE_FEED", "minutes": 50},
]
resets = FH.detect_counter_resets(ev_hist)
check("R: detect_counter_resets flags 890 -> 20 min as an engine-restart counter reset",
      len(resets) == 1 and resets[0]["from_minutes"] == 890 and resets[0]["to_minutes"] == 20)
check("R: detect_counter_resets ignores a monotonic 20 -> 50 min series",
      FH.detect_counter_resets(ev_hist[1:]) == [])
check("R: journal parser reads 'no new closed M15 candles for 50 min' -> 50",
      FH._minutes_from_detail("no new closed M15 candles for 50 min - check the mt5feed sidecar") == 50)

# 4. Event names per failure mode (the dashboard timeline becomes readable)
check("R: distinct journal events per failure mode",
      FH.format_feed_problem({"reason": FH.REASON_MISSING, "path": "/opt/nasdaq/mt5_last_candle.json"})[0]
      == "FEED_FILE_MISSING"
      and FH.format_feed_problem({"reason": FH.REASON_SYMBOL, "detail": "x"})[0] == "FEED_SYMBOL_MISMATCH"
      and FH.format_feed_problem({"reason": FH.REASON_OLD, "candle_age_s": 3600})[0] == "STALE_FEED")

# 5. Sidecar heartbeat: an "active" unit with a dead terminal is visible, not silent
_hp = os.path.join(tmp_r, "mt5_feed_health.json")
engine.MT5_FEED_HEALTH_FILE = _hp
with open(_hp, "w", encoding="utf-8") as fh:
    json.dump({"status": "TICK_FROZEN", "symbol": "US100Cash", "account": "12345678",
               "terminal_connected": False, "tick_age_s": 5400.0, "server_offset_s": None,
               "last_error": "terminal tick is 90 min old", "reconnects": 2,
               "updated_at": "2026-10-26 15:15:00"}, fh)
# The diagnosis uses the file mtime, not the JSON timestamp. Anchor the fixture
# to the same synthetic clock so this remains deterministic after October 2026.
os.utime(_hp, (_now_r - 180, _now_r - 180))
health_r = FH.diagnose_sidecar_health(_hp, now_epoch=_now_r)
_sum = FH.summarize_sidecar(health_r)
check("R: heartbeat detected as present but NOT fresh (120s rule)",
      health_r["present"] and health_r["fresh"] is False, f"age={health_r['age_s']}")
check("R: sidecar summary exposes frozen tick + unknown broker offset",
      "TICK_FROZEN" in _sum and "offset unknown" in _sum, _sum)

# 6. mt5_feed payload/heartbeat contracts (Wine sidecar side)
lcp = feed_mod.build_last_candle_payload(
    {"time": 1793039700, "open": 29300.0, "high": 29330.0, "low": 29290.0,
     "close": 29320.0, "tick_volume": 450}, 7200, "US100Cash", "M15", "12345678")
check("R: mt5_feed tags its bar with symbol/timeframe/account (foreign-bar detection)",
      lcp["symbol"] == "US100Cash" and lcp["timeframe"] == "M15" and lcp["account"] == "12345678"
      and lcp["ts"] == 1793039700 - 7200)
check("R: mt5_feed bar round-trips through the engine diagnosis as OK",
      FH.diagnose_feed_file(_r_feed(lcp), expected_symbol="US100Cash",
                            now_epoch=lcp["ts"] + 900 + 30)["reason"] == FH.REASON_OK)
_off = os.path.join(tmp_r, "mt5_feed_offset.json")
feed_mod.persist_server_offset(7200, "US100Cash", "12345678", path=_off)
check("R: learned broker offset persists so a restart can publish before a fresh tick",
      feed_mod.load_saved_offset(_off) == 7200 and feed_mod.load_saved_offset(os.path.join(tmp_r, "x.json")) is None)
check("R: frozen-tick age math (now - (server tick time - offset))",
      abs(feed_mod.tick_age_seconds(1793040000, 7200, 1793040600) - 7800) < 1
      and feed_mod.tick_age_seconds(0, 7200, 1793040600) is None)
_hp2 = feed_mod.build_health_payload("OK", symbol="US100Cash", account="12345678", connected=True,
                                     tick_age_s=1.4, server_offset_s=7200, last_candle_payload=lcp,
                                     started_epoch=_now_r - 120)
check("R: heartbeat payload carries status/account/tick age/offset/last bar",
      _hp2["status"] == "OK" and _hp2["account"] == "12345678" and _hp2["tick_age_s"] == 1.4
      and _hp2["server_offset_s"] == 7200 and _hp2["last_candle_open_utc"])

# 7. engine status.json exposes the feed verdict to the dashboard
with redirect_stdout(io.StringIO()):
    eng_r.save_status()
with open(engine.STATUS_FILE_PATH) as fh:
    _st = json.load(fh)
check("R: status.json carries feed.reason/frozen_since_utc for the dashboard",
      _st.get("feed", {}).get("reason") in (FH.REASON_OLD, FH.REASON_OK)
      and "frozen_since_utc" in _st["feed"], json.dumps(_st.get("feed", {}))[:80])

# 8. tools/feed_doctor.py triage on a broken stack vs a healthy one
inc = os.path.join(tmp_r, "incident")
os.makedirs(os.path.join(inc, "xm_data"), exist_ok=True)
with open(os.path.join(inc, "journal.csv"), "w", newline="") as fh:
    fh.write("Timestamp,Event,Detail\n")
    fh.write("2026-10-26 15:39:34,STALE_FEED,no new closed M15 candles for 890 min - check the mt5feed sidecar\n")
    fh.write("2026-10-26 16:00:39,STALE_FEED,no new closed M15 candles for 20 min - check the mt5feed sidecar\n")
_q_now = datetime.now(timezone.utc)
with open(os.path.join(inc, "xm_data", f"quotes-{_q_now:%Y-%m-%d}.csv"), "w", newline="") as fh:
    fh.write("ts_utc,bid,ask,spread,spread_min,spread_max,n_ok,n_bad,tick_age_s,srv_offset_s,flags\n")
    fh.write(f"{_q_now:%Y-%m-%d %H:%M:%S},29300.0,29302.9,2.9,2.9,2.9,6,0,1.0,7200,\n")
_bar_close_r = _q_now - timedelta(minutes=21)
with open(os.path.join(inc, "xm_data", "candles_m15.csv"), "w", newline="") as fh:
    fh.write("bar_close_utc,open,high,low,close,tick_volume,spread_pts,time_srv,offset_s\n")
    fh.write(f"{_bar_close_r:%Y-%m-%d %H:%M:%S},29300.0,29330.0,29290.0,29320.0,450,290,0,7200\n")
_doc_broken = subprocess.run([sys.executable, os.path.join(HERE, "feed_doctor.py"),
                              "--data-dir", inc, "--journal", os.path.join(inc, "journal.csv"),
                              "--assume-open"], cwd=ROOT, capture_output=True, text=True, timeout=60)
check("R: feed_doctor exits 2 (BROKEN) on a missing feed file",
      _doc_broken.returncode == 2 and "FILE_MISSING" in _doc_broken.stdout,
      (_doc_broken.stderr or _doc_broken.stdout).strip()[-120:])
check("R: feed_doctor proves MT5 is fine when the quote logger is still recording",
      "MT5 itself is fine" in _doc_broken.stdout)
check("R: feed_doctor flags the 890 -> 20 min counter reset in the journal",
      "counter reset" in _doc_broken.stdout)
check("R: feed_doctor never raises a traceback", "Traceback" not in _doc_broken.stderr)
check("R: feed_doctor witness treats bar_close_utc as a CLOSE time (21 min old, not 6)",
      "closed 21 min ago" in _doc_broken.stdout)

_healthy = os.path.join(tmp_r, "healthy")
os.makedirs(os.path.join(_healthy, "xm_data"), exist_ok=True)
_n2 = time.time()
with open(os.path.join(_healthy, "mt5_last_candle.json"), "w") as fh:
    json.dump(feed_mod.build_last_candle_payload(
        {"time": int(_n2) - 900 + 7200, "open": 29300.0, "high": 29330.0, "low": 29290.0,
         "close": 29320.0, "tick_volume": 450}, 7200, "US100Cash", "M15", "12345678"), fh)
with open(os.path.join(_healthy, "mt5_feed_health.json"), "w") as fh:
    json.dump(feed_mod.build_health_payload("OK", symbol="US100Cash", account="12345678",
                                            connected=True, tick_age_s=1.0, server_offset_s=7200,
                                            started_epoch=_n2 - 60), fh)
_doc_ok = subprocess.run([sys.executable, os.path.join(HERE, "feed_doctor.py"),
                          "--data-dir", _healthy, "--assume-open"],
                         cwd=ROOT, capture_output=True, text=True, timeout=60)
check("R: feed_doctor exits 0 (OK) on a fresh, tagged, matching feed",
      _doc_ok.returncode == 0 and "OK" in _doc_ok.stdout,
      (_doc_ok.stderr or _doc_ok.stdout).strip()[-160:])
_doc_json = subprocess.run([sys.executable, os.path.join(HERE, "feed_doctor.py"),
                            "--data-dir", _healthy, "--assume-open", "--json"],
                           cwd=ROOT, capture_output=True, text=True, timeout=60)
try:
    _jr = json.loads(_doc_json.stdout)
    _json_ok = _jr["verdict"][0] == "OK" and _jr["sidecar"]["status"] == "OK"
except Exception as exc:
    _json_ok = False
check("R: feed_doctor --json is machine-readable for autosync/alerting", _json_ok,
      _doc_json.stdout[:80])
try:
    _jr_actions = json.loads(_doc_json.stdout).get("actions")
    _actions_ok = (isinstance(_jr_actions, dict)
                   and "restart_sidecar" in _jr_actions
                   and _jr_actions["restart_sidecar"] is None)   # healthy feed
except Exception:
    _actions_ok = False
check("R: feed_doctor --json exposes actions.restart_sidecar (autosync self-heal gate)",
      _actions_ok, _doc_json.stdout[:120])

shutil.rmtree(tmp_r, ignore_errors=True)

# ==============================================================================
# Scenario S: shared readiness gates + Flask dashboard (no trading APIs)
# ==============================================================================
print("\nScenario S: readiness data gates + dashboard fail-closed regression tests")
# Autosync invokes system python, while Flask is usually installed in the
# dashboard venv. Prefer any available interpreter that can exercise the UI.
_test_python = sys.executable
for candidate in (sys.executable, os.path.join(ROOT, "venv", "bin", "python"),
                  os.path.join(ROOT, ".venv", "bin", "python")):
    if os.path.isfile(candidate):
        has_flask = subprocess.run([candidate, "-c", "import flask"],
                                   capture_output=True, timeout=15).returncode == 0
        if has_flask:
            _test_python = candidate
            break
_readiness_tests = subprocess.run(
    [_test_python, "-m", "unittest", "discover", "-s", "tests"],
    cwd=ROOT, capture_output=True, text=True, timeout=120)
check("S: readiness regression suite passes", _readiness_tests.returncode == 0,
      (_readiness_tests.stderr or _readiness_tests.stdout).strip()[-400:])
if "skipped=" in _readiness_tests.stderr:
    print("  [info] Flask is unavailable; dashboard route tests were skipped. "
          "Install Flask and rerun this suite before dashboard deployment.")

# ==============================================================================
# Summary
# ==============================================================================
print("\n" + "=" * 66)
if FAILURES:
    print(f"SMOKE TEST FAILED ({len(FAILURES)} failure(s)):")
    for f in FAILURES:
        print(f"  - {f}")
    sys.exit(1)
print("ALL SMOKE TEST SCENARIOS (A-S) PASSED FOR US100Cash M15 (/opt/nasdaq)")
sys.exit(0)
