#!/usr/bin/env python3
"""MT5 -> file sidecar for the Nasdaq-100 (US100Cash) M15 engine.

The Linux engine cannot import MetaTrader5 (no Linux wheels on PyPI), so this
daemon runs under Wine Python in the SAME prefix as the XM MT5 terminal and
publishes:

  1) Z:/opt/nasdaq/mt5_warmup_candles.json  (250 closed M15 bars on startup and
     refreshed periodically so engine.py warms EMA200/EMA50/RSI/ATR immediately
     on Day 0 instead of waiting 50 trading hours!)
  2) Z:/opt/nasdaq/mt5_last_candle.json     (latest closed M15 candle every 5s,
     with broker-server time converted to UTC via a fresh tick offset. The
     payload is tagged with `symbol`/`timeframe`/`account` so a second MT5 stack
     on the same VPS cannot silently feed this engine a foreign instrument.)
  3) Z:/opt/nasdaq/mt5_feed_health.json     (heartbeat every poll: terminal
     connected?, tick age, broker UTC offset, last error, reconnect count. This
     is what distinguishes "the unit is active" from "the feed is healthy".)
  4) Z:/opt/nasdaq/mt5_feed_offset.json     (the last learned broker/UTC offset,
     so a restart can keep publishing before a fresh tick arrives instead of
     going silent until the market moves.)

Run (manual test):
  export WINEPREFIX=~/.mt5 && xvfb-run --auto-servernum \\
      wine C:/Python312/python.exe Z:/opt/nasdaq/tools/mt5_feed.py

Or as a service (see deploy/mt5feed.service):
  sudo systemctl enable --now mt5feed-nasdaq

Imports cleanly on Linux without MetaTrader5 so tools/smoke_test.py can import
pure helpers (estimate_server_offset, build_warmup_payload, ...) directly.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

SYMBOL = os.environ.get("MT5_FEED_SYMBOL", "US100Cash")
OUT = os.environ.get("MT5_FEED_FILE", "Z:/opt/nasdaq/mt5_last_candle.json")
WARMUP_OUT = os.environ.get("MT5_WARMUP_FILE", "Z:/opt/nasdaq/mt5_warmup_candles.json")
HEALTH_OUT = os.environ.get("MT5_FEED_HEALTH_FILE", "Z:/opt/nasdaq/mt5_feed_health.json")
OFFSET_FILE = os.environ.get("MT5_FEED_OFFSET_FILE", "Z:/opt/nasdaq/mt5_feed_offset.json")
WARMUP_BARS = int(os.environ.get("MT5_WARMUP_BARS", "250"))
WARMUP_REFRESH_S = 6 * 3600
POLL = float(os.environ.get("MT5_FEED_POLL", "5"))
TIMEFRAME_NAME = os.environ.get("MT5_FEED_TIMEFRAME", "M15").strip().upper()
# A tick older than this means the terminal is not receiving market data (frozen
# session, kicked login, dead Wine terminal). The engine sees a frozen candle.
TICK_FROZEN_S = float(os.environ.get("MT5_FEED_FROZEN_S", "300"))
RECONNECT_EVERY_S = float(os.environ.get("MT5_FEED_RECONNECT_S", "120"))

# Heartbeat statuses (mirrored in feed_health.summarize_sidecar / docs)
ST_OK = "OK"
ST_INIT_FAILED = "INIT_FAILED"
ST_SYMBOL_FAILED = "SYMBOL_SELECT_FAILED"
ST_TERMINAL_DOWN = "TERMINAL_DISCONNECTED"
ST_NO_TICK = "NO_TICK"
ST_TICK_FROZEN = "TICK_FROZEN"
ST_OFFSET_UNKNOWN = "OFFSET_UNKNOWN"
ST_NO_RATES = "NO_RATES"

# Statuses that mean "the MT5 handle we hold is no longer usable" and are worth
# one bounded shutdown()+initialize(). NO_RATES belongs here: a dead IPC channel
# (MetaTrader5 error -10004 "No IPC connection", typically after the terminal or
# the Wine session restarts) makes copy_rates_from_pos return None forever, so a
# sidecar that only reconnects on TICK_FROZEN can never heal itself - it just
# keeps heartbeating with a frozen feed file (observed 2026-10-30 -> 2026-11-02).
# OFFSET_UNKNOWN belongs here too: the offset is learned from a tick, so a dead
# handle at cold start (no saved offset file) would otherwise stall forever.
RECONNECT_STATUSES = (ST_TICK_FROZEN, ST_NO_RATES, ST_TERMINAL_DOWN, ST_OFFSET_UNKNOWN)


def should_reconnect(status, now_mono: float, last_reconnect_mono: float,
                     every_s: float = RECONNECT_EVERY_S) -> bool:
    """One bounded reconnect per `every_s` while the MT5 handle looks dead."""
    if status not in RECONNECT_STATUSES:
        return False
    return (now_mono - float(last_reconnect_mono or 0.0)) >= every_s


def _resolve_timeframe(mt5_mod, tf_name: str):
    if mt5_mod is None:
        return {"M1": 1, "M5": 5, "M15": 15, "H1": 16385}.get(tf_name, 15)
    tfs = {
        "M1": getattr(mt5_mod, "TIMEFRAME_M1", 1),
        "M5": getattr(mt5_mod, "TIMEFRAME_M5", 5),
        "M15": getattr(mt5_mod, "TIMEFRAME_M15", 15),
        "H1": getattr(mt5_mod, "TIMEFRAME_H1", 16385),
    }
    return tfs.get(tf_name, getattr(mt5_mod, "TIMEFRAME_M15", 15))


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_str(epoch) -> str:
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def estimate_server_offset(server_ts: int, now_epoch: float = None):
    """Return the broker-server offset from UTC for a fresh tick, or None if stale."""
    if not server_ts:
        return None
    now = time.time() if now_epoch is None else float(now_epoch)
    delta = float(server_ts) - now
    candidate = int(round(delta / 3600.0) * 3600)
    if abs(candidate) > 14 * 3600 or abs(delta - candidate) > 300:
        return None
    return candidate


def tick_age_seconds(tick_ts_srv, server_offset, now_epoch: float = None):
    """Age of the terminal's newest tick in seconds (None if unknown)."""
    if not tick_ts_srv or server_offset is None:
        return None
    now = time.time() if now_epoch is None else float(now_epoch)
    return max(0.0, now - (int(tick_ts_srv) - int(server_offset)))


def load_saved_offset(path: str = None):
    """Broker/UTC offset persisted by a previous run (survives a restart)."""
    path = OFFSET_FILE if path is None else path
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        offset = data.get("server_offset_s")
        if offset is None:
            return None
        offset = int(offset)
        if abs(offset) > 14 * 3600:
            return None
        return offset
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def persist_server_offset(server_offset: int, symbol: str = SYMBOL, account=None, path: str = None) -> None:
    path = OFFSET_FILE if path is None else path
    atomic_write_json(path, {
        "symbol": symbol,
        "account": account,
        "server_offset_s": int(server_offset),
        "updated_at": utc_now_str(),
    })


def build_warmup_payload(rates, server_offset: int, symbol: str = SYMBOL,
                         timeframe: str = TIMEFRAME_NAME, account=None) -> dict:
    """Convert historical MT5 rate rows into UTC warm-up candles for engine.py."""
    candles = []
    # MetaTrader5 returns a NumPy structured array here.  Do not use
    # ``rates or ()``: coercing a multi-row ndarray to bool raises
    # "The truth value of an array with more than one element is ambiguous"
    # and traps every poll in the error path before candle publishing.
    if rates is None:
        rates = ()
    for r in rates:
        srv_ts = int(r["time"])
        candles.append({
            "ts": srv_ts - int(server_offset),
            "server_ts": srv_ts,
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "tick_volume": int(r["tick_volume"]),
        })
    candles.sort(key=lambda c: c["ts"])
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "account": account,
        "server_offset_s": int(server_offset),
        "updated_at": utc_now_str(),
        "candles": candles,
    }


def build_last_candle_payload(rate, server_offset: int, symbol: str = SYMBOL,
                              timeframe: str = TIMEFRAME_NAME, account=None) -> dict:
    """Latest CLOSED M15 bar as the engine expects it, tagged with its instrument."""
    srv_ts = int(rate["time"])
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "account": account,
        "ts": srv_ts - int(server_offset),
        "server_ts": srv_ts,
        "server_offset_s": int(server_offset),
        "open": float(rate["open"]),
        "high": float(rate["high"]),
        "low": float(rate["low"]),
        "close": float(rate["close"]),
        "tick_volume": int(rate["tick_volume"]),
        "updated_at": utc_now_str(),
    }


def build_health_payload(status: str, symbol: str = SYMBOL, timeframe: str = TIMEFRAME_NAME,
                         account=None, connected=None, tick_age_s=None, server_offset_s=None,
                         last_candle_payload=None, last_error=None, reconnects=0,
                         started_epoch=None, extra=None) -> dict:
    """Heartbeat the engine/tools/feed_doctor.py read to explain a silent feed."""
    candle_ts = None
    if last_candle_payload:
        candle_ts = last_candle_payload.get("ts")
    payload = {
        "schema": 1,
        "status": status,
        "symbol": symbol,
        "timeframe": timeframe,
        "account": account,
        "terminal_connected": connected,
        "tick_age_s": None if tick_age_s is None else round(float(tick_age_s), 1),
        "server_offset_s": server_offset_s,
        "last_candle_open_utc": utc_str(candle_ts),
        "last_candle_written_utc": (last_candle_payload or {}).get("updated_at"),
        "last_error": last_error,
        "reconnects": int(reconnects),
        "updated_at": utc_now_str(),
        "pid": os.getpid(),
        "uptime_s": None if started_epoch is None else round(time.time() - started_epoch, 1),
    }
    if extra:
        payload.update(extra)
    return payload


def write_health(status: str, **kwargs) -> None:
    try:
        atomic_write_json(HEALTH_OUT, build_health_payload(status, **kwargs))
    except Exception as e:                      # never let the heartbeat kill the feed
        print(f"mt5_feed: health write failed: {e}", flush=True)


def _account_label(account) -> str:
    """Login only - never a password/server string. Used to spot login collisions."""
    if account is None:
        return None
    login = getattr(account, "login", None)
    return str(login) if login is not None else None


def _exit_with(status: str, message: str, account=None, started_epoch=None) -> None:
    """Record a hard failure in the heartbeat before exiting (systemd restarts)."""
    print(f"mt5_feed: {message}", flush=True)
    write_health(status, account=account, last_error=message, started_epoch=started_epoch)
    sys.exit(1)


def main(mt5_mod=None) -> int:
    m = mt5 if mt5_mod is None else mt5_mod
    started = time.time()
    if m is None:
        print("mt5_feed: MetaTrader5 package missing - install it in the WINE python:", flush=True)
        print("  wine C:/Python312/python.exe -m pip install MetaTrader5", flush=True)
        sys.exit(1)

    timeframe = _resolve_timeframe(m, TIMEFRAME_NAME)
    if not m.initialize():
        _exit_with(ST_INIT_FAILED,
                   f"MT5 initialize failed: {m.last_error()} "
                   "(is the terminal running and logged in in this prefix?)",
                   started_epoch=started)
    if not m.symbol_select(SYMBOL, True):
        _exit_with(ST_SYMBOL_FAILED,
                   f"symbol_select({SYMBOL}) failed: {m.last_error()} "
                   "(check exact symbol name in XM Market Watch, e.g. US100Cash)",
                   started_epoch=started)

    try:
        account = _account_label(m.account_info())
    except Exception:
        account = None
    acct = f" | account {account}" if account else ""
    print(f"mt5_feed: publishing closed {TIMEFRAME_NAME} {SYMBOL} candles -> {OUT}{acct}", flush=True)

    # Keep publishing across restarts even before a fresh tick arrives.
    server_offset = load_saved_offset()
    if server_offset is not None:
        print(f"mt5_feed: resumed broker UTC offset {server_offset:+d}s from {OFFSET_FILE}", flush=True)

    prev_tick_ts = None
    last_warmup_write = 0.0
    last_candle_payload = None
    last_error = None
    reconnects = 0
    last_reconnect_mono = 0.0
    status = ST_OK if server_offset is not None else ST_OFFSET_UNKNOWN

    while True:
        try:
            try:
                tinfo = m.terminal_info()
                connected = bool(getattr(tinfo, "connected", False)) if tinfo is not None else None
            except Exception:
                connected = None

            tick = m.symbol_info_tick(SYMBOL)
            tick_ts = int(getattr(tick, "time", 0) or 0) if tick else 0
            if tick_ts and (prev_tick_ts is None or tick_ts != prev_tick_ts):
                learned = estimate_server_offset(tick_ts)
                if learned is not None and learned != server_offset:
                    if server_offset is not None:
                        print(f"mt5_feed: broker UTC offset {server_offset:+d}s -> {learned:+d}s", flush=True)
                    server_offset = learned
                    persist_server_offset(server_offset, SYMBOL, account)
            if tick_ts:
                prev_tick_ts = tick_ts
            age = tick_age_seconds(tick_ts, server_offset)

            now_mono = time.monotonic()
            if server_offset is not None and WARMUP_BARS > 0 and (
                last_warmup_write == 0.0 or (now_mono - last_warmup_write) >= WARMUP_REFRESH_S
            ):
                hist = m.copy_rates_from_pos(SYMBOL, timeframe, 1, WARMUP_BARS)
                if hist is not None and len(hist) > 0:
                    warm_payload = build_warmup_payload(hist, server_offset, SYMBOL,
                                                        TIMEFRAME_NAME, account)
                    atomic_write_json(WARMUP_OUT, warm_payload)
                    last_warmup_write = now_mono
                    print(f"mt5_feed: wrote {len(warm_payload['candles'])} {TIMEFRAME_NAME} warm-up bars -> {WARMUP_OUT}",
                          flush=True)

            if server_offset is None:
                status = ST_OFFSET_UNKNOWN
                last_error = "waiting for a fresh tick to learn the broker UTC offset"
                print(f"mt5_feed: {last_error} (no feed file written until then)", flush=True)
            else:
                rates = m.copy_rates_from_pos(SYMBOL, timeframe, 1, 1)
                if rates is not None and len(rates) > 0:
                    last_candle_payload = build_last_candle_payload(rates[0], server_offset, SYMBOL,
                                                                    TIMEFRAME_NAME, account)
                    atomic_write_json(OUT, last_candle_payload)
                    if age is not None and age > TICK_FROZEN_S:
                        status = ST_TICK_FROZEN
                        last_error = (f"terminal tick is {age / 60:.0f} min old - no live data "
                                      "(session kicked? second terminal on the same login?)")
                        print(f"mt5_feed: {last_error}", flush=True)
                    elif connected is False:
                        status = ST_TERMINAL_DOWN
                        last_error = "terminal_info().connected is False"
                    else:
                        status = ST_OK
                        last_error = None
                else:
                    status = ST_NO_RATES
                    last_error = f"copy_rates_from_pos failed: {m.last_error()}"
                    print(f"mt5_feed: no {TIMEFRAME_NAME} data for {SYMBOL}: {last_error}", flush=True)

            # Dead handle (frozen tick / no rates / terminal gone / never learned the
            # offset) -> one bounded reconnect attempt, never a tight loop. This sits
            # outside the offset branch because a dead handle also blocks learning the
            # offset: without it, NO_RATES would heartbeat forever (2026-11-02 incident).
            if should_reconnect(status, now_mono, last_reconnect_mono):
                last_reconnect_mono = now_mono
                reconnects += 1
                print(f"mt5_feed: reconnect attempt #{reconnects} (shutdown + initialize)", flush=True)
                try:
                    m.shutdown()
                except Exception:
                    pass
                try:
                    if m.initialize():
                        m.symbol_select(SYMBOL, True)
                    else:
                        last_error = f"re-initialize failed: {m.last_error()}"
                except Exception as e:
                    last_error = f"re-initialize error: {e}"

            write_health(
                status,
                symbol=SYMBOL,
                timeframe=TIMEFRAME_NAME,
                account=account,
                connected=connected,
                tick_age_s=age,
                server_offset_s=server_offset,
                last_candle_payload=last_candle_payload,
                last_error=last_error,
                reconnects=reconnects,
                started_epoch=started,
                extra={"feed_file": OUT, "warmup_file": WARMUP_OUT},
            )
            time.sleep(POLL)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"mt5_feed: error: {e} (retrying)", flush=True)
            try:
                write_health(ST_NO_RATES, symbol=SYMBOL, timeframe=TIMEFRAME_NAME, account=account,
                             server_offset_s=server_offset, last_candle_payload=last_candle_payload,
                             last_error=f"{type(e).__name__}: {e}", reconnects=reconnects,
                             started_epoch=started)
            except Exception:
                pass
            time.sleep(POLL)
    m.shutdown()
    print("mt5_feed: stopped", flush=True)
    return 0


if __name__ == "__main__":
    main()
