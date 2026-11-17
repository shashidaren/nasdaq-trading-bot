#!/usr/bin/env python3
"""Feed diagnosis core for the US100Cash M15 stack (`/opt/nasdaq`).

One place that answers the only question that matters when a `STALE_FEED`
journal row appears: **why** is there no usable closed M15 bar?

The engine (`engine.py`) and the triage CLI (`tools/feed_doctor.py`) both import
this module, so the alert text in `journal.csv` and the doctor's verdict can
never drift apart.

Three artefacts are diagnosed:

  `mt5_last_candle.json`   published by `tools/mt5_feed.py` - the closed M15 bar
  `mt5_feed_health.json`   published by `tools/mt5_feed.py` - sidecar heartbeat
  `journal.csv`            engine alerts - used to spot counter resets

Key design rule: **"sidecar unit is active" is not "feed is healthy", and
"engine has not processed a bar recently" is not "the feed is 20 minutes old".**
A missing/unreadable/foreign feed file used to be reported as
"no new closed M15 candles for 20 min", where 20 min was really just *engine
process uptime* - the number reset to zero on every restart. This module always
reports a `reason`, the true freeze instant, and the file's own age.

Importable without MetaTrader5, Flask, or any third-party package.
"""
import csv
import json
import os
import time
from datetime import datetime, timezone

CANDLE_SECONDS = 900           # M15

# --- reasons (stable strings: they land in journal.csv and status.json) ---
REASON_OK = "OK"
REASON_MISSING = "FILE_MISSING"
REASON_UNREADABLE = "FILE_UNREADABLE"
REASON_SCHEMA = "SCHEMA_INVALID"
REASON_SYMBOL = "SYMBOL_MISMATCH"
REASON_OLD = "CANDLE_OLD"
REASON_FUTURE = "CANDLE_IN_FUTURE"

STRUCTURAL_REASONS = (REASON_MISSING, REASON_UNREADABLE, REASON_SCHEMA, REASON_SYMBOL)

# Journal event names, one per failure mode, so the dashboard timeline is readable.
EVENT_BY_REASON = {
    REASON_MISSING: "FEED_FILE_MISSING",
    REASON_UNREADABLE: "FEED_FILE_UNREADABLE",
    REASON_SCHEMA: "FEED_FILE_SCHEMA",
    REASON_SYMBOL: "FEED_SYMBOL_MISMATCH",
    REASON_OLD: "STALE_FEED",
    REASON_FUTURE: "FEED_CLOCK_SKEW",
}

REQUIRED_KEYS = ("ts", "open", "high", "low", "close", "tick_volume")


def _now(now_epoch=None) -> float:
    return time.time() if now_epoch is None else float(now_epoch)


def utc_str(epoch) -> str:
    """UTC timestamp string for an epoch, else None (never raises)."""
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError, TypeError):
        return None


def normalize_symbol(name) -> str:
    """Case/punctuation-insensitive symbol key (US100Cash ~ us100cash ~ US100.CASH)."""
    if not name:
        return ""
    return "".join(ch for ch in str(name).upper() if ch.isalnum())


def read_json_file(path):
    """Return (payload, error_message). Exactly one of them is None."""
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh), None
    except FileNotFoundError:
        return None, f"{os.path.basename(path)} not found"
    except Exception as exc:                      # unreadable / truncated / not JSON
        return None, f"{os.path.basename(path)} unreadable: {type(exc).__name__}: {exc}"


def file_age_seconds(path, now_epoch=None):
    """Age of a file's mtime in seconds, or None when the file does not exist."""
    try:
        return max(0.0, _now(now_epoch) - os.path.getmtime(path))
    except OSError:
        return None


def _empty_diag(path):
    return {
        "path": path,
        "ok": False,
        "usable": False,          # structurally valid (a foreign/stale bar must never trade)
        "reason": REASON_MISSING,
        "detail": "",
        "exists": False,
        "file_age_s": None,
        "file_mtime_utc": None,
        "symbol": None,
        "raw_symbol": None,
        "timeframe": None,
        "server_offset_s": None,
        "updated_at": None,
        "candle_open_ts": None,
        "candle_open_utc": None,
        "candle_age_s": None,
        "frozen_since_utc": None,
        "rates": None,
    }


def diagnose_feed_file(path, expected_symbol=None, now_epoch=None,
                       candle_seconds=CANDLE_SECONDS, max_age_s=20 * 60) -> dict:
    """Fully classify `mt5_last_candle.json`.

    Returns a dict (see `_empty_diag`) where:
      `ok`       the bar is fresh enough to trade on
      `usable`   the file parses, matches `expected_symbol`, and holds a
                 plausible non-future candle (safe to hand to the engine)
      `reason`   one of REASON_* - the answer to "why is there no bar?"
    """
    diag = _empty_diag(path)
    now = _now(now_epoch)

    payload, err = read_json_file(path)
    if payload is None:
        diag["reason"] = REASON_MISSING if not os.path.exists(path) else REASON_UNREADABLE
        diag["detail"] = err
        diag["exists"] = os.path.exists(path)
        diag["file_age_s"] = file_age_seconds(path, now)
        diag["file_mtime_utc"] = utc_str(now - diag["file_age_s"]) if diag["file_age_s"] is not None else None
        return diag

    diag["exists"] = True
    diag["file_age_s"] = file_age_seconds(path, now)
    diag["file_mtime_utc"] = utc_str(now - diag["file_age_s"]) if diag["file_age_s"] is not None else None

    if not isinstance(payload, dict):
        diag["reason"] = REASON_SCHEMA
        diag["detail"] = f"{os.path.basename(path)} holds {type(payload).__name__}, not an object"
        return diag

    diag["raw_symbol"] = payload.get("symbol")
    diag["symbol"] = payload.get("symbol")
    diag["timeframe"] = payload.get("timeframe")
    diag["server_offset_s"] = payload.get("server_offset_s")
    diag["updated_at"] = payload.get("updated_at")

    missing = [k for k in REQUIRED_KEYS if k not in payload]
    if missing:
        diag["reason"] = REASON_SCHEMA
        diag["detail"] = (f"{os.path.basename(path)} is missing key(s) {missing} "
                          f"(keys present: {sorted(payload)[:8]})")
        return diag

    try:
        candle_ts = int(payload["ts"])
        rates = [{
            "time": candle_ts,
            "open": float(payload["open"]),
            "high": float(payload["high"]),
            "low": float(payload["low"]),
            "close": float(payload["close"]),
            "tick_volume": int(payload["tick_volume"]),
        }]
    except (TypeError, ValueError) as exc:
        diag["reason"] = REASON_SCHEMA
        diag["detail"] = f"{os.path.basename(path)} has non-numeric OHLC: {exc}"
        return diag

    if expected_symbol and diag["raw_symbol"] and \
            normalize_symbol(diag["raw_symbol"]) != normalize_symbol(expected_symbol):
        diag["reason"] = REASON_SYMBOL
        diag["detail"] = (f"{os.path.basename(path)} carries {diag['raw_symbol']} but this engine "
                          f"trades {expected_symbol} - two stacks are writing the same feed path")
        return diag

    # MT5 `time` is the bar's OPEN time; it is closed `candle_seconds` later.
    candle_age = now - (candle_ts + candle_seconds)
    diag["candle_open_ts"] = candle_ts
    diag["candle_open_utc"] = utc_str(candle_ts)
    diag["candle_age_s"] = max(0.0, candle_age)
    diag["frozen_since_utc"] = utc_str(candle_ts + candle_seconds)
    diag["rates"] = rates

    if candle_age < -candle_seconds:
        diag["reason"] = REASON_FUTURE
        diag["detail"] = (f"bar opens {utc_str(candle_ts)} UTC, in the future vs the engine clock "
                          f"({utc_str(now)} UTC) - check tzdata / server_offset_s")
        diag["usable"] = False
        return diag

    diag["usable"] = True
    if candle_age > max_age_s:
        diag["reason"] = REASON_OLD
        diag["ok"] = False
        diag["detail"] = (f"last closed M15 bar ({utc_str(candle_ts)} UTC open) closed at "
                          f"{diag['frozen_since_utc']} UTC")
    else:
        diag["reason"] = REASON_OK
        diag["ok"] = True
        diag["detail"] = f"last closed bar {utc_str(candle_ts)} UTC, {candle_age / 60:.0f} min old"
    return diag


def diagnose_sidecar_health(path, now_epoch=None, stale_after_s=120) -> dict:
    """Read `mt5_feed_health.json` (heartbeat written every poll by tools/mt5_feed.py)."""
    out = {
        "path": path,
        "present": False,
        "fresh": False,
        "age_s": None,
        "updated_at": None,
        "status": None,
        "tick_age_s": None,
        "terminal_connected": None,
        "server_offset_s": None,
        "symbol": None,
        "account": None,
        "last_error": None,
        "reconnects": None,
        "uptime_s": None,
    }
    payload, _err = read_json_file(path)
    if payload is None:
        return out
    out["present"] = True
    out["age_s"] = file_age_seconds(path, now_epoch)
    out["fresh"] = out["age_s"] is not None and out["age_s"] <= stale_after_s
    for key in ("updated_at", "status", "tick_age_s", "terminal_connected",
                "server_offset_s", "symbol", "account", "last_error", "reconnects", "uptime_s"):
        if key in payload:
            out[key] = payload[key]
    return out


def summarize_sidecar(health: dict) -> str:
    """One-line sidecar verdict for the alert body ('' when no heartbeat exists)."""
    if not health or not health.get("present"):
        return ""
    parts = [f"sidecar status={health.get('status') or 'unknown'}"]
    if health.get("age_s") is not None:
        parts.append(f"heartbeat {health['age_s'] / 60:.0f} min old" if health["age_s"] >= 90
                     else f"heartbeat {health['age_s']:.0f}s old")
    if not health.get("fresh"):
        parts.append("(sidecar may be stopped)")
    if health.get("tick_age_s") is not None:
        parts.append(f"tick_age={health['tick_age_s'] / 60:.0f} min"
                     if health["tick_age_s"] >= 90 else f"tick_age={health['tick_age_s']:.0f}s")
    if health.get("server_offset_s") is None:
        parts.append("broker UTC offset unknown")
    if health.get("last_error"):
        parts.append(f"err={health['last_error']}")
    return ", ".join(parts)


def format_feed_problem(diag: dict, health: dict = None, engine_uptime_s=None) -> tuple:
    """Map a diagnosis to (journal_event, human detail) for journal.csv.

    Every variant names the *reason*, the artefact, and the concrete next command,
    and every variant reports the same freeze instant on repeats (so three alerts
    an hour apart no longer look like three separate incidents).
    """
    event = EVENT_BY_REASON.get(diag.get("reason"), "STALE_FEED")
    reason = diag.get("reason")
    base = os.path.basename(diag.get("path") or "mt5_last_candle.json")
    sidecar = summarize_sidecar(health)
    tail = " - run python3 tools/feed_doctor.py" + (f" [{sidecar}]" if sidecar else "")

    if reason == REASON_MISSING:
        if not engine_uptime_s or engine_uptime_s < 60:
            up = "engine just started"
        else:
            up = f"engine up {engine_uptime_s / 60:.0f} min"
        detail = f"{base} missing ({diag.get('path')}; {up}) - mt5feed sidecar is not publishing"
        # A live heartbeat with no file on this path means a path mismatch, not a dead sidecar.
        if health and health.get("present") and health.get("fresh") and \
                str(health.get("status") or "").upper() == "OK":
            detail += " (its heartbeat is healthy: it is writing a DIFFERENT path - compare MT5_FEED_FILE)"
    elif reason == REASON_UNREADABLE:
        detail = f"{base} is unreadable ({diag.get('detail')}) - writer died mid-write or a foreign writer"
    elif reason == REASON_SCHEMA:
        detail = f"{base} has an unexpected schema ({diag.get('detail')})"
    elif reason == REASON_SYMBOL:
        detail = str(diag.get("detail"))
    elif reason == REASON_FUTURE:
        detail = str(diag.get("detail"))
    elif reason == REASON_OLD:
        age = diag.get("candle_age_s")
        age_txt = f"{age / 60:.0f} min" if age is not None else "unknown age"
        file_age = diag.get("file_age_s")
        if file_age is None:
            file_txt = f"{base} age unknown"
        elif file_age < 5 * 60:
            file_txt = f"{base} still refreshed {file_age:.0f}s ago - sidecar alive, only bars frozen"
        else:
            file_txt = f"{base} last written {file_age / 60:.0f} min ago - sidecar stopped publishing"
        detail = (f"feed frozen: last closed M15 bar opened {diag.get('candle_open_utc')} UTC, "
                  f"closed {diag.get('frozen_since_utc')} UTC ({age_txt} ago; {file_txt})")
    else:
        detail = f"{base} unusable ({reason})"
    return event, detail + tail


def parse_journal_feed_events(path, limit=400) -> list:
    """Newest-first feed alerts from journal.csv with their reported minutes."""
    rows = []
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            for row in csv.DictReader(fh):
                event = (row.get("Event") or "").strip()
                if not (event.startswith("STALE_FEED") or event.startswith("FEED_")):
                    continue
                detail = (row.get("Detail") or "").strip()
                rows.append({
                    "timestamp": (row.get("Timestamp") or "").strip(),
                    "event": event,
                    "detail": detail,
                    "minutes": _minutes_from_detail(detail),
                })
    except (OSError, csv.Error):
        return []
    return rows[-limit:]


def _minutes_from_detail(detail: str):
    """'no new closed M15 candles for 80 min - ...' -> 80 (None if absent)."""
    marker = " for "
    if marker not in detail:
        return None
    rest = detail.split(marker, 1)[1].strip()
    number = rest.split()[0] if rest else ""
    try:
        return float(number)
    except ValueError:
        return None


def detect_counter_resets(events) -> list:
    """Flag alerts whose reported age went DOWN: the counter restarted.

    The historical bug: the engine measured "stale" from *its own* last processed
    bar of the current process, so a restart turned "890 min" into "20 min".
    A drop of more than 5 minutes between consecutive alerts means the clock was
    reset (engine restart), not that the feed recovered.
    """
    resets = []
    prev = None
    for ev in events:
        mins = ev.get("minutes")
        if mins is None:
            prev = ev
            continue
        if prev is not None and ev.get("minutes") is not None and \
                mins < prev["minutes"] - 5:
            resets.append({
                "timestamp": ev.get("timestamp"),
                "from_minutes": prev["minutes"],
                "to_minutes": mins,
                "note": ("earlier alerts measured engine uptime, not feed age - the feed "
                         "did not recover, the counter restarted"),
            })
        prev = ev
    return resets
