#!/usr/bin/env python3
import csv
import json
import math
import os
import time
from datetime import datetime, timezone
from functools import lru_cache

from flask import Flask, render_template_string, request

import feed_health
import pressure as tape_pressure
from tools import live_readiness
from trade_filter import MAX_DAILY_LOSS_USD, is_market_quiet

app = Flask(__name__)

DATA_DIR = os.getenv("NASDAQ_DATA_DIR", "/opt/nasdaq")
LOG_FILE_PATH = os.getenv("NASDAQ_LOG_FILE", os.path.join(DATA_DIR, "forward_test_log.csv"))
STATUS_FILE_PATH = os.getenv("NASDAQ_STATUS_FILE", os.path.join(DATA_DIR, "status.json"))
TRADES_FILE_PATH = os.getenv("NASDAQ_TRADES_FILE", os.path.join(DATA_DIR, "trades.csv"))
JOURNAL_FILE_PATH = os.getenv("NASDAQ_JOURNAL_FILE", os.path.join(DATA_DIR, "journal.csv"))
MT5_FEED_FILE = os.getenv("MT5_FEED_FILE", os.path.join(DATA_DIR, "mt5_last_candle.json"))
MT5_FEED_HEALTH_FILE = os.getenv("MT5_FEED_HEALTH_FILE", os.path.join(DATA_DIR, "mt5_feed_health.json"))
AUTOSYNC_HEARTBEAT_FILE = os.getenv("AUTOSYNC_HEARTBEAT_FILE", os.path.join(DATA_DIR, "autosync_heartbeat.json"))
AUTOSYNC_STALE_SECONDS = 45 * 60


def autosync_status(heartbeat_path=None, now_epoch=None, max_age_s=AUTOSYNC_STALE_SECONDS):
    """Read-only liveness summary of tools/autosync.sh's heartbeat stamp (STALE > 45 min)."""
    heartbeat_path = heartbeat_path or AUTOSYNC_HEARTBEAT_FILE
    now = time.time() if now_epoch is None else float(now_epoch)
    stale_min = int(max_age_s // 60)
    base = os.path.basename(heartbeat_path)
    if not os.path.exists(heartbeat_path):
        return {
            "ok": False, "stale": False, "present": False, "level": "NO HEARTBEAT",
            "age_s": None, "age_label": None, "ts_utc": None, "stale_min": stale_min,
            "detail": f"{base} not found — waiting for next cron run, or cron is paused.",
        }

    def unreadable(reason):
        return {
            "ok": False, "stale": True, "present": True, "level": "UNREADABLE",
            "age_s": None, "age_label": None, "ts_utc": None, "stale_min": stale_min,
            "detail": f"{base} {reason}; check root crontab and /var/log/nasdaq-autosync.log.",
        }

    try:
        with open(heartbeat_path, encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return unreadable("is unreadable or invalid JSON")
    if not isinstance(payload, dict):
        return unreadable("must be a JSON object")

    ts_epoch = None
    raw_epoch = payload.get("ts_epoch")
    if raw_epoch is not None:
        if isinstance(raw_epoch, bool):
            return unreadable("has invalid ts_epoch")
        try:
            ts_epoch = float(raw_epoch)
        except (TypeError, ValueError):
            return unreadable("has invalid ts_epoch")
        if not math.isfinite(ts_epoch) or ts_epoch <= 0:
            return unreadable("has invalid ts_epoch")

    raw_utc = payload.get("ts_utc")
    parsed_utc = tape_pressure.parse_ts(raw_utc) if isinstance(raw_utc, str) else None
    if ts_epoch is None:
        if parsed_utc is None:
            return unreadable("is missing ts_epoch/ts_utc")
        ts_epoch = parsed_utc.replace(tzinfo=timezone.utc).timestamp()

    try:
        ts_utc = (parsed_utc.strftime("%Y-%m-%d %H:%M:%S") if parsed_utc is not None
                  else datetime.fromtimestamp(ts_epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    except (OSError, OverflowError, ValueError):
        return unreadable("has out-of-range timestamp")

    raw_rc = payload.get("exit_code", 0)
    if isinstance(raw_rc, bool):
        return unreadable("has invalid exit_code")
    try:
        exit_code = int(raw_rc)
    except (TypeError, ValueError):
        return unreadable("has invalid exit_code")

    raw_status = payload.get("status", "ok")
    if not isinstance(raw_status, str) or raw_status.strip().lower() not in ("ok", "warn", "error"):
        return unreadable("has invalid status")
    run_status = raw_status.strip().lower()

    raw_age = now - ts_epoch
    if raw_age < -60:
        return {
            "ok": False, "stale": True, "present": True, "level": "STALE",
            "age_s": None, "age_label": "clock skew", "ts_utc": ts_utc, "stale_min": stale_min,
            "detail": "Heartbeat timestamp is in the future; check host clock.",
        }

    age_s = max(0.0, raw_age)
    if age_s < 90:
        age_label = f"{int(round(age_s))}s ago"
    elif max_age_s < age_s < max_age_s + 30:
        age_label = f"{int(age_s // 60)}m {int(age_s % 60):02d}s ago"
    elif age_s < 120 * 60:
        age_label = f"{int(round(age_s / 60))} min ago"
    else:
        age_label = f"{age_s / 3600:.1f} h ago"

    meta_parts = []
    for key in ("push", "deploy", "notify"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            clean = val.strip()
            if len(clean) > 48:
                clean = clean[:45] + "..."
            meta_parts.append(f"{key}: {clean}")
    meta = " · ".join(meta_parts)

    if age_s > max_age_s:
        suffix = f" ({meta})" if meta else ""
        return {
            "ok": False, "stale": True, "present": True, "level": "STALE",
            "age_s": round(age_s, 1), "age_label": age_label, "ts_utc": ts_utc,
            "stale_min": stale_min,
            "detail": f"Older than {stale_min} min — check root crontab & /var/log/nasdaq-autosync.log{suffix}",
        }
    if exit_code != 0 or run_status == "error":
        phase = str(payload.get("phase") or "unknown").strip()[:24]
        suffix = f" · {meta}" if meta else ""
        return {
            "ok": False, "stale": False, "present": True, "level": "ERROR",
            "age_s": round(age_s, 1), "age_label": age_label, "ts_utc": ts_utc,
            "stale_min": stale_min,
            "detail": f"exit {exit_code} ({phase}){suffix}",
        }
    if run_status == "warn":
        return {
            "ok": False, "stale": False, "present": True, "level": "WARN",
            "age_s": round(age_s, 1), "age_label": age_label, "ts_utc": ts_utc,
            "stale_min": stale_min,
            "detail": meta or "Completed with warnings; inspect /var/log/nasdaq-autosync.log.",
        }
    return {
        "ok": True, "stale": False, "present": True, "level": "OK",
        "age_s": round(age_s, 1), "age_label": age_label, "ts_utc": ts_utc,
        "stale_min": stale_min,
        "detail": meta or "Last autosync completed cleanly.",
    }


def feed_banner(status, health_path=None, feed_path=None, now_epoch=None):
    """Diagnose the actual feed now; an old status.json OK is never a fallback."""
    health_path = health_path or MT5_FEED_HEALTH_FILE
    feed_path = feed_path or MT5_FEED_FILE
    now = time.time() if now_epoch is None else float(now_epoch)
    health_error = None
    try:
        health = feed_health.diagnose_sidecar_health(health_path, now)
        sidecar = feed_health.summarize_sidecar(health)
    except (TypeError, ValueError, OverflowError) as exc:
        health = {"present": False, "fresh": False}
        sidecar = ""
        health_error = f"heartbeat data is invalid: {type(exc).__name__}"
    try:
        diag = feed_health.diagnose_feed_file(feed_path, expected_symbol="US100Cash", now_epoch=now)
    except (TypeError, ValueError, OverflowError) as exc:
        diag = {"ok": False, "reason": feed_health.REASON_SCHEMA, "exists": os.path.exists(feed_path),
                "detail": f"feed data is invalid: {type(exc).__name__}"}
    rates = diag.get("rates") or []
    if diag.get("usable") and rates:
        r = rates[0]
        values = [r[k] for k in ("open", "high", "low", "close")]
        if (not all(math.isfinite(value) for value in values) or min(values) <= 0
                or r["high"] < max(r["open"], r["close"])
                or r["low"] > min(r["open"], r["close"]) or r["tick_volume"] < 0):
            diag.update(ok=False, reason=feed_health.REASON_SCHEMA, detail="Feed OHLC/volume is invalid.")
        elif (feed_health.normalize_symbol(diag.get("symbol")) != "US100CASH"
              or str(diag.get("timeframe") or "").upper() != "M15"):
            diag.update(ok=False, reason=feed_health.REASON_SCHEMA,
                        detail="Feed must be explicitly tagged US100Cash M15.")
    # A forming bar is not a closed M15 candle, even if its open time is plausible.
    if diag.get("ok") and diag["candle_open_ts"] + feed_health.CANDLE_SECONDS > now:
        diag.update(ok=False, reason=feed_health.REASON_FUTURE,
                    detail="The reported M15 bar has not closed yet; check feed clock/offset.")
    quiet = is_market_quiet(datetime.fromtimestamp(now, tz=timezone.utc))
    heartbeat_in_future = False
    if health.get("present"):
        try:
            heartbeat_in_future = os.path.getmtime(health_path) > now + 60
        except OSError:
            heartbeat_in_future = True
    sidecar_ok = (health.get("present") and health.get("fresh") and not heartbeat_in_future
                  and health.get("status") == "OK" and health.get("terminal_connected") is True
                  and feed_health.normalize_symbol(health.get("symbol")) == "US100CASH")

    if diag.get("ok") and sidecar_ok:
        level = "OK"
    elif diag.get("reason") in feed_health.STRUCTURAL_REASONS:
        level = "BROKEN"
    elif quiet and diag.get("reason") == feed_health.REASON_OLD:
        level = "MARKET QUIET"
    elif diag.get("ok"):
        level = "DEGRADED"
    else:
        level = "STALE"

    def mins(seconds):
        return "n/a" if seconds is None else (f"{seconds / 60:.0f} min" if seconds >= 90
                                              else f"{seconds:.0f}s")

    lines = []
    if diag.get("exists"):
        lines.append(f"last closed M15 bar: {diag.get('candle_open_utc')} UTC open "
                     f"({mins(diag.get('candle_age_s'))} old), file written "
                     f"{mins(diag.get('file_age_s'))} ago")
    else:
        lines.append(f"{os.path.basename(feed_path)} missing - the mt5feed sidecar is not publishing")
    lines.append(health_error or sidecar or "no mt5feed heartbeat (mt5_feed_health.json)")
    if heartbeat_in_future:
        lines.append("heartbeat file timestamp is in the future; check clocks and shared storage.")
    if quiet:
        lines.append("CME market-quiet window; a quiet market is not proof of a healthy feed.")
    return {
        "ok": level == "OK",
        "level": level,
        "reason": diag.get("reason"),
        "detail": diag.get("detail"),
        "frozen_since_utc": diag.get("frozen_since_utc"),
        "lines": lines,
    }


def _file_signature(path):
    stat = os.stat(path)
    return stat.st_ino, stat.st_size, stat.st_mtime_ns


@lru_cache(maxsize=8)
def _cached_evidence(trades_path, trades_signature, log_path, log_signature):
    """Avoid repeating the bootstrap on every five-second dashboard refresh."""
    trades, bars = live_readiness.load_readiness_data(trades_path, log_path)
    if (_file_signature(trades_path) != trades_signature or
            _file_signature(log_path) != log_signature):
        raise ValueError("evidence changed while being read; retry the refresh")
    evidence = live_readiness.evaluate_data_gates(trades, bars)
    if (_file_signature(trades_path) != trades_signature or
            _file_signature(log_path) != log_signature):
        raise ValueError("evidence changed during calculation; retry the refresh")
    return trades, bars, evidence


def readiness_snapshot(status, status_available, feed, now_epoch=None):
    """Build a fail-closed, read-only snapshot without authorizing LIVE trading."""
    now = time.time() if now_epoch is None else float(now_epoch)
    result = {
        "ok": False, "verdict": "NOT READY — EVIDENCE UNAVAILABLE", "error": None,
        "gates": [], "gates_passed": 0, "gates_total": 0,
        "non_data": live_readiness.NON_DATA_GATES, "stats": None,
        "min_trades": live_readiness.MIN_TRADES, "spread": live_readiness.DEFAULT_SPREAD,
        "snapshot_issues": [], "last_bar": None,
    }
    try:
        trades, bars, evidence = _cached_evidence(
            TRADES_FILE_PATH, _file_signature(TRADES_FILE_PATH),
            LOG_FILE_PATH, _file_signature(LOG_FILE_PATH))
        pg = evidence["post_gate"]
        wins = sum(t["profit"] > 0 for t in pg)
        losses = sum(t["profit"] < 0 for t in pg)
        result["stats"] = {
            "closed": len(trades), "post_gate": len(pg), "wins": wins, "losses": losses,
            "flat": len(pg) - wins - losses, "win_rate": 100 * wins / len(pg) if pg else None,
            "net": sum(t["profit"] - live_readiness.DEFAULT_SPREAD for t in pg),
        }
        result["gates"] = evidence["gates"]
        result["gates_total"] = len(evidence["gates"])
        result["gates_passed"] = sum(g["ok"] is True for g in evidence["gates"])
        result["last_bar"] = bars[-1]["dt"].strftime("%Y-%m-%d %H:%M UTC") if bars else None

        def fresh(dt):
            if dt is None:
                return False
            age = now - dt.replace(tzinfo=timezone.utc).timestamp()
            return -60 <= age <= 20 * 60

        issues = result["snapshot_issues"]
        if not bars or not fresh(bars[-1]["dt"]):
            issues.append("The latest logged M15 bar is missing or more than 20 minutes old; "
                          "check the engine and market hours.")
        if not status_available:
            issues.append("status.json is missing or unreadable; engine balance and risk state are unknown.")
        else:
            if not fresh(tape_pressure.parse_ts(status.get("last_update"))):
                issues.append("The engine status timestamp is missing or more than 20 minutes old.")
            if status.get("symbol") != "US100Cash":
                issues.append("The engine status does not identify US100Cash.")
            if status["daily_losses"] >= status["max_daily_losses"]:
                issues.append("The reported daily stop-loss circuit breaker is halted.")
            today = datetime.fromtimestamp(now, tz=timezone.utc).date()
            daily_pnl = sum(t["profit"] for t in trades if t["xt"].date() == today)
            if daily_pnl <= -MAX_DAILY_LOSS_USD:
                issues.append(f"The ledger's UTC-day P/L (${daily_pnl:+.2f}) trips the "
                              f"${MAX_DAILY_LOSS_USD:.2f} daily loss limit.")
            ledger_wins = sum(t["reason"] == "TP" for t in trades)
            ledger_losses = sum(t["reason"] == "SL" for t in trades)
            for key, expected in (("total_trades", len(trades)),
                                  ("wins", ledger_wins), ("losses", ledger_losses)):
                try:
                    if int(status.get(key)) != expected:
                        raise ValueError
                except (TypeError, ValueError):
                    issues.append("status.json trade counters do not match the ledger.")
                    break

        if not feed["ok"]:
            result["verdict"] = "NOT READY — FEED"
        elif issues:
            result["verdict"] = "NOT READY — SNAPSHOT"
        elif result["gates_passed"] != result["gates_total"]:
            result["verdict"] = "NOT READY — DATA GATES"
        else:
            result["ok"] = True
            result["verdict"] = "DATA GATES PASS — MANUAL REVIEW REQUIRED"
    except (OSError, ValueError, csv.Error) as exc:
        result["error"] = f"Evidence unavailable or invalid: {exc}"
    except Exception as exc:
        # Read-only UI must fail closed rather than hide a broken computation.
        result["error"] = f"Readiness computation failed: {type(exc).__name__}: {exc}"
    return result

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nasdaq-100 (US100Cash) Trading Engine Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>.hidden { display: none !important; }</style>
    <meta http-equiv="refresh" content="5">
</head>
<body class="bg-slate-900 text-slate-100 font-sans p-6">
    <div class="max-w-7xl mx-auto">

        <!-- Tab navigation -->
        <nav aria-label="Dashboard sections" class="flex flex-wrap gap-2 mb-6 border-b border-slate-700 pb-2">
            <a id="tab-dashboard" href="/" {% if active_tab == 'dashboard' %}aria-current="page"{% endif %}
                class="px-4 py-2 rounded-lg text-sm font-semibold {{ 'bg-cyan-600 text-white shadow-md' if active_tab == 'dashboard' else 'text-slate-400 hover:text-slate-300' }}">
                NDX Dashboard
            </a>
            <a id="tab-journal" href="/journal" {% if active_tab == 'journal' %}aria-current="page"{% endif %}
                class="px-4 py-2 rounded-lg text-sm font-semibold {{ 'bg-cyan-600 text-white shadow-md' if active_tab == 'journal' else 'text-slate-400 hover:text-slate-300' }}">
                Journal (event log)
            </a>
            <a id="tab-readiness" href="/readiness" {% if active_tab == 'readiness' %}aria-current="page"{% endif %}
                class="px-4 py-2 rounded-lg text-sm font-semibold {{ 'bg-cyan-600 text-white shadow-md' if active_tab == 'readiness' else 'text-slate-400 hover:text-slate-300' }}">
                Readiness
            </a>
        </nav>

        <div id="panel-dashboard" class="tab-panel{% if active_tab != 'dashboard' %} hidden{% endif %}">

            <!-- Header -->
        <header class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 border-b border-slate-800 pb-4 gap-4">
            <div>
                <h1 class="text-3xl font-bold text-cyan-400">Nasdaq-100 ({{ status.symbol or 'US100Cash' }}) Engine</h1>
                <p class="text-slate-400 text-sm">
                    XM US100Cash Bidirectional M15 &bull; Auto-refreshes every 5s &bull; Last update: {{ status.last_update or '&mdash;' }}
                </p>
            </div>
            <div class="flex items-center gap-4">
                <div class="bg-slate-800 px-4 py-2 rounded-xl border border-slate-700 text-right">
                    <span class="text-xs text-slate-400 block">Daily SLs</span>
                    <span class="text-xl font-bold {{ 'text-rose-400' if status_available and status.daily_losses >= status.max_daily_losses else 'text-slate-200' }}">
                        {% if status_available %}
                        {{ status.daily_losses }} / {{ status.max_daily_losses }}
                        {% if status.daily_losses >= status.max_daily_losses %}
                        <span class="text-xs bg-rose-900/60 text-rose-300 px-2 py-0.5 rounded ml-1 font-semibold">HALTED</span>
                        {% endif %}
                        {% else %}Unavailable{% endif %}
                    </span>
                </div>
                <div class="bg-slate-800 px-5 py-3 rounded-xl border border-slate-700 text-right">
                    <span class="text-xs text-slate-400 block">Engine Ledger Balance</span>
                    <span class="text-3xl font-semibold {{ 'text-emerald-400' if status_available and status.equity >= 200 else 'text-slate-400' }}">
                        {% if status_available %}${{ "%.2f"|format(status.equity) }}{% else %}Unavailable{% endif %}
                    </span>
                    <span class="text-[10px] text-slate-500 block">0.10 lot US100Cash ($0.10/pt)</span>
                </div>
            </div>
        </header>

        {% if feed and not feed.ok %}
        <div role="status" class="bg-rose-950/60 border border-rose-700/60 text-rose-100 rounded-lg px-4 py-3 mb-5 text-sm">
            <p class="font-semibold">M15 feed: {{ feed.level }} ({{ feed.reason }})</p>
            <p class="text-rose-200/90">{{ feed.detail }}</p>
            <ul class="mt-1 text-xs text-rose-300/80 list-disc list-inside">
                {% for line in feed.lines %}<li>{{ line }}</li>{% endfor %}
            </ul>
            <p class="mt-1 text-xs text-rose-300/70">Triage: <code>python3 tools/feed_doctor.py</code></p>
        </div>
        {% endif %}

        {% if not status_available %}
        <div role="status" class="bg-amber-950/60 border border-amber-700/60 text-amber-200 rounded-lg px-4 py-3 mb-5 text-sm">
            Engine status is unavailable or invalid. Risk state, balance and status-derived counters are not verified.
        </div>
        {% endif %}
        {% if trade_stats_warning %}
        <div role="status" class="bg-amber-950/60 border border-amber-700/60 text-amber-200 rounded-lg px-4 py-3 mb-5 text-sm">
            {{ trade_stats_warning }}
        </div>
        {% endif %}

        <!-- Main Stats -->
        <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-7 gap-4 mb-6">
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">Closed Trades</p>
                <p class="text-2xl font-bold text-cyan-400">{{ status.total_trades }}</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">Wins</p>
                <p class="text-2xl font-bold text-emerald-400">{{ status.wins }}</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">Losses</p>
                <p class="text-2xl font-bold text-rose-400">{{ status.losses }}</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">Win Rate</p>
                <p class="text-2xl font-bold text-blue-400">{{ status.win_rate }}%</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">Next Trade #</p>
                <p class="text-2xl font-bold text-cyan-400">{{ status.next_trade_num or '&mdash;' }}</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">Active Trade</p>
                <p class="text-2xl font-bold {{ 'text-emerald-400' if status.trade_active else 'text-slate-500' }}">
                    {% if status.trade_active %}
                        {{ status.trade_type or 'BUY' }}
                    {% else %}
                        No
                    {% endif %}
                </p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">M15 Candles</p>
                <p class="text-2xl font-bold">{{ total }}</p>
            </div>
        </div>

        <!-- Active Trade Panel -->
        {% if status.trade_active %}
        <div class="bg-slate-800 border {{ 'border-emerald-600/50' if status.trade_type == 'BUY' else 'border-rose-600/50' }} rounded-xl p-4 mb-6">
            <div class="flex items-center justify-between mb-2">
                <p class="text-sm font-semibold {{ 'text-emerald-400' if status.trade_type == 'BUY' else 'text-rose-400' }}">
                    Active {{ status.trade_type or 'BUY' }} Position (#{{ status.current_trade_num or '&mdash;' }})
                </p>
                <span class="text-xs text-slate-400">Entered: {{ status.entry_time or '&mdash;' }}</span>
            </div>
            <div class="grid grid-cols-3 gap-4 text-sm">
                <div>
                    <span class="text-slate-400">Entry</span><br>
                    <span class="text-lg font-semibold">{{ "%.2f"|format(status.entry_price or 0) }}</span>
                </div>
                <div>
                    <span class="text-slate-400">Stop Loss</span><br>
                    <span class="text-lg font-semibold text-rose-400">{{ "%.2f"|format(status.stop_loss or 0) }}</span>
                </div>
                <div>
                    <span class="text-slate-400">Take Profit</span><br>
                    <span class="text-lg font-semibold text-emerald-400">{{ "%.2f"|format(status.take_profit or 0) }}</span>
                </div>
            </div>
        </div>
        {% endif %}

        <!-- Indicator Snapshot -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">EMA 50 (M15)</p>
                <p class="text-xl font-bold text-cyan-400">
                    {% if status.ema_fast %}{{ "%.2f"|format(status.ema_fast) }}{% else %}&mdash;{% endif %}
                </p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">EMA 200 (M15)</p>
                <p class="text-xl font-bold text-blue-400">
                    {% if status.ema_slow %}{{ "%.2f"|format(status.ema_slow) }}{% else %}&mdash;{% endif %}
                </p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">RSI (14)</p>
                <p class="text-xl font-bold text-purple-400">
                    {% if status.rsi %}{{ "%.1f"|format(status.rsi) }}{% else %}&mdash;{% endif %}
                </p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-xs text-slate-400">ATR (14, pts)</p>
                <p class="text-xl font-bold text-orange-400">
                    {% if status.atr %}{{ "%.2f"|format(status.atr) }}{% else %}&mdash;{% endif %}
                </p>
            </div>
        </div>

        <!-- Descriptive tape-pressure read (never an entry/exit signal) -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
            <section class="lg:col-span-2 bg-slate-800 p-5 rounded-xl border border-slate-700">
                <div class="flex flex-wrap items-start justify-between gap-3 mb-4">
                    <div>
                        <h2 class="text-lg font-semibold text-slate-100">Buy / Sell Tape Pressure</h2>
                        <p class="text-xs text-slate-400">{{ pressure.window if pressure else 60 }} closed M15 bars · 15-hour context · {{ pressure.as_of if pressure and pressure.as_of else 'warming up' }}</p>
                    </div>
                    <span class="text-xs px-2.5 py-1 rounded-full bg-amber-900/40 text-amber-300 border border-amber-700/40">Context only · not a signal</span>
                </div>
                {% if pressure %}
                <div class="flex items-center justify-between text-sm mb-2">
                    <span class="font-semibold text-emerald-400">BUY {{ "%.1f"|format(pressure.buy_pct) }}%</span>
                    <span class="text-slate-300">{{ pressure.label }}</span>
                    <span class="font-semibold text-rose-400">SELL {{ "%.1f"|format(pressure.sell_pct) }}%</span>
                </div>
                <div class="h-3 w-full rounded-full overflow-hidden bg-rose-500/80 flex mb-5" role="img" aria-label="Buy pressure {{ '%.1f'|format(pressure.buy_pct) }} percent; sell pressure {{ '%.1f'|format(pressure.sell_pct) }} percent">
                    <div class="h-full bg-emerald-500" style="width: {{ '%.2f'|format(pressure.buy_pct) }}%"></div>
                    <div class="h-full bg-rose-500" style="width: {{ '%.2f'|format(pressure.sell_pct) }}%"></div>
                </div>
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-3">
                    {% for item in pressure.components %}
                    <div>
                        <div class="flex justify-between gap-2 text-xs mb-1">
                            <span class="text-slate-300">{{ item.label }} <span class="text-slate-500">({{ "%.0f"|format(item.weight * 100) }}%)</span></span>
                            <span class="font-mono text-slate-300">{{ item.raw }} {{ item.unit }}</span>
                        </div>
                        <div class="h-1.5 bg-slate-700 rounded-full overflow-hidden">
                            <div class="h-full bg-cyan-500 rounded-full" style="width: {{ '%.2f'|format(item.buy_pct) }}%"></div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
                <p class="mt-4 text-xs text-slate-500">Weighted descriptive composite of tick-count-weighted M15 candle flow, 60-bar momentum, EMA trend, EMA slope and RSI. Context only; never a standalone predictor.</p>
                {% else %}
                <p class="text-sm text-slate-400 py-8 text-center">Waiting for enough valid M15 candles and indicators to calculate the 15-hour read.</p>
                {% endif %}
            </section>

            <section class="bg-slate-800 p-5 rounded-xl border border-slate-700">
                <h2 class="text-lg font-semibold text-slate-100 mb-1">20-bar M15 Location</h2>
                <p class="text-xs text-slate-400 mb-5">Current close against the previous 20 M15 candle range (5h)</p>
                {% if location %}
                <div class="mb-4">
                    <div class="flex justify-between text-xs text-slate-400 mb-1"><span>Floor {{ "%.2f"|format(location.floor) }}</span><span>Ceiling {{ "%.2f"|format(location.ceiling) }}</span></div>
                    <div class="relative h-2 rounded-full bg-slate-700">
                        {% set loc_pct = (((location.close - location.floor) / (location.ceiling - location.floor)) * 100) if location.ceiling > location.floor else 50 %}
                        <div class="absolute -top-1 h-4 w-1 rounded bg-cyan-400" style="left: {{ '%.2f'|format([0, [loc_pct, 100]|min]|max) }}%"></div>
                    </div>
                    <div class="text-center text-sm font-semibold text-cyan-300 mt-2">Close {{ "%.2f"|format(location.close) }}</div>
                </div>
                <div class="grid grid-cols-2 gap-3 text-sm">
                    <div class="bg-slate-900/60 p-3 rounded-lg"><span class="text-xs text-slate-400">Above floor</span><br><span class="font-semibold">{{ "%.2f"|format(location.floor_atr) if location.floor_atr is not none else '—' }} ATR</span></div>
                    <div class="bg-slate-900/60 p-3 rounded-lg"><span class="text-xs text-slate-400">Below ceiling</span><br><span class="font-semibold">{{ "%.2f"|format(location.ceiling_atr) if location.ceiling_atr is not none else '—' }} ATR</span></div>
                </div>
                {% if location.floor_logged is not none and (location.floor_logged - location.floor)|abs > 0.02 %}
                <p class="text-xs text-amber-300 mt-3">Level check: recomputed floor differs from logged floor by {{ "%.2f"|format((location.floor_logged - location.floor)|abs) }} pts.</p>
                {% endif %}
                {% else %}
                <p class="text-sm text-slate-400 py-8 text-center">Waiting for at least 21 usable M15 candles.</p>
                {% endif %}
                <p class="text-xs text-slate-500 mt-4">Location only. The panel does not evaluate or alter risk gates.</p>
            </section>
        </div>

        <!-- Funnel Stats -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <h3 class="text-sm font-semibold text-emerald-400 mb-3">Buy (Long) Funnel</h3>
                <div class="grid grid-cols-3 gap-2 text-xs">
                    <div><span class="text-slate-400">Tested Floor:</span> <span class="font-bold text-blue-400">{{ funnel.tested_floor or 0 }}</span></div>
                    <div><span class="text-slate-400">Valid Rej:</span> <span class="font-bold text-orange-400">{{ funnel.valid_rejection or 0 }}</span></div>
                    <div><span class="text-slate-400">Held Floor:</span> <span class="font-bold text-emerald-400">{{ funnel.held_support or 0 }}</span></div>
                    <div><span class="text-slate-400">Trend OK:</span> <span class="font-bold text-cyan-400">{{ funnel.trend_confirmed or 0 }}</span></div>
                    <div><span class="text-slate-400">Slope OK:</span> <span class="font-bold text-purple-400">{{ funnel.slope_confirmed or 0 }}</span></div>
                    <div><span class="text-slate-400">All Confirmed:</span> <span class="font-bold text-emerald-300">{{ funnel.all_confirmed or 0 }}</span></div>
                </div>
            </div>

            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <h3 class="text-sm font-semibold text-rose-400 mb-3">Sell (Short) Funnel</h3>
                <div class="grid grid-cols-3 gap-2 text-xs">
                    <div><span class="text-slate-400">Tested Ceil:</span> <span class="font-bold text-blue-400">{{ funnel.sell_tested_ceiling or 0 }}</span></div>
                    <div><span class="text-slate-400">Valid Rej:</span> <span class="font-bold text-orange-400">{{ funnel.sell_valid_rejection or 0 }}</span></div>
                    <div><span class="text-slate-400">Held Ceil:</span> <span class="font-bold text-rose-400">{{ funnel.sell_held_resistance or 0 }}</span></div>
                    <div><span class="text-slate-400">Trend OK:</span> <span class="font-bold text-cyan-400">{{ funnel.sell_trend_confirmed or 0 }}</span></div>
                    <div><span class="text-slate-400">Slope OK:</span> <span class="font-bold text-purple-400">{{ funnel.sell_slope_confirmed or 0 }}</span></div>
                    <div><span class="text-slate-400">All Confirmed:</span> <span class="font-bold text-rose-300">{{ funnel.sell_all_confirmed or 0 }}</span></div>
                </div>
            </div>
        </div>

        <!-- Candle Log -->
        <div class="bg-slate-800 rounded-xl border border-slate-700 overflow-hidden">
            <div class="px-6 py-4 border-b border-slate-700 font-semibold text-slate-200">
                Recent M15 Candle Log (last 30)
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm text-slate-300">
                    <thead class="bg-slate-900/50 text-slate-400 uppercase text-xs">
                        <tr>
                            <th class="px-3 py-3">Timestamp</th>
                            <th class="px-3 py-3">Close</th>
                            <th class="px-3 py-3">Wick %</th>
                            <th class="px-3 py-3">Floor</th>
                            <th class="px-3 py-3">EMA50</th>
                            <th class="px-3 py-3">EMA200</th>
                            <th class="px-3 py-3">RSI</th>
                            <th class="px-3 py-3">Trend</th>
                            <th class="px-3 py-3">Tested</th>
                            <th class="px-3 py-3">Held</th>
                            <th class="px-3 py-3">Rejection</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-700">
                        {% for row in rows %}
                        <tr class="hover:bg-slate-700/40">
                            <td class="px-3 py-2 font-mono text-xs">{{ row.Timestamp or '&mdash;' }}</td>
                            <td class="px-3 py-2 font-semibold">
                                {% if not row.Close or row.Close == 'Calculating' %}&mdash;{% else %}{{ row.Close }}{% endif %}
                            </td>
                            <td class="px-3 py-2">{{ row.Wick_Ratio or '&mdash;' }}</td>
                            <td class="px-3 py-2">
                                {% if not row.Dynamic_Floor or row.Dynamic_Floor == 'Calculating' %}&mdash;{% else %}{{ row.Dynamic_Floor }}{% endif %}
                            </td>
                            <td class="px-3 py-2">
                                {% if not row.EMA_50 or row.EMA_50 == 'Calculating' %}&mdash;{% else %}{{ row.EMA_50 }}{% endif %}
                            </td>
                            <td class="px-3 py-2">
                                {% if not row.EMA_200 or row.EMA_200 == 'Calculating' %}&mdash;{% else %}{{ row.EMA_200 }}{% endif %}
                            </td>
                            <td class="px-3 py-2">
                                {% if not row.RSI or row.RSI == 'Calculating' %}&mdash;{% else %}{{ row.RSI }}{% endif %}
                            </td>
                            <td class="px-3 py-2">
                                <span class="px-2 py-0.5 rounded text-xs
                                    {{ 'bg-emerald-900/50 text-emerald-400' if row.Trend_Confirmed == 'True' else 'bg-rose-900/50 text-rose-400' }}">
                                    {{ row.Trend_Confirmed or 'False' }}
                                </span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="px-2 py-0.5 rounded text-xs
                                    {{ 'bg-blue-900/50 text-blue-400' if row.Tested_Floor == 'True' else 'bg-slate-700 text-slate-500' }}">
                                    {{ row.Tested_Floor or 'False' }}
                                </span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="px-2 py-0.5 rounded text-xs
                                    {{ 'bg-emerald-900/50 text-emerald-400' if row.Held_Floor == 'True' else 'bg-slate-700 text-slate-500' }}">
                                    {{ row.Held_Floor or 'False' }}
                                </span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="px-2 py-0.5 rounded text-xs
                                    {{ 'bg-orange-900/50 text-orange-400' if row.Valid_Rejection == 'True' else 'bg-slate-700 text-slate-500' }}">
                                    {{ row.Valid_Rejection or 'False' }}
                                </span>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

        </div>

        <!-- Journal panel -->
        <div id="panel-journal" class="tab-panel{% if active_tab != 'journal' %} hidden{% endif %}">
            <div class="flex justify-between items-start md:items-center gap-3 mb-4">
                <div>
                    <h2 class="text-lg font-semibold text-slate-100">Event Journal</h2>
                    <p class="text-xs text-slate-400">
                        engine.py journals every US100Cash trade entry, exit, skip and feed alert here;
                        this tab shows the most recent 100 entries (newest first).
                    </p>
                </div>
                {% if journal_error %}
                <span class="text-xs text-rose-400 bg-rose-900/30 border border-rose-700/30 px-2 py-1 rounded">
                    {{ journal_error }}
                </span>
                {% endif %}
            </div>
            {% if journal %}
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm text-slate-300">
                    <thead class="bg-slate-900/50 text-slate-400 uppercase text-xs sticky top-0 z-10">
                        <tr>
                            <th class="px-3 py-3">Timestamp (UTC)</th>
                            <th class="px-3 py-3">Event</th>
                            <th class="px-3 py-3">Detail</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-700">
                        {% for entry in journal %}
                        <tr class="hover:bg-slate-700/40">
                            <td class="px-3 py-2 font-mono text-xs">{{ entry.Timestamp }}</td>
                            <td class="px-3 py-2">
                                <span class="px-2 py-0.5 rounded text-xs
                                    {{ 'bg-emerald-900/50 text-emerald-400' if entry.Event == 'TRADE_ENTRY' else
                                       'bg-rose-900/50 text-rose-400' if entry.Event == 'TRADE_EXIT' else
                                       'bg-blue-900/50 text-blue-400' if entry.Event == 'LIVE_ENTRY' else
                                       'bg-orange-900/50 text-orange-400' if entry.Event == 'LIVE_EXIT' else
                                       'bg-amber-900/50 text-amber-400' if entry.Event in ('BUY_SKIP', 'SELL_SKIP') else
                                       'bg-slate-900 text-slate-400' }}">
                                    {{ entry.Event }}
                                </span>
                            </td>
                            <td class="px-3 py-2 font-mono text-xs">{{ entry.Detail }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            {% else %}
            <p class="text-sm text-slate-500 py-8 text-center">No journal events yet.</p>
            {% endif %}
        </div>

        <!-- Readiness panel -->
        <div id="panel-readiness" class="tab-panel{% if active_tab != 'readiness' %} hidden{% endif %}">
            <div class="flex justify-between items-start md:items-center gap-3 mb-4">
                <div>
                    <h2 class="text-lg font-semibold text-slate-100">Go / No-Go Readiness</h2>
                    <p class="text-xs text-slate-400">
                        US100Cash M15 · Forward-test evidence at ${{ "%.2f"|format(readiness.spread) }} assumed cost per trade.
                        The six data gates use the same calculations as
                        <code>python3 tools/live_readiness.py --spread {{ "%.2f"|format(readiness.spread) }}</code>.
                        Passing them never authorizes real-money trading by itself.
                    </p>
                </div>
                <span class="text-xs px-2.5 py-1 rounded-full
                    {{ 'bg-emerald-900/40 text-emerald-300 border border-emerald-700/40' if readiness.ok else 'bg-rose-900/40 text-rose-300 border border-rose-700/40' if readiness.ok == False else 'bg-slate-900/40 text-slate-400 border border-slate-700/40' }}">
                    {{ readiness.verdict }}
                </span>
            </div>

            {% if readiness.error %}
            <div role="status" class="bg-rose-950/60 border border-rose-700/60 text-rose-100 rounded-lg px-4 py-3 mb-5 text-sm">
                <p class="font-semibold">Readiness could not be computed.</p>
                <p class="text-rose-200/90">{{ readiness.error }}</p>
            </div>
            {% endif %}
            {% if readiness.snapshot_issues %}
            <div role="status" class="bg-amber-950/50 border border-amber-700/60 text-amber-200 rounded-lg px-4 py-3 mb-5 text-sm">
                <p class="font-semibold">Snapshot needs attention</p>
                <ul class="list-disc list-inside mt-1 text-xs">
                    {% for issue in readiness.snapshot_issues %}<li>{{ issue }}</li>{% endfor %}
                </ul>
            </div>
            {% endif %}

            <!-- Feed + verdict strip -->
            <div class="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-4 mb-6">
                <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                    <p class="text-xs text-slate-400">M15 feed · checked now</p>
                    <p class="text-xl font-bold mt-1 {{ 'text-emerald-400' if feed.ok else 'text-rose-400' }}">{{ feed.level }}</p>
                    <p class="text-xs text-slate-400 mt-1">{{ feed.reason }} · {{ feed.detail or 'no feed data' }}</p>
                    <p class="text-xs text-slate-500 mt-1">{{ feed.lines[-1] }}</p>
                </div>
                <div data-card="autosync" class="bg-slate-800 p-4 rounded-xl border {{ 'border-rose-700/70' if autosync.stale or autosync.level == 'ERROR' else 'border-amber-700/60' if autosync.level == 'WARN' else 'border-slate-700' }}">
                    <p class="text-xs text-slate-400">Autosync last run age</p>
                    <p class="text-xl font-bold mt-1 {{ 'text-emerald-400' if autosync.ok else 'text-rose-400' if autosync.stale or autosync.level == 'ERROR' else 'text-amber-300' }}">{{ autosync.level }}{% if autosync.age_label %} · {{ autosync.age_label }}{% endif %}</p>
                    <p class="text-xs text-slate-400 mt-1">{% if autosync.ts_utc %}{{ autosync.ts_utc }} UTC{% else %}no stamp{% endif %} · stale &gt; {{ autosync.stale_min }} min</p>
                    <p class="text-xs text-slate-500 mt-1">{{ autosync.detail }}</p>
                </div>
                <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                    <p class="text-xs text-slate-400">Post-gate closed trades</p>
                    {% if readiness.stats %}
                    <p class="text-2xl font-bold text-cyan-400 mt-1">{{ readiness.stats.post_gate }} <span class="text-sm text-slate-500">/ {{ readiness.min_trades }} minimum</span></p>
                    <p class="text-xs text-slate-400 mt-1">{{ readiness.stats.wins }}W / {{ readiness.stats.losses }}L{% if readiness.stats.flat %} / {{ readiness.stats.flat }} flat{% endif %}{% if readiness.stats.win_rate is not none %} · {{ "%.1f"|format(readiness.stats.win_rate) }}% wins{% endif %} · {{ readiness.stats.closed }} total closed</p>
                    <p class="text-xs text-slate-500 mt-1">${{ "%+.2f"|format(readiness.stats.net) }} post-gate, after assumed cost</p>
                    {% else %}<p class="text-slate-400 mt-1">Unavailable</p>{% endif %}
                </div>
                <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                    <p class="text-xs text-slate-400">Engine ledger · gross</p>
                    {% if status_available %}
                    <p class="text-2xl font-bold text-slate-100 mt-1">${{ "%.2f"|format(status.equity) }}</p>
                    <p class="text-xs text-slate-500 mt-1">Status updated {{ status.last_update or 'unknown' }} UTC</p>
                    {% else %}<p class="text-slate-400 mt-1">Unavailable</p>{% endif %}
                </div>
                <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                    <p class="text-xs text-slate-400">Last logged M15 bar</p>
                    <p class="text-lg font-bold text-slate-100 mt-1">{{ readiness.last_bar or 'Unavailable' }}</p>
                    {% if status_available %}
                    <p class="text-xs text-slate-500 mt-1">Reported daily SLs: {{ status.daily_losses or 0 }} / {{ status.max_daily_losses or 3 }}{% if (status.daily_losses or 0) >= (status.max_daily_losses or 3) %} · HALTED{% endif %}</p>
                    {% else %}<p class="text-xs text-slate-500 mt-1">Daily circuit breaker state unknown</p>{% endif %}
                </div>
            </div>

            <!-- Data gates -->
            <div class="bg-slate-800 p-5 rounded-xl border border-slate-700 mb-6">
                <h3 class="text-sm font-semibold text-slate-100 mb-3">Data Gates (live_readiness.py)</h3>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-sm text-slate-300">
                        <thead class="bg-slate-900/50 text-slate-400 uppercase text-xs sticky top-0">
                            <tr>
                                <th class="px-3 py-3">Gate</th>
                                <th class="px-3 py-3">Result</th>
                                <th class="px-3 py-3 text-right">Value</th>
                                <th class="px-3 py-3 text-slate-500">Note</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-700">
                            {% for gate in readiness.gates %}
                            <tr data-gate="{{ gate.key }}" class="hover:bg-slate-700/40">
                                <td class="px-3 py-2 font-mono text-xs">{{ gate.label }}</td>
                                <td class="px-3 py-2">
                                    <span class="px-2 py-0.5 rounded text-xs
                                        {{ 'bg-slate-700 text-slate-300' if gate.ok is none else 'bg-emerald-900/50 text-emerald-400' if gate.ok else 'bg-rose-900/50 text-rose-400' }}">
                                        {{ 'UNKNOWN' if gate.ok is none else 'PASS' if gate.ok else 'FAIL' }}
                                    </span>
                                </td>
                                <td class="px-3 py-2 font-mono text-xs text-right">{{ gate.value }}</td>
                                <td class="px-3 py-2 text-xs text-slate-500">{{ gate.note }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
                <p class="mt-3 text-xs text-slate-500">
                    {% if readiness.gates_total > 0 %}
                    {{ readiness.gates_passed }}/{{ readiness.gates_total }} data gates pass.
                    {% if readiness.gates_passed == readiness.gates_total %}
                    Necessary, not sufficient — see the manual checklist below.
                    {% else %}
                    Not ready on data alone; UNKNOWN means the evidence is insufficient, not a pass.
                    {% endif %}
                    {% else %}
                    Gate results unavailable; fix the evidence error above before drawing a conclusion.
                    {% endif %}
                </p>
            </div>

            <!-- Non-data checklist -->
            <div class="bg-slate-800 p-5 rounded-xl border border-slate-700 mb-6">
                <h3 class="text-sm font-semibold text-slate-100 mb-3">Non-Data Gates (sign off by hand)</h3>
                <ul class="space-y-2 text-sm">
                    {% for item in readiness.non_data %}
                    <li class="flex items-start gap-3">
                        <span class="text-xs text-slate-500 mt-0.5">[ ]</span>
                        <span class="text-slate-300">{{ item }}</span>
                    </li>
                    {% endfor %}
                </ul>
                <p class="mt-3 text-xs text-slate-500">
                    These items are not automatically checked or signed off here. Record evidence and sign-off
                    separately using docs/EXECUTION-PLAN.md and docs/START-LEVER1.md.
                    The execution-path probe uses fake MT5; it does not replace a demo soak.
                </p>
            </div>

            <!-- Evidence commands -->
            <div class="bg-slate-900/50 p-4 rounded-xl border border-slate-700">
                <h3 class="text-xs font-semibold text-slate-400 uppercase mb-2 tracking-wider">Evidence commands</h3>
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs font-mono text-slate-300">
                    <div>python3 tools/live_readiness.py --spread {{ "%.2f"|format(readiness.spread) }}</div>
                    <div>python3 tools/backtest.py xm_data/candles_m15.csv --spread 0.25</div>
                    <div>python3 tools/feed_doctor.py</div>
                    <div>python3 tools/check_data.py</div>
                    <div>python3 tools/handoff_check.py</div>
                    <div>python3 tools/live_path_probe.py</div>
                    <div>python3 tools/smoke_test.py</div>
                    <div>python3 tools/handoff_check.py --update</div>
                </div>
            </div>
        </div>

    </div>
</body>
</html>
"""


@app.route("/")
@app.route("/journal")
@app.route("/readiness")
def index():
    status = {
        "symbol": "US100Cash",
        "equity": 200.00,
        "total_trades": 0,
        "next_trade_num": None,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "trade_active": False,
        "trade_type": None,
        "daily_losses": 0,
        "max_daily_losses": 3,
        "entry_price": None,
        "stop_loss": None,
        "take_profit": None,
        "last_update": None,
        "rsi": None,
        "ema_fast": None,
        "ema_slow": None,
        "atr": None,
        "funnel": {},
    }

    status_available = False
    try:
        with open(STATUS_FILE_PATH, encoding="utf-8") as f:
            payload = json.load(f)
        required = {"symbol", "equity", "total_trades", "wins", "losses",
                    "daily_losses", "max_daily_losses", "last_update"}
        if not isinstance(payload, dict) or not required.issubset(payload):
            raise ValueError("status.json is missing required fields")
        for key in ("equity", "win_rate", "entry_price", "stop_loss", "take_profit",
                    "rsi", "ema_fast", "ema_slow", "atr"):
            if payload.get(key) is not None:
                payload[key] = float(payload[key])
                if not (-1e100 < payload[key] < 1e100):
                    raise ValueError("invalid numeric status value")
        for key in ("total_trades", "wins", "losses", "daily_losses", "max_daily_losses"):
            payload[key] = int(payload[key])
            if payload[key] < 0:
                raise ValueError("negative status count")
        if (payload["equity"] is None or not isinstance(payload["symbol"], str)
                or payload["max_daily_losses"] == 0 or not isinstance(payload.get("funnel", {}), dict)):
            raise ValueError("invalid status balance, symbol, risk or funnel state")
        status.update(payload)
        status_available = True
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass
    # The regular dashboard may replace displayed counts with the ledger below;
    # readiness must compare the original engine snapshot, not those replacements.
    status_for_readiness = dict(status)

    ledger_stats = tape_pressure.trade_summary(tape_pressure.read_rows(TRADES_FILE_PATH))
    trade_stats_warning = None
    if ledger_stats["total_trades"]:
        status_counts_match = True
        for key in ("total_trades", "wins", "losses"):
            try:
                status_counts_match = status_counts_match and int(status.get(key)) == ledger_stats[key]
            except (TypeError, ValueError):
                status_counts_match = False
        status.update(ledger_stats)
        if not status_counts_match:
            trade_stats_warning = (
                "Closed-trade counters in status.json differ from trades.csv. "
                "Showing ledger counts; the engine balance remains the internal status.json value "
                "(not all-time P/L). No engine state was modified."
            )

    funnel = status.get("funnel") or {}
    rows = []
    total_rows = 0

    if os.path.isfile(LOG_FILE_PATH):
        try:
            with open(LOG_FILE_PATH, mode="r") as f:
                reader = csv.DictReader(f)
                for line in reader:
                    rows.append(line)
                    total_rows += 1
        except Exception as e:
            print(f"Error reading log: {e}")

    pressure_read = tape_pressure.pressure(rows[-500:])
    location_read = tape_pressure.location(rows[-25:])
    recent_rows = list(reversed(rows))[:30]

    journal = []
    journal_error = None
    if os.path.isfile(JOURNAL_FILE_PATH):
        try:
            with open(JOURNAL_FILE_PATH, newline="") as f:
                for row in csv.DictReader(f):
                    journal.append(row)
        except Exception as e:
            journal_error = f"could not read journal: {e}"
    journal = list(reversed(journal))[:100]

    feed = feed_banner(status_for_readiness)
    autosync = autosync_status()
    readiness = readiness_snapshot(status_for_readiness, status_available, feed)
    active_tab = request.path.strip("/") or "dashboard"
    return render_template_string(
        HTML_TEMPLATE,
        active_tab=active_tab,
        rows=recent_rows,
        status=status,
        status_available=status_available,
        total=total_rows,
        funnel=funnel,
        pressure=pressure_read,
        location=location_read,
        trade_stats_warning=trade_stats_warning,
        feed=feed,
        autosync=autosync,
        journal=journal,
        journal_error=journal_error,
        readiness=readiness,
    )


@app.route("/healthz")
def healthz():
    # Liveness only: feed and trading readiness are deliberately not implied.
    return {"status": "ok", "scope": "dashboard process only"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6001)
