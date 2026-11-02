#!/usr/bin/env python3
"""One-command triage for "no new closed M15 candles" / STALE_FEED alerts.

Answers the four questions an operator actually has, in order:

  1. Is the feed artefact itself healthy?  (mt5_last_candle.json: exists? parses?
     right symbol? how old is the bar, how old is the file?)
  2. Is the sidecar alive but silent?      (mt5_feed_health.json heartbeat: status,
     tick age, broker offset, last error, reconnect count)
  3. Is MT5 itself feeding?                (an independent witness: the read-only
     quote logger's xm_data/quotes-YYYY-MM-DD.csv and candles_m15.csv)
  4. Is the journal lying to me?           (STALE_FEED rows whose reported age went
     DOWN = the counter restarted because the engine restarted, not because the feed
     recovered)

Exit codes: 0 feed healthy, 1 feed stale/degraded (market open), 2 feed broken
(missing/unreadable/foreign/wrong schema) - never 2 just because the market is shut.

Usage:
  python3 tools/feed_doctor.py
  python3 tools/feed_doctor.py --json
  python3 tools/feed_doctor.py --data-dir /opt/nasdaq --journal /opt/nasdaq/journal.csv
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

import feed_health as FH  # noqa: E402


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _market_state(now_epoch):
    """(is_quiet, label) using trade_filter when importable."""
    try:
        sys.path.insert(0, ROOT)
        import trade_filter
        now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
        return bool(trade_filter.is_market_quiet(now_dt)), "CME hours (America/New_York)"
    except Exception as exc:                       # pragma: no cover - env dependent
        return None, f"unknown ({type(exc).__name__}: {exc})"


def _last_csv_row(path):
    """Last non-empty row of a CSV as a list of fields (cheap tail read)."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            back = min(size, 8192)
            f.seek(size - back)
            chunk = f.read().decode("utf-8", "replace").strip().splitlines()
        return chunk[-1].split(",") if chunk else None
    except OSError:
        return None


def _csv_header(path):
    """First line of a CSV as a list of column names, or None."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first = f.readline().strip()
        return first.split(",") if first else None
    except OSError:
        return None


def witness_quotes(xm_dir, now_epoch):
    """Independent evidence from the read-only quote logger (different service, same login)."""
    out = {"quotes_file": None, "quotes_age_s": None, "quotes_tick_age_s": None,
           "candles_file": None, "last_bar_close_age_s": None, "last_bar_open_utc": None,
           "last_bar_close_utc": None,
           "last_quote_utc": None, "server_offset_s": None}
    if not os.path.isdir(xm_dir):
        return out

    today = datetime.fromtimestamp(now_epoch, tz=timezone.utc).strftime("%Y-%m-%d")
    qpath = os.path.join(xm_dir, f"quotes-{today}.csv")
    if os.path.isfile(qpath):
        out["quotes_file"] = qpath
        out["quotes_age_s"] = FH.file_age_seconds(qpath, now_epoch)
        row = _last_csv_row(qpath)
        if row and len(row) >= 2:
            out["last_quote_utc"] = row[0]
            try:                                    # columns: ts_utc,...,tick_age_s,srv_offset_s
                out["quotes_tick_age_s"] = float(row[8])
            except (IndexError, ValueError):
                pass
            try:
                out["server_offset_s"] = int(row[9])
            except (IndexError, ValueError):
                pass

    cpath = os.path.join(xm_dir, "candles_m15.csv")
    if os.path.isfile(cpath):
        out["candles_file"] = cpath
        # The logger's schema (tools/mt5_quotes.py CANDLE_FIELDS) keys each row by
        # bar_CLOSE_utc. Treating that value as an open time and adding another
        # CANDLE_SECONDS understates the bar age by exactly one bar (15 min), so a
        # witness bar that closed e.g. 21 min ago was reported as "closed 6 min
        # ago" and could wrongly prove MT5 alive. Resolve the timestamp column
        # from the header and fail closed (age unknown) on an unknown schema.
        header = _csv_header(cpath)
        row = _last_csv_row(cpath)
        ts_idx, ts_is_close = 0, True
        if header:
            if "bar_close_utc" in header:
                ts_idx, ts_is_close = header.index("bar_close_utc"), True
            elif "bar_open_utc" in header:
                ts_idx, ts_is_close = header.index("bar_open_utc"), False
            else:
                row = None            # unknown schema: do not guess a timestamp
        if row and len(row) > ts_idx:
            try:
                ts = datetime.strptime(row[ts_idx], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc).timestamp()
            except ValueError:
                ts = None             # e.g. header-only file: no bar to report
            if ts is not None:
                bar_close = ts if ts_is_close else ts + FH.CANDLE_SECONDS
                out["last_bar_close_utc"] = FH.utc_str(bar_close)
                out["last_bar_open_utc"] = FH.utc_str(bar_close - FH.CANDLE_SECONDS)
                out["last_bar_close_age_s"] = max(0.0, now_epoch - bar_close)
    return out


def build_report(data_dir, journal_path, now_epoch=None, expected_symbol="US100Cash",
                 max_age_s=20 * 60, assume_open=False):
    now_epoch = time.time() if now_epoch is None else float(now_epoch)
    feed_path = os.path.join(data_dir, "mt5_last_candle.json")
    health_path = os.path.join(data_dir, "mt5_feed_health.json")
    warmup_path = os.path.join(data_dir, "mt5_warmup_candles.json")
    offset_path = os.path.join(data_dir, "mt5_feed_offset.json")

    diag = FH.diagnose_feed_file(feed_path, expected_symbol=expected_symbol,
                                 now_epoch=now_epoch, max_age_s=max_age_s)
    health = FH.diagnose_sidecar_health(health_path, now_epoch)
    witness = witness_quotes(os.path.join(data_dir, "xm_data"), now_epoch)

    warmup = {"path": warmup_path, "present": os.path.isfile(warmup_path),
              "age_s": FH.file_age_seconds(warmup_path, now_epoch), "bars": None}
    payload, _err = FH.read_json_file(warmup_path)
    if isinstance(payload, dict):
        warmup["bars"] = len(payload.get("candles") or [])

    offset = {"path": offset_path, "present": os.path.isfile(offset_path), "server_offset_s": None}
    payload, _err = FH.read_json_file(offset_path)
    if isinstance(payload, dict):
        offset["server_offset_s"] = payload.get("server_offset_s")

    events = FH.parse_journal_feed_events(journal_path) if journal_path else []
    resets = FH.detect_counter_resets(events)

    quiet, hours_label = _market_state(now_epoch)
    if assume_open:
        quiet, hours_label = False, "forced open (--assume-open)"
    verdict = _verdict(diag, health, witness, quiet)

    return {
        "checked_utc": FH.utc_str(now_epoch),
        "data_dir": data_dir,
        "expected_symbol": expected_symbol,
        "market": {"quiet": quiet, "label": hours_label},
        "feed": {k: v for k, v in diag.items() if k != "rates"},
        "sidecar": health,
        "warmup": warmup,
        "persisted_offset": offset,
        "witness": witness,
        "journal": {
            "path": journal_path,
            "feed_alerts": events[-12:],
            "counter_resets": resets,
        },
        "verdict": verdict,
    }


def _verdict(diag, health, witness, quiet):
    """(level, headline, [next steps]) - the part an operator reads first."""
    reason = diag.get("reason")
    steps = []
    mt5_alive = None
    if witness.get("quotes_age_s") is not None:
        mt5_alive = witness["quotes_age_s"] <= 180 or (
            witness.get("last_bar_close_age_s") is not None
            and witness["last_bar_close_age_s"] <= 20 * 60)
    if health.get("present") and not health.get("fresh"):
        steps.append("sidecar heartbeat is stale - check: systemctl status mt5feed-nasdaq; "
                     "journalctl -u mt5feed-nasdaq -n 50")
    last_err = str(health.get("last_error") or "")
    if "IPC" in last_err.upper() or "-10004" in last_err:
        steps.append("the sidecar's MT5 handle is dead (No IPC connection) - the terminal or "
                     "Wine session it attached to restarted while the sidecar kept running: "
                     "restart the sidecar (systemctl restart mt5feed-nasdaq). If the error "
                     "persists, the MT5 terminal in this WINEPREFIX is not running/logged in")
    if health.get("status") in ("TICK_FROZEN", "TERMINAL_DISCONNECTED", "NO_TICK"):
        steps.append("terminal has no live ticks: confirm the Wine terminal under the service "
                     "WINEPREFIX is logged in (systemctl show mt5feed-nasdaq -p Environment), "
                     "and check no second terminal is using the same login")
    if health.get("status") in ("NO_RATES", "INIT_FAILED", "SYMBOL_SELECT_FAILED"):
        steps.append(f"sidecar is alive but gets no {diag.get('timeframe') or 'M15'} rates "
                     f"(status {health.get('status')}) - it cannot self-heal from a lost IPC "
                     "channel, so restart mt5feed-nasdaq and watch its journal")
    if health.get("present") and health.get("server_offset_s") is None and reason != "OK":
        steps.append("sidecar cannot learn the broker UTC offset (needs one fresh tick) - "
                     "fix the terminal connection first")
    if reason == FH.REASON_MISSING:
        steps.append("mt5feed-nasdaq is not writing its feed file - it is either stopped, "
                     "crash-looping, or pointed at a different path "
                     "(systemctl cat mt5feed-nasdaq | grep MT5_FEED_FILE)")
    if reason == FH.REASON_SYMBOL:
        steps.append("a different stack is writing this feed path - give every stack its own "
                     "MT5_FEED_FILE / MT5_WARMUP_FILE / MT5_ORDER_DIR (see docs/STALE-FEED-TRIAGE.md)")
    if mt5_alive and reason != FH.REASON_OK:
        steps.append("MT5 itself is fine (the quote logger is still recording quotes/candles), so "
                     "broker connectivity is not the problem - the fault is isolated to the "
                     "mt5feed-nasdaq service/process; after it recovers, confirm the engine logs "
                     "new bars (docs/STALE-FEED-TRIAGE.md, resume-after-outage)")

    if reason == FH.REASON_OK:
        return ("OK", f"feed healthy - {diag.get('detail')}", steps)
    if quiet:
        return ("QUIET", f"feed {reason} but the market is closed ({diag.get('detail')})",
                steps or ["expected between Fri 16:55 ET and Sun 18:10 ET, and 16:55-18:10 ET daily"])
    if reason in (FH.REASON_MISSING, FH.REASON_UNREADABLE, FH.REASON_SCHEMA, FH.REASON_SYMBOL):
        return ("BROKEN", f"feed {reason}: {diag.get('detail')}", steps)
    return ("STALE", f"feed {reason}: {diag.get('detail')}", steps)


def _fmt_minutes(seconds):
    if seconds is None:
        return "n/a"
    return f"{seconds / 60:.0f} min" if seconds >= 90 else f"{seconds:.0f}s"


def _print_detail(report):
    feed = report["feed"]
    level, headline, _steps = report["verdict"]
    print(f"feed_doctor | {report['checked_utc']} UTC | data_dir {report['data_dir']}")
    print(f"market      : {'QUIET/closed' if report['market']['quiet'] else 'OPEN'}"
          f" ({report['market']['label']})")
    print(f"verdict     : {level} - {headline}")
    print()
    sc = report["sidecar"]
    w = report["witness"]
    print(f"feed file   : {feed['path']}")
    print(f"  reason    : {feed['reason']}  (ok={feed['ok']}, usable={feed['usable']})")
    print(f"  exists    : {feed['exists']}  file age: {_fmt_minutes(feed['file_age_s'])}"
          f"  mtime: {feed['file_mtime_utc']}")
    print(f"  symbol    : {feed['symbol'] or '(untagged)'}  timeframe: {feed['timeframe'] or '-'}")
    print(f"  candle    : open {feed['candle_open_utc']}  age {_fmt_minutes(feed['candle_age_s'])}"
          f"  frozen_since {feed['frozen_since_utc']}")
    print(f"  detail    : {feed['detail']}")
    print()
    print(f"sidecar     : {sc['path']}")
    if sc["present"]:
        print(f"  status    : {sc['status']}  heartbeat age {_fmt_minutes(sc['age_s'])}"
              f"  fresh={sc['fresh']}")
        print(f"  account   : {sc['account']}  symbol: {sc['symbol']}")
        print(f"  tick age  : {_fmt_minutes(sc['tick_age_s'])}  terminal_connected: {sc['terminal_connected']}")
        print(f"  offset    : {sc['server_offset_s']}  reconnects: {sc['reconnects']}")
        print(f"  last error: {sc['last_error']}")
    else:
        print("  no heartbeat file (tools/mt5_feed.py not running, or writing elsewhere)")
    print()
    print(f"warm-up     : {report['warmup']['path']}  present={report['warmup']['present']}"
          f"  age={_fmt_minutes(report['warmup']['age_s'])}  bars={report['warmup']['bars']}")
    print(f"offset file : {report['persisted_offset']['path']}  "
          f"offset={report['persisted_offset']['server_offset_s']}")
    print()
    print(f"witness     : {w['quotes_file']}")
    print(f"  last quote: {w['last_quote_utc']} ({_fmt_minutes(w['quotes_age_s'])} ago)"
          f"  tick_age={w['quotes_tick_age_s']}  srv_offset={w['server_offset_s']}")
    print(f"  last bar  : open {w['last_bar_open_utc']} close {w['last_bar_close_utc']}"
          f" (closed {_fmt_minutes(w['last_bar_close_age_s'])} ago)"
          f"  file={w['candles_file']}")
    print()
    events = report["journal"]["feed_alerts"]
    print(f"journal     : {report['journal']['path']}")
    if events:
        for ev in events[-6:]:
            print(f"  {ev['timestamp']}  {ev['event']:<20} {ev['detail'][:110]}")
    else:
        print("  no feed alerts recorded")
    for reset in report["journal"]["counter_resets"][-3:]:
        print(f"  ! counter reset at {reset['timestamp']}: {reset['from_minutes']:.0f} min -> "
              f"{reset['to_minutes']:.0f} min ({reset['note']})")
    print()
    level, _headline, steps = report["verdict"]
    if not steps:
        steps = (["none - feed is healthy"] if level == "OK"
                 else ["no automatic step matched this state - inspect the feed file, the "
                       "sidecar heartbeat and journal.csv by hand (docs/STALE-FEED-TRIAGE.md)"])
    print("next steps:")
    for s in steps:
        print(f"  - {s}")


def main(argv=None) -> int:
    now = time.time()
    ap = argparse.ArgumentParser(description="Diagnose US100Cash M15 feed staleness")
    ap.add_argument("--data-dir", default=os.getenv("NASDAQ_DATA_DIR", "/opt/nasdaq"))
    ap.add_argument("--journal", default=None,
                    help="journal.csv path (default: <data-dir>/journal.csv)")
    ap.add_argument("--symbol", default=os.getenv("SYMBOL_MT5", "US100Cash"))
    ap.add_argument("--max-age-min", type=float, default=20.0,
                    help="candle age that counts as stale (default 20)")
    ap.add_argument("--json", action="store_true", help="machine-readable report")
    ap.add_argument("--quiet", action="store_true", help="print only the verdict line")
    ap.add_argument("--assume-open", action="store_true",
                    help="treat the market as open even outside CME hours (triage a weekend feed)")
    args = ap.parse_args(argv)

    journal = args.journal or os.path.join(args.data_dir, "journal.csv")
    report = build_report(args.data_dir, journal, now_epoch=now, expected_symbol=args.symbol,
                          max_age_s=args.max_age_min * 60, assume_open=args.assume_open)
    level = report["verdict"][0]

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    elif args.quiet:
        print(f"{level} - {report['verdict'][1]}")
    else:
        _print_detail(report)
    if level == "OK" or level == "QUIET":
        return 0
    return 2 if level == "BROKEN" else 1


if __name__ == "__main__":
    sys.exit(main())
