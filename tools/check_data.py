#!/usr/bin/env python3
"""
Data-integrity checker for the Nasdaq-100 (US100Cash) M15 forward-test CSVs.

Safe on Day 0 (empty 0-trade / 0-bar files) and market-hours-aware (CME weekend
closure and daily 17:00-18:00 ET maintenance break are NOT flagged as outages).

Exit code 0 = clean (warnings allowed), 1 = FAIL-level problems found.
Read-only - no files are modified.

Usage: python3 tools/check_data.py [repo_root]
"""
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

try:
    from trade_filter import is_market_quiet  # noqa: E402
except Exception:
    is_market_quiet = lambda dt=None: False

TRADES = os.path.join(ROOT, "trades.csv")
LOG = os.path.join(ROOT, "forward_test_log.csv")
SKIPS = os.path.join(ROOT, "skipped_trades.csv")
STATUS = os.path.join(ROOT, "status.json")

STARTING_BALANCE = 200.00
BAR_SECONDS = 900                # M15 = 900s
GAP_SECONDS = 45 * 60            # M15 log: normal spacing is 15 min, flag > 45 min during open market
ENTRY_MATCH_SECONDS = 18 * 60    # trade entry must have an M15 log row within +-18 min
LOT_SIZE = 0.10

# --- engine entry rules, for check_gate_conformance() (keep in sync with engine.py) ---
RSI_MIN, RSI_MAX = 40.0, 70.0
MAX_BELOW_EMA_ATR = 0.50
GATES_LIVE = datetime(2026, 10, 24, 0, 0)

TRADES_FIELDS = [
    "Trade_Num", "Trade_Type", "Entry_Time", "Exit_Time", "Entry_Price", "Stop_Loss", "Take_Profit",
    "Exit_Price", "Exit_Reason", "Profit", "Balance_After", "RSI_At_Entry",
    "ATR_At_Entry", "Wick_Ratio_At_Entry", "EMA50_At_Entry", "EMA200_At_Entry",
]
LOG_FIELDS = ["Timestamp", "Open", "High", "Low", "Close", "Wick_Ratio", "Volume", "Vol_MA",
              "Vol_Confirmed", "Dynamic_Floor", "EMA_50", "EMA_200", "Trend_Confirmed",
              "Tested_Floor", "Valid_Rejection", "Held_Floor", "RSI", "ATR"]
SKIP_FIELDS = ["Timestamp", "Reason", "Price", "ATR"]

FAILS, WARNS = [], []


def fail(msg):
    FAILS.append(msg)
    print(f"  [FAIL] {msg}")


def warn(msg):
    WARNS.append(msg)
    print(f"  [WARN] {msg}")


def ok(msg):
    print(f"  [ ok ] {msg}")


TORN = b"\x00"


def check_torn_writes(path, label):
    """A NUL byte is never legal in these CSVs - name the line instead of skipping it.

    A torn/partial write leaves a row padded with NULs so it no longer starts at
    column 0. The row values can be intact, but silently treating the row as
    "unparsable (skipped)" is what let a padded row reach the readiness report.
    """
    with open(path, "rb") as f:
        data = f.read()
    if TORN not in data:
        return False
    first = data.index(TORN)
    line = data.count(b"\n", 0, first) + 1
    count = data.count(TORN)
    fail(f"{label}: {count} NUL byte(s) starting at line {line} - torn or partial write; "
         f"run python3 tools/repair_log.py --check (then --apply) to repair it without "
         f"changing any value")
    return True


def parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def fnum(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _gap_is_market_closure(a_naive: datetime, b_naive: datetime) -> bool:
    """True when every missing M15 slot inside (a, b) falls during CME market closure."""
    t = a_naive + timedelta(seconds=BAR_SECONDS)
    while t < b_naive:
        mid_utc = (t - timedelta(minutes=7)).replace(tzinfo=timezone.utc)
        if not is_market_quiet(mid_utc):
            return False
        t += timedelta(seconds=BAR_SECONDS)
    return True


def check_trades():
    print("\n== trades.csv ==")
    check_torn_writes(TRADES, "trades.csv")
    with open(TRADES, newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)
        rows = [r for r in rdr if r]
    if header is None:
        fail("empty file")
        return []
    if [h.strip() for h in header] != TRADES_FIELDS:
        fail(f"header mismatch (expected 16-field Trade_Type schema). Got: {header}")
    else:
        ok(f"header matches the 16-field schema ({len(rows)} rows)")

    odd = [r for r in rows if len(r) != len(TRADES_FIELDS)]
    if odd:
        fail(f"{len(odd)} rows with wrong field count (e.g. trade #{odd[0][0]})")
    else:
        ok("all rows have 16 fields")

    trades = []
    nums, bad_reason, bad_side, bad_time = [], 0, 0, 0
    blank_num_lines, bad_num_rows = [], []
    for lineno, r in enumerate(rows, start=2):
        if len(r) != len(TRADES_FIELDS):
            continue
        d = dict(zip(TRADES_FIELDS, r))
        raw_num = str(d["Trade_Num"]).strip().replace(",", "")
        if not raw_num:
            blank_num_lines.append(lineno)
            continue
        try:
            d["num"] = int(raw_num)
        except ValueError:
            bad_num_rows.append((lineno, d["Trade_Num"]))
            continue
        nums.append(d["num"])
        try:
            d["profit"] = fnum(d["Profit"])
            d["bal"] = fnum(d["Balance_After"])
            d["entry_dt"] = parse_dt(d["Entry_Time"])
            d["exit_dt"] = parse_dt(d["Exit_Time"])
            if d["profit"] is None or d["bal"] is None:
                raise ValueError
        except ValueError:
            bad_reason += 1
            continue
        if d["Trade_Type"] not in ("BUY", "SELL"):
            bad_side += 1
        if d["Exit_Reason"] not in ("TP", "SL"):
            bad_reason += 1
        if not d["entry_dt"] or not d["exit_dt"] or d["entry_dt"] >= d["exit_dt"]:
            bad_time += 1
        trades.append(d)
    if bad_reason:
        fail(f"{bad_reason} rows with unparsable numbers or Exit_Reason not in TP/SL")
    else:
        ok("Exit_Reason domain + numeric fields all parse")
    if bad_side:
        fail(f"{bad_side} rows with Trade_Type not in BUY/SELL")
    if bad_time:
        fail(f"{bad_time} rows with Entry_Time >= Exit_Time")
    else:
        ok("entry/exit times ordered")

    num_failed = False
    if blank_num_lines:
        named = ", ".join(str(ln) for ln in blank_num_lines[:10])
        if len(blank_num_lines) > 10:
            named += f" ... (+{len(blank_num_lines) - 10} more)"
        fail(f"{len(blank_num_lines)} row(s) with missing/blank Trade_Num at line(s) {named}")
        num_failed = True
    if bad_num_rows:
        named = ", ".join(f"line {ln} ({val!r})" for ln, val in bad_num_rows[:10])
        if len(bad_num_rows) > 10:
            named += f" ... (+{len(bad_num_rows) - 10} more)"
        fail(f"{len(bad_num_rows)} row(s) with unparsable Trade_Num: {named}")
        num_failed = True

    non_pos = sorted({n for n in nums if n < 1})
    if non_pos:
        named = ", ".join(f"#{n}" for n in non_pos[:10])
        fail(f"{len(non_pos)} non-positive Trade_Num value(s): {named}")
        num_failed = True

    counts = Counter(nums)
    dup_nums = sorted(n for n, cnt in counts.items() if cnt > 1)
    if dup_nums:
        dup_extra = sum(counts[n] - 1 for n in dup_nums)
        named = ", ".join(f"#{n} (x{counts[n]})" for n in dup_nums[:10])
        if len(dup_nums) > 10:
            named += f" ... (+{len(dup_nums) - 10} more)"
        fail(f"{dup_extra} duplicate Trade_Num value(s): {named}")
        num_failed = True

    pos_nums = {n for n in nums if n >= 1}
    if pos_nums:
        max_num = max(pos_nums)
        missing_nums = [n for n in range(1, max_num + 1) if n not in pos_nums]
        if missing_nums:
            named = ", ".join(f"#{n}" for n in missing_nums[:10])
            if len(missing_nums) > 10:
                named += f" ... (+{len(missing_nums) - 10} more)"
            fail(f"{len(missing_nums)} missing Trade_Num value(s) in #1..#{max_num}: {named}")
            num_failed = True

    regress = [(a, b) for a, b in zip(nums, nums[1:]) if b < a]
    if regress:
        named = ", ".join(f"#{a}->#{b}" for a, b in regress[:10])
        if len(regress) > 10:
            named += f" ... (+{len(regress) - 10} more)"
        fail(f"{len(regress)} out-of-order Trade_Num transition(s): {named} (must be strictly increasing)")
        num_failed = True

    if not num_failed:
        span = f" (#1..#{nums[-1]})" if nums else " (0 rows)"
        ok(f"trade numbering unique, strictly increasing, and contiguous{span}")

    breaks = 0
    for prev, cur in zip(trades, trades[1:]):
        if abs(cur["bal"] - (prev["bal"] + cur["profit"])) > 0.02:
            breaks += 1
    if breaks:
        warn(f"{breaks} Balance_After continuity breaks")
    else:
        ok("Balance_After ledger continuous")

    true_pnl = sum(t["profit"] for t in trades)
    wins = sum(1 for t in trades if t["Exit_Reason"] == "TP")
    losses = len(trades) - wins
    last_bal = trades[-1]["bal"] if trades else STARTING_BALANCE
    drift = last_bal - (STARTING_BALANCE + true_pnl) if trades else 0.0
    print(f"  [info] {wins}W/{losses}L over {len(trades)} trades | true P&L from ${STARTING_BALANCE:.0f}: "
          f"{true_pnl:+.2f} -> ${STARTING_BALANCE + true_pnl:.2f} | engine ledger: ${last_bal:.2f} "
          f"(drift {drift:+.2f})")
    if abs(drift) > 0.02:
        warn("engine ledger disagrees with sum of profits (reset gap)")
    return trades


def check_log():
    print("\n== forward_test_log.csv ==")
    check_torn_writes(LOG, "forward_test_log.csv")
    with open(LOG, newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)
        raw = [r for r in rdr if r]
    if [h.strip() for h in (header or [])] != LOG_FIELDS:
        fail(f"header mismatch: {header}")
    else:
        ok(f"header ok ({len(raw)} rows)")

    dts, bad = [], []
    for lineno, r in enumerate(raw, start=2):
        dt = parse_dt(r[0]) if r else None
        if dt is None:
            bad.append(lineno)
            continue
        dts.append(dt)
        try:
            float(str(r[1]).replace(",", "")); float(str(r[2]).replace(",", ""))
            float(str(r[3]).replace(",", "")); float(str(r[4]).replace(",", ""))
        except (ValueError, IndexError):
            bad.append(lineno)
    if bad:
        named = ", ".join(str(line) for line in bad[:5]) + (" ..." if len(bad) > 5 else "")
        fail(f"{len(bad)} rows with unparsable timestamp/OHLC at line(s) {named} - a malformed "
             f"forward-test row is corruption, not a warning; run python3 tools/repair_log.py "
             f"--check (it names torn rows) and repair anything else by hand")
    else:
        ok("timestamps + OHLC all parse")
    regress = sum(1 for a, b in zip(dts, dts[1:]) if b <= a)
    if regress:
        fail(f"{regress} timestamp regressions (log must be strictly increasing)")
    else:
        ok("timestamps strictly increasing")
    gaps = [
        (a, b) for a, b in zip(dts, dts[1:])
        if (b - a).total_seconds() > GAP_SECONDS and not _gap_is_market_closure(a, b)
    ]
    if gaps:
        warn(f"{len(gaps)} open-market gaps > 45 min: "
             + ", ".join(f"{a:%m-%d %H:%M}->{b:%H:%M} ({(b-a).total_seconds()/60:.0f}m)"
                         for a, b in gaps[:8])
             + (" ..." if len(gaps) > 8 else ""))
    else:
        ok("no open-market gaps > 45 min")
    return dts


def check_skips():
    print("\n== skipped_trades.csv ==")
    check_torn_writes(SKIPS, "skipped_trades.csv")
    with open(SKIPS, newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)
        rows = [r for r in rdr if r]
    if [h.strip() for h in (header or [])] != SKIP_FIELDS:
        fail(f"header mismatch: {header}")
    else:
        ok(f"header ok ({len(rows)} skip rows)")
    return rows


def check_cross(trades, log_dts):
    print("\n== cross-file ==")
    for t in trades:
        if not t.get("entry_dt"):
            continue
        near = [d for d in log_dts if abs((d - t["entry_dt"]).total_seconds()) <= ENTRY_MATCH_SECONDS]
        if not near:
            warn(f"trade #{t['num']} entry {t['entry_dt']} has no M15 price-log row within 18 min")
    ok("trade entries matched to price log" if not any("no M15 price-log row" in w for w in WARNS)
       else "see warnings above")

    spans = 0
    for t in trades:
        if not t.get("entry_dt") or not t.get("exit_dt"):
            continue
        for a, b in zip(log_dts, log_dts[1:]):
            if (b - a).total_seconds() > GAP_SECONDS and not _gap_is_market_closure(a, b) \
                    and t["entry_dt"] <= a and t["exit_dt"] >= b:
                spans += 1
    if spans:
        warn(f"{spans} trades were open across an unscheduled price-log gap")
    else:
        ok("no trade open across an unscheduled price-log gap")

    if os.path.isfile(STATUS):
        try:
            with open(STATUS) as sf:
                st = json.load(sf)
        except Exception as e:
            warn(f"status.json unparsable: {e}")
            return
        mism = []
        if st.get("total_trades") not in (None, len(trades)):
            mism.append(f"total_trades {st.get('total_trades')} != {len(trades)}")
        if trades:
            w = sum(1 for t in trades if t["Exit_Reason"] == "TP")
            if st.get("wins") not in (None, w):
                mism.append(f"wins {st.get('wins')} != {w}")
            if st.get("equity") not in (None, trades[-1]["bal"]):
                mism.append(f"equity {st.get('equity')} != ledger {trades[-1]['bal']}")
        if mism:
            warn("status.json stale vs trades.csv (engine restart will resync): " + "; ".join(mism))
        else:
            ok("status.json agrees with trades.csv")


def check_geometry(trades):
    print("\n== trades.csv geometry / sizing ==")
    if not trades:
        ok("0 closed trades yet (Day-0 clean ledger)")
        return
    eras = {}
    for t in trades:
        atr = fnum(t.get("ATR_At_Entry"))
        entry, sl, tp = fnum(t.get("Entry_Price")), fnum(t.get("Stop_Loss")), fnum(t.get("Take_Profit"))
        if not atr or None in (entry, sl, tp) or not t.get("entry_dt"):
            continue
        slm, tpm = abs(entry - sl) / atr, abs(tp - entry) / atr
        key = (round(slm, 2), round(tpm, 2))
        eras.setdefault(key, []).append(t)

    for (slm, tpm), ts in sorted(eras.items(), key=lambda kv: kv[1][0]["entry_dt"]):
        w = sum(1 for t in ts if t["Exit_Reason"] == "TP")
        rr = tpm / slm if slm else 0
        first, last = ts[0]["entry_dt"], ts[-1]["entry_dt"]
        tag = "  <- LIVE RULE" if (slm, tpm) == (2.0, 4.0) else ""
        print(f"  [info] SL {slm:.2f}xATR / TP {tpm:.2f}xATR (RR 1:{rr:.2f}, breakeven WR "
              f"{1/(1+rr)*100:.1f}%): n={len(ts)} {w}W/{len(ts)-w}L  {first:%Y-%m-%d %H:%M} -> "
              f"{last:%Y-%m-%d %H:%M}{tag}")

    drift = 0
    for t in trades:
        if not t.get("entry_dt") or t["entry_dt"] < GATES_LIVE:
            continue
        atr = fnum(t.get("ATR_At_Entry"))
        entry, sl = fnum(t.get("Entry_Price")), fnum(t.get("Stop_Loss"))
        profit = fnum(t.get("Profit"))
        if not atr or None in (entry, sl, profit):
            continue
        risk_price = abs(entry - sl)
        if risk_price <= 0:
            continue
        realized = profit / (risk_price * LOT_SIZE)
        planned = -1.0 if t["Exit_Reason"] == "SL" else 2.0
        if abs(realized - planned) > 0.25:
            drift += 1
            warn(f"trade #{t['num']} {t['entry_dt']:%m-%d %H:%M} {t['Exit_Reason']} realized "
                 f"{realized:+.2f}R vs planned {planned:+.2f}R (lot/size or gap fill drift)")
    if not drift:
        ok("realized R matches the live 1:2 geometry on every US100Cash row")


def check_bar_census(dts):
    print("\n== forward_test_log.csv M15 bar census ==")
    if len(dts) < 2:
        ok(f"M15 log warming ({len(dts)} bar(s))")
        return
    step = BAR_SECONDS
    start = dts[0].replace(second=0)
    have = {}
    for d in dts:
        key = d.replace(second=0)
        have[key] = have.get(key, 0) + 1
    dups = [k for k, v in have.items() if v > 1]
    if dups:
        warn(f"{len(dups)} slots with more than one log row: "
             + ", ".join(f"{k:%m-%d %H:%M}" for k in sorted(dups)[:6]))
    else:
        ok("no duplicate M15 bar slots in the log")
    exp, missing = [], []
    t = start
    end = dts[-1]
    while t <= end:
        mid_utc = (t - timedelta(minutes=7)).replace(tzinfo=timezone.utc)
        if not is_market_quiet(mid_utc):
            exp.append(t)
        t = t + timedelta(seconds=step)
    for x in exp:
        if x not in have:
            missing.append(x)
    if missing:
        runs = []
        for m in missing:
            if runs and (m - runs[-1][-1]).total_seconds() == step:
                runs[-1].append(m)
            else:
                runs.append([m])
        warn(f"{len(missing)} missing open-market M15 slot(s) in {len(runs)} run(s): "
             + ", ".join(f"{r[0]:%m-%d %H:%M}..{r[-1]:%H:%M} ({len(r)} bars)" for r in runs[:6]))
    else:
        ok(f"complete open-market M15 grid ({len(exp)} slots) with no missing bars")


def check_gate_conformance(trades):
    print("\n== trades.csv entry-gate conformance (today's rules) ==")
    if not trades:
        ok("0 closed trades yet (conformance ready)")
        return
    trend_bad, rsi_bad, prox_bad = [], [], []
    for t in trades:
        side = t.get("Trade_Type")
        e50, e200 = fnum(t.get("EMA50_At_Entry")), fnum(t.get("EMA200_At_Entry"))
        rsi, atr, entry = fnum(t.get("RSI_At_Entry")), fnum(t.get("ATR_At_Entry")), fnum(t.get("Entry_Price"))
        if None in (e50, e200, rsi, atr, entry) or side not in ("BUY", "SELL"):
            continue
        if not t.get("entry_dt") or t["entry_dt"] < GATES_LIVE:
            continue
        tag = f"#{t.get('num')} {side} {t['entry_dt']:%m-%d %H:%M}"
        if (side == "BUY") != (e50 > e200):
            trend_bad.append(f"{tag} EMA50 {e50:.2f} vs EMA200 {e200:.2f}")
        lo, hi = (RSI_MIN, RSI_MAX) if side == "BUY" else (100 - RSI_MAX, 100 - RSI_MIN)
        if not lo < rsi < hi:
            rsi_bad.append(f"{tag} RSI {rsi:.1f} outside {lo:.0f}-{hi:.0f}")
        adverse = (e50 - entry) if side == "BUY" else (entry - e50)
        if adverse > MAX_BELOW_EMA_ATR * atr + 1e-9:
            prox_bad.append(f"{tag} entry {adverse / atr:+.2f} ATR adverse of EMA50")
    for label, hits in (("trend gate (EMA50 vs EMA200)", trend_bad),
                        ("RSI window", rsi_bad),
                        (f"near-EMA gate ({MAX_BELOW_EMA_ATR:.2f} ATR adverse)", prox_bad)):
        if hits:
            warn(f"{len(hits)} row(s) violate the {label}: " + "; ".join(hits[:4]))
        else:
            ok(f"every row satisfies the {label}")


def main():
    print(f"nasdaq-trading-bot (US100Cash M15) data integrity check - {ROOT}")
    for path in (TRADES, LOG, SKIPS):
        if not os.path.isfile(path):
            print(f"MISSING: {path}")
            sys.exit(1)
    trades = check_trades()
    log_dts = check_log()
    check_bar_census(log_dts)
    check_skips()
    check_cross(trades, log_dts)
    check_geometry(trades)
    check_gate_conformance(trades)
    print(f"\n== result: {len(FAILS)} fail, {len(WARNS)} warn ==")
    if FAILS:
        print("Fix FAIL items before analysing this data.")
        sys.exit(1)
    print("Data is analysis-ready (warnings are informational).")
    sys.exit(0)


if __name__ == "__main__":
    main()
