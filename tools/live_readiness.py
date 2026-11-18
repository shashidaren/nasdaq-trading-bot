#!/usr/bin/env python3
"""
live_readiness - "is the US100Cash M15 book ready for real money?", in numbers.

The other tools answer "what did the book do?". This one answers the question
the handoff keeps deferring: does the forward-test evidence justify risking
money, and how much of that evidence is noise? The CLI and dashboard share the
same gate calculations and era constants from tools/replay_lib.py.

Sections
  1. EDGE        t-stat, bootstrap P(net/trade > 0) iid and by calendar day,
                 net R/trade, and how many trades it takes to confirm the edge
  2. COST        break-even round-trip spread and net P/L across spreads
  3. FRAGILITY   how much of the net P/L is the best few trades / best days
  4. WHERE       exploratory post-hoc cuts (ATR bucket, side, weekday/weekend):
                 HYPOTHESES for prospective logging, never parameters
  5. STABILITY   halves, last-N, per-week, drawdown, and a Monte Carlo of what a
                 0.10-lot US100Cash account ($0.10 per index point) would see if
                 the edge is real vs if it is zero, incl. how often a candidate
                 kill-switch floor would be touched
  6. LATENCY     coarse M15 re-walk with hypothetical delayed fill prices
                 (not measured broker execution latency)
  7. OPERATIONS  price-log coverage and gaps (a dead feed is a live-money risk)
  8. GATES       proposed go/no-go evidence gates + the non-data checklist

Everything is the strictly post-gate slice (entries >= replay_lib.GATES_DEPLOY,
the date today's entry gates actually went live) because earlier rows ran other
rules (docs/HANDOFF.md §9). P/L is NET of --spread per trade; the ledger itself
is gross. This one DEFAULTS to the assumed $0.25/trade (= 2.5 index points x
$0.10/pt at the 0.10 lot; the measured XM round trip at ~29,000 index is ~$0.29),
because a go/no-go on gross numbers would be misleading; --spread 0 gives gross.
Deterministic: the bootstrap/Monte Carlo use a seeded RNG, so the same data gives
the same output.

The gate thresholds below are PROPOSALS, not facts about the market - they are
constants at the top of this file on purpose, so the user can change them in one
place. Passing every data gate is NECESSARY, not sufficient: the non-data
checklist (live order path, spread profile, demo parity, kill switches) decides
the rest.

Usage:
  python3 tools/live_readiness.py --spread 0.25            # the assumed cost (also the default)
  python3 tools/live_readiness.py --spread 0.29 --horizon 200 --boot 20000   # measured XM round trip

Exit code: 0 = every data gate passes, 1 = at least one fails (so a script can
branch on it). Read-only: nothing is written to disk and no network is used.
Pure stdlib.
"""
import argparse
import bisect
import csv
import math
import os
import random
import statistics as st
import sys
from collections import defaultdict
from datetime import timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
import replay_lib as R  # noqa: E402
from trade_filter import is_market_quiet  # noqa: E402

# ---- proposed gate thresholds (edit here, not in the logic) -----------------
MIN_TRADES = 100          # strictly post-gate closed trades
MIN_P_EDGE = 0.90         # day-block bootstrap P(net/trade > 0)
MIN_SPREAD_COVER = 2.0    # break-even spread must be >= this x the assumed spread
TOP_FRAC = 0.05           # best 5% of trades (>= 3) are removed in the fragility gate
RECENT_N = 25             # most recent closed trades that must not be net-negative
MAX_GAP_MIN = 60          # longest tolerated price-log gap ...
GAP_WINDOW_DAYS = 14      # ... inside this trailing window of the log
FEED_GAP_FLAG_MIN = 45    # check_data's gap threshold (M15 = 15m, flag > 45m)
Z_ALPHA = 1.96            # two-sided 5%
Z_POWER = 0.8416          # 80% power
MC_PATHS = 6000           # Monte Carlo paths per scenario
KILL_FLOORS = (20, 30, 40, 50)   # $ drawdown floors whose touch probability is reported
DEFAULT_SPREAD = 0.25
DEFAULT_SEED = 2026
DEFAULT_BOOT = 5000
MIN_ANALYSIS_TRADES = 10   # below this, the statistical report does not score the book

# These require human evidence; neither the dashboard nor the report signs them off.
NON_DATA_GATES = (
    "Contract and cost: verify $10 per +100 points at 0.10 lot, minimum volume <= 0.10, "
    "median spread <= 3.5 points and M15 parity (tools/xm_quote_report.py).",
    "LIVE order path smoke-tested, including position-id reconciliation, restart, "
    "spread guard, filling mode and uncertain order results.",
    "An order executor can run where MT5 runs, with the real-account lock verified.",
    "XM spread profile and commission/swap measured across sessions; use the measured cost.",
    "Demo soak: 5–10 real-path round trips, no orphaned or duplicate positions, "
    "position-id reconciliation and simulator-versus-demo fill/slippage parity reviewed.",
    "Same-feed parity: a forward run on DATA_SOURCE=MT5 shows comparable signal flow.",
    "Kill switches tested, including equity floor, stale-feed alarm, max spread and HALT flatten; "
    "remove HALT only after the position is confirmed closed.",
    "Micro-live caps agreed in writing: 0.10 lot, weekday-only and max $15 / 3 SLs per day.",
)


# --------------------------------------------------------------------------- #
# small statistics helpers
# --------------------------------------------------------------------------- #
def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def pct(sorted_xs, q):
    return sorted_xs[min(len(sorted_xs) - 1, max(0, int(q * len(sorted_xs))))]


def boot_iid(xs, B, rng):
    m = len(xs)
    out = sorted(sum(xs[rng.randrange(m)] for _ in range(m)) / m for _ in range(B))
    return out


def boot_days(groups, B, rng):
    """Resample whole calendar days (trades cluster by day), mean per trade."""
    keys = list(groups)
    nd = len(keys)
    out = []
    for _ in range(B):
        tot = cnt = 0
        for _ in range(nd):
            g = groups[keys[rng.randrange(nd)]]
            tot += sum(g)
            cnt += len(g)
        out.append(tot / cnt)
    out.sort()
    return out


def fisher_two_sided(a, b, c, d):
    """Fisher exact p for [[a,b],[c,d]] (wins/losses of two groups)."""
    n = a + b + c + d

    def hyp(x, y, z, w):
        return math.comb(x + y, x) * math.comb(z + w, z) / math.comb(n, x + z)

    p_obs = hyp(a, b, c, d)
    tot = 0.0
    for x in range(0, min(a + b, a + c) + 1):
        y, z = (a + b) - x, (a + c) - x
        w = (c + d) - z
        if min(y, z, w) < 0:
            continue
        p = hyp(x, y, z, w)
        if p <= p_obs + 1e-12:
            tot += p
    return min(1.0, tot)


def equity_stats(seq):
    """seq = [(exit_dt, net)] chronological -> (final, maxdd, loss_streak, underwater_trades, underwater_days)."""
    eq = peak = mdd = 0.0
    streak = best_streak = 0
    peak_i, peak_t = 0, seq[0][0]
    uw_trades, uw_days = 0, 0.0
    for i, (xt, x) in enumerate(seq):
        eq += x
        if x <= 0:
            streak += 1
            best_streak = max(best_streak, streak)
        else:
            streak = 0
        if eq > peak:
            peak, peak_i, peak_t = eq, i, xt
        else:
            uw_trades = max(uw_trades, i - peak_i)
            uw_days = max(uw_days, (xt - peak_t).total_seconds() / 86400)
        mdd = max(mdd, peak - eq)
    return eq, mdd, best_streak, uw_trades, uw_days


def monte_carlo(xs, horizon, B, rng, floors=()):
    """Bootstrap paths of `horizon` trades. Returns (sorted finals, sorted max drawdowns,
    {floor: P(equity touches -floor at any point)})."""
    m = len(xs)
    finals, dds = [], []
    touched = {f: 0 for f in floors}
    for _ in range(B):
        eq = peak = mdd = lowest = 0.0
        for _ in range(horizon):
            eq += xs[rng.randrange(m)]
            if eq > peak:
                peak = eq
            elif peak - eq > mdd:
                mdd = peak - eq
            if eq < lowest:
                lowest = eq
        finals.append(eq)
        dds.append(mdd)
        for f in floors:
            if lowest <= -f:
                touched[f] += 1
    finals.sort()
    dds.sort()
    return finals, dds, {f: touched[f] / B for f in floors}


def latency_stress(pg, bars, spread):
    """Coarsely re-walk on M15 bars with a hypothetical price between the signal
    close and the next bar close (0 = signal close, 1 = next close). These are
    price interpolations, not measurements of execution latency. SL/TP retain
    their ATR distances. Ambiguous bars resolve SL-first; TIME marks at 240 min.
    Read rows relative to each other, not against actual broker fills.
    """
    dts = [b["dt"] for b in bars]
    rows = []
    for frac in (0.0, 0.2, 0.5, 1.0):
        tot, n, w, l = 0.0, 0, 0, 0
        for t in pg:
            j = bisect.bisect_left(dts, t["et"] - timedelta(seconds=150))
            if j >= len(dts) or abs((dts[j] - t["et"]).total_seconds()) > 150 or j + 2 >= len(bars):
                continue
            atr = abs(t["entry"] - t["sl"]) / R.ATR_SL_MULT
            c0, c1 = t["entry"], bars[j + 1]["c"]
            if frac >= 1.0:
                e, seg = c1, bars[j + 2:]
            else:
                e, seg = c0 + frac * (c1 - c0), bars[j + 1:]
            out, r, _, _ = R.walk(e, t["side"], atr, seg, horizon_min=240)
            tot += r * R.r_money(atr) - spread
            n += 1
            w += out == "TP"
            l += out == "SL"
        rows.append((frac, n, w, l, tot))
    return rows


def line(label, ts, spread):
    n = len(ts)
    if not n:
        return f"  {label:30s} n=  0"
    w = sum(1 for t in ts if t["profit"] > 0)
    g = sum(t["profit"] for t in ts)
    net = g - spread * n
    lo, hi = R.wilson(w, n)
    return (f"  {label:30s} n={n:3d} {w:2d}W/{n - w:2d}L WR {100 * w / n:5.1f}% [{lo:4.1f},{hi:4.1f}]"
            f"  gross {g:+7.2f}  net {net:+7.2f}  net/trade {net / n:+6.2f}")


# --------------------------------------------------------------------------- #
def torn_write_report(path):
    """Byte-level scan for NUL padding: a NUL is never legal in these CSVs.

    A crash, a full disk, or a merge/rollback racing an append can leave the row
    bytes intact but preceded by NULs, so the row no longer starts at column 0.
    Returns (nul_bytes, line, column) for the first NUL, else None. Read-only.
    """
    with open(path, "rb") as fh:
        data = fh.read()
    if b"\x00" not in data:
        return None
    first = data.index(b"\x00")
    return data.count(b"\x00"), data.count(b"\n", 0, first) + 1, first - (data.rfind(b"\n", 0, first) + 1)


def load_readiness_data(trades_path, log_path):
    """Read the evidence conservatively; never silently discard malformed rows.

    replay_lib's permissive loaders are useful for exploration, but silently
    skipping a losing trade or a bad bar could make a go-live gate look better.
    Torn writes are named explicitly (file/line/column) with the repair command,
    because the strict failure below is otherwise indistinguishable from a
    genuinely malformed row.
    """
    for path in (trades_path, log_path):
        torn = torn_write_report(path)
        if torn:
            total, line, column = torn
            raise ValueError(
                f"{os.path.basename(path)} has {total} NUL byte(s) starting at line {line}, "
                f"column {column} - a torn or partial write left the row padded, so it no "
                f"longer starts at column 0. The row values are intact, but this loader will "
                f"not guess. Repair without changing values: run python3 tools/repair_log.py "
                f"--check, then python3 tools/repair_log.py --apply")

    def read_rows(path, required):
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if (not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames)
                    or not required.issubset(reader.fieldnames)):
                raise ValueError(f"{os.path.basename(path)} has a missing or invalid header")
            rows = list(reader)
            if any(None in row or any(value is None for value in row.values()) for row in rows):
                raise ValueError(f"{os.path.basename(path)} contains incomplete or extra fields")
            return rows

    trade_rows = read_rows(trades_path, {
        "Trade_Num", "Trade_Type", "Entry_Time", "Exit_Time", "Entry_Price",
        "Stop_Loss", "Take_Profit", "ATR_At_Entry", "Exit_Reason", "Profit",
    })
    log_rows = read_rows(log_path, {"Timestamp", "Open", "High", "Low", "Close"})
    trades = R.load_trades(trades_path)
    bars = R.load_bars(log_path)
    if len(trades) != len(trade_rows):
        raise ValueError("trades.csv contains unparseable rows or changed while being read")
    if len(bars) != len(log_rows):
        raise ValueError("forward_test_log.csv contains unparseable rows or changed while being read")

    try:
        numbers = [int(str(t["num"]).replace(",", "")) for t in trades]
    except (TypeError, ValueError):
        raise ValueError("trades.csv has invalid trade numbers") from None
    if any(n <= 0 for n in numbers) or len(set(numbers)) != len(numbers):
        raise ValueError("trades.csv has missing or duplicate trade numbers")
    for t in trades:
        values = (t["entry"], t["sl"], t["tp"], t["atr"], t["profit"])
        if (any(v is None or not math.isfinite(v) for v in values)
                or t["atr"] <= 0 or min(t["entry"], t["sl"], t["tp"]) <= 0
                or t["sl"] == t["entry"] or t["xt"] <= t["et"]
                or t["reason"] not in ("TP", "SL")):
            raise ValueError(f"trades.csv has invalid values for trade {t['num']}")
    last = None
    for offset, row in enumerate(log_rows):
        dt = R.parse_ts(row.get("Timestamp"))
        values = [R.fnum(row.get(k)) for k in ("Open", "High", "Low", "Close")]
        if (dt is None or dt.minute % 15 or dt.second
                or (last is not None and dt <= last)
                or any(v is None or not math.isfinite(v) for v in values)):
            raise ValueError(f"forward_test_log.csv has invalid, duplicate, or unordered M15 "
                             f"bars at line {offset + 2} (Timestamp={row.get('Timestamp')!r})")
        o, h, low, c = values
        if low > min(o, c) or h < max(o, c) or low <= 0:
            raise ValueError(f"forward_test_log.csv has invalid OHLC values at line {offset + 2} "
                             f"(Timestamp={row.get('Timestamp')!r})")
        last = dt
    return trades, bars


def feed_report(bars):
    """Open-market missing M15 runs, using the same midpoint as check_data.py.

    Scheduled CME closures split runs rather than becoming 75-minute or weekend
    outages. Each run's duration includes the normal 15-minute bar interval, so
    five consecutive missing open-market bars make a 90-minute gap.
    """
    if not bars:
        return [], 0, 0, 0
    have = {b["dt"].replace(second=0) for b in bars}
    step = timedelta(minutes=15)
    t, end = bars[0]["dt"].replace(second=0), bars[-1]["dt"]
    expected = missing = 0
    gaps, run = [], []

    def finish_run():
        if run:
            minutes = (len(run) + 1) * 15
            gaps.append((run[0] - step, run[-1] + step, minutes))
            run.clear()

    while t <= end:
        mid_utc = (t - timedelta(minutes=7)).replace(tzinfo=timezone.utc)
        if not is_market_quiet(mid_utc):
            expected += 1
            if t not in have:
                missing += 1
                run.append(t)
            else:
                finish_run()
        else:
            finish_run()
        t += step
    finish_run()
    return gaps, expected - missing, expected, missing


def evaluate_data_gates(trades, bars, spread=DEFAULT_SPREAD,
                        seed=DEFAULT_SEED, boot=DEFAULT_BOOT):
    """Single source of truth for the CLI and dashboard's six data gates.

    A gate's `ok` is True, False, or None (not enough evidence to evaluate).
    The short-sample report never infers an edge from fewer than ten trades.
    Passing these gates is not authorization to trade real money.
    """
    if not math.isfinite(spread) or spread < 0 or boot <= 0:
        raise ValueError("spread must be finite and non-negative, and boot must be positive")
    pg = [t for t in trades if t["et"] >= R.GATES_DEPLOY]
    n = len(pg)
    gaps, have, expected, missing = feed_report(bars)
    rng = random.Random(seed)
    b_iid, b_day, by_day = [], [], defaultdict(list)
    p_day = None
    if n >= MIN_ANALYSIS_TRADES:
        nets = [t["profit"] - spread for t in pg]
        b_iid = boot_iid(nets, boot, rng)
        for t, net in zip(pg, nets):
            by_day[t["xt"].date()].append(net)
        b_day = boot_days(by_day, boot, rng)
        p_day = sum(1 for x in b_day if x > 0) / len(b_day)

    too_small = f"Requires at least {MIN_ANALYSIS_TRADES} post-gate trades for the report."
    gates = [{"key": "sample", "label": f"sample: >= {MIN_TRADES} post-gate trades",
              "ok": n >= MIN_TRADES, "value": str(n),
              "note": f"Entries on or after {R.GATES_DEPLOY:%Y-%m-%d} UTC."}]
    gates.append({"key": "edge", "label": f"edge: by-day bootstrap P(net/trade > 0) >= {MIN_P_EDGE:.0%}",
                  "ok": p_day >= MIN_P_EDGE if p_day is not None else None,
                  "value": f"{p_day:.1%}" if p_day is not None else "—",
                  "note": f"{boot:,} bootstrap samples; seed {seed}." if p_day is not None else too_small})
    mean_gross = st.mean(t["profit"] for t in pg) if n >= MIN_ANALYSIS_TRADES else None
    cover = mean_gross / spread if mean_gross is not None and spread > 0 else None
    gates.append({"key": "cost", "label": f"cost: break-even spread >= {MIN_SPREAD_COVER:.1f}x assumed",
                  "ok": (cover >= MIN_SPREAD_COVER if cover is not None else
                         False if spread == 0 else None),
                  "value": f"{cover:.2f}x" if cover is not None else "—",
                  "note": (f"Gross mean ${mean_gross:+.2f} / assumed ${spread:.2f} per trade."
                           if cover is not None else "Cannot pass with zero assumed cost."
                           if spread == 0 else too_small)})
    k_drop = max(3, math.ceil(TOP_FRAC * n))
    without_top = (sum(t["profit"] - spread for t in pg)
                   - sum(sorted((t["profit"] - spread for t in pg), reverse=True)[:k_drop])
                   if n >= MIN_ANALYSIS_TRADES else None)
    gates.append({"key": "fragility", "label": f"fragility: net > 0 without the best {k_drop} trades",
                  "ok": without_top > 0 if without_top is not None else None,
                  "value": f"${without_top:+.2f}" if without_top is not None else "—",
                  "note": "Best 5% of trades, at least three, removed." if without_top is not None else too_small})
    recent = sorted(pg, key=lambda t: t["xt"])[-RECENT_N:]
    recent_net = (sum(t["profit"] - spread for t in recent)
                  if n >= MIN_ANALYSIS_TRADES else None)
    gates.append({"key": "recent", "label": f"recent form: last {RECENT_N} closed trades net >= 0",
                  "ok": recent_net >= 0 if recent_net is not None else None,
                  "value": f"${recent_net:+.2f}" if recent_net is not None else "—",
                  "note": f"{len(recent)} available trades, net of assumed cost." if recent_net is not None else too_small})
    cutoff = bars[-1]["dt"] - timedelta(days=GAP_WINDOW_DAYS) if bars else None
    recent_gaps = [g for g in gaps if cutoff is not None and g[1] >= cutoff]
    worst = max((g[2] for g in recent_gaps), default=0)
    enough_bars = len(bars) >= 2 and have >= 2
    covers_trades = True
    if enough_bars and pg:
        first_entry = min(t["et"] for t in pg)
        last_entry = max(t["et"] for t in pg)
        start_needed = max(first_entry, cutoff)
        covers_trades = (bars[0]["dt"] <= start_needed + timedelta(minutes=15)
                         and bars[-1]["dt"] >= last_entry - timedelta(minutes=15))
    feed_known = enough_bars and covers_trades
    feed_ok = False if worst > MAX_GAP_MIN else True if feed_known else None
    if not enough_bars:
        feed_note = "At least two open-market bars are required."
    elif not covers_trades:
        feed_note = "Log does not span the recent post-gate trade window; coverage is incomplete."
    else:
        feed_note = "Scheduled market closures excluded; current feed freshness is checked separately."
    gates.append({"key": "feed", "label": f"feed: no open-market gap > {MAX_GAP_MIN} min in the last {GAP_WINDOW_DAYS} days of the log",
                  "ok": feed_ok, "value": f"worst {worst} min" if enough_bars else "—",
                  "note": feed_note})
    return {"gates": gates, "post_gate": pg, "bootstrap_iid": b_iid,
            "bootstrap_day": b_day, "by_day": by_day, "rng": rng,
            "feed_report": (gaps, have, expected, missing)}


def print_gates(gates):
    print("\n== 8. GATES (proposed thresholds - constants at the top of this file) ==")
    for gate in gates:
        result = "PASS" if gate["ok"] is True else "FAIL" if gate["ok"] is False else "UNKNOWN"
        print(f"  [{result}] {gate['label']:72s} {gate['value']}")
    passed = sum(g["ok"] is True for g in gates)
    print(f"\n  DATA GATES: {passed}/{len(gates)} pass. ", end="")
    if passed != len(gates):
        print("NOT READY - keep forward-testing; see failing or unknown gates above.")
    else:
        print("Data gates pass; this is necessary, not sufficient (checklist below).")
    print("\n  NON-DATA GATES (manual evidence and sign-off required; see docs/EXECUTION-PLAN.md):")
    for item in NON_DATA_GATES:
        print(f"    [ ] {item}")
    return 0 if passed == len(gates) else 1


def main():
    ap = argparse.ArgumentParser(description="US100Cash M15 live-readiness evidence + gates")
    ap.add_argument("--spread", type=float, default=DEFAULT_SPREAD,
                    help="round-trip $/trade at 0.10 lot on US100Cash. Default 0.25 "
                         "(2.5 index points x $0.10/pt). 0 = gross.")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--boot", type=int, default=DEFAULT_BOOT, help="bootstrap iterations")
    ap.add_argument("--horizon", type=int, default=100, help="Monte Carlo trades per path")
    ap.add_argument("--root", default=ROOT)
    a = ap.parse_args()
    sp = a.spread
    if not math.isfinite(sp) or sp < 0 or a.boot <= 0 or a.horizon <= 0:
        ap.error("spread must be finite and non-negative; boot and horizon must be positive")

    try:
        trades, bars = load_readiness_data(os.path.join(a.root, "trades.csv"),
                                           os.path.join(a.root, "forward_test_log.csv"))
        evidence = evaluate_data_gates(trades, bars, sp, a.seed, a.boot)
    except (OSError, ValueError, csv.Error) as exc:
        print(f"live_readiness: evidence unavailable or invalid: {exc}")
        return 1
    pg = evidence["post_gate"]
    n = len(pg)
    if n < MIN_ANALYSIS_TRADES:
        print(f"live_readiness: only {n} post-gate trades - nothing meaningful to say yet")
        return print_gates(evidence["gates"])
    rng = evidence["rng"]

    nets = [t["profit"] - sp for t in pg]
    gross = [t["profit"] for t in pg]
    oneR = [abs(t["entry"] - t["sl"]) * R.LOT_SIZE for t in pg]
    span_days = max(1e-6, (pg[-1]["xt"] - pg[0]["et"]).total_seconds() / 86400)
    per_week = n / span_days * 7
    wins = sum(1 for t in pg if t["profit"] > 0)
    mu, sd = st.mean(nets), st.stdev(nets)
    se = sd / math.sqrt(n)
    tstat = mu / se if se else 0.0

    print(f"live_readiness - {n} post-gate trades (entries >= {R.GATES_DEPLOY:%Y-%m-%d}) of {len(trades)} total, "
          f"{len(bars)} M15 bars, spread ${sp:.2f}/trade, seed {a.seed}")
    print(f"  span {span_days:.1f} days, {n / span_days:.1f} trades/day ({per_week:.1f}/week); "
          f"P/L below is {'NET of the spread' if sp else 'GROSS'}")
    if not sp:
        print("  WARNING: --spread 0 is GROSS. Every figure below ignores trading cost, so the gates are")
        print("           not meaningful. Use --spread 0.25 (the default) for any live-readiness conclusion.")

    # ---- 1. EDGE -----------------------------------------------------------
    print("\n== 1. EDGE: is it distinguishable from zero? ==")
    print(line("post-gate", pg, sp))
    print(f"  net/trade {mu:+.3f}  sd {sd:.2f}  se {se:.3f}  t {tstat:+.2f}  "
          f"one-sided p~{1 - norm_cdf(tstat):.2f}  95% CI [{mu - 1.997 * se:+.2f}, {mu + 1.997 * se:+.2f}]")
    b_iid = evidence["bootstrap_iid"]
    b_day = evidence["bootstrap_day"]
    by_day = evidence["by_day"]
    p_iid = sum(1 for x in b_iid if x > 0) / len(b_iid)
    p_day = sum(1 for x in b_day if x > 0) / len(b_day)
    print(f"  bootstrap P(net/trade > 0): iid {100 * p_iid:.0f}% | by calendar day ({len(by_day)} days) {100 * p_day:.0f}%"
          f"   95% band by day [{pct(b_day, .025):+.2f}, {pct(b_day, .975):+.2f}]")
    r_g = [g / o for g, o in zip(gross, oneR)]
    r_n = [x / o for x, o in zip(nets, oneR)]
    cost_share = [sp / o for o in oneR]
    print(f"  per-trade R: gross {st.mean(r_g):+.2f}  net {st.mean(r_n):+.2f}   "
          f"median 1R ${st.median(oneR):.2f}; spread = {100 * st.median(cost_share):.0f}% of median 1R "
          f"(worst trade {100 * max(cost_share):.0f}%)")
    if mu > 0:
        n_obs = ((Z_ALPHA + Z_POWER) * sd / mu) ** 2
        n_half = ((Z_ALPHA + Z_POWER) * sd / (mu / 2)) ** 2
        print(f"  trades needed to confirm (80% power, 5% two-sided): {n_obs:,.0f} if the true edge is the observed "
              f"${mu:.2f}/trade (~{max(0, n_obs - n) / per_week:.0f} more weeks); {n_half:,.0f} if it is half that "
              f"(~{max(0, n_half - n) / per_week:.0f} more weeks)")
    else:
        print("  observed net/trade is not positive - nothing to confirm")

    # ---- 2. COST -----------------------------------------------------------
    print("\n== 2. COST: how much room is there? ==")
    g_mean = st.mean(gross)
    print(f"  gross/trade {g_mean:+.3f} => break-even round trip ${g_mean:.2f}/trade ({10 * g_mean:.1f} pts on US100Cash at 0.10 lot);"
          f" assumed ${sp:.2f} => cover {g_mean / sp:.1f}x" if sp else f"  gross/trade {g_mean:+.3f}")
    row = "  net P/L at spread $/trade:  "
    for s in (0.25, 0.40, 0.60, 0.80, 1.00, 1.50):
        row += f"{s:.2f}: {sum(gross) - s * n:+7.2f}   "
    print(row)
    be_wr = (1 + sp / st.median(oneR)) / 3 if sp else 1 / 3
    lo, hi = R.wilson(wins, n)
    print(f"  cost-adjusted break-even WR at median 1R ${st.median(oneR):.2f}: {100 * be_wr:.1f}%   "
          f"observed {100 * wins / n:.1f}% (Wilson {lo:.1f}-{hi:.1f})")

    # ---- 3. FRAGILITY ------------------------------------------------------
    print("\n== 3. FRAGILITY: how many trades carry the result? ==")
    total = sum(nets)
    ranked = sorted(nets, reverse=True)
    for k in (1, 3, 5, 10):
        s = sum(ranked[:k])
        print(f"  best {k:2d} trades = {s:+7.2f} ({100 * s / total if total else 0:4.0f}% of net);"
              f" net without them {total - s:+7.2f}")
    days_sorted = sorted(((sum(v), d) for d, v in by_day.items()), reverse=True)
    top3 = sum(v for v, _ in days_sorted[:3])
    print(f"  best 3 days {[str(d) for _, d in days_sorted[:3]]} = {top3:+.2f} "
          f"({100 * top3 / total if total else 0:.0f}% of net); net without them {total - top3:+.2f}")
    up = [v for v in (sum(g) for g in by_day.values()) if v > 0]
    dn = [v for v in (sum(g) for g in by_day.values()) if v <= 0]
    print(f"  days up {len(up)} (avg {st.mean(up) if up else 0:+.2f}) / down {len(dn)} (avg {st.mean(dn) if dn else 0:+.2f})")

    # ---- 4. WHERE ----------------------------------------------------------
    print("\n== 4. WHERE THE EDGE LIVES (post-hoc cuts of one regime - hypotheses, not parameters) ==")
    for lo_a, hi_a in ((0, 60), (60, 90), (90, 130), (130, 1e9)):
        ts = [t for t in pg if lo_a <= t["atr"] < hi_a]
        if not ts:
            continue
        r1 = st.median([abs(t["entry"] - t["sl"]) * R.LOT_SIZE for t in ts])
        be = (1 + sp / r1) / 3
        hi_lbl = "inf" if hi_a > 1e8 else str(int(hi_a))
        print(line(f"entry ATR ${int(lo_a)}-{hi_lbl}", ts, sp) + f"   [cost {100 * sp / r1:.0f}% of 1R, BE WR {100 * be:.0f}%]")
    if sp:
        cs = [(sp / (abs(t["entry"] - t["sl"]) * R.LOT_SIZE), t) for t in pg]
        for lo_c, hi_c in ((0, .20), (.20, .30), (.30, .40), (.40, 99)):
            ts = [t for c, t in cs if lo_c <= c < hi_c]
            if ts:
                hi_lbl = "inf" if hi_c > 1 else f"{int(100 * hi_c)}"
                print(line(f"spread = {int(100 * lo_c)}-{hi_lbl}% of 1R", ts, sp))
        print("  a spread guard that blocked cost-share > X would have (in-sample, retrospective):")
        for x in (.25, .30, .40):
            keep = sum(t["profit"] - sp for c, t in cs if c <= x)
            blk = [t for c, t in cs if c > x]
            print(f"    X={x:.2f}: keeps {len(cs) - len(blk):2d} trades net {keep:+7.2f}   "
                  f"blocks {len(blk):2d} trades net {sum(t['profit'] - sp for t in blk):+7.2f}")
    for side in ("BUY", "SELL"):
        print(line(f"side {side}", [t for t in pg if t["side"] == side], sp))
    wk = [t for t in pg if t["et"].weekday() < 5]
    we = [t for t in pg if t["et"].weekday() >= 5]
    print(line("weekday entries", wk, sp))
    print(line("weekend entries (Sat/Sun)", we, sp))
    if we and wk:
        ww, wl = sum(1 for t in we if t["profit"] > 0), sum(1 for t in we if t["profit"] <= 0)
        kw, kl = sum(1 for t in wk if t["profit"] > 0), sum(1 for t in wk if t["profit"] <= 0)
        wk_days = sorted({t["et"].date() for t in we})
        print(f"  weekend vs weekday win rate: Fisher exact p = {fisher_two_sided(ww, wl, kw, kl):.3f}; "
              f"{len(wk_days)} weekend dates. This is one of ~10 slices looked at - a Bonferroni bar is ~0.005.")

    # ---- 5. STABILITY AND RISK --------------------------------------------
    print("\n== 5. STABILITY AND RISK ==")
    half = n // 2
    print(line("first half (older)", pg[:half], sp))
    print(line("second half (newer)", pg[half:], sp))
    print(line(f"last {RECENT_N} closed", sorted(pg, key=lambda t: t["xt"])[-RECENT_N:], sp))
    weekly = defaultdict(list)
    for t in pg:
        weekly[t["et"].isocalendar()[:2]].append(t)
    for k in sorted(weekly):
        ts = weekly[k]
        print(line(f"ISO week {k[0]}-W{k[1]:02d}", ts, sp) + f"   median ATR ${st.median([t['atr'] for t in ts]):.0f}")
    seq = sorted(zip([t["xt"] for t in pg], nets), key=lambda x: x[0])
    final, mdd, streak, uw_t, uw_d = equity_stats(seq)
    print(f"  equity {final:+.2f}; max drawdown ${mdd:.2f}; longest losing streak {streak}; "
          f"longest stretch below a prior equity high {uw_t} trades / {uw_d:.1f} days")
    print(f"\n  Monte Carlo, {a.horizon} trades at 0.10 lot on US100Cash "
          f"(${R.LOT_SIZE:.2f}/index point, ~{a.horizon / (n / span_days):.0f} days), "
          f"bootstrap of the {n} post-gate net trades, {MC_PATHS} paths each:")
    print(f"  {'scenario':34s} {'P(net<0)':>8s} {'5th pct':>8s} {'median':>8s} {'95th pct':>9s}"
          f" {'median maxDD':>13s} {'95th maxDD':>11s}")
    demeaned = [x - mu for x in nets]
    scenarios = (("edge as observed", nets),
                 ("ZERO edge (same dispersion)", demeaned),
                 (f"spread doubles (+${sp:.2f}/trade)" if sp else "extra $0.25/trade", [x - (sp or 0.25) for x in nets]))
    touch_rows = []
    for label, xs in scenarios:
        f, d, tch = monte_carlo(xs, a.horizon, MC_PATHS, rng, KILL_FLOORS)
        pn = sum(1 for x in f if x < 0) / len(f)
        print(f"  {label:34s} {100 * pn:7.0f}% {pct(f, .05):+8.1f} {pct(f, .5):+8.1f} {pct(f, .95):+9.1f}"
              f" {pct(d, .5):13.1f} {pct(d, .95):11.1f}")
        touch_rows.append((label, tch))
    print(f"\n  P(equity touches -$X at any point within {a.horizon} trades) - how informative a kill-switch floor is:")
    print(f"  {'scenario':34s} " + " ".join(f"{'-$' + str(f):>7s}" for f in KILL_FLOORS))
    for label, tch in touch_rows:
        print(f"  {label:34s} " + " ".join(f"{100 * tch[f]:6.0f}%" for f in KILL_FLOORS))

    # ---- 6. LATENCY --------------------------------------------------------
    print("\n== 6. FILL-PRICE STRESS: coarse M15 re-walk, not measured broker latency ==")
    for frac, nn, w_, l_, tot_ in latency_stress(pg, bars, sp):
        lab = {0.0: "signal-bar close (simulator)", 0.2: "20% toward next bar close",
               0.5: "50% toward next bar close", 1.0: "next M15 bar close"}[frac]
        per_trade = f"{tot_ / nn:+.3f}" if nn else "n/a"
        print(f"  {lab:38s} n={nn} {w_}W/{l_}L/{nn - w_ - l_}T  net {tot_:+7.2f}  net/trade {per_trade}")

    # ---- 7. OPERATIONS -----------------------------------------------------
    print("\n== 7. OPERATIONS: price-log health ==")
    gaps, have, expected, missing = evidence["feed_report"]
    if bars and expected:
        print(f"  {have} of {expected} expected open-market M15 slots present "
              f"({100 * have / expected:.1f}%), {missing} missing "
              f"(same midpoint schedule as check_data), "
              f"{bars[0]['dt']:%m-%d %H:%M} -> {bars[-1]['dt']:%m-%d %H:%M}")
        flagged = [gap for gap in gaps if gap[2] > FEED_GAP_FLAG_MIN]
        if flagged:
            for start, end, minutes in flagged:
                print(f"  open-market gap > {FEED_GAP_FLAG_MIN} min: "
                      f"{start:%m-%d %H:%M} -> {end:%m-%d %H:%M} ({minutes:.0f} min)")
        else:
            print(f"  no open-market missing run > {FEED_GAP_FLAG_MIN} min")
    else:
        print("  no open-market M15 log coverage available")

    # ---- 8. GATES ----------------------------------------------------------
    return print_gates(evidence["gates"])


if __name__ == "__main__":
    sys.exit(main())
