#!/usr/bin/env python3
"""
repair_log - repair torn (NUL-padded) rows in the forward-test evidence CSVs.

Why this exists
---------------
A crash, a full disk, or a merge/rollback racing an append can leave a *hole*
in a CSV: the row's own bytes are intact, but they are preceded by NUL padding,
so the row no longer starts at column 0 of its line. Every strict reader in this
repo then fails closed - `tools/live_readiness.py` exits 1 and the dashboard
Readiness tab renders "Readiness could not be computed. Evidence unavailable or
invalid: forward_test_log.csv contains unparseable rows..." - which is correct,
but it leaves the tab unable to show anything at all.

What this tool does (and deliberately does NOT do)
--------------------------------------------------
  * removes NUL bytes - never a legal byte in these CSVs - from every row;
  * appends a trailing newline only when the last row is missing one;
  * never edits a non-NUL character, never drops a row, never reorders rows,
    never backfills a bar and never touches a numeric value;
  * writes a timestamped backup (`<file>.bak-YYYYmmddTHHMMSSZ`) next to each
    repaired file before writing;
  * re-validates the repaired pair with `tools/live_readiness.py`'s strict
    loader, and restores the backups and exits 2 if the evidence still does not
    validate (real corruption, not padding, needs a human).

Typical repair (2026-11-18: 140 NUL bytes before the 2026-11-04 19:30 bar):

    $ python3 tools/repair_log.py --check        # exit 1, names the line
    $ python3 tools/repair_log.py --apply        # backup + repair + validate
    $ python3 tools/repair_log.py --check        # exit 0

Usage:
  python3 tools/repair_log.py                       # report (same as --check)
  python3 tools/repair_log.py --apply               # repair in place
  python3 tools/repair_log.py --root /opt/nasdaq    # run against live data
  python3 tools/repair_log.py --file trades.csv     # limit to one evidence file

Exit codes: 0 = nothing to repair, or repaired and re-validated;
            1 = repair needed (--check wrote nothing);
            2 = unsafe/unrepairable - nothing written, or backups restored.
Read-only in --check mode. Never uses the network.
"""
import argparse
import os
import shutil
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
from tools import live_readiness as LR  # noqa: E402  (strict evidence loader)

EVIDENCE_FILES = ("trades.csv", "forward_test_log.csv")
NUL = b"\x00"


def _rows(data):
    """Logical CSV rows: split on LF, ignore bordering whitespace/CR/NULs."""
    return [line for line in data.split(b"\n") if line.strip(b" \t\r" + NUL)]


def scan(path):
    """Byte-level scan of one CSV. Read-only; never raises on bad content."""
    with open(path, "rb") as fh:
        data = fh.read()
    nul_lines = [(lineno, line.count(NUL), line.index(NUL))
                 for lineno, line in enumerate(data.split(b"\n"), start=1) if NUL in line]
    repaired = data.replace(NUL, b"")
    trailing_newline_added = bool(repaired) and not repaired.endswith(b"\n")
    # A line that becomes empty once the NULs are gone held no row bytes at all:
    # that is lost evidence, not padding, and only a human can decide about it.
    lost_lines = [lineno for lineno, line in enumerate(data.split(b"\n"), start=1)
                  if NUL in line and not line.replace(NUL, b"").strip(b" \t\r")]
    if trailing_newline_added:
        repaired += b"\n"
    return {
        "path": path,
        "size": len(data),
        "nul_total": data.count(NUL),
        "nul_lines": nul_lines,
        "rows": len(_rows(data)),
        "repaired": repaired,
        "missing_final_newline": trailing_newline_added,
        "lost_lines": lost_lines,
        "needs_repair": bool(nul_lines) or trailing_newline_added,
    }


def describe(info):
    name = os.path.basename(info["path"])
    lines = [f"  {name}: {info['size']} bytes, {info['rows']} rows incl. header, "
             f"{info['nul_total']} NUL byte(s)"]
    for lineno, count, column in info["nul_lines"][:10]:
        lines.append(f"    line {lineno}: {count} NUL byte(s), first at column {column} "
                     f"(row starts at column {column}) -> torn/partial write")
    if len(info["nul_lines"]) > 10:
        lines.append(f"    ... and {len(info['nul_lines']) - 10} more NUL line(s)")
    if info["missing_final_newline"]:
        lines.append("    last row is missing its trailing newline")
    for lineno in info["lost_lines"][:10]:
        lines.append(f"    line {lineno}: padding with no row bytes - that row is lost, "
                     f"needs a human decision")

    return "\n".join(lines)


def repair(path, info, apply):
    """Write the NUL-stripped bytes atomically; returns the backup path or None."""
    if not apply:
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = f"{path}.bak-{stamp}"
    shutil.copy2(path, backup)
    with open(path, "rb") as original, open(backup, "rb") as copy:
        if copy.read() != original.read():
            raise OSError(f"backup verification failed for {backup}")
    tmp = f"{path}.repair-{os.getpid()}"
    try:
        with open(tmp, "wb") as fh:
            fh.write(info["repaired"])
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return backup


def _validate_pair(root, paths):
    """Re-run the repo's strict loader on the repaired evidence."""
    trades = os.path.join(root, "trades.csv")
    log = os.path.join(root, "forward_test_log.csv")
    if not (os.path.isfile(trades) and os.path.isfile(log)):
        return None  # single-file repair: nothing to cross-validate against
    LR.load_readiness_data(trades, log)
    return True


def main(argv=None):
    ap = argparse.ArgumentParser(description="Repair NUL-padded (torn) rows in the evidence CSVs")
    ap.add_argument("--root", default=ROOT, help="directory holding the CSVs (default: repo root)")
    ap.add_argument("--file", action="append", dest="files", metavar="NAME",
                    help=f"evidence file to scan (default: {' '.join(EVIDENCE_FILES)})")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="report only (default)")
    mode.add_argument("--apply", action="store_true", help="repair in place, then re-validate")
    a = ap.parse_args(argv)

    names = a.files or list(EVIDENCE_FILES)
    root = os.path.abspath(a.root)
    print(f"repair_log - {'repair' if a.apply else 'check'} {root}")

    paths, missing = [], []
    for name in names:
        path = name if os.path.isabs(name) else os.path.join(root, name)
        if os.path.isfile(path):
            paths.append(path)
        else:
            missing.append(path)
    for path in missing:
        print(f"  missing: {path} (skipped)")
    if not paths:
        print("== result: nothing to scan ==")
        return 2

    infos = [scan(path) for path in paths]
    dirty = [info for info in infos if info["needs_repair"]]
    for info in infos:
        print(describe(info))
    if not dirty:
        print("== result: CLEAN - every row starts at column 0, no NUL bytes, "
              "trailing newline present ==")
        return 0

    if not a.apply:
        print("== result: REPAIR NEEDED - run: python3 tools/repair_log.py --apply "
              "(writes a backup first) ==")
        return 1

    # A repair may only remove NULs and add one newline: the row count must not change.
    backups = {}
    try:
        for info in dirty:
            before, after = info["rows"], len(_rows(info["repaired"]))
            if info["lost_lines"]:
                raise ValueError(
                    f"{os.path.basename(info['path'])} line(s) {info['lost_lines']}: padding "
                    f"with no row bytes - that evidence is gone, not padded; review by hand")
            if before != after:
                raise ValueError(
                    f"{os.path.basename(info['path'])}: NUL removal would change the row "
                    f"count ({before} -> {after}); this is lost data, not padding - review by hand")
            backups[info["path"]] = repair(info["path"], info, apply=True)
    except (OSError, ValueError) as exc:
        print(f"  [refused] {exc}")
        for path, backup in backups.items():
            if backup and os.path.exists(backup):
                os.replace(backup, path)
        print("== result: NOT REPAIRED - backups restored, files unchanged ==")
        return 2

    try:
        _validate_pair(root, [info["path"] for info in dirty])
    except (OSError, ValueError) as exc:
        print(f"  [invalid] repaired evidence still fails the strict loader: {exc}")
        for path, backup in backups.items():
            if backup and os.path.exists(backup):
                os.replace(backup, path)
        print("== result: NOT HOLDING A REPAIR THAT DOES NOT VALIDATE - backups restored ==")
        return 2

    for path, backup in backups.items():
        print(f"  repaired {os.path.basename(path)} -> backup {os.path.basename(backup)}")
    print(f"== result: REPAIRED - {len(backups)} file(s), values unchanged, evidence reloads ==")
    return 0


if __name__ == "__main__":
    sys.exit(main())
