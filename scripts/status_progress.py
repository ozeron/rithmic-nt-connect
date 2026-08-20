#!/usr/bin/env python3
"""Rollup + drift checker for docs/STATUS.md.

STATUS.md is the single source of truth for planned-vs-done. Its marks use the
legend `[x]` done · `[~]` partial · `[ ]` not started · `N/A` out of scope.
This script parses those marks and prints a rollup so a human can see, at a
glance, how much of the adapter is done / partial / left / not advertised.

`--check` compares the parsed totals against the "At-a-glance" rollup table that
the doc is expected to carry, and exits non-zero if they drift. That keeps the
human-friendly table honest: you cannot edit the prose marks without also
updating (or regenerating) the scoreboard.

Pure stdlib — runs under `python scripts/status_progress.py` (no venv needed).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# A checkbox mark is one of these tokens, optionally surrounded by whitespace and
# an optional trailing " (...)" note. We accept them anywhere a list item uses them.
MARK_RE = re.compile(r"\[([ xX~])\]")  # [x] [~] [ ]  (case-insensitive x)
# Lines that are part of a fenced code block must be ignored.
FENCE_RE = re.compile(r"^\s*(```|~~~)")
# Section header "## Title"
SECTION_RE = re.compile(r"^(#{2,6})\s+(.*?)\s*$")
# The At-a-glance rollup table rows we expect, e.g.:
#   | **Capability matrix** | 21 | 10 | 8 | 3 | 0 |
# column order: label | Total | [x] | [~] | [ ] | N/A
ROLLUP_ROW_RE = re.compile(
    r"^\|\s*\*\*(.+?)\*\*\s*\|"  # bold label
    r"\s*(\d+)\s*\|"  # total
    r"\s*(\d+)\s*\|"  # done
    r"\s*(\d+)\s*\|"  # partial
    r"\s*(\d+)\s*\|"  # not started
    r"\s*(\d+)\s*\|\s*$"  # N/A
)


def parse_marks(path: Path) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """Return (per_section counts, grand totals).

    `N/A` is not a checkbox so it is not counted as a mark; it is excluded from
    both section and grand totals. Only `[x]/[~]/[ ]` rows feed the rollup.
    """
    text = path.read_text(encoding="utf-8").splitlines()
    in_fence = False
    section = "(preamble)"
    per_section: dict[str, dict[str, int]] = {section: _zero()}
    grand = _zero()
    for line in text:
        if FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = SECTION_RE.match(line)
        if m and m.group(1) == "##":
            section = m.group(2).strip()
            per_section.setdefault(section, _zero())
            continue
        for mm in MARK_RE.finditer(line):
            token = mm.group(1).lower()
            per_section[section][token] += 1
            grand[token] += 1
    return per_section, grand


def _zero() -> dict[str, int]:
    return {"x": 0, "~": 0, " ": 0}


def pct(done: int, partial: int, total: int) -> str:
    if total == 0:
        return "n/a"
    implemented = done + 0.5 * partial
    return f"{implemented / total * 100:.0f}%"


def render_rollup(per_section: dict[str, dict[str, int]], grand: dict[str, int]) -> str:
    lines = []
    lines.append("## At-a-glance (generated — run `python scripts/status_progress.py`)")
    lines.append("")
    lines.append(
        "Legend: **Done** `[x]` · **Partial** `[~]` · **Not started** `[ ]` · `N/A` out of scope."
    )
    lines.append("")
    lines.append("| Area | Total | Done | Partial | Not started | N/A |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    # Ignore the preamble (counts before first section) — it is prose, no marks.
    for name, counts in per_section.items():
        if name == "(preamble)":
            continue
        total = counts["x"] + counts["~"] + counts[" "]
        if total == 0:
            continue
        na = 0  # N/A is not a checkbox; we do not track it per section here
        lines.append(
            f"| **{name}** | {total} | {counts['x']} | {counts['~']} | {counts[' ']} | {na} |"
        )
    total = grand["x"] + grand["~"] + grand[" "]
    lines.append(
        f"| **TOTAL** | {total} | {grand['x']} | {grand['~']} | {grand[' ']} | 0 |"
    )
    lines.append("")
    lines.append(
        f"**Implemented:** {pct(grand['x'], grand['~'], total)} "
        f"(`{grand['x']}` done + `{grand['~']}` partial of `{total}` marked items; "
        f"partial counts as half)."
    )
    return "\n".join(lines)


def parse_doc_rollup(path: Path) -> dict[str, dict[str, int]] | None:
    """Read the At-a-glance rollup table back out of the doc, if present."""
    text = path.read_text(encoding="utf-8").splitlines()
    rows: dict[str, dict[str, int]] = {}
    for line in text:
        m = ROLLUP_ROW_RE.match(line)
        if not m:
            continue
        label = m.group(1).strip()
        rows[label] = {
            "total": int(m.group(2)),
            "x": int(m.group(3)),
            "~": int(m.group(4)),
            " ": int(m.group(5)),
            "na": int(m.group(6)),
        }
    return rows or None


def main() -> int:
    ap = argparse.ArgumentParser(description="Rollup + drift check for docs/STATUS.md")
    ap.add_argument(
        "--status",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "docs" / "STATUS.md",
        help="Path to STATUS.md (default: repo docs/STATUS.md)",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the doc's At-a-glance table drifts from parsed marks",
    )
    args = ap.parse_args()

    per_section, grand = parse_marks(args.status)

    if not args.check:
        print(render_rollup(per_section, grand))
        return 0

    # --check mode: compare doc rollup against parsed marks.
    doc_rows = parse_doc_rollup(args.status)
    if doc_rows is None:
        print(
            "ERROR: --check: no At-a-glance rollup table found in STATUS.md",
            file=sys.stderr,
        )
        return 2
    errors = []
    # Match the per-section rows we generate (skip the TOTAL row in the doc; recompute it).
    generated = render_rollup(per_section, grand)
    gen_rows: dict[str, dict[str, int]] = {}
    for line in generated.splitlines():
        m = ROLLUP_ROW_RE.match(line)
        if not m:
            continue
        gen_rows[m.group(1).strip()] = {
            "total": int(m.group(2)),
            "x": int(m.group(3)),
            "~": int(m.group(4)),
            " ": int(m.group(5)),
        }
    for label, want in gen_rows.items():
        got = doc_rows.get(label)
        if got is None:
            errors.append(f"  missing rollup row for '{label}'")
            continue
        for key, klabel in (
            ("x", "Done"),
            ("~", "Partial"),
            (" ", "Not started"),
            ("total", "Total"),
        ):
            if got.get(key) != want.get(key):
                errors.append(
                    f"  '{label}' {klabel}: doc={got.get(key)} parsed={want.get(key)}"
                )
    if errors:
        print(
            "ERROR: STATUS.md At-a-glance rollup drifted from its marks:",
            file=sys.stderr,
        )
        print("\n".join(errors), file=sys.stderr)
        print("\nRegenerate with: python scripts/status_progress.py", file=sys.stderr)
        return 1
    print("OK: STATUS.md At-a-glance rollup matches parsed marks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
