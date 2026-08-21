#!/usr/bin/env python3
"""Rollup + drift checker for docs/STATUS.md.

STATUS.md marks use `[x]` done · `[~]` partial · `[ ]` not started · `N/A` out of
scope. This script prints a rollup, and `--check` exits non-zero if the doc's
"At-a-glance" table drifts from those marks (regenerate it after editing).

Pure stdlib — `python scripts/status_progress.py` (no venv needed).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Checkbox marks: [x] done · [~] partial · [ ] not started.
MARK_RE = re.compile(r"\[([ xX~])\]")
# Out-of-scope marker on status rows (`**N/A**` in tables, plain `N/A` elsewhere).
NA_RE = re.compile(r"\bN/A\b")
# Skip fenced code blocks.
FENCE_RE = re.compile(r"^\s*(```|~~~)")
SECTION_RE = re.compile(r"^##\s+(.*?)\s*$")
# The generated scoreboard itself must never be re-parsed as work items.
GENERATED_SECTION_PREFIX = "At-a-glance"
# Rollup table row: | **Label** | total | done | partial | not started | N/A |
ROLLUP_ROW_RE = re.compile(
    r"^\|\s*\*\*(.+?)\*\*\s*\|"
    r"\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*$"
)

PREAMBLE = "(preamble)"
SKIPPED = "(skipped)"
KEYS = ("x", "~", " ", "na")


def _zero() -> dict[str, int]:
    return {k: 0 for k in KEYS}


def parse_marks(path: Path) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """Return (per-section counts, grand totals) for status marks.

    Counts ``[x]/[~]/[ ]`` checkboxes plus inline ``N/A`` markers. The
    preamble (legend prose before the first heading) and the generated
    At-a-glance section are excluded so their example glyphs are never
    counted as work items.
    """
    per_section: dict[str, dict[str, int]] = {PREAMBLE: _zero()}
    grand = _zero()
    section = PREAMBLE
    in_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if m := SECTION_RE.match(line):
            title = m.group(1)
            if title.startswith(GENERATED_SECTION_PREFIX):
                section = SKIPPED
                continue
            section = title
            per_section.setdefault(section, _zero())
            continue
        if section in (PREAMBLE, SKIPPED):
            continue
        na = len(NA_RE.findall(line))
        if na:
            per_section[section]["na"] += na
            grand["na"] += na
        for mm in MARK_RE.finditer(line):
            token = mm.group(1).lower()
            per_section[section][token] += 1
            grand[token] += 1
    return per_section, grand


def pct(done: int, partial: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{(done + 0.5 * partial) / total * 100:.0f}%"


def rollup_rows(
    per_section: dict[str, dict[str, int]], grand: dict[str, int]
) -> list[str]:
    """Render the At-a-glance scoreboard as markdown lines."""
    rows = [
        f"## {GENERATED_SECTION_PREFIX} "
        "(generated — run `python scripts/status_progress.py`)",
        "",
        "Legend: **Done** `[x]` · **Partial** `[~]` · **Not started** `[ ]` · "
        "`N/A` out of scope.",
        "",
        "| Area | Total | Done | Partial | Not started | N/A |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, counts in per_section.items():
        if name in (PREAMBLE, SKIPPED):
            continue
        total = sum(counts[k] for k in KEYS)
        if total == 0:
            continue
        rows.append(
            f"| **{name}** | {total} | {counts['x']} | {counts['~']} | "
            f"{counts[' ']} | {counts['na']} |"
        )
    total = sum(grand[k] for k in KEYS)
    rows += [
        f"| **TOTAL** | {total} | {grand['x']} | {grand['~']} | "
        f"{grand[' ']} | {grand['na']} |",
        "",
        f"**Implemented:** {pct(grand['x'], grand['~'], total - grand['na'])} "
        f"(`{grand['x']}` done + `{grand['~']}` partial of `{total - grand['na']}` "
        f"in-scope marked items (`{grand['na']}` N/A excluded); "
        f"partial counts as half).",
    ]
    return rows


def parse_doc_rows(path: Path) -> dict[str, tuple[int, int, int, int, int]] | None:
    """Read the rollup table out of the doc, keyed by label: (total, x, ~, ' ', na)."""
    rows: dict[str, tuple[int, int, int, int, int]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if m := ROLLUP_ROW_RE.match(line):
            rows[m.group(1).strip()] = tuple(int(g) for g in m.groups()[1:])
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
        print("\n".join(rollup_rows(per_section, grand)))
        return 0

    doc_rows = parse_doc_rows(args.status)
    if doc_rows is None:
        print(
            "ERROR: --check: no At-a-glance rollup table found in STATUS.md",
            file=sys.stderr,
        )
        return 2

    # The TOTAL row is recomputed from the same marks, so only compare section rows.
    generated = {
        m.group(1).strip(): tuple(int(g) for g in m.groups()[1:])
        for line in rollup_rows(per_section, grand)
        if (m := ROLLUP_ROW_RE.match(line))
    }
    errors = []
    for label, want in generated.items():
        got = doc_rows.get(label)
        if got is None:
            errors.append(f"  missing rollup row for '{label}'")
            continue
        for i, name in enumerate(("Total", "Done", "Partial", "Not started", "N/A")):
            if got[i] != want[i]:
                errors.append(f"  '{label}' {name}: doc={got[i]} parsed={want[i]}")
    for label in doc_rows:
        if label != "TOTAL" and label not in generated:
            errors.append(f"  obsolete rollup row for '{label}'")
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
