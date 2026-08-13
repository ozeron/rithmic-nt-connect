#!/usr/bin/env python3
"""Resolve front month, record live trades, compare to history, write verify JSON.

Exit codes:
  0 — verify OK
  1 — runtime / verify failure
  2 — credentials missing (CI-safe skip)

Example:
  python scripts/verify_live_vs_history.py --root NQ --seconds 12 --out artifacts/verify.json
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def _load_dotenv_files() -> None:
    """Load repo `.env`, then optional paths from `RITHMIC_CONNECT_DOTENV` (os.pathsep)."""
    _load_dotenv(ROOT / ".env")
    extra = os.environ.get("RITHMIC_CONNECT_DOTENV", "")
    for part in extra.split(os.pathsep):
        part = part.strip()
        if part:
            _load_dotenv(Path(part))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=None, help="Product root (default: env symbol or NQ)")
    parser.add_argument("--exchange", default=None, help="Exchange (default: env or CME)")
    parser.add_argument("--seconds", type=float, default=10.0, help="Live record duration")
    parser.add_argument(
        "--min-overlap",
        type=float,
        default=0.0,
        help="Minimum live/history overlap ratio required for OK",
    )
    parser.add_argument(
        "--out",
        default="artifacts/verify_live_vs_history.json",
        help="Path to write the verify report JSON",
    )
    parser.add_argument(
        "--record-dir",
        default="artifacts/live_ticks",
        help="Directory for JSONL live tick recording",
    )
    args = parser.parse_args(argv)

    _load_dotenv_files()

    try:
        from rithmic_nt_connect.config import SessionConfig
        from rithmic_nt_connect.session import create_rust_session
        from rithmic_nt_connect.verify import run_front_month_verify
    except Exception as exc:  # noqa: BLE001
        print(f"import failed: {exc}", file=sys.stderr)
        return 1

    try:
        cfg = SessionConfig.from_env()
    except Exception as exc:  # noqa: BLE001
        print(f"VERIFY SKIP (no credentials): {exc}")
        return 2

    root = args.root or cfg.symbol or "NQ"
    exchange = args.exchange or cfg.exchange or "CME"
    print(f"NOTE: close MotiveWave first. Resolving front for {root}/{exchange}…")

    session = create_rust_session(cfg)
    session.connect()
    try:
        report = run_front_month_verify(
            session,
            root=root,
            exchange=exchange,
            record_sec=args.seconds,
            min_overlap_ratio=args.min_overlap,
            record_dir=args.record_dir,
        )
        out = report.write_json(args.out)
        print(report.summary)
        print(f"wrote {out}")
        if report.recorded_live_path:
            print(f"recorded {report.recorded_live_path}")
        return 0 if report.ok else 1
    finally:
        try:
            session.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
