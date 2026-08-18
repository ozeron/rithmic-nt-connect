#!/usr/bin/env python3
"""List Rithmic system names for a gateway (no login).

Usage:
  python scripts/list_systems.py
  python scripts/list_systems.py --url wss://rituz00100.rithmic.com:443
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


def main() -> int:
    from rithmic_nt_connect import load_dotenv

    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="List Rithmic systems on a gateway")
    parser.add_argument(
        "--url",
        default=os.environ.get("RITHMIC_GATEWAY"),
        help="Gateway URL (default: RITHMIC_GATEWAY or Lucid production)",
    )
    args = parser.parse_args()

    try:
        from rithmic_nt_connect.constants import DEFAULT_GATEWAY_URL
        from rithmic_nt_connect.systems import list_systems
    except Exception as exc:
        print(f"import failed: {exc}", file=sys.stderr)
        print("Build with: maturin develop --features python", file=sys.stderr)
        return 1

    url = args.url or DEFAULT_GATEWAY_URL
    print(f"Connecting to {url} ...")
    try:
        names = list_systems(url)
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("Available systems:")
    for name in names:
        print(f"  - {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
