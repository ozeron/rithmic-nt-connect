#!/usr/bin/env python3
"""LucidTrading acceptance smoke (credentials-gated).

Exit codes:
  0 — smoke OK
  2 — credentials missing (safe skip for CI)
  1 — runtime failure
"""

from __future__ import annotations

import os
import sys
import time
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


def main() -> int:
    _load_dotenv_files()

    try:
        from rithmic_connect.config import SessionConfig
    except Exception as exc:  # noqa: BLE001
        print(f"import failed: {exc}", file=sys.stderr)
        return 1

    try:
        session_cfg = SessionConfig.from_env()
    except Exception as exc:  # noqa: BLE001
        print(f"SMOKE SKIP (no credentials): {exc}")
        return 2

    try:
        from rithmic_connect.session import create_rust_session
    except Exception as exc:  # noqa: BLE001
        print(f"SMOKE FAIL: extension unavailable: {exc}", file=sys.stderr)
        print("Build with: maturin develop --features python", file=sys.stderr)
        return 1

    print(
        f"connecting system={session_cfg.system_name!r} "
        f"url={session_cfg.url!r} user={session_cfg.user[:2]}***"
    )
    print("NOTE: close MotiveWave / R|Trader first (one session per login).")

    session = create_rust_session(session_cfg)
    try:
        session.connect()
        symbol = session_cfg.symbol or "NQ"
        exchange = session_cfg.exchange or "CME"
        front = session.get_front_month(symbol, exchange)
        print(f"front month: {front}")
        trading = (
            front.get("trading_symbol")
            if isinstance(front, dict)
            else None
        ) or symbol
        session.subscribe(str(trading), exchange)
        got = 0
        deadline = time.time() + 15
        while time.time() < deadline and got < 3:
            ev = session.poll_event()
            if ev is None:
                time.sleep(0.05)
                continue
            print(f"event: type={ev.get('type')}")
            got += 1
        print(f"events_received={got}")
        if session_cfg.has_account():
            try:
                session.subscribe_pnl()
                print("subscribe_pnl OK")
            except Exception as exc:  # noqa: BLE001
                print(f"subscribe_pnl soft-fail: {exc}")
        # AE4 Phase 1 smoke: trading not exercised here. Order APIs may exist for
        # Phase 2 but this smoke never calls them.
        if hasattr(session, "place_order"):
            print("order API present (Phase 2); smoke does not place orders")
        print("SMOKE OK")
        return 0 if got > 0 else 1
    except Exception as exc:  # noqa: BLE001
        print(f"SMOKE FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            session.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
