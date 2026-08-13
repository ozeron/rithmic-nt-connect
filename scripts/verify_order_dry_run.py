"""Phase 2 order-plant dry-run / gated live verify harness.

Default mode is **dry-run**: connect, login order plant, subscribe to order
updates, optionally poll for a few seconds, and exit **without placing orders**.

Live place is intentionally gated behind ``--live-place`` **and**
``RITHMIC_ENABLE_TRADING=1``. Do not run ``--live-place`` until conformance /
``app_name`` authorization is confirmed.

Examples::

    # Dry-run only (safe; default)
    python scripts/verify_order_dry_run.py --seconds 5

    # Explicitly refuse live place even if env is set
    python scripts/verify_order_dry_run.py --no-live-place
"""

from __future__ import annotations

import argparse
import json
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
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def _load_dotenv_files() -> None:
    _load_dotenv(ROOT / ".env")
    extra = os.environ.get("RITHMIC_CONNECT_DOTENV", "")
    for part in extra.split(os.pathsep):
        part = part.strip()
        if part:
            _load_dotenv(Path(part))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=3.0, help="poll window")
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="optional JSON report path under artifacts/",
    )
    parser.add_argument(
        "--live-place",
        action="store_true",
        help="DANGEROUS: place a 1-lot DAY limit far from market (requires env gate)",
    )
    parser.add_argument(
        "--no-live-place",
        action="store_true",
        help="force dry-run even if RITHMIC_ENABLE_TRADING is set",
    )
    parser.add_argument("--root", default="NQ", help="root symbol for front-month resolve")
    parser.add_argument("--exchange", default="CME")
    args = parser.parse_args(argv)

    _load_dotenv_files()

    from rithmic_connect.config import SessionConfig
    from rithmic_connect.front_month import resolve_front_month
    from rithmic_connect.session import create_rust_session

    session_cfg = SessionConfig.from_env()
    if not session_cfg.has_account():
        print("FAIL: account_id/fcm_id/ib_id required for order plant", file=sys.stderr)
        return 2

    env_trading = os.environ.get("RITHMIC_ENABLE_TRADING", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    allow_live = bool(args.live_place) and env_trading and not args.no_live_place
    if args.live_place and not allow_live:
        print(
            "REFUSING --live-place: set RITHMIC_ENABLE_TRADING=1 and omit --no-live-place",
            file=sys.stderr,
        )
        return 3

    session = create_rust_session(session_cfg)
    report: dict = {
        "mode": "live_place" if allow_live else "dry_run",
        "system_name": session_cfg.system_name,
        "app_name": session_cfg.app_name,
        "events": [],
        "placed": False,
    }
    try:
        session.connect()
        front = resolve_front_month(session, args.root, args.exchange)
        report["front"] = {
            "root": args.root,
            "trading_symbol": front["trading_symbol"],
            "trading_exchange": front["trading_exchange"],
        }
        session.subscribe_order_updates()
        print(
            f"order plant subscribed; front={front['trading_symbol']}.{front['trading_exchange']}; "
            f"mode={report['mode']}"
        )

        if allow_live:
            # Far OTM limit — still real venue risk; only for authorized smoke.
            print("WARNING: placing live limit order (far from market)", file=sys.stderr)
            session.place_order(
                front["trading_symbol"],
                front["trading_exchange"],
                "BUY",
                "LIMIT",
                1,
                "rithmic-connect-dryrun",
                price=1.0,
                duration="DAY",
            )
            report["placed"] = True
            print("place_order sent; waiting for notifications…")

        deadline = time.monotonic() + max(0.0, args.seconds)
        while time.monotonic() < deadline:
            ev = session.poll_order_event()
            if ev is None:
                time.sleep(0.05)
                continue
            slim = {
                "type": ev.get("type"),
                "source": ev.get("source"),
                "notify_type_name": ev.get("notify_type_name"),
                "status": ev.get("status"),
                "basket_id": ev.get("basket_id"),
                "symbol": ev.get("symbol"),
            }
            report["events"].append(slim)
            print(f"order_event: {slim}")

        report["event_count"] = len(report["events"])
        if report["placed"]:
            if report["event_count"] == 0:
                print(
                    "FAIL: place_order sent but no order events received",
                    file=sys.stderr,
                )
                return 1
            print(
                f"LIVE PLACE OK event_count={report['event_count']} placed={report['placed']}"
            )
        else:
            print(f"DRY-RUN OK event_count={report['event_count']} placed={report['placed']}")
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(report, indent=2) + "\n")
            print(f"wrote {args.out}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            session.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
