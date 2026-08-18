#!/usr/bin/env python3
"""Single-symbol tick consumer over the shared ``rithmic-gateway``.

Designed to run as one of several processes against the same credential set.
Uses ``GatewayClient`` auto-spawn (or an already-running parent).

Exit codes:
  0 — received ``--min-ticks`` MD events for the resolved front-month symbol
  2 — credentials / config missing (CI-safe skip)
  1 — runtime failure
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


def _gateway_pid(cfg) -> int | None:
    """PID written into the credential flock by the parent, if present."""
    from rithmic_gateway.flock import lock_path

    path = lock_path(cfg.user, cfg.system_name, cfg.url, cfg.env)
    try:
        text = path.read_text().strip()
        return int(text) if text else None
    except (OSError, ValueError):
        return None


def _resolve_trading_symbol(client, root: str, exchange: str) -> str:
    front = client.get_front_month(root, exchange)
    trading = None
    if isinstance(front, dict):
        trading = front.get("trading_symbol") or front.get("symbol")
    return str(trading or root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=os.environ.get("RITHMIC_SYMBOL", "NQ"))
    parser.add_argument("--exchange", default=os.environ.get("RITHMIC_EXCHANGE", "CME"))
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--min-ticks", type=int, default=3)
    parser.add_argument(
        "--label",
        default="",
        help="Optional tag printed on log lines (e.g. consumer-nq)",
    )
    args = parser.parse_args(argv)
    label = args.label or args.symbol

    from rithmic_nt_connect import load_dotenv_files

    load_dotenv_files(ROOT / ".env")
    os.environ.setdefault("RITHMIC_CONNECT_MODE", "gateway")

    try:
        from rithmic_gateway import GatewayClient, GatewayConfig
        from rithmic_gateway.flock import SessionLock, SessionLockError
        from rithmic_nt_connect.config import SessionConfig
    except Exception as exc:
        print(f"[{label}] import failed: {exc}", file=sys.stderr)
        return 1

    try:
        session_cfg = SessionConfig.from_env()
    except Exception as exc:
        print(f"[{label}] SKIP (no credentials): {exc}")
        return 2

    gcfg = GatewayConfig(
        user=session_cfg.user,
        system_name=session_cfg.system_name,
        url=session_cfg.url,
        env=session_cfg.env,
        account_id=session_cfg.account_id or "",
        fcm_id=session_cfg.fcm_id or "",
        ib_id=session_cfg.ib_id or "",
        auth_token=getattr(session_cfg, "gateway_auth_token", None) or "",
        listen=getattr(session_cfg, "gateway_listen", None),
        auto_spawn=bool(getattr(session_cfg, "gateway_auto_spawn", True)),
        gateway_bin=getattr(session_cfg, "gateway_bin", None)
        or os.environ.get("RITHMIC_GATEWAY_BIN"),
    )

    client = GatewayClient(gcfg, rpc_timeout_sec=30.0)
    try:
        print(
            f"[{label}] connecting socket={gcfg.socket_path} "
            f"auto_spawn={gcfg.auto_spawn} user={gcfg.user[:2]}***"
        )
        client.connect()
        pid = _gateway_pid(gcfg)
        print(f"[{label}] READY gateway_pid={pid} scopes={client.scopes}")

        # Prove another process owns the flock (this consumer must not hold it).
        try:
            stolen = SessionLock.try_acquire(
                gcfg.user, gcfg.system_name, gcfg.url, gcfg.env
            )
            stolen.close()
            print(
                f"[{label}] FAIL: credential flock was free after connect "
                "(parent did not hold session)",
                file=sys.stderr,
            )
            return 1
        except SessionLockError:
            pass

        trading = _resolve_trading_symbol(client, args.symbol, args.exchange)
        print(f"[{label}] front_month root={args.symbol} trading_symbol={trading}")
        client.subscribe(trading, args.exchange)

        got = 0
        deadline = time.time() + float(args.seconds)
        while time.time() < deadline and got < args.min_ticks:
            ev = client.poll_event(timeout_ms=200)
            if ev is None:
                continue
            sym = str(ev.get("symbol") or "")
            # Fan-out is per SubKey; still filter defensively.
            if sym and sym != trading and not sym.startswith(args.symbol):
                continue
            got += 1
            print(f"[{label}] tick#{got} type={ev.get('type')} symbol={sym or trading}")

        pid = _gateway_pid(gcfg)
        print(f"[{label}] events_received={got} gateway_pid={pid}")
        if got < args.min_ticks:
            print(
                f"[{label}] FAIL: expected >= {args.min_ticks} ticks",
                file=sys.stderr,
            )
            return 1
        print(f"[{label}] OK")
        return 0
    except Exception as exc:
        print(f"[{label}] FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        with contextlib.suppress(Exception):
            client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
