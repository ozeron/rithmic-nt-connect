#!/usr/bin/env python3
"""Rithmic Test: hydrate-sized 1m Load* vs live time-bar subscribe (RC2.3 probe).

Measures whether a session-scale history window can succeed while live 1m
intents are active, and whether live bars keep arriving during the Load*.

Uses gateway mode + auto-spawn. Forces Rithmic Test (does not touch Lucid).

Exit codes:
  0 — ran both arms; prints PASS/DENY/FAIL lines (always 0 if measurement OK)
  1 — runtime failure
  2 — credentials missing

Example::

    cd /path/to/rithmic-connect
    uv run python scripts/probe_hydrate_under_live_md.py --root MNQ --hours 6
"""

from __future__ import annotations

import argparse
import contextlib
import itertools
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

# Rithmic TimeBarType: MINUTE_BAR=2
_MINUTE_BAR = 2


def _force_rithmic_test() -> None:
    """Load connector Test creds; drop any Lucid live env already in the shell."""
    from rithmic_nt_connect.config import _parse_dotenv

    # Shell often has Lucid from qgw `.env` source; setdefault would keep it.
    for key in list(os.environ):
        if key.startswith("RITHMIC_"):
            os.environ.pop(key, None)

    env_path = ROOT / ".env"
    if env_path.is_file():
        for key, value in _parse_dotenv(env_path).items():
            if key.startswith("RITHMIC_") or key.startswith("NAUTILUS_"):
                os.environ[key] = value

    os.environ["RITHMIC_SYSTEM_NAME"] = "Rithmic Test"
    test_url = os.environ.get(
        "RITHMIC_TEST_GATEWAY", "wss://rituz00100.rithmic.com:443"
    )
    # Gateway bin reads RITHMIC_URL (not RITHMIC_GATEWAY). Wrong URL + "Rithmic Test"
    # → plant reject [1067] invalid system name.
    os.environ["RITHMIC_GATEWAY"] = test_url
    os.environ["RITHMIC_URL"] = test_url
    os.environ["RITHMIC_ENV"] = "Live"
    os.environ["RITHMIC_CONNECT_MODE"] = "gateway"
    os.environ.setdefault("RITHMIC_ENABLE_TRADING", "0")
    # Test often lacks continuous root front-month; Ready probe needs a listed
    # contract (see looks_like_listed_contract in rithmic-plants).
    os.environ.setdefault("RITHMIC_HISTORY_READY_SYMBOL", "NQU6")
    os.environ.setdefault("RITHMIC_HISTORY_READY_EXCHANGE", "CME")
    os.environ.setdefault("RITHMIC_SYMBOL", "NQ")
    os.environ.setdefault("RITHMIC_EXCHANGE", "CME")
    for key in ("RITHMIC_FCM_ID", "RITHMIC_IB_ID", "RITHMIC_ACCOUNT_ID"):
        os.environ.pop(key, None)
    os.environ.setdefault(
        "RITHMIC_GATEWAY_BIN",
        str(ROOT / "target" / "release" / "rithmic-gateway"),
    )


def _resolve_front(client: Any, root: str, exchange: str, explicit: str | None) -> str:
    if explicit:
        return explicit
    try:
        front = client.get_front_month(root, exchange)
        if isinstance(front, dict):
            trading = front.get("trading_symbol") or front.get("symbol")
            if trading:
                return str(trading)
    except Exception as exc:
        print(f"get_front_month failed ({exc}); falling back to listed hint")
    # Rithmic Test often has no continuous root; prefer Ready-proven listed.
    hint = os.environ.get("RITHMIC_HISTORY_READY_SYMBOL", "")
    if hint.upper().startswith(root.upper()) and len(hint) > len(root):
        return hint
    # Sep quarter hint (best-effort for Test smoke).
    return f"{root}U6"


def _window(hours: float) -> tuple[int, int]:
    end = datetime.now(tz=UTC)
    start = end - timedelta(hours=hours)
    return int(start.timestamp()), int(end.timestamp())


def _try_load(
    client: Any, *, symbol: str, exchange: str, start_sec: int, end_sec: int
) -> tuple[str, int, float, str]:
    """Return (status, n_bars, elapsed_sec, detail)."""
    t0 = time.perf_counter()
    try:
        bars = client.load_time_bars(
            symbol,
            exchange,
            start_sec,
            end_sec,
            bar_type=_MINUTE_BAR,
            period=1,
        )
        elapsed = time.perf_counter() - t0
        n = len(bars) if bars is not None else 0
        return "ok", n, elapsed, f"bars={n}"
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        code = getattr(exc, "code", "") or ""
        msg = str(exc)
        if code == "capability_denied" or "history_denied_live_md" in msg:
            return "denied", 0, elapsed, f"{code or 'capability_denied'}: {msg}"
        return "error", 0, elapsed, f"{code or type(exc).__name__}: {msg}"


def _drain_live(client: Any, *, deadline: float, sink: list[float]) -> int:
    """Poll MD + history(time_bar) events until deadline; append wall times."""
    n = 0
    while time.time() < deadline:
        rem_ms = max(1, int((deadline - time.time()) * 1000))
        chunk = min(100, rem_ms)
        ev = client.poll_event(timeout_ms=chunk)
        if ev is None:
            ev = client.poll_history_event(timeout_ms=0)
        if ev is None:
            continue
        sink.append(time.time())
        n += 1
    return n


def _gap_stats(times: list[float]) -> tuple[float, float]:
    if len(times) < 2:
        return 0.0, 0.0
    gaps = [b - a for a, b in itertools.pairwise(times)]
    return max(gaps), sum(gaps) / len(gaps)


def _make_client() -> Any:
    from rithmic_gateway import GatewayClient, GatewayConfig
    from rithmic_nt_connect.config import SessionConfig

    session_cfg = SessionConfig.from_env()
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
    client = GatewayClient(gcfg, rpc_timeout_sec=60.0)
    client.connect()
    print(
        f"READY system={gcfg.system_name!r} env={gcfg.env} "
        f"socket={gcfg.socket_path} scopes={client.scopes}"
    )
    return client


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="NQ")
    parser.add_argument("--exchange", default="CME")
    parser.add_argument(
        "--symbol",
        default="",
        help="Listed contract override (default: front month or NQU6-style hint)",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=6.0,
        help="Hydrate-sized 1m history window (default 6h ≈ session lookback)",
    )
    parser.add_argument(
        "--soak-sec",
        type=float,
        default=70.0,
        help="Seconds to collect live 1m bars before/during Load*",
    )
    parser.add_argument(
        "--two-peer",
        action="store_true",
        help="Peer A holds live 1m; peer B issues Load* (Kamal-overlap shape)",
    )
    args = parser.parse_args(argv)

    from rithmic_nt_connect import load_dotenv_files  # noqa: F401 — keep import side

    _force_rithmic_test()
    print(
        f"probe user={os.environ.get('RITHMIC_USER', '')[:4]}*** "
        f"system={os.environ.get('RITHMIC_SYSTEM_NAME')!r} "
        f"url={os.environ.get('RITHMIC_GATEWAY')}"
    )
    try:
        client_a = _make_client()
    except Exception as exc:
        print(f"SKIP/FAIL connect: {exc}")
        return 2 if "credential" in str(exc).lower() else 1

    client_b = None
    try:
        trading = _resolve_front(
            client_a, args.root, args.exchange, args.symbol.strip() or None
        )
        start_sec, end_sec = _window(args.hours)
        print(
            f"front={trading} window_h={args.hours} "
            f"start={datetime.fromtimestamp(start_sec, tz=UTC).isoformat()} "
            f"end={datetime.fromtimestamp(end_sec, tz=UTC).isoformat()}"
        )

        # --- Arm 1: cold Load* (no live intents) ---
        status, _n, elapsed, detail = _try_load(
            client_a,
            symbol=trading,
            exchange=args.exchange,
            start_sec=start_sec,
            end_sec=end_sec,
        )
        print(f"ARM1_COLD_LOAD status={status} elapsed_s={elapsed:.2f} {detail}")
        if status == "error":
            return 1

        # --- Arm 2: live intents (1m bars + ticker for gap resolution), then Load* ---
        print(f"subscribe_time_bars {trading} MINUTE_BAR period=1")
        client_a.subscribe_time_bars(trading, args.exchange, _MINUTE_BAR, 1)
        print(f"subscribe ticker {trading} (gap probe; also counts as live MD intent)")
        client_a.subscribe(trading, args.exchange)

        live_times: list[float] = []
        pre_deadline = time.time() + min(20.0, max(8.0, args.soak_sec * 0.25))
        pre_n = _drain_live(client_a, deadline=pre_deadline, sink=live_times)
        print(f"live_pre_events={pre_n}")

        load_client = client_a
        if args.two_peer:
            client_b = _make_client()
            load_client = client_b
            print("two-peer: peer B will Load* while peer A keeps live intent")

        # Drain ticks in a side thread while Load* runs
        # (mutex starve would show as gap).
        stop = {"done": False}

        def _bg_drain() -> None:
            while not stop["done"]:
                _drain_live(client_a, deadline=time.time() + 0.25, sink=live_times)

        import threading

        bg = threading.Thread(target=_bg_drain, name="live-drain", daemon=True)
        bg.start()
        status2, _n2, elapsed2, detail2 = _try_load(
            load_client,
            symbol=trading,
            exchange=args.exchange,
            start_sec=start_sec,
            end_sec=end_sec,
        )
        stop["done"] = True
        bg.join(timeout=2.0)
        post_n = _drain_live(client_a, deadline=time.time() + 5.0, sink=live_times)
        max_gap, mean_gap = _gap_stats(live_times)
        print(
            f"ARM2_UNDER_LIVE status={status2} elapsed_s={elapsed2:.2f} {detail2} "
            f"live_events={len(live_times)} post_drain={post_n} "
            f"max_gap_s={max_gap:.2f} mean_gap_s={mean_gap:.2f}"
        )

        if status2 == "denied":
            print(
                "VERDICT: RC2.3 DENIES hydrate-sized Load* while live 1m intent "
                "exists (current production behavior)."
            )
        elif status2 == "ok":
            print(
                "VERDICT: hydrate-sized Load* SUCCEEDED under live 1m — "
                f"inspect max_gap_s={max_gap:.2f} before considering allowlist."
            )
        else:
            print(f"VERDICT: unexpected status={status2}")
            return 1

        with contextlib.suppress(Exception):
            client_a.unsubscribe_time_bars(trading, args.exchange, _MINUTE_BAR, 1)
        with contextlib.suppress(Exception):
            client_a.unsubscribe(trading, args.exchange)
        return 0
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        with contextlib.suppress(Exception):
            if client_b is not None:
                client_b.disconnect()
        with contextlib.suppress(Exception):
            client_a.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
