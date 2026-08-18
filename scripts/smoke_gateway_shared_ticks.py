#!/usr/bin/env python3
"""Live smoke: two tick consumers (NQ + MNQ) share one ``rithmic-gateway`` parent.

Pre-spawns one ``rithmic-gateway`` parent, then starts two
``gateway_tick_consumer.py`` subprocesses against the same listen URL / flock.
Both must receive ticks and report the same ``gateway_pid``.

Exit codes:
  0 — both consumers OK and reported the same gateway_pid
  2 — credentials missing (CI-safe skip)
  1 — runtime / shared-parent failure

Prereqs:
  - MotiveWave / R|Trader closed (one Rithmic login)
  - ``rithmic-gateway`` on PATH or ``RITHMIC_GATEWAY_BIN``, or a debug build at
    ``target/debug/rithmic-gateway`` (this script will ``cargo build`` if needed)
  - Creds in ``.env`` (``RITHMIC_USER`` / ``RITHMIC_PASSWORD`` / gateway URL)

Example::

    uv run python scripts/smoke_gateway_shared_ticks.py --seconds 25
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

CONSUMER = ROOT / "scripts" / "gateway_tick_consumer.py"
_PID_RE = re.compile(r"gateway_pid=(\d+)")


def _ensure_gateway_bin() -> str | None:
    """Return a path to ``rithmic-gateway``, building debug if needed."""
    explicit = os.environ.get("RITHMIC_GATEWAY_BIN")
    if explicit and Path(explicit).is_file():
        return str(Path(explicit).resolve())
    which = subprocess.run(
        ["which", "rithmic-gateway"], capture_output=True, text=True, check=False
    )
    if which.returncode == 0 and which.stdout.strip():
        return which.stdout.strip()
    for candidate in (
        ROOT / "target" / "debug" / "rithmic-gateway",
        ROOT / "target" / "release" / "rithmic-gateway",
    ):
        if candidate.is_file():
            return str(candidate)
    print("building rithmic-gateway (debug)…")
    built = subprocess.run(
        ["cargo", "build", "-p", "rithmic-gateway", "--bin", "rithmic-gateway"],
        cwd=ROOT,
        check=False,
    )
    if built.returncode != 0:
        return None
    path = ROOT / "target" / "debug" / "rithmic-gateway"
    return str(path) if path.is_file() else None


def _parse_pids(output: str) -> list[int]:
    return [int(m.group(1)) for m in _PID_RE.finditer(output) if m.group(1) != "None"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=25.0)
    parser.add_argument("--min-ticks", type=int, default=3)
    parser.add_argument(
        "--symbols",
        default="NQ,MNQ",
        help="Comma-separated root symbols (default: NQ,MNQ)",
    )
    parser.add_argument("--exchange", default=os.environ.get("RITHMIC_EXCHANGE", "CME"))
    parser.add_argument(
        "--stagger-sec",
        type=float,
        default=1.0,
        help="Delay before starting the second consumer (lets first own spawn)",
    )
    args = parser.parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if len(symbols) < 2:
        print("need at least two --symbols", file=sys.stderr)
        return 1

    from rithmic_nt_connect import load_dotenv_files

    load_dotenv_files(ROOT / ".env")

    try:
        from rithmic_gateway import GatewayConfig
        from rithmic_gateway.spawn import spawn_gateway
        from rithmic_nt_connect.config import SessionConfig
    except Exception as exc:
        print(f"import failed: {exc}", file=sys.stderr)
        return 1

    bin_path = _ensure_gateway_bin()
    if not bin_path:
        print(
            "SMOKE FAIL: rithmic-gateway binary not found; set RITHMIC_GATEWAY_BIN "
            "or install on PATH",
            file=sys.stderr,
        )
        return 1
    os.environ["RITHMIC_GATEWAY_BIN"] = bin_path
    # Explicit connect mode for dual-mode helpers; consumers use GatewayClient.
    os.environ["RITHMIC_CONNECT_MODE"] = "gateway"
    os.environ.setdefault("RITHMIC_GATEWAY_AUTO_SPAWN", "1")

    try:
        session_cfg = SessionConfig.from_env()
    except Exception as exc:
        print(f"SMOKE SKIP (no credentials): {exc}")
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
        auto_spawn=True,
        gateway_bin=bin_path,
    )
    # Propagate resolved listen so both consumers dial the same (possibly clamped) path.
    os.environ["RITHMIC_GATEWAY_LISTEN"] = gcfg.listen or f"unix://{gcfg.socket_path}"

    print(
        f"shared-gateway smoke: symbols={symbols} exchange={args.exchange} "
        f"bin={bin_path} listen={gcfg.listen}"
    )
    print("NOTE: close MotiveWave / R|Trader first (one session per login).")

    parent = None
    try:
        print("pre-spawning shared rithmic-gateway parent…")
        parent = spawn_gateway(gcfg, wait_socket=True)
        print(f"parent pid={parent.pid} socket={gcfg.socket_path}")
    except Exception as exc:
        print(f"SMOKE FAIL: could not spawn parent: {exc}", file=sys.stderr)
        return 1

    env = os.environ.copy()
    procs: list[subprocess.Popen[str]] = []
    try:
        for i, symbol in enumerate(symbols):
            if i > 0 and args.stagger_sec > 0:
                time.sleep(float(args.stagger_sec))
            cmd = [
                sys.executable,
                str(CONSUMER),
                "--symbol",
                symbol,
                "--exchange",
                args.exchange,
                "--seconds",
                str(args.seconds),
                "--min-ticks",
                str(args.min_ticks),
                "--label",
                f"consumer-{symbol.lower()}",
            ]
            print(f"starting {' '.join(cmd)}")
            procs.append(
                subprocess.Popen(
                    cmd,
                    cwd=ROOT,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            )

        outputs: list[str] = []
        codes: list[int] = []
        # Allow connect + tick window + slack.
        wait_budget = float(args.seconds) + 60.0
        deadline = time.time() + wait_budget
        for proc in procs:
            remaining = max(deadline - time.time(), 1.0)
            try:
                out, _ = proc.communicate(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()
                out, _ = proc.communicate()
                codes.append(1)
                outputs.append((out or "") + "\nTIMEOUT")
                continue
            codes.append(proc.returncode if proc.returncode is not None else 1)
            outputs.append(out or "")

        for symbol, code, out in zip(symbols, codes, outputs, strict=True):
            print(f"----- consumer-{symbol.lower()} exit={code} -----")
            print(out.rstrip() or "(no output)")

        if any(c == 2 for c in codes):
            print("SMOKE SKIP: a consumer reported missing credentials")
            return 2
        if any(c != 0 for c in codes):
            print("SMOKE FAIL: one or more consumers failed", file=sys.stderr)
            return 1

        pid_sets = [_parse_pids(out) for out in outputs]
        if not all(pid_sets):
            print(
                "SMOKE FAIL: could not parse gateway_pid from consumer logs",
                file=sys.stderr,
            )
            return 1
        # Last READY/OK pid per consumer should match.
        finals = [pids[-1] for pids in pid_sets]
        if len(set(finals)) != 1:
            print(
                (
                    f"SMOKE FAIL: consumers reported different gateway_pid values: "
                    f"{finals}"
                ),
                file=sys.stderr,
            )
            return 1
        shared_pid = finals[0]
        if parent is not None and shared_pid != parent.pid:
            print(
                f"SMOKE FAIL: consumers saw gateway_pid={shared_pid} but "
                f"pre-spawned parent pid={parent.pid}",
                file=sys.stderr,
            )
            return 1
        # Confirm the parent process still looks like the gateway binary.
        try:
            cmdline = (
                Path(f"/proc/{shared_pid}/cmdline").read_bytes().replace(b"\0", b" ")
            )
            cmd_txt = cmdline.decode(errors="replace")
        except OSError:
            # macOS: fall back to ps
            ps = subprocess.run(
                ["ps", "-p", str(shared_pid), "-o", "command="],
                capture_output=True,
                text=True,
                check=False,
            )
            cmd_txt = (ps.stdout or "").strip()
            if ps.returncode != 0 or not cmd_txt:
                print(
                    f"SMOKE FAIL: gateway_pid={shared_pid} is not a live process",
                    file=sys.stderr,
                )
                return 1
        if "rithmic-gateway" not in cmd_txt:
            print(
                (
                    f"SMOKE FAIL: pid {shared_pid} command is not rithmic-gateway: "
                    f"{cmd_txt!r}"
                ),
                file=sys.stderr,
            )
            return 1

        print(f"SMOKE OK: shared rithmic-gateway pid={shared_pid} for {symbols}")
        return 0
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        if parent is not None:
            # Prefer idle-exit after last client (spawn default grace 5s).
            try:
                parent.wait(timeout=15)
            except subprocess.TimeoutExpired:
                if parent.poll() is None:
                    parent.terminate()
                    try:
                        parent.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        parent.kill()
        # Best-effort: remove listen sock we created for this run.
        with contextlib.suppress(Exception):
            Path(gcfg.socket_path).unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
