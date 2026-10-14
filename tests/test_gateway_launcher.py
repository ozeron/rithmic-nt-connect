"""``python -m rithmic_gateway``: explicit parent launcher (exec, env passthrough)."""

from __future__ import annotations

import os
import subprocess  # nosec B404 - runs the launcher under test
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_SRC = REPO_ROOT / "python"


def _launch(
    env: dict[str, str], *, python_path: Path = PYTHON_SRC
) -> subprocess.CompletedProcess[str]:
    full_env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(python_path)}
    full_env.update(env)
    return subprocess.run(  # nosec B603 - fixed argv, test-controlled env
        [sys.executable, "-m", "rithmic_gateway"],
        env=full_env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _fake_binary(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "rithmic-gateway"
    path.write_text(f"#!/bin/sh\n{body}\n")
    path.chmod(0o755)
    return path


def test_launcher_execs_binary_with_callers_environment(tmp_path: Path) -> None:
    binary = _fake_binary(
        tmp_path,
        'echo "pw=$RITHMIC_PASSWORD idle=${RITHMIC_GATEWAY_IDLE_EXIT_SEC-unset}"\n'
        "exit 7",
    )
    proc = _launch(
        {"RITHMIC_GATEWAY_BIN": str(binary), "RITHMIC_PASSWORD": "parent-secret"}
    )
    assert proc.returncode == 7
    assert "pw=parent-secret" in proc.stdout
    # The launcher must not set idle-exit: the parent's lifetime is the operator's.
    assert "idle=unset" in proc.stdout


def test_launcher_preserves_an_explicit_idle_exit(tmp_path: Path) -> None:
    binary = _fake_binary(tmp_path, 'echo "idle=$RITHMIC_GATEWAY_IDLE_EXIT_SEC"')
    proc = _launch(
        {"RITHMIC_GATEWAY_BIN": str(binary), "RITHMIC_GATEWAY_IDLE_EXIT_SEC": "30"}
    )
    assert proc.returncode == 0
    assert "idle=30" in proc.stdout


def test_launcher_replaces_the_process_instead_of_detaching(tmp_path: Path) -> None:
    binary = _fake_binary(tmp_path, 'echo "pid=$$"')
    full_env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(PYTHON_SRC),
        "RITHMIC_GATEWAY_BIN": str(binary),
    }
    proc = subprocess.Popen(  # nosec B603 - fixed argv, test-controlled env
        [sys.executable, "-m", "rithmic_gateway"],
        env=full_env,
        stdout=subprocess.PIPE,
        text=True,
    )
    out, _ = proc.communicate(timeout=30)
    assert out.strip() == f"pid={proc.pid}"


def test_launcher_without_a_binary_names_the_env_var(tmp_path: Path) -> None:
    proc = _launch({"RITHMIC_GATEWAY_BIN": str(tmp_path / "missing")})
    assert proc.returncode == 2
    assert "RITHMIC_GATEWAY_BIN" in proc.stderr
    assert proc.stdout == ""
