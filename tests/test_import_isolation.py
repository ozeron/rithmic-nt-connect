"""The adapter and the lake package stay independent of each other.

Each check runs in a fresh interpreter with the other side's modules made
unimportable, so a stray top-level or lazy import shows up as an ImportError
(openspec: rust-unified-session, adapter-wire-facade).
"""

from __future__ import annotations

import os
import subprocess  # nosec B404 - runs this repo's own interpreter
import sys
import textwrap
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

_BLOCK = textwrap.dedent(
    """
    import sys
    for name in {blocked!r}:
        sys.modules[name] = None  # any import of it raises ImportError
    """
)


def _run(blocked: list[str], body: str) -> subprocess.CompletedProcess[str]:
    code = _BLOCK.format(blocked=blocked) + textwrap.dedent(body)
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(_ROOT / "python"), str(_ROOT / "scripts")]),
    }
    return subprocess.run(  # nosec B603 - fixed argv, no shell
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )


def test_the_adapter_creates_both_session_modes_without_protobuf_or_the_lake() -> None:
    pytest.importorskip("rithmic_nt_connect._lib")
    proc = _run(
        ["google.protobuf", "rithmic_gateway"],
        """
        from rithmic_nt_connect.config import ConnectMode, SessionConfig
        from rithmic_nt_connect.session import create_session

        gateway = create_session(
            SessionConfig(
                user="u-import-isolation",
                connect_mode=ConnectMode.GATEWAY,
                gateway_listen="unix:///tmp/rgw-import-isolation.sock",
            )
        )
        direct = create_session(
            SessionConfig(
                user="u-import-isolation-direct",
                password="pw",
                connect_mode=ConnectMode.DIRECT,
            )
        )
        direct.disconnect()
        print("ok")
        """,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().endswith("ok")


def test_the_adapter_config_reads_gateway_env_without_the_lake() -> None:
    proc = _run(
        ["google.protobuf", "rithmic_gateway"],
        """
        from rithmic_nt_connect.config import SessionConfig

        cfg = SessionConfig.from_env(
            {
                "RITHMIC_USER": "u",
                "RITHMIC_CONNECT_MODE": "gateway",
            }
        )
        assert cfg.password is None
        print("ok")
        """,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().endswith("ok")


def test_the_lake_client_imports_without_nautilus_or_the_extension() -> None:
    proc = _run(
        ["nautilus_trader", "rithmic_nt_connect"],
        """
        from rithmic_gateway import GatewayClient, GatewayConfig

        assert GatewayClient and GatewayConfig
        print("ok")
        """,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().endswith("ok")
