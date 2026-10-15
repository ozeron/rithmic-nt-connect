"""Exercise pytest's actual terminal and JUnit outputs with synthetic secrets."""

from __future__ import annotations

import os
import subprocess  # nosec B404 - runs an offline pytest regression suite
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "password,token",
    [
        ("offline-password-193", "offline-token-294"),
        ('offline-pass-\\-"-é-193', 'offline-token-\\-"-é-294'),
        ("offline-password-193", "offline-token-294-" + "long" * 100),
    ],
    ids=["plain", "escaped", "long"],
)
def test_pytest_scrubs_setup_call_teardown_ids_locals_and_captured_output(
    tmp_path, password, token
):
    root = Path(__file__).resolve().parents[1]
    # A standalone suite: never import the live e2e fixtures or open plants.
    source = tmp_path / "test.env"
    source.write_text(
        f"RITHMIC_PASSWORD={password}\nRITHMIC_GATEWAY_AUTH_TOKEN={token}\n"
    )
    (tmp_path / "test_failure.py").write_text("""
import logging
import os
import pytest

# Read fake values directly, including plain strings which repr wrappers cannot protect.
PASSWORD = os.environ["FAKE_PASSWORD"]
TOKEN = os.environ["RITHMIC_GATEWAY_AUTH_TOKEN"]

@pytest.fixture
def broken():
    password = PASSWORD
    raise RuntimeError(password)

def test_setup(broken):
    pass

@pytest.fixture
def teardown():
    yield
    raise RuntimeError(TOKEN)

@pytest.mark.parametrize("value", [TOKEN], ids=[TOKEN])
def test_call(value, teardown):
    password = PASSWORD
    print(password)
    logging.warning(value)
    assert password == "different"

@pytest.mark.parametrize("value", [PASSWORD])
def test_auto_id(value):
    assert value == "different"
""")
    env = {
        **os.environ,
        "PYTHONPATH": str(root / "python"),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "RITHMIC_TEST_DOTENV": str(source),
        "FAKE_PASSWORD": password,
        "RITHMIC_GATEWAY_AUTH_TOKEN": token,
    }
    result = subprocess.run(  # nosec B603 - fixed argv, synthetic tests only
        [
            sys.executable,
            "-m",
            "pytest",
            "-vv",
            "--showlocals",
            "--tb=long",
            "--log-cli-level=WARNING",
            "--junitxml=report.xml",
            "-c",
            str(root / "pyproject.toml"),
            "--rootdir",
            str(tmp_path),
            str(tmp_path / "test_failure.py"),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    for output in (result.stdout, result.stderr, (tmp_path / "report.xml").read_text()):
        assert password not in output
        assert token not in output
        assert token[:24] not in output
    assert "***" in result.stdout
    assert "2 failed, 2 errors" in result.stdout
