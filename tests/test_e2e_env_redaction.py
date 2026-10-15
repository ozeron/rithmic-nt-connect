"""The live test env fixture must not print secrets in tracebacks."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "e2e"))

from conftest import RedactedEnv


def test_repr_hides_password_and_tokens() -> None:
    env = RedactedEnv(
        {
            "RITHMIC_USER": "someone@example.com",
            "RITHMIC_PASSWORD": "hunter2",
            "RITHMIC_GATEWAY_AUTH_TOKEN": "tok",
        }
    )
    shown = repr(env) + str(env)
    assert "hunter2" not in shown
    assert "tok'" not in shown
    assert "someone@example.com" in shown
    assert env["RITHMIC_PASSWORD"] == "hunter2"
