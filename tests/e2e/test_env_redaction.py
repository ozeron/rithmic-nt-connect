"""The live test env fixture must not print secrets in tracebacks."""

from __future__ import annotations

from rithmic_nt_connect._redact import RedactedEnv


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


def test_password_aliases_and_parent_fixture_tuple_keep_safe_repr() -> None:
    env = RedactedEnv({"RITHMIC_TEST_PW": "offline-alias-395"})
    password = env.get("RITHMIC_TEST_PW")
    # Gateway fixtures return the password inside a tuple; copying the env or
    # extracting a value must preserve its safe repr without changing its value.
    assert "offline-alias-395" not in repr((dict(env), password))
    assert password == "offline-alias-395"
