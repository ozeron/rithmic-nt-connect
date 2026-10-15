"""Secret displays must be safe without changing the credentials sent on the wire."""

from __future__ import annotations

from dataclasses import asdict

from rithmic_gateway.config import GatewayConfig
from rithmic_nt_connect.config import ConnectMode, SessionConfig, explicit_test_env


def test_explicit_env_and_extracted_values_have_safe_repr(tmp_path):
    source = tmp_path / "test.env"
    source.write_text(
        "RITHMIC_USER=offline-user\n"
        "RITHMIC_PASSWORD=offline-password-193\n"
        "RITHMIC_GATEWAY_AUTH_TOKEN=offline-token-294\n"
        "RITHMIC_TEST_PW=offline-alias-395\n"
        "RITHMIC_SYSTEM_NAME=Rithmic Test\n"
        "RITHMIC_CONNECT_MODE=direct\n"
    )
    values = explicit_test_env({"RITHMIC_TEST_DOTENV": str(source)})
    for key in ("RITHMIC_PASSWORD", "RITHMIC_GATEWAY_AUTH_TOKEN", "RITHMIC_TEST_PW"):
        secret = str(values[key])
        assert secret not in repr(values)
        assert secret not in repr(dict(values))
        assert secret not in repr(values[key])
        assert values[key] == secret
    assert "offline-user" in repr(values)


def test_session_secrets_stay_hidden_in_dataclass_and_attribute_displays():
    config = SessionConfig(
        user="offline-user",
        connect_mode=ConnectMode.DIRECT,
        password="offline-password-193",
        gateway_auth_token="offline-token-294",
    )
    for shown in (repr(config), str(config), repr(asdict(config)), repr(vars(config))):
        assert "offline-password-193" not in shown
        assert "offline-token-294" not in shown
    assert config.password == "offline-password-193"
    assert config.gateway_auth_token == "offline-token-294"
    assert config.to_dict(redact=False)["password"] == "offline-password-193"


def test_gateway_token_stays_hidden_in_dataclass_and_attribute_displays():
    config = GatewayConfig(
        user="offline-user",
        system_name="Rithmic Test",
        url="wss://offline.example",
        listen="unix:///tmp/offline.sock",
        auth_token="offline-token-294",
    )
    for shown in (repr(config), str(config), repr(asdict(config)), repr(vars(config))):
        assert "offline-token-294" not in shown
    assert config.auth_token == "offline-token-294"


def test_handshake_and_frame_displays_hide_tokens_without_changing_wire():
    from rithmic_gateway.v1 import session_pb2 as pb

    handshake = pb.Handshake(user="offline-user", auth_token="offline-token-294")
    frame = pb.Frame(handshake=handshake)
    for message in (handshake, frame):
        assert "offline-token-294" not in repr(message)
        assert "offline-token-294" not in str(message)
        assert "offline-user" in repr(message)
    decoded = pb.Frame.FromString(frame.SerializeToString())
    assert decoded.handshake.auth_token == "offline-token-294"


def test_text_redaction_handles_escaping_and_overlapping_values():
    import json

    from rithmic_nt_connect._redact import redact_text

    secret = 'offline-quote-"-slash-\\-é'
    for text in (secret, repr(secret), repr(secret.encode()), json.dumps(secret)):
        assert redact_text(text, [secret]) in ("***", "'***'", '"***"', "b'***'")
    assert redact_text("long-secret short", ["long", "long-secret"]) == "*** short"
