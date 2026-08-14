//! Framing + Handshake round-trip tests (U1 / AE8 / AE9).

use prost::Message;
use rithmic_gateway::framing::{decode_frame, encode_frame, FrameError, MAX_FRAME_LEN};
use rithmic_gateway::pb::{frame::Body, Frame, Handshake, Ready};

#[test]
fn handshake_round_trip_with_empty_auth_token() {
    let hs = Handshake {
        user: "u".into(),
        system_name: "LucidTrading".into(),
        url: "wss://rprotocol.rithmic.com:443".into(),
        env: "Live".into(),
        account_id: String::new(),
        fcm_id: String::new(),
        ib_id: String::new(),
        auth_token: String::new(),
    };
    let frame = Frame {
        request_id: 1,
        body: Some(Body::Handshake(hs.clone())),
    };
    let payload = frame.encode_to_vec();
    let wire = encode_frame(&payload).expect("encode");
    let (decoded_payload, _) = decode_frame(&wire).expect("decode frame");
    let decoded = Frame::decode(decoded_payload).expect("decode proto");
    match decoded.body {
        Some(Body::Handshake(got)) => {
            assert_eq!(got.user, "u");
            assert_eq!(got.auth_token, "");
            assert_eq!(got.system_name, hs.system_name);
        }
        other => panic!("expected Handshake, got {other:?}"),
    }
}

#[test]
fn ready_advertises_scopes() {
    let ready = Ready {
        scopes: vec!["md".into(), "history".into()],
        trading_enabled: false,
        cancel_all_enabled: false,
    };
    let frame = Frame {
        request_id: 0,
        body: Some(Body::Ready(ready)),
    };
    let wire = encode_frame(&frame.encode_to_vec()).unwrap();
    let (payload, _) = decode_frame(&wire).unwrap();
    let decoded = Frame::decode(payload).unwrap();
    match decoded.body {
        Some(Body::Ready(r)) => {
            assert_eq!(r.scopes, vec!["md", "history"]);
            assert!(!r.trading_enabled);
        }
        other => panic!("expected Ready, got {other:?}"),
    }
}

#[test]
fn truncated_length_prefix_errors() {
    let err = decode_frame(&[0, 0]).unwrap_err();
    assert_eq!(err, FrameError::TruncatedLengthPrefix);
}

#[test]
fn frame_too_large_rejected() {
    let mut prefix = (MAX_FRAME_LEN + 1).to_be_bytes().to_vec();
    prefix.extend_from_slice(&[0u8; 8]);
    let err = decode_frame(&prefix).unwrap_err();
    assert!(matches!(err, FrameError::FrameTooLarge(_)));
}

#[test]
fn proto_schema_has_auth_token_not_password_field() {
    let proto = include_str!("../../../proto/rithmic_gateway/v1/session.proto");
    assert!(
        proto.contains("auth_token"),
        "Handshake must declare auth_token for remote-ready auth"
    );
    for line in proto.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }
        assert!(
            !trimmed.contains("password"),
            "non-comment proto line must not declare password: {trimmed}"
        );
    }
}

#[test]
fn proto_schema_has_bracket_rpcs_parity_with_plants() {
    let proto = include_str!("../../../proto/rithmic_gateway/v1/session.proto");
    for needle in [
        "SubscribeBracketUpdatesRequest",
        "PlaceBracketOrderRequest",
        "AdjustBracketStopRequest",
        "AdjustBracketTargetRequest",
        "place_bracket_order",
        "subscribe_bracket_updates",
        "adjust_bracket_stop",
        "adjust_bracket_target",
    ] {
        assert!(
            proto.contains(needle),
            "gateway proto must keep direct/gateway parity for {needle}"
        );
    }
}
