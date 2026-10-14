"""Python ``WireSession`` ↔ Rust ``Session`` trait parity.

The Rust ``Session`` trait is the single definition of the plant surface. The
extension stub (``_lib.pyi``) and the Python ``WireSession`` protocol must expose
exactly its methods, so adding a plant RPC in Rust without surfacing it here (or
vice versa) fails this test. Fill/order-history reads are ordinary trait
methods: both modes serve them.
"""

from __future__ import annotations

import ast
import typing
from pathlib import Path

from rithmic_nt_connect.session import WireSession

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND = REPO_ROOT / "crates" / "rithmic-plants" / "src" / "backend.rs"
PYI = REPO_ROOT / "python" / "rithmic_nt_connect" / "_lib.pyi"


def _trait_methods(source: str, trait: str, until: str | None) -> set[str]:
    body = source.split(f"pub trait {trait}")[1]
    if until is not None:
        body = body.split(f"pub trait {until}")[0]
    return {
        line.strip().removeprefix("async fn ").split("(")[0]
        for line in body.splitlines()
        if line.strip().startswith("async fn ")
    }


def _session_trait() -> set[str]:
    return _trait_methods(BACKEND.read_text(), "Session", until=None)


def _extension_methods() -> set[str]:
    tree = ast.parse(PYI.read_text())
    cls = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Session"
    )
    return {
        n.name
        for n in cls.body
        if isinstance(n, ast.FunctionDef) and not n.name.startswith("__")
    }


# Gateway-mode hooks the extension adds on top of the trait; inert in direct mode.
GATEWAY_HOOKS = {
    "trading_enabled",
    "cancel_all_enabled",
    "transport_generation",
    "transport_connected",
    "ensure_transport_live",
    "subscribe_transport_events",
    # Direct-mode gate surface (the Rust session enforces the gates in both modes;
    # only direct can be configured, a gateway client raises).
    "is_direct",
    "configure_gates",
}


def _wire_methods() -> set[str]:
    """Members of the ``WireSession`` protocol (3.12-compatible)."""
    members = getattr(typing, "get_protocol_members", None)
    if members is not None:
        return set(members(WireSession))
    return set(getattr(WireSession, "__protocol_attrs__"))  # noqa: B009


def test_the_traits_were_parsed() -> None:
    assert len(_session_trait()) > 30
    assert {
        "load_fill_history",
        "load_order_history",
        "load_order_history_dates",
    } <= _session_trait()


def test_extension_stub_exposes_exactly_the_trait_surface() -> None:
    expected = _session_trait() | GATEWAY_HOOKS
    actual = _extension_methods()
    assert actual - expected == set(), "extension methods with no trait method"
    assert expected - actual == set(), "trait methods the extension does not expose"


def test_wire_session_protocol_is_exactly_the_session_trait() -> None:
    wire = _wire_methods()
    trait = _session_trait()
    assert wire - trait == set(), "WireSession methods with no Session trait method"
    assert trait - wire == set(), "Session trait methods missing from WireSession"
