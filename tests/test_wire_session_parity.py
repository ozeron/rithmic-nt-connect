"""Every ``WireSession`` adapter must expose the same surface as the protocol.

Direct (PyO3) and gateway sessions are two adapters of one seam, so a plant RPC
added to one path and not the other (AGENTS.md "direct <-> gateway parity")
must fail here, not in a live session. Adapters checked: the in-process
facade, the gateway facade over a real ``GatewayClient``, the test double, and
the compiled extension (through its hand-written ``_lib.pyi`` stub, which is in
turn checked against the extension itself).

Compatibility rule (callers pass required arguments positionally): an adapter
may not require more positional arguments than the protocol, must accept at
least as many, and every argument the protocol defaults must exist at the same
position with the same name and a default. Required-argument *names* may
differ per adapter (``start_ssboe`` vs ``start_time_sec``).
"""

from __future__ import annotations

import ast
import inspect
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import rithmic_nt_connect
from _stubs import WireSessionStub
from rithmic_gateway.client import GatewayClient
from rithmic_gateway.config import GatewayConfig
from rithmic_nt_connect.gateway_wire import GatewayWireSession
from rithmic_nt_connect.session import (
    TransportAware,
    WireSession,
    _FlockedDirectSession,
)


def _protocol_members(protocol: type) -> list[str]:
    """Public methods declared by a ``Protocol`` and its protocol bases."""
    members: set[str] = set()
    for klass in protocol.__mro__:
        if klass.__module__ in ("typing", "builtins"):
            continue
        members |= {
            name
            for name, value in vars(klass).items()
            if inspect.isfunction(value) and not name.startswith("_")
        }
    return sorted(members)


PROTOCOL_MEMBERS = _protocol_members(WireSession)
TRANSPORT_MEMBERS = _protocol_members(TransportAware)
PYI = Path(rithmic_nt_connect.__file__).parent / "_lib.pyi"


@dataclass(frozen=True)
class Param:
    name: str
    has_default: bool
    variadic: bool = False


def _params_of(fn: Callable[..., Any]) -> list[Param]:
    out: list[Param] = []
    for p in inspect.signature(fn).parameters.values():
        if p.name == "self":
            continue
        out.append(
            Param(
                p.name,
                p.default is not inspect.Parameter.empty,
                p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD),
            )
        )
    return out


def _incompatibilities(proto: list[Param], adapter: list[Param]) -> list[str]:
    if any(p.variadic for p in adapter):
        return []  # permissive double (``*args, **kwargs``)
    problems: list[str] = []
    required_proto = [p for p in proto if not p.has_default]
    required_adapter = [p for p in adapter if not p.has_default]
    if len(required_adapter) > len(required_proto):
        problems.append(
            f"requires {len(required_adapter)} arguments, "
            f"protocol {len(required_proto)}"
        )
    if len(adapter) < len(proto):
        problems.append(f"accepts {len(adapter)} arguments, protocol {len(proto)}")
    for i, p in enumerate(proto):
        if p.has_default and not (
            i < len(adapter) and adapter[i].name == p.name and adapter[i].has_default
        ):
            problems.append(f"default argument {p.name!r} not at position {i}")
    return problems


def _check_surface(adapter: Any, label: str) -> None:
    failures: list[str] = []
    for member in PROTOCOL_MEMBERS:
        proto_fn = getattr(WireSession, member)
        try:
            fn = getattr(adapter, member)
        except AttributeError:
            failures.append(f"{member}: missing")
            continue
        for problem in _incompatibilities(_params_of(proto_fn), _params_of(fn)):
            failures.append(f"{member}: {problem}")
    assert not failures, f"{label} drifted from WireSession:\n  " + "\n  ".join(
        failures
    )


def _gateway_facade() -> GatewayWireSession:
    config = GatewayConfig(user="u", system_name="s", url="wss://x", env="Test")
    return GatewayWireSession(GatewayClient(config))


def test_protocol_is_non_trivial() -> None:
    assert len(PROTOCOL_MEMBERS) > 30
    assert {"place_order", "load_orders", "poll_event", "subscribe"} <= set(
        PROTOCOL_MEMBERS
    )


def test_direct_facade_matches_protocol() -> None:
    _check_surface(_FlockedDirectSession, "_FlockedDirectSession")


def test_gateway_facade_matches_protocol() -> None:
    _check_surface(_gateway_facade(), "GatewayWireSession")


def test_test_double_matches_protocol() -> None:
    _check_surface(WireSessionStub(), "WireSessionStub")


@pytest.mark.parametrize(
    "adapter",
    [WireSessionStub(), _gateway_facade()],
    ids=["stub", "gateway"],
)
def test_transport_aware_adapters_expose_transport_events(adapter: Any) -> None:
    for member in TRANSPORT_MEMBERS:
        assert callable(getattr(adapter, member)), member


# --------------------------------------------------------------------------- #
# The compiled extension, via its stub.
# --------------------------------------------------------------------------- #


def _pyi_session_methods() -> dict[str, list[Param]]:
    tree = ast.parse(PYI.read_text())
    session = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Session"
    )
    out: dict[str, list[Param]] = {}
    for node in session.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        args = [a.arg for a in node.args.args if a.arg != "self"]
        n_default = len(node.args.defaults)
        first_default = len(args) - n_default
        out[node.name] = [
            Param(name, i >= first_default) for i, name in enumerate(args)
        ]
    return out


@pytest.fixture(scope="module")
def rust_session() -> Any:
    try:
        from rithmic_nt_connect import _lib
    except ImportError:
        if os.environ.get("CI"):
            raise
        pytest.skip("compiled extension not built (run `uv sync` / maturin develop)")
    return _lib.Session


def test_pyi_matches_protocol() -> None:
    stub = _pyi_session_methods()
    failures: list[str] = []
    for member in PROTOCOL_MEMBERS:
        if member not in stub:
            failures.append(f"{member}: missing from _lib.pyi")
            continue
        # ``resolved_account`` etc. live on the extension; plant-level facades
        # (reset_ticker, request_plants, ...) are protocol members too.
        proto = _params_of(getattr(WireSession, member))
        for problem in _incompatibilities(proto, stub[member]):
            failures.append(f"{member}: {problem}")
    assert not failures, "_lib.pyi drifted from WireSession:\n  " + "\n  ".join(
        failures
    )


def test_pyi_matches_built_extension(rust_session: Any) -> None:
    stub = _pyi_session_methods()
    runtime = {
        name: _params_of(fn)
        for name, fn in inspect.getmembers(rust_session, inspect.isroutine)
        if not name.startswith("_")
    }
    assert set(stub) - {"__init__"} == set(runtime), (
        "undeclared or stale methods: "
        f"{sorted((set(stub) - {'__init__'}) ^ set(runtime))}"
    )
    mismatches = [
        f"{name}: pyi={[p.name for p in stub[name]]} "
        f"extension={[p.name for p in runtime[name]]}"
        for name in runtime
        if [(p.name, p.has_default) for p in stub[name]]
        != [(p.name, p.has_default) for p in runtime[name]]
    ]
    assert not mismatches, "_lib.pyi drifted from the extension:\n  " + "\n  ".join(
        mismatches
    )


def test_pyi_constructor_matches_built_extension(rust_session: Any) -> None:
    tree = ast.parse(PYI.read_text())
    session = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Session"
    )
    init = next(
        n
        for n in session.body
        if isinstance(n, ast.FunctionDef) and n.name == "__init__"
    )
    args = [a.arg for a in init.args.args if a.arg != "self"]
    first_default = len(args) - len(init.args.defaults)
    declared = [(a, i >= first_default) for i, a in enumerate(args)]
    actual = [
        (p.name, p.default is not inspect.Parameter.empty)
        for p in inspect.signature(rust_session).parameters.values()
    ]
    assert declared == actual
