"""Live proof of the gateway path through a real parent (Rithmic Test only).

Mechanism assertions only, no orders: the order-plant checks are read-only
(subscribe to updates, drain the working-order set), and the recovery check
restarts the *parent* process. Runs against both gateway clients.

Needs ``RITHMIC_TEST_DOTENV`` (a test/demo system; production is refused). The
parent needs trading enabled for the order-plant reads (``RITHMIC_ENABLE_TRADING=1``;
it gates the order-plant login only: nothing here places or cancels an order).

Usage:
  RITHMIC_TEST_DOTENV=/secure/test.env RITHMIC_ENABLE_TRADING=1 \\
  RITHMIC_HISTORY_READY_PROBE=0 \\
      uv run pytest tests/e2e/test_gateway_live.py -v
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator
from typing import Any, Literal, Protocol, cast

import pytest
from _gateway_parent import gateway_config_from_session, start_parent, stop_parent
from conftest import parent_trading_env
from rithmic_gateway import GatewayClient
from rithmic_gateway.attach import config_flock_held
from rithmic_gateway.config import GatewayConfig
from rithmic_nt_connect.config import (
    ConnectMode,
    SessionConfig,
    session_config_from_explicit_test_env,
)
from rithmic_nt_connect.errors import GatewayError as ExtensionGatewayError
from rithmic_nt_connect.session import create_rust_gateway_session
from rithmic_nt_connect.transport import TransportUp

pytestmark = pytest.mark.live


class _OrderPlantClient(Protocol):
    """The surface both gateway clients share for the read-only order-plant check."""

    @property
    def trading_enabled(self) -> bool: ...
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def subscribe_order_updates(self) -> None: ...
    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]: ...


class _RustRecoveryClient(Protocol):
    """The Rust client's transport hooks (methods, unlike the Python client)."""

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def transport_generation(self) -> int: ...
    def transport_connected(self) -> bool: ...
    def ensure_transport_live(self) -> int: ...
    def subscribe_transport_events(
        self, callback: Callable[[object], None]
    ) -> object: ...


ClientKind = Literal["python", "rust"]


def _gateway_session_config() -> SessionConfig:
    cfg = session_config_from_explicit_test_env()
    # Whatever the env file says, this suite is about the gateway path.
    cfg.connect_mode = ConnectMode.GATEWAY
    return cfg


ParentCtx = tuple[SessionConfig, GatewayConfig, str, dict[str, str]]


@pytest.fixture
def parent_ctx(test_env, require_test_plant, credentials_available) -> ParentCtx:
    """(session config, gateway config, password, trading env) for a test plant."""
    if not credentials_available:
        pytest.skip("RITHMIC_TEST_DOTENV with test credentials is not set")
    session_cfg = _gateway_session_config()
    return (
        session_cfg,
        gateway_config_from_session(session_cfg),
        str((test_env or {}).get("RITHMIC_PASSWORD") or ""),
        parent_trading_env(test_env),
    )


@contextlib.contextmanager
def _client(
    kind: ClientKind, session_cfg: SessionConfig, gcfg: GatewayConfig
) -> Iterator[_OrderPlantClient]:
    client: _OrderPlantClient = (
        cast(_OrderPlantClient, create_rust_gateway_session(session_cfg))
        if kind == "rust"
        else GatewayClient(gcfg)
    )
    client.connect()
    try:
        yield client
    finally:
        with contextlib.suppress(Exception):
            client.disconnect()


@pytest.mark.parametrize("client_kind", ["python", "rust"])
def test_order_plant_reads_complete_through_the_parent(
    client_kind: ClientKind, parent_ctx: ParentCtx
) -> None:
    session_cfg, gcfg, password, trading_env = parent_ctx
    if not trading_env:
        pytest.skip("order-plant reads need RITHMIC_ENABLE_TRADING=1 on the parent")
    if config_flock_held(gcfg):
        # The session fixture's parent already serves this fingerprint.
        proc = None
    else:
        proc = start_parent(gcfg, password=password, extra_env=trading_env)
    try:
        with _client(client_kind, session_cfg, gcfg) as client:
            assert client.trading_enabled
            # Read-only: order notifications for this account, then the working set.
            client.subscribe_order_updates()
            orders = client.load_orders(0, 0)
            # Data or empty, never a hang; rows are typed dicts when present.
            assert isinstance(orders, list)
            for row in orders:
                assert isinstance(row, dict)
    finally:
        if proc is not None:
            stop_parent(proc)


def test_the_rust_client_recovers_after_the_parent_restarts(
    parent_ctx: ParentCtx,
) -> None:
    session_cfg, gcfg, password, trading_env = parent_ctx
    if config_flock_held(gcfg):
        pytest.skip("this test restarts the parent, so it needs to own it")
    proc = start_parent(gcfg, password=password, extra_env=trading_env)
    try:
        client = cast(_RustRecoveryClient, create_rust_gateway_session(session_cfg))
        client.connect()
        try:
            before = client.transport_generation()
            ups: list[TransportUp] = []

            def _on_event(event: object) -> None:
                if isinstance(event, TransportUp):
                    ups.append(event)

            client.subscribe_transport_events(_on_event)

            stop_parent(proc)
            proc = start_parent(gcfg, password=password, extra_env=trading_env)

            deadline = time.monotonic() + 60
            while not ups and time.monotonic() < deadline:
                time.sleep(0.2)
            assert ups, "recovery never reported the transport live again"
            assert client.ensure_transport_live() > before
            assert client.transport_connected()
        finally:
            with contextlib.suppress(Exception):
                client.disconnect()
    finally:
        stop_parent(proc)


def test_a_missing_parent_is_a_typed_attach_error_not_a_hang(
    parent_ctx: ParentCtx,
) -> None:
    session_cfg, gcfg, _password, _trading = parent_ctx
    if config_flock_held(gcfg):
        pytest.skip("a parent is running for this fingerprint")
    client = create_rust_gateway_session(session_cfg)
    started = time.monotonic()
    with pytest.raises(ExtensionGatewayError) as err:
        client.connect()
    assert err.value.code == "parent_not_running"
    assert time.monotonic() - started < 10
