"""Client-agnostic gateway scenarios against the real server over a fake plant.

Each scenario drives ``rithmic-gateway-fake`` (the production server code path:
handshake, gates, fan-out, event pump, reconnect + intent restore, credential
flock) through a ``make_client`` factory. The suite runs once per client
implementation, so the Python clients are pinned today and the Rust client must
pass the identical list. No Rithmic credentials are needed.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from gateway_fake import FakeParent, build_fake_gateway, running_fake
from rithmic_gateway.client import GatewayClient, GatewayError
from rithmic_gateway.runtime import GatewayRuntimeRegistry, create_gateway_client

NQ = "NQH6"
EXCHANGE = "CME"

MakeClient = Callable[[FakeParent], Any]


def _python_plain(parent: FakeParent) -> Any:
    client = GatewayClient(parent.config(), rpc_timeout_sec=10.0)
    client.connect()
    return client


def _python_mux(parent: FakeParent) -> Any:
    client = create_gateway_client(parent.config(), rpc_timeout_sec=10.0)
    client.connect()
    return client


CLIENTS: dict[str, MakeClient] = {
    "python-plain": _python_plain,
    "python-mux": _python_mux,
}


@pytest.fixture(scope="module")
def fake_bin():
    binary = build_fake_gateway()
    if binary is None:
        pytest.skip("rithmic-gateway-fake unavailable (no cargo / build failed)")
    return binary


class ClientFactory:
    """Creates clients of one implementation and disconnects them at teardown."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.made: list[Any] = []

    def __call__(self, parent: FakeParent) -> Any:
        client = CLIENTS[self.kind](parent)
        self.made.append(client)
        return client

    def close(self) -> None:
        for client in self.made:
            with contextlib.suppress(Exception):
                client.disconnect()


@pytest.fixture(params=sorted(CLIENTS))
def make_client(request: pytest.FixtureRequest) -> Iterator[ClientFactory]:
    GatewayRuntimeRegistry.reset_for_tests()
    factory = ClientFactory(request.param)
    yield factory
    factory.close()
    GatewayRuntimeRegistry.reset_for_tests()


def reattach(make_client: ClientFactory, client: Any) -> None:
    """Recover a client after the parent drops its connection (adapter-owned replay).

    On a plant reconnect the parent bumps its transport epoch and disconnects
    clients so they re-handshake; the plain client re-dials itself, the mux
    runtime re-dials through ``ensure_transport_live``.
    """
    if make_client.kind == "python-plain":
        client.disconnect()
        client.connect()
    else:
        client.ensure_transport_live()


def drain(poll: Callable[..., Any], want: int, timeout_sec: float = 10.0) -> list[Any]:
    """Collect up to ``want`` events from ``poll(timeout_ms=...)``."""
    got: list[Any] = []
    deadline = time.monotonic() + timeout_sec
    while len(got) < want and time.monotonic() < deadline:
        event = poll(timeout_ms=100)
        if event is not None:
            got.append(event)
    return got


def wait_for(cond: Callable[[], bool], timeout_sec: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return cond()


# -- subscribe + fan-out + refcount -------------------------------------------


def test_two_clients_share_one_venue_subscription(fake_bin, make_client) -> None:
    with running_fake(fake_bin) as parent:
        a, b = make_client(parent), make_client(parent)
        a.subscribe(NQ, EXCHANGE)
        b.subscribe(NQ, EXCHANGE)
        assert parent.count(f"subscribe {NQ} {EXCHANGE}") == 1, parent.calls()

        parent.push_last_trades(NQ, 3)
        for client in (a, b):
            events = drain(client.poll_event, 3)
            assert len(events) == 3
            assert {e["symbol"] for e in events} == {NQ}

        if make_client.kind == "python-plain":
            a.unsubscribe(NQ, EXCHANGE)
            assert parent.count("unsubscribe") == 0, "peer still subscribed"
        # Mux facades share ONE parent connection, so the parent cannot refcount
        # them: the first facade's unsubscribe leaves the venue topic (recorded
        # in rust-unified-session design, "Current-behavior findings").
        b.unsubscribe(NQ, EXCHANGE)
        assert parent.count(f"unsubscribe {NQ} {EXCHANGE}") >= 1, parent.calls()


def test_disconnect_leaves_the_venue_when_the_last_peer_goes(
    fake_bin, make_client
) -> None:
    with running_fake(fake_bin) as parent:
        a = make_client(parent)
        a.subscribe(NQ, EXCHANGE)
        a.disconnect()
        assert wait_for(lambda: parent.count(f"unsubscribe {NQ} {EXCHANGE}") == 1), (
            parent.calls()
        )
        assert parent.proc is not None and parent.proc.poll() is None, (
            "parent keeps running"
        )


# -- trading gate parity ------------------------------------------------------


def test_trading_off_denies_every_gated_rpc_without_reaching_the_plant(
    fake_bin, make_client
) -> None:
    with running_fake(fake_bin, trading=False) as parent:
        client = make_client(parent)
        assert client.trading_enabled is False
        gated = [
            lambda: client.place_order(NQ, EXCHANGE, "BUY", "LIMIT", 1, user_tag="t"),
            lambda: client.cancel_order("basket-1"),
            lambda: client.subscribe_order_updates(),
            lambda: client.subscribe_bracket_updates(),
            lambda: client.load_orders(0, 1),
            lambda: client.load_product_rms_info(),
            lambda: client.load_account_rms_info(),
        ]
        for call in gated:
            with pytest.raises(GatewayError) as err:
                call()
            assert err.value.code == "trading_disabled"
        assert parent.count("place_order") == 0
        assert parent.count("cancel_order") == 0
        assert parent.count("subscribe_order_updates") == 0


def test_trading_on_reaches_the_plant(fake_bin, make_client) -> None:
    with running_fake(fake_bin, trading=True) as parent:
        client = make_client(parent)
        assert client.trading_enabled is True
        client.place_order(NQ, EXCHANGE, "BUY", "LIMIT", 1, user_tag="tag-1", price=1.0)
        client.cancel_order("basket-1")
        assert "place_order tag-1" in parent.calls()
        assert "cancel_order basket-1" in parent.calls()


def test_cancel_all_has_its_own_gate_independent_of_trading(
    fake_bin, make_client
) -> None:
    with running_fake(fake_bin, trading=False, cancel_all=True) as parent:
        client = make_client(parent)
        client.cancel_all_orders()
        assert parent.count("cancel_all_orders") == 1
        with pytest.raises(GatewayError) as err:
            client.place_order(NQ, EXCHANGE, "BUY", "LIMIT", 1, user_tag="t")
        assert err.value.code == "trading_disabled"

    with running_fake(fake_bin, trading=True, cancel_all=False) as parent:
        client = make_client(parent)
        with pytest.raises(GatewayError) as err:
            client.cancel_all_orders()
        assert err.value.code == "cancel_all_denied"
        assert parent.count("cancel_all_orders") == 0


# -- restore after plant drop -------------------------------------------------


def test_every_intent_is_reissued_after_a_plant_drop(fake_bin, make_client) -> None:
    """Ticker, book, time bars, PnL, order and bracket intents all survive."""
    with running_fake(fake_bin, trading=True) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        client.subscribe_order_book_summary(NQ, EXCHANGE)
        client.subscribe_time_bars(NQ, EXCHANGE, 2, 1)
        client.subscribe_pnl()
        client.subscribe_order_updates()
        client.subscribe_bracket_updates()
        parent.clear_calls()

        parent.drop_plants()
        assert wait_for(lambda: parent.count("subscribe_bracket_updates") >= 1), (
            parent.calls()
        )

        calls = parent.calls()
        for expected in (
            "connect",
            f"subscribe {NQ} {EXCHANGE}",
            f"subscribe_book {NQ} {EXCHANGE}",
            f"subscribe_time_bars {NQ} {EXCHANGE} 2 1",
            "subscribe_pnl",
            "subscribe_order_updates",
            "subscribe_bracket_updates",
        ):
            assert expected in calls, (expected, calls)

        # The parent then drops its clients (transport epoch bump) and their
        # teardown leaves the venue topics; the adapter re-attaches and replays.
        reattach(make_client, client)
        client.subscribe(NQ, EXCHANGE)
        client.subscribe_order_updates()
        parent.push_last_trades(NQ, 1)
        parent.push("order", "accepted", "basket-9", "tag-9", NQ)
        assert len(drain(client.poll_event, 1)) == 1
        orders = drain(client.poll_order_event, 1)
        assert orders and orders[0].get("basket_id") == "basket-9"


def test_one_shot_commands_register_no_intent(fake_bin, make_client) -> None:
    with running_fake(fake_bin, trading=True) as parent:
        client = make_client(parent)
        client.place_order(NQ, EXCHANGE, "BUY", "LIMIT", 1, user_tag="tag-1", price=1.0)
        client.cancel_order("basket-1")
        parent.clear_calls()
        parent.drop_plants()
        assert wait_for(lambda: parent.count("connect") >= 1), parent.calls()
        time.sleep(0.5)  # let any (wrongly) restored intents show up
        calls = parent.calls()
        assert not [c for c in calls if c.startswith(("place_order", "cancel_order"))]
        assert not [c for c in calls if c.startswith("subscribe")], calls


# -- ticker reset -------------------------------------------------------------


def test_ticker_reset_reissues_ticker_intents(fake_bin, make_client) -> None:
    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        parent.clear_calls()
        client.reset_ticker_plant()
        calls = parent.calls()
        assert calls[0] == "reset_ticker", calls
        assert f"subscribe {NQ} {EXCHANGE}" in calls, calls


# -- history refused while live ticker is active ------------------------------


def test_history_is_refused_under_a_live_ticker_intent(fake_bin, make_client) -> None:
    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        with pytest.raises(GatewayError) as err:
            client.load_ticks(NQ, EXCHANGE, 0, 10)
        assert err.value.code in {"capability_denied", "history_denied_live_md"}
        assert parent.count("load_ticks") == 0, "refused before reaching the plant"

        client.unsubscribe(NQ, EXCHANGE)
        assert client.load_ticks(NQ, EXCHANGE, 0, 10) == []
        assert parent.count("load_ticks") == 1


# -- slow client overflow -----------------------------------------------------


def test_a_slow_client_is_disconnected_without_hurting_its_peer(
    fake_bin, make_client
) -> None:
    if make_client.kind != "python-plain":
        pytest.skip("mux clients share one socket; overflow isolation is per-socket")
    with running_fake(fake_bin) as parent:
        slow, fast = make_client(parent), make_client(parent)
        slow.subscribe(NQ, EXCHANGE)
        fast.subscribe(NQ, EXCHANGE)

        parent.push_last_trades(NQ, 20_000)
        received = 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and received < 1_000:
            if fast.poll_event(timeout_ms=100) is not None:
                received += 1
        assert received >= 1_000, "the fast peer keeps receiving"

        def slow_is_gone() -> bool:
            try:
                slow.get_live_md_state()
            except (GatewayError, OSError):
                return True
            return False

        assert wait_for(slow_is_gone, 30), "the parent drops a client that never reads"
        assert fast.get_live_md_state()["ticker_intents"] >= 1


# -- parent restart / transport recovery --------------------------------------


def test_mux_client_recovers_after_the_parent_restarts(fake_bin, make_client) -> None:
    if make_client.kind != "python-mux":
        pytest.skip("transport recovery is a mux-runtime behavior")
    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        before = client.ensure_transport_live()

        parent.restart()
        after = client.ensure_transport_live()
        assert after > before, "transport generation increases after a re-dial"

        client.subscribe(NQ, EXCHANGE)  # the adapter re-issues intent after UP
        parent.push_last_trades(NQ, 1)
        assert len(drain(client.poll_event, 1)) == 1
