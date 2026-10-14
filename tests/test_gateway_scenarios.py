"""Client-agnostic gateway scenarios against the real server over a fake plant.

Each scenario drives ``rithmic-gateway-fake`` (the production server code path:
handshake, gates, fan-out, event pump, reconnect + intent restore, credential
flock) through a ``make_client`` factory. The suite runs once per client
implementation (the lake's plain Python client and the Rust client), which must
pass the identical list. No Rithmic credentials are needed.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from gateway_fake import FakeParent, build_fake_gateway, running_fake
from rithmic_gateway.client import GatewayClient
from rithmic_gateway.client import GatewayError as LakeGatewayError
from rithmic_nt_connect.errors import GatewayError as ExtensionGatewayError

# The Python clients raise the lake error, the Rust client the extension's; both
# carry the wire ``code``.
GatewayError = (LakeGatewayError, ExtensionGatewayError)

NQ = "NQH6"
EXCHANGE = "CME"

MakeClient = Callable[[FakeParent], Any]


def _python_plain(parent: FakeParent) -> Any:
    client = GatewayClient(parent.config(), rpc_timeout_sec=10.0)
    client.connect()
    return client


def _rust(parent: FakeParent) -> Any:
    from rithmic_nt_connect._lib import Session

    cfg = parent.config()
    client = Session(
        user=cfg.user,
        system_name=cfg.system_name,
        url=cfg.url,
        env=cfg.env,
        connect_mode="gateway",
        gateway_listen=cfg.listen,
    )
    client.connect()
    return client


CLIENTS: dict[str, MakeClient] = {
    "python-plain": _python_plain,
    "rust": _rust,
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
    factory = ClientFactory(request.param)
    yield factory
    factory.close()


def reattach(make_client: ClientFactory, client: Any) -> None:
    """Recover a client after the parent drops its connection (adapter-owned replay).

    On a plant reconnect the parent bumps its transport epoch and disconnects
    clients so they re-handshake; the plain client re-dials itself, the Rust
    session re-dials through ``ensure_transport_live``.
    """
    if make_client.kind == "python-plain":
        client.disconnect()
        client.connect()
    else:
        # The Rust session re-dials through ensure_transport_live.
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

        # The parent restores the intents itself and clients stay attached; the
        # re-handshake below is idempotent (refcounted intent).
        reattach(make_client, client)
        client.subscribe(NQ, EXCHANGE)
        client.subscribe_order_updates()
        parent.push_last_trades(NQ, 1)
        parent.push("order", "accepted", "basket-9", "tag-9", NQ)
        assert len(drain(client.poll_event, 1)) == 1
        orders = drain(client.poll_order_event, 1)
        assert orders and orders[0].get("basket_id") == "basket-9"


def test_plant_drop_keeps_clients_attached_and_signals_resumed(
    fake_bin, make_client
) -> None:
    """The parent restores intents; a client sees a typed resume, not a teardown.

    No re-subscribe or re-attach is needed: after the drop the next tick arrives
    on the same connection and the venue saw exactly one subscribe for the intent.
    """
    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        parent.clear_calls()

        parent.drop_plants()
        assert wait_for(lambda: parent.count(f"subscribe {NQ} {EXCHANGE}") >= 1), (
            parent.calls()
        )
        assert wait_for(lambda: parent.count("connect") >= 1)

        parent.push_last_trades(NQ, 1)
        events: list[Any] = []
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not any(
            e.get("type") == "last_trade" for e in events
        ):
            ev = client.poll_event(timeout_ms=100)
            if ev is not None:
                events.append(ev)
        types = [e.get("type") for e in events]
        assert "last_trade" in types, types
        assert parent.count(f"subscribe {NQ} {EXCHANGE}") == 1, parent.calls()


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
        reset = getattr(client, "reset_ticker_plant", None) or client.reset_ticker
        reset()
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
        pytest.skip(
            "the Rust client shares one socket; overflow isolation is per-socket"
        )
    with running_fake(fake_bin) as parent:
        slow, fast = make_client(parent), make_client(parent)
        slow.subscribe(NQ, EXCHANGE)
        fast.subscribe(NQ, EXCHANGE)

        # Enough frames to overrun the slow client's kernel socket buffer.
        parent.push_last_trades(NQ, 200_000)
        received = 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and received < 1_000:
            if fast.poll_event(timeout_ms=100) is not None:
                received += 1
        assert received >= 1_000, "the fast peer keeps receiving"

        # The parent absorbs a burst (gaps, not drops) and cuts a client only when
        # its socket stops draining past the writer timeout (5 s). Probing the slow
        # client earlier would itself read its socket, so leave it alone first.
        # The healthy peer keeps draining throughout, so only the slow one stalls.
        quiet_until = time.monotonic() + 7
        while time.monotonic() < quiet_until:
            fast.poll_event(timeout_ms=50)

        def slow_is_gone() -> bool:
            try:
                slow.get_live_md_state()
            except (*GatewayError, OSError):
                return True
            return False

        assert wait_for(slow_is_gone, 30), "the parent drops a client that never reads"
        assert fast.get_live_md_state()["ticker_intents"] >= 1


# -- parent restart / transport recovery --------------------------------------


def test_rust_client_recovers_after_the_parent_restarts(fake_bin, make_client) -> None:
    if make_client.kind == "python-plain":
        pytest.skip("transport recovery is a Rust-session behavior")
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


# -- a plant lag is a typed gap, never silence ----------------------------------


def test_a_plant_lag_is_reported_in_order_not_swallowed(fake_bin, make_client) -> None:
    from rithmic_nt_connect.errors import ChannelLaggedError

    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        parent.push_last_trades(NQ, 1)
        parent.push_gap("ticker", 41)
        parent.push_last_trades(NQ, 1)

        assert len(drain(client.poll_event, 1)) == 1
        if make_client.kind == "python-plain":
            # The lake client delivers the gap in-band, in order, as a typed event.
            gap = drain(client.poll_event, 1)[0]
            assert gap["type"] == "gap" and gap["stream"] == "ticker"
            assert int(gap["skipped"]) == 41
        else:
            # The adapter's session reports it as ChannelLaggedError so the
            # existing resync path runs; the next poll continues in order.
            with pytest.raises(ChannelLaggedError):
                for _ in range(100):
                    client.poll_event(timeout_ms=100)
        assert len(drain(client.poll_event, 1)) == 1


def test_next_events_returns_a_batch_with_the_gap_in_order(
    fake_bin, make_client
) -> None:
    if make_client.kind == "python-plain":
        pytest.skip("batch retrieval is an adapter-session API")
    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        client.subscribe(NQ, EXCHANGE)
        parent.push_last_trades(NQ, 2)
        parent.push_gap("ticker", 9)
        parent.push_last_trades(NQ, 1)
        got: list[dict[str, Any]] = []
        deadline = time.monotonic() + 10
        while len(got) < 4 and time.monotonic() < deadline:
            got.extend(client.next_events("ticker", 64, 200))
        assert [e["type"] for e in got] == [
            "last_trade",
            "last_trade",
            "gap",
            "last_trade",
        ]
        assert got[2]["stream"] == "ticker" and got[2]["skipped"] == 9


def test_next_events_rejects_an_unknown_stream(fake_bin, make_client) -> None:
    if make_client.kind == "python-plain":
        pytest.skip("batch retrieval is an adapter-session API")
    with running_fake(fake_bin) as parent:
        client = make_client(parent)
        with pytest.raises(ValueError, match="unknown stream"):
            client.next_events("quotes", 8, 10)


# -- failure classes survive the wire in every client ----------------------------


@pytest.mark.parametrize(
    ("kind", "plain_code", "rust_code"),
    [
        ("rejected", "venue_rejected", "venue_rejected"),
        ("not_sent", "not_sent", "not_sent"),
        # The plain lake client reports the wire code as is; the adapter's session
        # reports the same outcome under the extension's name for it.
        ("unknown", "venue_unknown", "outcome_unknown"),
    ],
)
def test_a_failed_place_carries_its_class(
    fake_bin, make_client, kind: str, plain_code: str, rust_code: str
) -> None:
    with running_fake(fake_bin, trading=True) as parent:
        client = make_client(parent)
        parent.fail_next("place_order", kind, "boom")
        with pytest.raises(GatewayError) as err:
            client.place_order(
                NQ, EXCHANGE, "BUY", "LIMIT", 1, user_tag="tag-1", price=1.0
            )
        want = plain_code if make_client.kind == "python-plain" else rust_code
        assert err.value.code == want
