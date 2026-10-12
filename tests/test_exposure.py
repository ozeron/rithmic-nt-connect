"""Venue exposure from the node's own exec session (no second session)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

from _stubs import _CacheStub, _Log, _TestClient
from nautilus_trader.model.identifiers import AccountId
from rithmic_nt_connect.errors import VenueQueryUnavailable
from rithmic_nt_connect.exposure import (
    PNL_SETTLE_NS,
    ExposureSnapshot,
    exposure_topic,
    matches_root,
    product_root,
    signed_net,
)
from rithmic_nt_connect.session import WireSession


def _row(basket: str, symbol: str, status: str = "OPEN") -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "rithmic",
        "kind": "accepted",
        "status": status,
        "basket_id": basket,
        "user_tag": f"T-{basket}",
        "symbol": symbol,
        "account_id": "ACC1",
        "quantity": 1,
        "total_fill_size": 0,
        "price": 21000.0,
        "ts_event_ns": 1_700_000_000_000_000_000,
        "transaction_type": 2,
        "price_type": 4,
        "duration": 1,
    }


def _pnl(symbol: str, net: int) -> dict[str, object]:
    return {
        "type": "instrument_pnl",
        "symbol": symbol,
        "exchange": "CME",
        "net_quantity": net,
    }


def _client(now: list[int], drain: Any) -> _TestClient:
    client = _TestClient.__new__(_TestClient)
    client._log = _Log()
    client._clock = SimpleNamespace(timestamp_ns=lambda: now[0])
    client._cache = _CacheStub()
    client.account_id = AccountId("RITHMIC-ACC1")
    client._positions = {}
    client._pnl_snapshot_observed = asyncio.Event()
    client._account_seeded = True

    async def _load(start: int, end: int) -> list[dict[str, object]]:
        if isinstance(drain, Exception):
            raise drain
        return drain

    object.__setattr__(client, "_load_orders_events", _load)
    return client


def test_product_root_and_signed_net() -> None:
    assert product_root("MNQZ6") == "MNQ"
    assert product_root("MNQ") == "MNQ"
    assert product_root("ESH27") == "ES"
    positions = {
        "a": {"symbol": "MNQZ6", "position_side": "LONG", "quantity": 2},
        "b": {"symbol": "MNQU6", "position_side": "SHORT", "quantity": 1},
        "c": {"symbol": "NQZ6", "position_side": "LONG", "quantity": 5},
    }
    assert signed_net(positions, "MNQ") == 1


def test_net_is_unknown_until_the_pnl_stream_is_live() -> None:
    now = [10]
    client = _client(now, [])
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert snap.net is None and not snap.complete and snap.reply
    assert client._msgbus.published[-1] == (exposure_topic("MNQ"), snap)


def test_complete_snapshot_counts_root_orders_and_net() -> None:
    now = [10]
    client = _client(
        now,
        [
            _row("B1", "MNQZ6"),
            _row("B2", "NQZ6"),
            _row("B3", "MNQZ6", status="CANCELLED"),
        ],
    )
    client._cache_position(_pnl("MNQZ6", -1))
    now[0] = 20  # right after the first PnL row: the burst may still be landing
    assert asyncio.run(client.request_exposure("MNQ")).complete is False
    now[0] = 10 + PNL_SETTLE_NS
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert (snap.net, snap.open_orders, snap.complete) == (-1, 1, True)


def test_an_uninterpretable_root_row_makes_the_count_unknown() -> None:
    bad = _row("B9", "MNQZ6")
    del bad["basket_id"]
    client = _client([10], [_row("B1", "MNQZ6"), bad])
    client._cache_position(_pnl("MNQZ6", 0))
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert snap.open_orders is None and not snap.complete


def test_root_matching_is_exact_never_a_prefix() -> None:
    assert matches_root("MNQZ6", "MNQ")
    assert not matches_root("MNQZ6", "MN")
    assert not matches_root("ESTX6", "ES")
    assert matches_root("ESZ6", "ES")


def test_failed_drain_is_unknown_not_zero() -> None:
    now = [10]
    client = _client(now, VenueQueryUnavailable("drain down"))
    client._cache_position(_pnl("MNQZ6", 0))
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert snap.net == 0 and snap.open_orders is None and not snap.complete


def test_reconnect_forgets_rows_from_the_previous_stream() -> None:
    now = [10]
    client = _client(now, [])
    client._cache_position(_pnl("MNQZ6", 2))
    client._forget_pnl_state()
    assert client._positions == {}
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert snap.net is None


def test_position_rows_push_a_net_only_snapshot() -> None:
    client = _client([5], [])
    client._cache_position(_pnl("MNQZ6", 1))
    topic, snap = client._msgbus.published[-1]
    assert topic == "rithmic.exposure.MNQ"
    assert isinstance(snap, ExposureSnapshot)
    assert snap.net == 1 and snap.open_orders is None and not snap.complete
    assert snap.reply is False


def test_a_malformed_row_is_not_masked_by_a_readable_sibling() -> None:
    # Parses, but no status report can be built (a bare TRIGGER row has no terms), so
    # the drain skips it — while a readable row of the same basket makes it "present".
    bad = _row("B1", "MNQZ6", status="TRIGGER_PENDING")
    bad["quantity"] = 0
    client = _client([10], [_row("B1", "MNQZ6"), bad])
    client._cache_position(_pnl("MNQZ6", 0))
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert snap.open_orders is None and not snap.complete


def test_pnl_resync_makes_exposure_unknown_until_fresh_rows_settle() -> None:
    now = [10]
    client = _client(now, [])
    client._session = cast(
        WireSession,
        SimpleNamespace(disconnect_pnl_plant=lambda: None, subscribe_pnl=lambda: None),
    )
    client._cache_position(_pnl("MNQZ6", 2))
    client._cache_position(_pnl("ESZ6", 1))
    now[0] = 10 + PNL_SETTLE_NS
    assert asyncio.run(client.request_exposure("MNQ")).net == 2
    asyncio.run(client._resync_pnl_subscription())  # the stream failed mid-session
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert snap.net is None and not snap.complete  # stale net is never reported
    now[0] = 20 + PNL_SETTLE_NS
    client._cache_position(_pnl("ESZ6", 1))  # MNQ went flat while down: not re-pushed
    now[0] = 20 + 2 * PNL_SETTLE_NS
    snap = asyncio.run(client.request_exposure("MNQ"))
    assert (snap.net, snap.complete) == (0, True)


def test_first_row_of_a_fresh_stream_counts_with_a_ticking_clock() -> None:
    ticks = iter(range(100, 10_000_000_000, 7))  # every clock read advances
    now = [0]

    def _tick() -> int:
        now[0] = next(ticks)
        return now[0]

    client = _client(now, [])
    client._clock = SimpleNamespace(timestamp_ns=_tick)
    client._cache_position(_pnl("MNQZ6", -2))  # first row: also starts the stream
    live = client._pnl_live_since_ns
    assert live is not None
    snap = client._exposure_snapshot("MNQ", 0, live + PNL_SETTLE_NS)
    assert snap.net == -2 and snap.complete  # never silently flat


def test_an_unreadable_position_row_makes_that_root_unknown() -> None:
    now = [10]
    client = _client(now, [])
    client._cache_position(_pnl("MNQZ6", 0))
    client._cache_position(_pnl("ESZ6", 0))
    client._cache_position({"type": "instrument_pnl", "symbol": "MNQZ6"})  # no net
    now[0] = 10 + PNL_SETTLE_NS
    mnq = asyncio.run(client.request_exposure("MNQ"))
    assert mnq.net is None and not mnq.complete  # unknown, never flat
    assert asyncio.run(client.request_exposure("ES")).net == 0  # other roots unaffected
    client._cache_position(_pnl("MNQZ6", 1))  # a readable row resolves it
    assert asyncio.run(client.request_exposure("MNQ")).net == 1


def test_an_unreadable_row_without_a_symbol_poisons_every_root() -> None:
    client = _client([10], [])
    client._cache_position(_pnl("ESZ6", 0))
    client._cache_position({"type": "instrument_pnl"})
    assert asyncio.run(client.request_exposure("ES")).net is None
    client._forget_pnl_state()  # a fresh stream starts clean
    client._cache_position(_pnl("ESZ6", 0))
    assert asyncio.run(client.request_exposure("ES")).net == 0
