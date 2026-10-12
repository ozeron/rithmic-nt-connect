"""Venue exposure from the node's own exec session (no second session)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

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
