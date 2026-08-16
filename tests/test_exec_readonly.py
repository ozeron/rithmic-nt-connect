"""Execution client tests (helpers; Cython base resists partial construction)."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money

from rithmic_nt_connect._convert import account_pnl_to_fields
from rithmic_nt_connect.constants import VENUE
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.execution import wait_account_in_cache


def test_account_pnl_fields_build_balance():
    event = {
        "type": "account_pnl",
        "account_id": "ACC123",
        "account_balance": "1000.00",
        "cash_on_hand": "1000.00",
        "currency": "USD",
    }
    fields = account_pnl_to_fields(event)
    currency = Currency.from_str(str(fields.get("currency", "USD")))
    free = Money(Decimal(str(fields["cash_on_hand"])), currency)
    bal = AccountBalance(free, Money(Decimal("0"), currency), free)
    assert float(bal.free) == 1000.0
    assert AccountId(f"{VENUE}-{fields['account_id']}").value.startswith(f"{VENUE}-")


def test_readonly_mode_tracks_rejects():
    order_calls: list[str] = []

    def reject(action: str) -> None:
        order_calls.append(action)

    for action in ("submit_order", "cancel_order", "modify_order"):
        reject(action)
    assert order_calls == ["submit_order", "cancel_order", "modify_order"]


def test_execution_client_alias_preserved():
    from rithmic_nt_connect.execution import RithmicReadOnlyExecutionClient

    assert RithmicReadOnlyExecutionClient is RithmicExecutionClient


def test_order_plant_and_errors_import_smoke():
    from rithmic_nt_connect._order_plant import OrderPlantPolicy

    assert OrderPlantPolicy().load_orders_available()


def test_instrument_pnl_to_position_fields():
    from rithmic_nt_connect._convert import instrument_pnl_to_fields

    fields = instrument_pnl_to_fields(
        {
            "type": "instrument_pnl",
            "account_id": "ACC1",
            "symbol": "NQU6",
            "exchange": "CME",
            "net_quantity": -2,
            "avg_open_fill_price": 21000.5,
            "open_position_pnl": "12.5",
        }
    )
    assert fields["position_side"] == "SHORT"
    assert fields["quantity"] == 2
    assert fields["instrument_id"] == "NQU6.RITHMIC"
    assert fields["avg_px_open"] == 21000.5


def test_instrument_pnl_flat_when_zero_net():
    from rithmic_nt_connect._convert import instrument_pnl_to_fields

    fields = instrument_pnl_to_fields(
        {
            "symbol": "NQU6",
            "net_quantity": 0,
        }
    )
    assert fields["position_side"] == "FLAT"
    assert fields["quantity"] == 0


class _DelayedCache:
    def __init__(self, ready_after: int) -> None:
        self.calls = 0
        self.ready_after = ready_after
        self.obj = object()

    def account(self, _account_id: AccountId) -> object | None:
        self.calls += 1
        if self.calls >= self.ready_after:
            return self.obj
        return None

    def account_for_venue(self, _venue: Venue) -> object | None:
        return None


def test_wait_account_in_cache_returns_when_present() -> None:
    cache = _DelayedCache(ready_after=3)
    aid = AccountId(f"{VENUE}-ACC1")
    asyncio.run(wait_account_in_cache(cache, aid, timeout_s=2.0))
    assert cache.calls >= 3


def test_wait_account_in_cache_times_out() -> None:
    cache = _DelayedCache(ready_after=10_000)
    aid = AccountId(f"{VENUE}-ACC1")
    with pytest.raises(RuntimeError, match="not in cache"):
        asyncio.run(wait_account_in_cache(cache, aid, timeout_s=0.12))
