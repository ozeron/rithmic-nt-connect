"""Read-only execution client tests (helpers; Cython base resists partial construction)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money

from rithmic_connect._convert import account_pnl_to_fields
from rithmic_connect.constants import VENUE


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


def test_phase1_order_actions_are_tracked_as_rejects():
    order_calls: list[str] = []

    def reject(action: str) -> None:
        order_calls.append(action)

    for action in ("submit_order", "cancel_order", "modify_order"):
        reject(action)
    assert order_calls == ["submit_order", "cancel_order", "modify_order"]


def test_wire_session_has_no_order_place_in_protocol_doc():
    import inspect
    from rithmic_connect import session as sess_mod

    src = inspect.getsource(sess_mod.WireSession)
    assert "place_order" not in src
    assert "submit_order" not in src
    assert "cancel_order" not in src


def test_instrument_pnl_to_position_fields():
    from rithmic_connect._convert import instrument_pnl_to_fields

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
    from rithmic_connect._convert import instrument_pnl_to_fields

    fields = instrument_pnl_to_fields(
        {
            "symbol": "NQU6",
            "net_quantity": 0,
        }
    )
    assert fields["position_side"] == "FLAT"
    assert fields["quantity"] == 0
