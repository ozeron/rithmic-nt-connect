"""Tests for CommissionRegistry collaborator."""

import asyncio
from decimal import Decimal
from unittest.mock import Mock

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency, Money
from rithmic_nt_connect.commission import CommissionRegistry


def test_commission_registry_loading_and_fallbacks() -> None:
    session = Mock()
    session.load_product_rms_info = Mock(
        return_value=[
            {"product_code": "MNQ", "commission_fill_rate": 0.52},
            {"product_code": "NQ", "commission_fill_rate": 1.40},
        ]
    )
    session.load_account_rms_info = Mock(
        return_value=[
            {"account_id": "ACT1", "default_commission": 2.05},
        ]
    )

    registry = CommissionRegistry()
    asyncio.run(registry.load(session, active_account="ACT1"))

    assert registry.rates == {
        "MNQ": Decimal("0.52"),
        "NQ": Decimal("1.40"),
    }
    assert registry.default_commission == Decimal("2.05")

    # Product calculation
    mnq_fee = registry.commission_money("MNQ", qty=3)
    assert mnq_fee == Money(Decimal("1.56"), Currency.from_str("USD"))

    # Fallback to account default for unknown product
    unknown_fee = registry.commission_money("UNKNOWN", qty=2)
    assert unknown_fee == Money(Decimal("4.10"), Currency.from_str("USD"))

    # Fallback when no product code and no default
    empty_reg = CommissionRegistry()
    assert empty_reg.commission_money("XYZ", qty=5) == Money(
        Decimal(0), Currency.from_str("USD")
    )


def test_product_code_resolution() -> None:
    registry = CommissionRegistry()
    cache = Mock()
    inst = Mock()
    inst.info = {"rithmic_product_code": "MNQ"}
    cache.instrument = Mock(return_value=inst)

    inst_id = InstrumentId.from_str("MNQU6.CME")
    code = registry.product_code_for_fill(cache, inst_id, "MNQU6")
    assert code == "MNQ"

    # Fallback when instrument not in cache
    cache.instrument = Mock(return_value=None)
    assert registry.product_code_for_fill(cache, inst_id, "MNQU6") == "MNQU6"
