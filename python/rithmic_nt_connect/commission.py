"""Venue commission rate cache and fee calculation for Rithmic."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency, Money

from rithmic_nt_connect._lib import CommissionTable
from rithmic_nt_connect.session import WireSession

logger = logging.getLogger(__name__)


class CommissionRegistry:
    """Venue commission rates fetched from the order-plant RMS info.

    Product fill rates keyed by product code (e.g. ``MNQ``), plus the
    account-level default as fallback. Which rate applies (product, then
    default, then none) is decided by the ``rithmic-exec`` crate; the rates
    cross the boundary as decimal text and the arithmetic stays ``Decimal``.
    """

    def __init__(
        self,
        rates: dict[str, Decimal] | None = None,
        default_commission: Decimal | None = None,
    ) -> None:
        self._table = CommissionTable()
        self.rates = rates or {}
        self.default_commission = default_commission

    @property
    def rates(self) -> dict[str, Decimal]:
        """Product-code → rate snapshot (assign to replace the table)."""
        return {code: Decimal(text) for code, text in self._table.rates.items()}

    @rates.setter
    def rates(self, value: dict[str, Decimal]) -> None:
        self._table.rates = {code: str(rate) for code, rate in value.items()}

    @property
    def default_commission(self) -> Decimal | None:
        text = self._table.default_rate
        return Decimal(text) if text is not None else None

    @default_commission.setter
    def default_commission(self, value: Decimal | None) -> None:
        self._table.default_rate = None if value is None else str(value)

    async def load(
        self,
        session: WireSession,
        active_account: str | None = None,
        log: Any | None = None,
    ) -> None:
        """Fetch venue commission rates (order-plant RMS info) asynchronously.

        Product fill rates keyed by product code, with the account default as
        fallback. The two fetches are independent: a failed account-default
        fetch must NOT clear an already-loaded product table (and vice versa).
        Best-effort: failure leaves the affected cache empty and fills report
        zero commission. Never raises.
        """
        log = log or logger
        try:
            rows = await asyncio.to_thread(session.load_product_rms_info)
        except Exception as exc:
            rows = []
            log.warning(f"product commission rates unavailable (0.0 fallback): {exc}")
        self._table.load_products(
            [
                (_text(row.get("product_code")), _text(row.get("commission_fill_rate")))
                for row in rows
            ]
        )
        try:
            account_rows = await asyncio.to_thread(session.load_account_rms_info)
        except Exception as exc:
            account_rows = []
            log.warning(f"account commission default unavailable: {exc}")
        self._table.load_account_default(
            [
                (str(row.get("account_id")), _text(row.get("default_commission")))
                for row in account_rows
            ],
            active_account,
        )
        default = self.default_commission
        log.info(
            f"commission rates: {len(self._table.rates)} products"
            + (f", account default {default}" if default is not None else "")
        )

    def product_code_for_fill(
        self, cache: Cache, instrument_id: InstrumentId | None, symbol: str | None
    ) -> str | None:
        """RMS product code for one fill's commission lookup."""
        if instrument_id is not None:
            instrument = cache.instrument(instrument_id)
            if instrument is not None:
                code = (getattr(instrument, "info", None) or {}).get(
                    "rithmic_product_code"
                )
                if code:
                    return str(code)
        return symbol

    def commission_money(self, product_code: str | None, qty: int) -> Money:
        """Venue commission for one fill: per-contract RMS rate x qty (USD).

        Unknown products fall back to the account default, then to zero.
        """
        text = self._table.rate_for(product_code)
        rate = Decimal(text) if text is not None else Decimal(0)
        return Money(rate * Decimal(qty), Currency.from_str("USD"))


def _text(value: Any) -> str | None:
    """A venue number as the decimal text Python's ``Decimal(str(x))`` would see."""
    return None if value is None else str(value)
