"""Venue commission rate cache and fee calculation for Rithmic."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency, Money

from rithmic_nt_connect.session import WireSession

logger = logging.getLogger(__name__)


class CommissionRegistry:
    """Venue commission rates fetched from the order-plant RMS info.

    Product fill rates keyed by product code (e.g. ``MNQ``), plus the
    account-level default as fallback.
    """

    def __init__(
        self,
        rates: dict[str, Decimal] | None = None,
        default_commission: Decimal | None = None,
    ) -> None:
        self._rates: dict[str, Decimal] = rates or {}
        self._default_commission: Decimal | None = default_commission

    @property
    def rates(self) -> dict[str, Decimal]:
        """Live product-code → rate map (mutations affect the registry)."""
        return self._rates

    @rates.setter
    def rates(self, value: dict[str, Decimal]) -> None:
        self._rates = dict(value)

    @property
    def default_commission(self) -> Decimal | None:
        return self._default_commission

    @default_commission.setter
    def default_commission(self, value: Decimal | None) -> None:
        self._default_commission = value

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
        rates: dict[str, Decimal] = {}
        for row in rows:
            code = row.get("product_code")
            rate = row.get("commission_fill_rate")
            if code and rate is not None:
                rates[str(code)] = Decimal(str(rate))
        self._rates = rates
        try:
            account_rows = await asyncio.to_thread(session.load_account_rms_info)
        except Exception as exc:
            account_rows = []
            log.warning(f"account commission default unavailable: {exc}")

        default = next(
            (
                r.get("default_commission")
                for r in account_rows
                if (
                    active_account is None or str(r.get("account_id")) == active_account
                )
                and r.get("default_commission") is not None
            ),
            None,
        )
        self._default_commission = (
            Decimal(str(default)) if default is not None else None
        )
        log.info(
            f"commission rates: {len(rates)} products"
            + (
                f", account default {self._default_commission}"
                if self._default_commission is not None
                else ""
            )
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
        if product_code is not None:
            rate = self._rates.get(product_code)
            if rate is not None:
                return Money(rate * Decimal(qty), Currency.from_str("USD"))
        if self._default_commission is not None:
            return Money(
                self._default_commission * Decimal(qty), Currency.from_str("USD")
            )
        return Money(Decimal(0), Currency.from_str("USD"))
