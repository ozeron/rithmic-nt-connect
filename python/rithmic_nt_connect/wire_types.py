"""Plant-level wire row shapes the extension returns for the RMS loads.

The extension returns plain dicts; unset fields are omitted (never ``None``).
They mirror ``rithmic_plants::dto::{ProductRmsInfoDto, AccountRmsInfoDto}``.
"""

from __future__ import annotations

from typing import NotRequired, TypedDict


class ProductRmsInfo(TypedDict):
    """One product-level RMS row: per-contract commission fill rate."""

    product_code: NotRequired[str]
    commission_fill_rate: NotRequired[float]
    presence_bits: NotRequired[int]


class AccountRmsInfo(TypedDict):
    """One account-level RMS row: default commission rate."""

    account_id: NotRequired[str]
    default_commission: NotRequired[float]
    presence_bits: NotRequired[int]
