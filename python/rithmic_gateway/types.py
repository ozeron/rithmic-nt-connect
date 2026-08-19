"""Plant-level wire row shapes returned by ``GatewayClient`` RPCs.

These are the dict shapes the adapter's session protocols expose (direct PyO3
dicts match key-for-key). Typed so consumers never need ``dict[str, Any]``.
"""

from __future__ import annotations

from typing import NotRequired, TypedDict


class ProductRmsInfo(TypedDict):
    """One product-level RMS row: per-contract commission fill rate.

    Mirrors ``rithmic_plants::dto::ProductRmsInfoDto``; unset fields are
    omitted from the wire dict (never ``None``).
    """

    product_code: NotRequired[str]
    commission_fill_rate: NotRequired[float]
    presence_bits: NotRequired[int]


class AccountRmsInfo(TypedDict):
    """One account-level RMS row: default commission rate.

    Mirrors ``rithmic_plants::dto::AccountRmsInfoDto``; unset fields are
    omitted from the wire dict (never ``None``).
    """

    account_id: NotRequired[str]
    default_commission: NotRequired[float]
    presence_bits: NotRequired[int]
