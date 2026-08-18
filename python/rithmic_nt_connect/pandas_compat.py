"""Shims for NautilusTrader 1.231 calling pandas 3-removed APIs."""

from __future__ import annotations

from typing import Any, cast

_PATCHED = False


def patch_nautilus_pandas() -> None:
    """Point ``Timestamp.utcnow`` and ``freq='d'`` at the pandas 3 replacements.

    Nautilus 1.231 still calls ``pd.Timestamp.utcnow()`` in ``BacktestEngine.run``
    and ``Timestamp.floor(freq="d")`` when aggregating INTERNAL bars. Those
    emit ``Pandas4Warning`` on pandas 3.x. We cannot change 1.231 in-tree.
    """
    global _PATCHED
    if _PATCHED:
        return
    import pandas as pd

    if getattr(pd.Timestamp.utcnow, "__nautilus_shim__", False):
        _PATCHED = True
        return

    def utcnow(cls) -> pd.Timestamp:
        return cls.now("UTC")

    # ``cast(Any, ...)`` keeps the dynamic monkey-patches outside the checker's
    # reach: the shim marker and class-attribute swaps are dynamic by design.
    cast(Any, utcnow).__nautilus_shim__ = True
    pd.Timestamp.utcnow = cast(Any, classmethod(utcnow))

    _floor = pd.Timestamp.floor

    def floor(self, freq: str | None = None, *args, **kwargs) -> pd.Timestamp:
        # pandas 3 requires the uppercase unit; None (the old default) maps to
        # the base's own default of ``"D"`` so the forwarded call always has one.
        freq = freq or "D"
        if freq == "d":
            freq = "D"
        return _floor(self, freq, *args, **kwargs)

    pd.Timestamp.floor = floor
    _PATCHED = True
