"""Shims for NautilusTrader 1.231 calling pandas 3-removed APIs."""

from __future__ import annotations

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

    def utcnow(cls) -> pd.Timestamp:  # type: ignore[no-untyped-def]
        return cls.now("UTC")

    utcnow.__nautilus_shim__ = True  # type: ignore[attr-defined]
    pd.Timestamp.utcnow = classmethod(utcnow)  # type: ignore[method-assign, assignment]

    _floor = pd.Timestamp.floor

    def floor(self, freq=None, *args, **kwargs):  # type: ignore[no-untyped-def]
        if freq == "d":
            freq = "D"
        return _floor(self, freq, *args, **kwargs)

    pd.Timestamp.floor = floor  # type: ignore[method-assign]
    _PATCHED = True
