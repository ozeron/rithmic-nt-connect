"""Nautilus 1.231 / pandas 3 shims."""

from __future__ import annotations

import warnings

from rithmic_nt_connect.pandas_compat import patch_nautilus_pandas


def test_utcnow_and_floor_d_do_not_warn() -> None:
    import pandas as pd
    from pandas.errors import Pandas4Warning

    patch_nautilus_pandas()
    with warnings.catch_warnings():
        warnings.simplefilter("error", Pandas4Warning)
        now = pd.Timestamp.utcnow()
        assert now.tzinfo is not None
        day = now.floor(freq="d")
        assert day.hour == 0
