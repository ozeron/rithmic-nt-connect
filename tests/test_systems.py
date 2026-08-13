from __future__ import annotations

from rithmic_nt_connect.systems import list_systems


def test_list_systems_importable() -> None:
    assert callable(list_systems)
