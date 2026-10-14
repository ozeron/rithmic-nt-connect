"""The Python ``rithmic_gateway`` package is the single-connection lake client;
sharing one socket across sessions is the Rust client's job
(openspec: retire-python-mux-and-noop-knobs)."""

from __future__ import annotations

import importlib

import pytest
import rithmic_gateway

_RETIRED_NAMES = [
    "GatewayMux",
    "MuxGatewayClient",
    "GatewayRuntime",
    "GatewayRuntimeRegistry",
    "create_gateway_client",
]


@pytest.mark.parametrize("module", ["mux", "runtime", "transport_recovery"])
def test_the_mux_modules_are_gone(module: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(f"rithmic_gateway.{module}")


@pytest.mark.parametrize("name", _RETIRED_NAMES)
def test_no_mux_names_are_exposed(name: str) -> None:
    assert not hasattr(rithmic_gateway, name)


def test_the_lake_client_is_still_exported() -> None:
    assert hasattr(rithmic_gateway, "GatewayClient")
    assert hasattr(rithmic_gateway, "GatewayConfig")


def test_range_loads_take_no_worker_count() -> None:
    import inspect

    sig = inspect.signature(rithmic_gateway.GatewayClient.load_time_bars_range)
    assert "max_workers" not in sig.parameters
