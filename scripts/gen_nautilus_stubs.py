"""Regenerate the local nautilus_trader stubs and re-apply known fixes.

The installed nautilus_trader ships no stubs for its compiled Cython classes
(``Order``, ``DataClient``, ``ExecutionClient``, ...), so checkers see them as
``Unknown``. ``stubgen`` can introspect the installed modules at runtime, but
its output needs fixes:

1. Drop bogus ``import X`` lines where the same stub also declares ``class X``
   (stubgen emits both for cdef classes; checkers resolve the broken import).
2. Hand-ported constructor signatures for the classes the adapter constructs
   or inherits from (stubgen mangles cdef parameter names, e.g.
   ``InstrumentIdinstrument_id``) plus the cdef attribute/handler surface
   adapters read or override directly (``_cache``, ``_log``, ``_send_*``...).
3. ``Order.price`` / ``Order.trigger_price`` declared on the base stub so
   adapters can read them guarded by ``has_price`` / ``has_trigger_price``.
   ``trigger_type`` is deliberately NOT on the base: only stop orders expose
   it, and unguarded access must stay a static error.
4. ``nautilus_trader/__init__.pyi`` truncated to empty: the package init
   imports ``nautilus_pyo3`` which adds no signal for adapter checks.

Run from the repo root (regenerates ``stubs/`` in place):
    uv run --with mypy python scripts/gen_nautilus_stubs.py
"""

from __future__ import annotations

import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
STUBS = ROOT / "stubs"

_FUTURES_CTOR_IMPORTS = """from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
"""

_FUTURES_CTOR = """    # Constructor hand-ported from futures_contract.pyx (stubgen mangles cdef names).
    def __init__(
        self,
        instrument_id: InstrumentId,
        raw_symbol: Symbol,
        asset_class: AssetClass,
        currency: Currency,
        price_precision: int,
        price_increment: Price,
        multiplier: Quantity,
        lot_size: Quantity,
        underlying: str,
        activation_ns: int,
        expiration_ns: int,
        ts_event: int,
        ts_init: int,
        margin_init: Decimal | None = None,
        margin_maint: Decimal | None = None,
        maker_fee: Decimal | None = None,
        taker_fee: Decimal | None = None,
        exchange: str | None = None,
        tick_scheme_name: str | None = None,
        info: dict | None = None,
    ) -> None: ..."""

_DATA_IMPORT = "\nfrom nautilus_trader.model.identifiers import InstrumentId\n"
_DATA_DELTAS_CTOR = (
    "    # Constructor hand-ported from data.pyx (stubgen mangles cdef names).\n"
    "    def __init__(self, instrument_id: InstrumentId, deltas: list) -> None: ..."
)

_BASE_PRICE_ATTRS = """    # ``price``/``trigger_price`` live on the concrete limit/stop subclasses,
    # not the base; declared here so adapters can read them guarded by
    # ``has_price``/``has_trigger_price``. ``trigger_type`` is deliberately
    # absent: only stop orders expose it, and unguarded access must stay an error.
    price: Incomplete
    trigger_price: Incomplete
"""

_COMPONENT_CDEF_ATTRS = """    # cdef attribute surface (from component.pxd) that subclasses use directly.
    _clock: Incomplete
    _log: Incomplete
    _msgbus: Incomplete
    _loop: Incomplete
"""

_DATA_CLIENT_IMPORT = "\nfrom nautilus_trader.cache.cache import Cache\n"
_DATA_CLIENT_ATTRS = """    # cdef readonly Cache _cache (data/client.pxd) — adapters read it via self._cache.
    _cache: Cache
"""

_ACCOUNT_STATE_CTOR = """    # Hand-ported from execution/client.pxd (stubgen mangles cdef params).
    def generate_account_state(
        self, balances: list, margins: list, reported: bool, ts_event: int, info: dict | None = None
    ) -> None: ...
    _cache: Incomplete
    def _set_account_id(self, account_id: Any) -> None: ...
    def _send_account_state(self, account_state: Any) -> None: ...
    def _send_order_event(self, event: Any) -> None: ...
    def _send_mass_status_report(self, report: Any) -> None: ...
    def _send_order_status_report(self, report: Any) -> None: ...
    def _send_fill_report(self, report: Any) -> None: ...
    def _send_position_status_report(self, report: Any) -> None: ..."""

_SUBMIT_ORDER_CTOR = """    # Hand-ported from execution/messages.pxd (stubgen mangles cdef params).
    def __init__(
        self,
        trader_id: Any,
        strategy_id: Any,
        order: Any,
        command_id: Any,
        ts_init: int,
        position_id: Any | None = None,
        client_id: Any | None = None,
        params: dict | None = None,
        correlation_id: Any | None = None,
    ) -> None: ..."""

_EXEC_RECON_CTORS = {
    "GenerateFillReports": """    # Hand-ported recon ctor from execution/messages.pyx (stubgen mangles cdef names).
    def __init__(
        self,
        instrument_id: Any,
        venue_order_id: Any,
        start: Any,
        end: Any,
        command_id: Any,
        ts_init: int,
        params: dict | None = None,
        correlation_id: Any | None = None,
    ) -> None: ...""",
    "GenerateOrderStatusReports": """    # Hand-ported recon ctor from execution/messages.pyx (stubgen mangles cdef names).
    def __init__(
        self,
        instrument_id: Any,
        start: Any,
        end: Any,
        open_only: bool,
        command_id: Any,
        ts_init: int,
        params: dict | None = None,
        log_receipt_level: Any = ...,
        correlation_id: Any | None = None,
    ) -> None: ...""",
    "GeneratePositionStatusReports": """    # Hand-ported recon ctor from execution/messages.pyx (stubgen mangles cdef names).
    def __init__(
        self,
        instrument_id: Any,
        start: Any,
        end: Any,
        command_id: Any,
        ts_init: int,
        params: dict | None = None,
        log_receipt_level: Any = ...,
        correlation_id: Any | None = None,
    ) -> None: ...""",
}

_LIVE_MDC_HANDLERS = """    # cdef handler surface adapters override (data_client.pxd).
    _instrument_provider: Incomplete
    # cdef handlers take the full native arg list; ``*args`` keeps the override
    # surface loose (adapter calls them with venue + payload + timestamps).
    def _handle_data(self, *args: Any, **kwargs: Any) -> None: ...
    def _handle_bars(self, *args: Any, **kwargs: Any) -> None: ...
    def _handle_trade_ticks(self, *args: Any, **kwargs: Any) -> None: ...
    def _handle_quote_ticks(self, *args: Any, **kwargs: Any) -> None: ...
"""


def _run_stubgen() -> None:
    # ``uv run --with mypy`` (not ``-m mypy.stubgen``): the with-injected env
    # has no code object for the module under ``sys.executable``. The full
    # package must be stubbed (not just model): ty hard-errors on compiled-only
    # modules that have no stub at all.
    subprocess.run(
        [
            "uv",
            "run",
            "--with",
            "mypy",
            "stubgen",
            "-p",
            "nautilus_trader",
            "-o",
            str(STUBS),
        ],
        cwd=ROOT,
        check=True,
    )


def _strip_self_imports() -> int:
    """Drop ``import X`` when the same stub also declares ``class X``."""
    fixed = 0
    for pyi in STUBS.rglob("*.pyi"):
        text = pyi.read_text()
        classes = set(re.findall(r"^class (\w+)", text, re.M))
        kept = [
            line
            for line in text.splitlines()
            if not (re.match(r"^import (\w+)$", line) and re.match(r"^import (\w+)$", line).group(1) in classes)
        ]
        if len(kept) != len(text.splitlines()):
            fixed += len(text.splitlines()) - len(kept)
        pyi.write_text("\n".join(kept) + "\n")
    return fixed


def _patch(path: pathlib.Path, old: str, new: str, *, marker: str) -> bool:
    """Idempotently apply ``old -> new`` guarded on a marker string."""
    text = path.read_text()
    if marker in text:
        return False
    if old not in text:
        raise SystemExit(f"gen_nautilus_stubs: anchor missing in {path}:\n{old}")
    path.write_text(text.replace(old, new, 1))
    return True


def _apply_hand_patches() -> int:
    applied = 0

    # The package init imports nautilus_pyo3; nothing the adapter checks needs it.
    init = STUBS / "nautilus_trader/__init__.pyi"
    if init.read_text().strip():
        init.write_text("")
        applied += 1

    component = STUBS / "nautilus_trader/common/component.pyi"
    applied += _patch(
        component,
        "class Component:\n    fully_qualified_name: ClassVar[method] = ...\n    __pyx_vtable__: ClassVar[PyCapsule] = ...\n",
        "class Component:\n    fully_qualified_name: ClassVar[method] = ...\n    __pyx_vtable__: ClassVar[PyCapsule] = ...\n"
        + _COMPONENT_CDEF_ATTRS,
        marker="# cdef attribute surface (from component.pxd)",
    )

    data_client = STUBS / "nautilus_trader/data/client.pyi"
    if "from nautilus_trader.cache.cache import Cache" not in data_client.read_text():
        text = data_client.read_text().replace(
            "from _typeshed import Incomplete\n",
            "from _typeshed import Incomplete\n" + _DATA_CLIENT_IMPORT,
            1,
        )
        data_client.write_text(text)
        applied += 1
    applied += _patch(
        data_client,
        "    is_connected: Incomplete\n    venue: Incomplete\n    def __init__(self, ClientIdclient_id",
        "    is_connected: Incomplete\n    venue: Incomplete\n" + _DATA_CLIENT_ATTRS
        + "    def __init__(self, ClientIdclient_id",
        marker="# cdef readonly Cache _cache",
    )

    exec_client = STUBS / "nautilus_trader/execution/client.pyi"
    applied += _patch(
        exec_client,
        "    def generate_account_state(self, listbalances, listmargins, boolreported, uint64_tts_event, dictinfo=...) -> void: ...\n",
        _ACCOUNT_STATE_CTOR + "\n",
        marker="# Hand-ported from execution/client.pxd",
    )
    applied += _patch(
        exec_client,
        "LiquiditySideliquidity_side, uint64_tts_event, dictinfo=...) -> void: ...",
        "LiquiditySideliquidity_side, uint64_tts_event, info: dict | None = ...) -> void: ...",
        marker="info: dict | None = ...",
    )

    exec_msgs = STUBS / "nautilus_trader/execution/messages.pyi"
    applied += _patch(
        exec_msgs,
        "    def __init__(self, TraderIdtrader_id, StrategyIdstrategy_id, Orderorder, UUID4command_id, uint64_tts_init, PositionIdposition_id: PositionId | None = ..., ClientIdclient_id=..., dictparams: dict | None = ..., UUID4correlation_id=...) -> None: ...\n",
        _SUBMIT_ORDER_CTOR + "\n",
        marker="# Hand-ported from execution/messages.pxd",
    )
    for cname, ctor in _EXEC_RECON_CTORS.items():
        text = exec_msgs.read_text()
        if f"# Hand-ported recon ctor ({cname})" in text:
            continue
        # Scope the replacement to the class body: the mangled ctor pattern also
        # matches on ``ExecutionReportCommand`` itself.
        start = text.index(f"class {cname}(ExecutionReportCommand):")
        rest = text[start + 1:]
        next_class = re.search(r"\nclass ", rest)
        end = len(text) if next_class is None else start + 1 + next_class.start()
        body = text[start:end]
        patched_body = re.sub(
            r"    def __init__\(self, InstrumentIdinstrument_id.*?\) -> None: \.\.\.\n",
            ctor + "\n",
            body,
            count=1,
        )
        if patched_body == body:
            raise SystemExit(f"gen_nautilus_stubs: {cname} ctor anchor missing")
        exec_msgs.write_text(text[:start] + patched_body + text[end:])
        applied += 1

    strategy = STUBS / "nautilus_trader/trading/strategy.pyi"
    text = strategy.read_text()
    # e2e strategy doubles override the order/position handlers; stubgen mangles
    # cdef param names (``OrderAcceptedevent``) so the names must be restored.
    if "# Hand-ported handler param names (strategy.pxd)" not in text:
        text = text.replace(
            "class Strategy(Actor):\n",
            "class Strategy(Actor):\n    # Hand-ported handler param names (strategy.pxd)\n",
            1,
        )
        text = re.sub(
            r"    def (on_\w+)\(self, (\w+)(event)\) -> void: \.\.\.\n",
            r"    def \1(self, \3) -> void: ...\n",
            text,
        )
        text = text.replace(
            "    def submit_order(self, Orderorder, PositionIdposition_id=..., ClientIdclient_id=..., dictparams=...) -> void: ...\n",
            "    def submit_order(self, order, position_id=..., client_id=..., params=...) -> void: ...\n",
        )
        text = text.replace(
            "    def submit_order_list(self, OrderListorder_list, PositionIdposition_id=..., ClientIdclient_id=..., dictparams=...) -> void: ...\n",
            "    def submit_order_list(self, order_list, position_id=..., client_id=..., params=...) -> void: ...\n",
        )
        strategy.write_text(text)
        applied += 1

    actor = STUBS / "nautilus_trader/common/actor.pyi"
    text = actor.read_text()
    if "# Hand-ported handler param names (actor.pxd)" not in text:
        text = text.replace(
            "class Actor(Component):\n",
            "class Actor(Component):\n    # Hand-ported handler param names (actor.pxd)\n",
            1,
        )
        for old, new in (
            ("    def on_bar(self, Barbar) -> void: ...\n", "    def on_bar(self, bar) -> void: ...\n"),
            ("    def on_quote_tick(self, QuoteTicktick) -> void: ...\n", "    def on_quote_tick(self, tick) -> void: ...\n"),
            ("    def on_trade_tick(self, TradeTicktick) -> void: ...\n", "    def on_trade_tick(self, tick) -> void: ...\n"),
            (
                "    def subscribe_quote_ticks(self, InstrumentIdinstrument_id, ClientIdclient_id=..., boolupdate_catalog=..., boolaggregate_spread_quotes=..., dictparams=...) -> void: ...\n",
                "    def subscribe_quote_ticks(self, instrument_id, client_id=..., update_catalog=..., aggregate_spread_quotes=..., params=...) -> void: ...\n",
            ),
        ):
            if old not in text:
                raise SystemExit(f"gen_nautilus_stubs: actor anchor missing:\n{old}")
            text = text.replace(old, new, 1)
        actor.write_text(text)
        applied += 1

    live_mdc = STUBS / "nautilus_trader/live/data_client.pyi"
    applied += _patch(
        live_mdc,
        "class LiveMarketDataClient(MarketDataClient):\n",
        "class LiveMarketDataClient(MarketDataClient):\n" + _LIVE_MDC_HANDLERS,
        marker="# cdef handler surface adapters override (data_client.pxd)",
    )

    futures = STUBS / "nautilus_trader/model/instruments/futures_contract.pyi"
    text = futures.read_text()
    if _FUTURES_CTOR not in text:
        # Idempotent: patch the mangled one-liner ctor and add the type imports.
        text = re.sub(
            r"    def __init__\(self, InstrumentIdinstrument_id.*?\) -> None: \.\.\.\n",
            _FUTURES_CTOR + "\n",
            text,
            count=1,
        )
        if "from nautilus_trader.model.enums import AssetClass" not in text:
            text = text.replace(
                "from typing import Any, ClassVar\n",
                "from typing import Any, ClassVar\n\n" + _FUTURES_CTOR_IMPORTS,
            )
        futures.write_text(text)
        applied += 1

    data = STUBS / "nautilus_trader/model/data.pyi"
    text = data.read_text()
    if _DATA_DELTAS_CTOR not in text:
        text = re.sub(
            r"    def __init__\(self, InstrumentIdinstrument_id, listdeltas\) -> None: \.\.\.\n",
            _DATA_DELTAS_CTOR + "\n",
            text,
            count=1,
        )
        if "from nautilus_trader.model.identifiers import InstrumentId" not in text:
            text = text.replace(
                "from typing import Any, Callable, ClassVar, overload\n",
                "from typing import Any, Callable, ClassVar, overload\n" + _DATA_IMPORT,
            )
        data.write_text(text)
        applied += 1

    base = STUBS / "nautilus_trader/model/orders/base.pyi"
    text = base.read_text()
    # Guard on the comment, not ``price: Incomplete`` — ``has_activation_price:
    # Incomplete`` contains that substring and would skip the patch.
    if "# ``price``/``trigger_price`` live on the concrete" not in text:
        text = text.replace(
            "    has_trigger_price: Incomplete\n",
            "    has_trigger_price: Incomplete\n" + _BASE_PRICE_ATTRS,
        )
        base.write_text(text)
        applied += 1

    return applied


def main() -> None:
    _run_stubgen()
    stripped = _strip_self_imports()
    applied = _apply_hand_patches()
    print(
        f"regenerated stubs under {STUBS} "
        f"(dropped {stripped} bogus self-imports, applied {applied} hand-patches)"
    )


if __name__ == "__main__":
    main()
