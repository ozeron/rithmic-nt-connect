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
5. Unused subpackages pruned: ``stubgen`` emits the whole package (~435 files)
   but the adapter + tests only use nine subpackages, so anything outside
   ``_EXPECTED_TOP_LEVEL`` is deleted (~300 files / ~9.6k lines). ``ty check``
   is the final gate: if code later imports a pruned subpackage, ty hard-errors
   on the missing module and the allowlist gets reviewed (never silent).

   Trade-off: kept stubs that import a pruned subpackage (e.g. ``live/node.pyi``
   -> ``portfolio``/``system``) resolve those symbols to ``Any`` rather than
   erroring, so pruning is not lossless for stub-internal references. That is
   fine as long as the adapter only consumes symbols from the kept set.

Run from the repo root (regenerates ``stubs/`` in place):
    uv run --with mypy python scripts/gen_nautilus_stubs.py
"""

from __future__ import annotations

import contextlib
import os
import pathlib
import re
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
STUBS = ROOT / "stubs"

# The subpackages the adapter + tests are allowed to pull in. Regeneration
# deletes any stub outside this set; ``ty check`` then hard-errors if the
# checked code actually imports a pruned subpackage, so the expansion is caught
# in CI (never silently) and this list is reviewed.
_EXPECTED_TOP_LEVEL = {
    "cache",
    "common",
    "config",
    "core",
    "data",
    "execution",
    "live",
    "model",
    "trading",
}

_FUTURES_CTOR_IMPORTS = """from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
"""

_FUTURES_CTOR = (
    "    # Constructor hand-ported from futures_contract.pyx "
    "(stubgen mangles cdef names).\n"
    "    def __init__(\n"
    "        self,\n"
    "        instrument_id: InstrumentId,\n"
    "        raw_symbol: Symbol,\n"
    "        asset_class: AssetClass,\n"
    "        currency: Currency,\n"
    "        price_precision: int,\n"
    "        price_increment: Price,\n"
    "        multiplier: Quantity,\n"
    "        lot_size: Quantity,\n"
    "        underlying: str,\n"
    "        activation_ns: int,\n"
    "        expiration_ns: int,\n"
    "        ts_event: int,\n"
    "        ts_init: int,\n"
    "        margin_init: Decimal | None = None,\n"
    "        margin_maint: Decimal | None = None,\n"
    "        maker_fee: Decimal | None = None,\n"
    "        taker_fee: Decimal | None = None,\n"
    "        exchange: str | None = None,\n"
    "        tick_scheme_name: str | None = None,\n"
    "        info: dict | None = None,\n"
    "    ) -> None: ..."
)

_DATA_IMPORT = "\nfrom nautilus_trader.model.identifiers import InstrumentId\n"
_DATA_DELTAS_CTOR = (
    "    # Constructor hand-ported from data.pyx (stubgen mangles cdef names).\n"
    "    def __init__(self, instrument_id: InstrumentId, deltas: list) -> None: ..."
)

_BASE_PRICE_ATTRS = (
    "    # ``price``/``trigger_price`` live on the concrete limit/stop "
    "subclasses,\n"
    "    # not the base; declared here so adapters can read them guarded by\n"
    "    # ``has_price``/``has_trigger_price``. ``trigger_type`` is deliberately\n"
    "    # absent: only stop orders expose it, and unguarded access must stay an "
    "error.\n"
    "    price: Incomplete\n"
    "    trigger_price: Incomplete\n"
)

_COMPONENT_CDEF_ATTRS = (
    "    # cdef attribute surface (from component.pxd) that subclasses use "
    "directly.\n"
    "    _clock: Incomplete\n"
    "    _log: Incomplete\n"
    "    _msgbus: Incomplete\n"
    "    _loop: Incomplete\n"
)

_DATA_CLIENT_IMPORT = "\nfrom nautilus_trader.cache.cache import Cache\n"
_DATA_CLIENT_ATTRS = (
    "    # cdef readonly Cache _cache (data/client.pxd) — adapters read it via "
    "self._cache.\n"
    "    _cache: Cache\n"
)

_ACCOUNT_STATE_CTOR = (
    "    # Hand-ported from execution/client.pxd (stubgen mangles cdef params).\n"
    "    def generate_account_state(\n"
    "        self, balances: list, margins: list, reported: bool, ts_event: int, "
    "info: dict | None = None\n"
    "    ) -> None: ...\n"
    "    _cache: Incomplete\n"
    "    def _set_account_id(self, account_id: Any) -> None: ...\n"
    "    def _send_account_state(self, account_state: Any) -> None: ...\n"
    "    def _send_order_event(self, event: Any) -> None: ...\n"
    "    def _send_mass_status_report(self, report: Any) -> None: ...\n"
    "    def _send_order_status_report(self, report: Any) -> None: ...\n"
    "    def _send_fill_report(self, report: Any) -> None: ...\n"
    "    def _send_position_status_report(self, report: Any) -> None: ..."
)

_SUBMIT_ORDER_CTOR = (
    "    # Hand-ported from execution/messages.pxd (stubgen mangles cdef params).\n"
    "    def __init__(\n"
    "        self,\n"
    "        trader_id: Any,\n"
    "        strategy_id: Any,\n"
    "        order: Any,\n"
    "        command_id: Any,\n"
    "        ts_init: int,\n"
    "        position_id: Any | None = None,\n"
    "        client_id: Any | None = None,\n"
    "        params: dict | None = None,\n"
    "        correlation_id: Any | None = None,\n"
    "    ) -> None: ..."
)

_EXEC_RECON_CTORS = {
    "GenerateFillReports": (
        "    # Hand-ported recon ctor from execution/messages.pyx "
        "(stubgen mangles cdef names).\n"
        "    def __init__(\n"
        "        self,\n"
        "        instrument_id: Any,\n"
        "        venue_order_id: Any,\n"
        "        start: Any,\n"
        "        end: Any,\n"
        "        command_id: Any,\n"
        "        ts_init: int,\n"
        "        params: dict | None = None,\n"
        "        correlation_id: Any | None = None,\n"
        "    ) -> None: ..."
    ),
    "GenerateOrderStatusReports": (
        "    # Hand-ported recon ctor from execution/messages.pyx "
        "(stubgen mangles cdef names).\n"
        "    def __init__(\n"
        "        self,\n"
        "        instrument_id: Any,\n"
        "        start: Any,\n"
        "        end: Any,\n"
        "        open_only: bool,\n"
        "        command_id: Any,\n"
        "        ts_init: int,\n"
        "        params: dict | None = None,\n"
        "        log_receipt_level: Any = ...,\n"
        "        correlation_id: Any | None = None,\n"
        "    ) -> None: ..."
    ),
    "GeneratePositionStatusReports": (
        "    # Hand-ported recon ctor from execution/messages.pyx "
        "(stubgen mangles cdef names).\n"
        "    def __init__(\n"
        "        self,\n"
        "        instrument_id: Any,\n"
        "        start: Any,\n"
        "        end: Any,\n"
        "        command_id: Any,\n"
        "        ts_init: int,\n"
        "        params: dict | None = None,\n"
        "        log_receipt_level: Any = ...,\n"
        "        correlation_id: Any | None = None,\n"
        "    ) -> None: ..."
    ),
}

_LIVE_MDC_LIVEDATACLIENT_HEADER = (
    "# 1.231.x models LiveMarketDataClient(MarketDataClient) and\n"
    "# LiveDataClient(DataClient) as parallel Cython branches, yet the class\n"
    "# genuinely provides the full LiveDataClient surface (loop, create_task,\n"
    "# run_after_delay, connect/disconnect, cancel_pending_tasks) and\n"
    "# LiveDataClientFactory.create contracts -> LiveDataClient. The adapter\n"
    "# subclasses LiveMarketDataClient only (the dual-base MRO breaks __init__:\n"
    "# either ordering lands on the other base's required ``loop``), so the stub\n"
    "# declares the LiveDataClient relationship to satisfy the factory contract\n"
    "# statically - the node builder registers the client, never isinstance-checks.\n"
)


_LIVE_MDC_HANDLERS = """    # cdef handler surface adapters override (data_client.pxd).
    _instrument_provider: Incomplete
    # cdef handlers take the full native arg list; ``*args`` keeps the override
    # surface loose (adapter calls them with venue + payload + timestamps).
    def _handle_data(self, *args: Any, **kwargs: Any) -> None: ...
    def _handle_bars(self, *args: Any, **kwargs: Any) -> None: ...
    def _handle_trade_ticks(self, *args: Any, **kwargs: Any) -> None: ...
    def _handle_quote_ticks(self, *args: Any, **kwargs: Any) -> None: ...
"""


def _run_stubgen(dest: pathlib.Path) -> None:
    # ``uv run --with mypy`` (not ``-m mypy.stubgen``): the with-injected env
    # has no code object for the module under ``sys.executable``. ``dest`` is a
    # fresh temp dir, so a failed run can't clobber the committed tree.
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
            str(dest),
        ],
        cwd=ROOT,
        check=True,
    )


def _strip_self_imports(pkg: pathlib.Path) -> int:
    """Drop ``import X`` when the same stub also declares ``class X``."""
    fixed = 0
    for pyi in pkg.rglob("*.pyi"):
        text = pyi.read_text()
        classes = set(re.findall(r"^class (\w+)", text, re.MULTILINE))
        lines = text.splitlines()
        kept = []
        for line in lines:
            m = re.match(r"^import (\w+)$", line)
            if m and m.group(1) in classes:
                continue
            kept.append(line)
        if len(kept) == len(lines):
            continue
        fixed += len(lines) - len(kept)
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


def _prune_unused_subpackages(pkg: pathlib.Path) -> int:
    """Delete stubs outside the subpackages the checked code uses.

    ``stubgen`` emits the whole package; ty only needs the subpackages our code
    imports (see ``_EXPECTED_TOP_LEVEL``). Anything else is pruned (~300 files /
    ~9.6k lines). The allowlist is also the change-control: if a Nautilus patch
    or new adapter code pulls in a subpackage outside it, ``ty check`` hard-errors
    on the missing module, so the expansion is noticed and the list reviewed.
    """
    removed = 0
    for pyi in pkg.rglob("*.pyi"):
        rel = pyi.relative_to(pkg)
        # ``nautilus_trader/__init__.pyi`` has a single part; keep it. Everything
        # else is ``<subpackage>/...`` and is pruned unless allowlisted.
        if len(rel.parts) > 1 and rel.parts[0] not in _EXPECTED_TOP_LEVEL:
            pyi.unlink()
            removed += 1
    # Drop the now-empty directories (git doesn't track empty dirs, but a clean
    # tree is nicer to inspect after a regenerate). Bottom-up so children are
    # removed before parents; ``os.rmdir`` only succeeds on an empty dir.
    for dirpath, _, _ in os.walk(pkg, topdown=False):
        if dirpath != str(pkg):
            with contextlib.suppress(OSError):
                os.rmdir(dirpath)
    return removed


def _apply_hand_patches(pkg: pathlib.Path) -> int:
    applied = 0

    # The package init imports nautilus_pyo3; nothing the adapter checks needs it.
    init = pkg / "__init__.pyi"
    if init.read_text().strip():
        init.write_text("")
        applied += 1

    component = pkg / "common/component.pyi"
    applied += _patch(
        component,
        "class Component:\n"
        "    fully_qualified_name: ClassVar[method] = ...\n"
        "    __pyx_vtable__: ClassVar[PyCapsule] = ...\n",
        "class Component:\n"
        "    fully_qualified_name: ClassVar[method] = ...\n"
        "    __pyx_vtable__: ClassVar[PyCapsule] = ...\n" + _COMPONENT_CDEF_ATTRS,
        marker="# cdef attribute surface (from component.pxd)",
    )

    data_client = pkg / "data/client.pyi"
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
        "    is_connected: Incomplete\n"
        "    venue: Incomplete\n"
        "    def __init__(self, ClientIdclient_id",
        "    is_connected: Incomplete\n    venue: Incomplete\n"
        + _DATA_CLIENT_ATTRS
        + "    def __init__(self, ClientIdclient_id",
        marker="# cdef readonly Cache _cache",
    )

    exec_client = pkg / "execution/client.pyi"
    applied += _patch(
        exec_client,
        "    def generate_account_state(self, listbalances, listmargins, "
        "boolreported, uint64_tts_event, dictinfo=...) -> void: ...\n",
        _ACCOUNT_STATE_CTOR + "\n",
        marker="# Hand-ported from execution/client.pxd",
    )
    applied += _patch(
        exec_client,
        "LiquiditySideliquidity_side, uint64_tts_event, dictinfo=...) -> void: ...",
        (
            "LiquiditySideliquidity_side, uint64_tts_event, "
            "info: dict | None = ...) -> "
            "void: ..."
        ),
        marker="info: dict | None = ...",
    )

    exec_msgs = pkg / "execution/messages.pyi"
    applied += _patch(
        exec_msgs,
        "    def __init__(self, TraderIdtrader_id, StrategyIdstrategy_id, "
        "Orderorder, UUID4command_id, uint64_tts_init, "
        "PositionIdposition_id: PositionId | None = ..., "
        "ClientIdclient_id=..., dictparams: dict | None = ..., "
        "UUID4correlation_id=...) -> None: ...\n",
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
        rest = text[start + 1 :]
        next_class = re.search(r"\nclass ", rest)
        end = len(text) if next_class is None else start + 1 + next_class.start()
        body = text[start:end]
        patched_body = re.sub(
            r" {4}def __init__\(self, InstrumentIdinstrument_id.*?\) -> None: \.\.\.\n",
            ctor + "\n",
            body,
            count=1,
        )
        if patched_body == body:
            raise SystemExit(f"gen_nautilus_stubs: {cname} ctor anchor missing")
        exec_msgs.write_text(text[:start] + patched_body + text[end:])
        applied += 1

    strategy = pkg / "trading/strategy.pyi"
    text = strategy.read_text()
    # e2e strategy doubles override the order/position handlers; stubgen mangles
    # cdef param names (``OrderAcceptedevent``) so the names must be restored.
    if "# Hand-ported handler param names (strategy.pxd)" not in text:
        text = text.replace(
            "class Strategy(Actor):\n",
            "class Strategy(Actor):\n"
            "    # Hand-ported handler param names (strategy.pxd)\n",
            1,
        )
        text = re.sub(
            r" {4}def (on_\w+)\(self, (\w+)(event)\) -> void: \.\.\.\n",
            r"    def \1(self, \3) -> void: ...\n",
            text,
        )
        text = text.replace(
            "    def submit_order(self, Orderorder, PositionIdposition_id=..., "
            "ClientIdclient_id=..., dictparams=...) -> void: ...\n",
            "    def submit_order(self, order, position_id=..., client_id=..., "
            "params=...) -> void: ...\n",
        )
        text = text.replace(
            "    def submit_order_list(self, OrderListorder_list, "
            "PositionIdposition_id=..., ClientIdclient_id=..., "
            "dictparams=...) -> void: ...\n",
            "    def submit_order_list(self, order_list, position_id=..., "
            "client_id=..., params=...) -> void: ...\n",
        )
        strategy.write_text(text)
        applied += 1

    actor = pkg / "common/actor.pyi"
    text = actor.read_text()
    if "# Hand-ported handler param names (actor.pxd)" not in text:
        text = text.replace(
            "class Actor(Component):\n",
            "class Actor(Component):\n"
            "    # Hand-ported handler param names (actor.pxd)\n",
            1,
        )
        for old, new in (
            (
                "    def on_bar(self, Barbar) -> void: ...\n",
                "    def on_bar(self, bar) -> void: ...\n",
            ),
            (
                "    def on_quote_tick(self, QuoteTicktick) -> void: ...\n",
                "    def on_quote_tick(self, tick) -> void: ...\n",
            ),
            (
                "    def on_trade_tick(self, TradeTicktick) -> void: ...\n",
                "    def on_trade_tick(self, tick) -> void: ...\n",
            ),
            (
                "    def subscribe_quote_ticks(self, InstrumentIdinstrument_id, "
                "ClientIdclient_id=..., boolupdate_catalog=..., "
                "boolaggregate_spread_quotes=..., dictparams=...) -> void: ...\n",
                "    def subscribe_quote_ticks(self, instrument_id, client_id=..., "
                "update_catalog=..., aggregate_spread_quotes=..., params=...) "
                "-> void: ...\n",
            ),
        ):
            if old not in text:
                raise SystemExit(f"gen_nautilus_stubs: actor anchor missing:\n{old}")
            text = text.replace(old, new, 1)
        actor.write_text(text)
        applied += 1

    live_mdc = pkg / "live/data_client.pyi"
    applied += _patch(
        live_mdc,
        "class LiveMarketDataClient(MarketDataClient):\n",
        "class LiveMarketDataClient(MarketDataClient, LiveDataClient):\n"
        + _LIVE_MDC_HANDLERS,
        marker="# cdef handler surface adapters override (data_client.pxd)",
    )
    applied += _patch(
        live_mdc,
        "class LiveMarketDataClient(MarketDataClient, LiveDataClient):\n",
        _LIVE_MDC_LIVEDATACLIENT_HEADER + "\n"
        "class LiveMarketDataClient(MarketDataClient, LiveDataClient):\n",
        marker="# 1.231.x models LiveMarketDataClient(MarketDataClient)",
    )

    futures = pkg / "model/instruments/futures_contract.pyi"
    text = futures.read_text()
    if _FUTURES_CTOR not in text:
        # Idempotent: patch the mangled one-liner ctor and add the type imports.
        text = re.sub(
            r" {4}def __init__\(self, InstrumentIdinstrument_id.*?\) -> None: \.\.\.\n",
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

    data = pkg / "model/data.pyi"
    text = data.read_text()
    if _DATA_DELTAS_CTOR not in text:
        text = re.sub(
            r"    def __init__\(self, InstrumentIdinstrument_id, "
            r"listdeltas\) -> None: \.\.\.\n",
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

    base = pkg / "model/orders/base.pyi"
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


def _install(staged_pkg: pathlib.Path) -> None:
    """Swap ``stubs/nautilus_trader`` for ``staged_pkg`` without a partial tree.

    Both live under ``STUBS`` (same filesystem), so ``os.replace``/``shutil.move``
    are renames. The current tree is moved aside first; if the move-in fails it is
    restored, so a bad generation never leaves the repo without stubs.
    """
    target = STUBS / "nautilus_trader"
    backup = STUBS / "nautilus_trader.old"
    shutil.rmtree(backup, ignore_errors=True)
    had_old = target.exists()
    if had_old:
        os.replace(target, backup)
    try:
        shutil.move(str(staged_pkg), str(target))
    except BaseException:
        if had_old:
            os.replace(backup, target)
        raise
    shutil.rmtree(backup, ignore_errors=True)


def main() -> None:
    # Stage into a temp dir inside ``stubs`` so the final swap is an atomic
    # rename (same filesystem) and a failed ``stubgen``/patch run never touches
    # the committed tree.
    with tempfile.TemporaryDirectory(dir=STUBS, prefix=".gen-") as tmp:
        dest = pathlib.Path(tmp)
        _run_stubgen(dest)
        pkg = dest / "nautilus_trader"
        stripped = _strip_self_imports(pkg)
        applied = _apply_hand_patches(pkg)
        pruned = _prune_unused_subpackages(pkg)
        _install(pkg)
    print(
        f"regenerated stubs under {STUBS / 'nautilus_trader'} "
        f"(dropped {stripped} bogus self-imports, applied {applied} hand-patches, "
        f"pruned {pruned} unused-subpackage stubs)"
    )


if __name__ == "__main__":
    main()
