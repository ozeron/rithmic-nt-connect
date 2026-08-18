"""Regenerate the local nautilus_trader model stubs and re-apply known fixes.

The installed nautilus_trader ships no stubs for its compiled Cython model
classes, so pyright sees them as ``Unknown``. ``stubgen`` can introspect the
installed modules at runtime, but its output needs three patches:

1. Drop bogus ``import X`` lines where the same stub declares ``class X``
   (stubgen emits both for cdef classes; pyright resolves the broken import).
2. Hand-ported constructor signatures for the classes the adapter constructs
   (stubgen mangles cdef parameter names, e.g. ``InstrumentIdinstrument_id``).
3. ``Order.price`` / ``Order.trigger_price`` declared on the base stub so
   adapters can read them guarded by ``has_price`` / ``has_trigger_price``.
   ``trigger_type`` is deliberately NOT on the base: only stop orders expose
   it, and unguarded access must stay a static error.

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


def _run_stubgen() -> None:
    # ``uv run --with mypy`` (not ``-m mypy.stubgen``): the with-injected env
    # has no code object for the module under ``sys.executable``.
    subprocess.run(
        [
            "uv",
            "run",
            "--with",
            "mypy",
            "stubgen",
            "-p",
            "nautilus_trader.model",
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


def _apply_hand_patches() -> None:
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


def main() -> None:
    _run_stubgen()
    stripped = _strip_self_imports()
    _apply_hand_patches()
    print(f"regenerated stubs under {STUBS} (dropped {stripped} bogus self-imports)")


if __name__ == "__main__":
    main()
