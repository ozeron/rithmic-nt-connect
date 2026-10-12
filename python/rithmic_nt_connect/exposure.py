"""Venue exposure for a product root, read from the node's own exec session.

A strategy that must know the *venue's* net position and working-order count (restart
reconcile, flat proof before a flatten) used to open a second short-lived session and
drain it on a thread. The exec client already holds both: the PnL plant's
``instrument_pnl`` rows (net per contract) and the bounded ``show_orders`` drain.

``complete`` is the only proof field. ``net`` is ``None`` until the PnL stream has been
observed since the last (re)connect; ``open_orders`` is ``None`` when the drain failed.
Never read ``None`` as zero.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

EXPOSURE_TOPIC_PREFIX = "rithmic.exposure."
EXPOSURE_ENDPOINT = "RithmicExecutionClient.request_exposure"


def exposure_topic(root: str) -> str:
    return f"{EXPOSURE_TOPIC_PREFIX}{root.strip().upper()}"


_CONTRACT = re.compile(r"^(?P<root>[A-Z0-9]+?)[FGHJKMNQUVXZ]\d{1,2}$")


def product_root(symbol: str) -> str:
    """``MNQZ6`` → ``MNQ``; a bare root (``MNQ``) is returned unchanged."""
    sym = symbol.strip().upper()
    m = _CONTRACT.match(sym)
    return m.group("root") if m else sym


def matches_root(symbol: str, root: str) -> bool:
    """``MNQZ6`` belongs to root ``MNQ`` (and exact ``MNQ`` matches itself)."""
    want = root.strip().upper()
    got = symbol.strip().upper()
    return bool(want) and (got == want or got.startswith(want))


@dataclass(frozen=True, slots=True)
class ExposureSnapshot:
    root: str
    net: int | None
    open_orders: int | None
    pnl_ts_ns: int | None
    drain_ts_ns: int | None
    ts_init: int
    # True for the answer to a request (drain attempted); False for the net-only
    # push every PnL row triggers.
    reply: bool = False

    @property
    def complete(self) -> bool:
        """Both legs read from the venue, the drain after the PnL stream was live."""
        return (
            self.net is not None
            and self.open_orders is not None
            and self.pnl_ts_ns is not None
            and self.drain_ts_ns is not None
            and self.drain_ts_ns >= self.pnl_ts_ns
        )


def signed_net(positions: Mapping[str, Mapping[str, Any]], root: str) -> int:
    """Sum of signed ``instrument_pnl`` quantities over every contract of ``root``."""
    net = 0
    for fields in positions.values():
        if not matches_root(str(fields.get("symbol") or ""), root):
            continue
        qty = int(fields.get("quantity") or 0)
        side = str(fields.get("position_side") or "FLAT")
        net += qty if side == "LONG" else (-qty if side == "SHORT" else 0)
    return net


def working_count(rows: Iterable[tuple[str, bool]], root: str) -> int:
    """Count working rows (``(symbol, is_open)``) of ``root``."""
    return sum(1 for symbol, is_open in rows if is_open and matches_root(symbol, root))


__all__ = [
    "EXPOSURE_ENDPOINT",
    "EXPOSURE_TOPIC_PREFIX",
    "ExposureSnapshot",
    "exposure_topic",
    "matches_root",
    "product_root",
    "signed_net",
    "working_count",
]
