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

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from rithmic_nt_connect import _lib

# The rules (root matching, net, completeness) live in the ``rithmic-exec`` crate.
# The PnL plant answers a (re)subscribe with a burst: an account row may land before
# the instrument rows. A drain is proof only once the stream has been live this long.
PNL_SETTLE_NS: int = _lib.PNL_SETTLE_NS
EXPOSURE_TOPIC_PREFIX = "rithmic.exposure."
EXPOSURE_ENDPOINT = "RithmicExecutionClient.request_exposure"


def exposure_topic(root: str) -> str:
    return f"{EXPOSURE_TOPIC_PREFIX}{root.strip().upper()}"


def product_root(symbol: str) -> str:
    """``MNQZ6`` → ``MNQ``; a bare root (``MNQ``) is returned unchanged."""
    return _lib.product_root(symbol)


def matches_root(symbol: str, root: str) -> bool:
    """``MNQZ6`` belongs to root ``MNQ`` (exact root, never a prefix)."""
    return _lib.matches_root(symbol, root)


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
        """Both legs read from the venue, the drain after the PnL burst settled."""
        return _lib.exposure_complete(
            self.net, self.open_orders, self.pnl_ts_ns, self.drain_ts_ns
        )


def signed_net(positions: Mapping[str, Mapping[str, Any]], root: str) -> int:
    """Sum of signed ``instrument_pnl`` quantities over every contract of ``root``."""
    rows = [
        (
            str(fields.get("symbol") or ""),
            int(fields.get("quantity") or 0),
            str(fields.get("position_side") or "FLAT"),
        )
        for fields in positions.values()
    ]
    return _lib.signed_net(rows, root)


__all__ = [
    "EXPOSURE_ENDPOINT",
    "EXPOSURE_TOPIC_PREFIX",
    "PNL_SETTLE_NS",
    "ExposureSnapshot",
    "exposure_topic",
    "matches_root",
    "product_root",
    "signed_net",
]
