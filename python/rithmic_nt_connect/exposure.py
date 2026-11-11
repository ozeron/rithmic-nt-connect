"""Venue exposure for a product root, read from the node's own exec session.

A strategy that must know the *venue's* net position and working-order count (restart
reconcile, flat proof before a flatten) used to open a second short-lived session and
drain it on a thread. The exec client already holds both: the PnL plant's
``instrument_pnl`` rows (net per contract) and the bounded ``show_orders`` drain.

``complete`` is the only proof field. ``net`` is ``None`` until the PnL stream has been
observed since the last (re)connect; ``open_orders`` is ``None`` when the drain failed.
Never read ``None`` as zero.

NautilusTrader 2 gives adapter clients no message bus, so snapshots travel through the
process-wide ``EXPOSURE_HUB``: the exec client registers as its responder on connect and
pushes a snapshot on every PnL position row; strategies ``subscribe(root, handler)`` and
``await EXPOSURE_HUB.request(root)``. Everything runs on the node's event loop.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from rithmic_nt_connect import _lib

# The rules (root matching, net, completeness) live in the ``rithmic-exec`` crate.
# The PnL plant answers a (re)subscribe with a burst: an account row may land before
# the instrument rows. A drain is proof only once the stream has been live this long.
PNL_SETTLE_NS: int = _lib.PNL_SETTLE_NS


def _root_key(root: str) -> str:
    return root.strip().upper()


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


ExposureHandler = Callable[[ExposureSnapshot], None]
ExposureResponder = Callable[[str], Awaitable[ExposureSnapshot]]


class ExposureHub:
    """In-process exposure push + request/reply (replaces the v1 msgbus topic).

    ``subscribe(root, handler)`` receives every snapshot for ``root`` (``"*"`` for
    all roots). ``request(root)`` asks the registered responder (the node's Rithmic
    exec client) for a fresh drained snapshot; it raises ``LookupError`` when no
    exec client is connected. One responder per process: the most recently
    connected exec client answers (a client that died without disconnecting must
    not keep answering).
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[ExposureHandler]] = {}
        self._responder: ExposureResponder | None = None

    def subscribe(self, root: str, handler: ExposureHandler) -> None:
        handlers = self._handlers.setdefault(_root_key(root), [])
        if handler not in handlers:
            handlers.append(handler)

    def unsubscribe(self, root: str, handler: ExposureHandler) -> None:
        handlers = self._handlers.get(_root_key(root), [])
        if handler in handlers:
            handlers.remove(handler)

    def publish(self, snap: ExposureSnapshot) -> None:
        for key in (_root_key(snap.root), "*"):
            for handler in list(self._handlers.get(key, ())):
                handler(snap)

    def register_responder(self, responder: ExposureResponder) -> bool:
        """Make ``responder`` answer requests; ``False`` if it replaced another."""
        replaced = self._responder is not None and self._responder != responder
        self._responder = responder
        return not replaced

    def reset(self) -> None:
        """Drop every subscriber and the responder (tests, node teardown)."""
        self._handlers.clear()
        self._responder = None

    def unregister_responder(self, responder: ExposureResponder) -> None:
        if self._responder == responder:
            self._responder = None

    @property
    def has_responder(self) -> bool:
        return self._responder is not None

    async def request(self, root: str) -> ExposureSnapshot:
        if self._responder is None:
            raise LookupError("no Rithmic exec client is connected to answer exposure")
        return await self._responder(_root_key(root))


EXPOSURE_HUB = ExposureHub()


__all__ = [
    "EXPOSURE_HUB",
    "PNL_SETTLE_NS",
    "ExposureHub",
    "ExposureSnapshot",
    "matches_root",
    "product_root",
    "signed_net",
]
