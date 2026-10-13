"""The Rust execution rules agree with the Python reference they replaced.

The reference below is the original regex implementation of ``product_root``.
It stays here only as a test oracle: the shipped rule lives in the
``rithmic-exec`` crate, and this differential test fails if the two diverge.
"""

from __future__ import annotations

import random
import re
import string

import pytest
from rithmic_nt_connect.exposure import matches_root, product_root

_CONTRACT = re.compile(r"^(?P<root>[A-Z0-9]+?)[FGHJKMNQUVXZ]\d{1,2}$", re.ASCII)


def _reference_root(symbol: str) -> str:
    sym = symbol.strip().upper()
    m = _CONTRACT.match(sym)
    return m.group("root") if m else sym


def _corpus() -> list[str]:
    rng = random.Random(20261013)
    alphabet = "ESMNQZFGHJKUVX6E0159 -_."
    fixed = [
        "MNQZ6",
        "MNQ",
        "ESH27",
        "NQU6",
        "6EH7",
        "ESZ123",
        "Z6",
        "6",
        "",
        " ",
        "ES-Z6",
        "ESA6",
        "esh7",
        "  MNQz6  ",
        "F12",
        "AF1F2",
        "ABF12",
        "ABF123",
    ]
    random_cases = [
        "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 7)))
        for _ in range(4000)
    ]
    return fixed + random_cases


def test_product_root_matches_the_reference() -> None:
    mismatches = [
        (symbol, product_root(symbol), _reference_root(symbol))
        for symbol in _corpus()
        if product_root(symbol) != _reference_root(symbol)
    ]
    assert not mismatches, mismatches[:5]


def test_matches_root_is_the_reference_comparison() -> None:
    rng = random.Random(7)
    symbols = _corpus()[:400]
    for symbol in symbols:
        for _ in range(3):
            root = "".join(
                rng.choice(string.ascii_uppercase) for _ in range(rng.randint(0, 3))
            )
            want = root.strip().upper()
            expected = bool(want) and _reference_root(symbol) == want
            assert matches_root(symbol, root) is expected, (symbol, root)


# -- order-notification kind ---------------------------------------------------


def _reference_kind(source, name, status=None, text=None):
    """The original Python mapping, kept as an oracle."""
    name = name.upper()
    if source == "rithmic":
        if name in ("OPEN", "TRIGGER_PENDING"):
            return "accepted"
        if name == "MODIFIED":
            return "updated"
        if name == "MODIFICATION_FAILED":
            return "modify_rejected"
        if name == "CANCELLATION_FAILED":
            return "cancel_rejected"
        if name == "COMPLETE":
            if str(status or "").upper() in {"CANCELLED", "CANCELED"}:
                return "canceled"
            return "rejected" if str(text or "").strip() else None
        return None
    if source == "exchange":
        return {
            "FILL": "filled",
            "REJECT": "rejected",
            "CANCEL": "canceled",
            "TRIGGER": "triggered",
            "NOT_MODIFIED": "modify_rejected",
            "NOT_CANCELLED": "cancel_rejected",
            "NOT_CANCELED": "cancel_rejected",
        }.get(name)
    return None


def test_kind_from_notify_matches_the_reference() -> None:
    from rithmic_nt_connect._orders import kind_from_notify

    names = [
        "OPEN",
        "open",
        "TRIGGER_PENDING",
        "MODIFIED",
        "MODIFICATION_FAILED",
        "CANCELLATION_FAILED",
        "COMPLETE",
        "OPEN_PENDING",
        "FILL",
        "REJECT",
        "CANCEL",
        "TRIGGER",
        "NOT_MODIFIED",
        "NOT_CANCELLED",
        "NOT_CANCELED",
        "",
        "WHATEVER",
    ]
    statuses = [None, "", "complete", "cancelled", "CANCELED", "Open", 0]
    texts = [None, "", "  ", "No such route exists."]
    for source in ("rithmic", "exchange", "other"):
        for name in names:
            for status in statuses:
                for text in texts:
                    assert kind_from_notify(source, name, status, text) == (
                        _reference_kind(source, name, status, text)
                    ), (source, name, status, text)


# -- instrument id codec -------------------------------------------------------


def _reference_id(symbol, exchange=None):
    sym = str(symbol).strip()
    if "-" in sym or "." in sym:
        return ("err", "symbol")
    if not sym:
        return ("err", "symbol")
    if exchange is not None and str(exchange).strip():
        exch = str(exchange).strip().upper()
        if "-" in exch or "." in exch:
            return ("err", "exchange")
        return ("ok", f"{sym}-{exch}.RITHMIC")
    return ("ok", f"{sym}.RITHMIC")


def test_instrument_id_codec_matches_the_reference() -> None:
    from rithmic_nt_connect._convert import (
        ConvertError,
        instrument_id_from_symbol,
        symbol_and_exchange_from_instrument_id,
    )

    rng = random.Random(11)
    alphabet = "NQU6cmeCBOT -._"
    cases = [
        (
            "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 6))),
            rng.choice(
                [
                    None,
                    "",
                    "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 4))),
                ]
            ),
        )
        for _ in range(3000)
    ]
    for symbol, exchange in cases:
        kind, expected = _reference_id(symbol, exchange)
        if kind == "ok":
            got = instrument_id_from_symbol(symbol, exchange)
            assert got == expected, (symbol, exchange)
            sym, exch = symbol_and_exchange_from_instrument_id(got)
            assert sym == symbol.strip()
            assert exch == (
                exchange.strip().upper() if exchange and exchange.strip() else None
            )
        else:
            with pytest.raises(ConvertError, match=expected):
                instrument_id_from_symbol(symbol, exchange)


# -- fill identity -------------------------------------------------------------


def _reference_trade_id(fields, ts_event):
    if fields.get("fill_id"):
        return str(fields["fill_id"])
    basket = fields.get("basket_id") or ""
    exch = fields.get("exchange_order_id") or ""
    return (
        f"{basket}:{exch}:{ts_event}:{fields.get('fill_size')}:"
        f"{fields.get('fill_price')}"
    )


def test_fill_identity_matches_the_reference() -> None:
    from rithmic_nt_connect._orders import fill_dedup_key, trade_id_from_fill_fields

    values = [None, "", "A1", 0, 1, 2.0, 21000.25, "x:y"]
    for fill_id in values:
        for basket in (None, "", "B1", 7):
            for size in (None, 1, 2.0, "3"):
                for price in (None, 21000.25, 0.0, "1.50"):
                    for acct in (None, "ACC"):
                        fields = {
                            "fill_id": fill_id,
                            "basket_id": basket,
                            "exchange_order_id": "X9",
                            "fill_size": size,
                            "fill_price": price,
                            "account_id": acct,
                            "instrument_id": "MNQZ6-CME.RITHMIC",
                        }
                        want_id = _reference_trade_id(fields, 1700)
                        assert trade_id_from_fill_fields(fields, 1700) == want_id
                        assert fill_dedup_key(fields, ts_event=1700) == (
                            f"{acct or ''}|MNQZ6-CME.RITHMIC|{want_id}"
                        )


# -- bounded recency cache -----------------------------------------------------


class _ReferenceCache:
    """The original ``OrderedDict`` LRU, kept as an oracle."""

    def __init__(self, max_size: int) -> None:
        from collections import OrderedDict

        self._entries: OrderedDict[str, object] = OrderedDict()
        self._max = max_size

    def has_seen(self, key: str) -> bool:
        if key in self._entries:
            self._entries.move_to_end(key)
            return True
        return False

    def mark(self, key: str, value: object = None) -> None:
        self._entries[key] = value
        self._entries.move_to_end(key)
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)

    def get(self, key: str, default: object = None) -> object:
        if key not in self._entries:
            return default
        self._entries.move_to_end(key)
        return self._entries[key]

    def __len__(self) -> int:
        return len(self._entries)


def test_seen_key_cache_matches_the_reference_under_random_operations() -> None:
    from rithmic_nt_connect._orders import SeenKeyCache

    rng = random.Random(3)
    for max_size in (0, 1, 3, 8):
        real, ref = SeenKeyCache(max_size), _ReferenceCache(max_size)
        for step in range(2000):
            key = f"k{rng.randint(0, 12)}"
            op = rng.choice(["mark", "has", "get"])
            if op == "mark":
                real.mark(key, step)
                ref.mark(key, step)
            elif op == "has":
                assert real.has_seen(key) == ref.has_seen(key), (max_size, step)
            else:
                assert real.get(key, "miss") == ref.get(key, "miss"), (max_size, step)
            assert len(real) == len(ref), (max_size, step)
