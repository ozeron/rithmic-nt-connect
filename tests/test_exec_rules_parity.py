"""The Rust execution rules agree with the Python reference they replaced.

The reference below is the original regex implementation of ``product_root``.
It stays here only as a test oracle: the shipped rule lives in the
``rithmic-exec`` crate, and this differential test fails if the two diverge.
"""

from __future__ import annotations

import random
import re
import string

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
