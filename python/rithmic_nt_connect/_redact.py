"""Safe diagnostic representations; explicit string/JSON serialization stays raw."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping


def secret_key(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in ("PASSWORD", "TOKEN", "SECRET")) or (
        upper == "PW" or upper.endswith("_PW")
    )


class SecretStr(str):
    """A wire-compatible string whose repr (including containers) hides its value.

    ``str(value)``, formatting and serialization deliberately expose the value
    for credential APIs. Output boundaries must still scrub those plain strings.
    """

    def __repr__(self) -> str:
        return "'***'"


class RedactedEnv(dict[str, str]):
    """Environment mapping safe to inspect, including extracted values and copies."""

    def __init__(self, values: Mapping[str, str]) -> None:
        super().__init__(
            (key, SecretStr(value) if secret_key(key) else value)
            for key, value in values.items()
        )

    def __repr__(self) -> str:
        shown = {
            key: "***" if secret_key(key) else value for key, value in self.items()
        }
        return f"RedactedEnv({shown!r})"

    __str__ = __repr__


def redact_text(text: str, secrets: Iterable[str]) -> str:
    """Scrub exact values and common Python/JSON/bytes escaped representations."""
    variants: set[str] = set()
    for value in secrets:
        value = str(value)
        if value:
            variants.update(
                (
                    value,
                    repr(value)[1:-1],
                    json.dumps(value)[1:-1],
                    repr(value.encode())[2:-1],
                )
            )
    if not variants:
        return text
    # One pass, longest first: overlapping credentials cannot expose a suffix.
    pattern = "|".join(
        re.escape(value) for value in sorted(variants, key=len, reverse=True)
    )
    return re.sub(pattern, "***", text)
