"""Redaction helpers for test output."""

from __future__ import annotations

_SECRET_KEY_MARKERS = ("PASSWORD", "TOKEN", "SECRET")


class RedactedEnv(dict[str, str]):
    """Test env values whose repr never shows secrets.

    pytest prints fixture values in failure tracebacks; a plain dict would put
    ``RITHMIC_PASSWORD`` in the output (AGENTS.md: secrets stay out of errors
    and logs).
    """

    def __repr__(self) -> str:
        shown = {
            key: "***" if any(m in key.upper() for m in _SECRET_KEY_MARKERS) else value
            for key, value in self.items()
        }
        return f"RedactedEnv({shown!r})"

    __str__ = __repr__
