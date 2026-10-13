"""``python -m rithmic_gateway`` — start the parent gateway explicitly.

Resolves the bundled ``rithmic-gateway`` binary and replaces this process with
it. The environment is passed through unchanged, so the parent's credentials
(``RITHMIC_PASSWORD`` etc.) come from the launching shell, service unit, or
container. This does not detach, set idle-exit, or edit the environment: the
parent's lifetime is its operator's.
"""

from __future__ import annotations

import os
import sys

from rithmic_gateway.binary import GatewayBinaryError, resolve_gateway_bin


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        binary = resolve_gateway_bin()
    except GatewayBinaryError as exc:
        print(
            f"rithmic_gateway: {exc} (set RITHMIC_GATEWAY_BIN to the "
            f"rithmic-gateway binary)",
            file=sys.stderr,
        )
        return 2
    sys.stdout.flush()
    sys.stderr.flush()
    os.execv(binary, [binary, *args])  # nosec B606 - resolved gateway binary
    return 0  # pragma: no cover - execv does not return


if __name__ == "__main__":
    raise SystemExit(main())
