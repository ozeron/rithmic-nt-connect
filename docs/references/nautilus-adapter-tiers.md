# Nautilus adapter tiers and out-of-tree notes

Canonical upstream doc (in a NautilusTrader checkout): `ADAPTERS.md`.

## Tiers

| Tier | Meaning |
| --- | --- |
| Official | In-tree / nautechsystems org; maintainer-supported |
| Community | Third-party, listed in `ADAPTERS.md` after criteria review |
| External | Third-party, not listed |

## Community listing criteria (summary)

- Trademark/disclaimer compliance (`TRADEMARK.md`)
- OSS license compatible with LGPL v3.0
- Identifiable maintainer
- Activity within last six months
- Install/usage docs

Example listing: [mt5-connect](https://github.com/aulekator/mt5-connect).

## Naming (trademark)

Upstream: [TRADEMARK.md](https://github.com/nautechsystems/nautilus_trader/blob/develop/TRADEMARK.md)
([policy page](https://nautilustrader.io/legal/trademark-policy/)). Community listing:
[ADAPTERS.md](https://github.com/nautechsystems/nautilus_trader/blob/develop/ADAPTERS.md).

- Do **not** use `nautilus-*` / `nautilus_trader` as a package or repo prefix (`nautilus-*` is reserved).
- Official not-compliant examples: `nautilus-mt5`, `nautilus-sinopac`.
- Official compliant examples / listed community adapters: `mt5-connect`, `mt5-nt-community`, `sinopac-nt-community`.
- Approved compatibility shorthand: `nt`.

This repo uses **`rithmic-nt-connect`** (import / crate lib: `rithmic_nt_connect`).

## Python runtime seam (important)

- **1.231.x (v1):** out-of-tree Python adapters via `TradingNode.add_data_client_factory` / `add_exec_client_factory` are the supported path for External adapters.
- **Python v2 / LiveNode on published wheels:** as of issue [#4694](https://github.com/nautechsystems/nautilus_trader/issues/4694), no supported out-of-tree adapter seam; factories must be compiled into the PyO3 module. Native Rust binaries can still link external adapters.

**This project Phase 1 targets 1.231.x.**
