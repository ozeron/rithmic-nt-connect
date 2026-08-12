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

Do **not** use `nautilus-*` / `nautilus_trader` as a package or repo prefix.
Approved shorthand for compatibility signal: `nt` (e.g. `rithmic-nt-community`).
This repo uses **`rithmic-connect`** (venue-first, same pattern as `mt5-connect`).

## Python runtime seam (important)

- **1.231.x (v1):** out-of-tree Python adapters via `TradingNode.add_data_client_factory` / `add_exec_client_factory` are the supported path for External adapters.
- **Python v2 / LiveNode on published wheels:** as of issue [#4694](https://github.com/nautechsystems/nautilus_trader/issues/4694), no supported out-of-tree adapter seam; factories must be compiled into the PyO3 module. Native Rust binaries can still link external adapters.

**This project Phase 1 targets 1.231.x.**
