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

- **1.231.x (v1):** out-of-tree Python adapters via `TradingNode.add_data_client_factory` / `add_exec_client_factory`.
- **2.0 (`LiveNode`):** out-of-tree Python clients subclass `nautilus_trader.live.clients.MarketDataClient` / `ExecutionClient`; factories subclass `DataClientFactory` / `ExecutionClientFactory` and are passed to `LiveNode.build(..., data_factories=, exec_factories=)` (or `LiveNodeBuilder.add_data_client`). The node treats a factory as Python only if it subclasses those bases. Clients see a read-only `ClientCache` and no message bus; their output is queued and applied by the core. Custom Python clients get no Redis/Postgres cache backing. Earlier wheels had no such seam (issue [#4694](https://github.com/nautechsystems/nautilus_trader/issues/4694)).

**This project targets 2.0.0rc9** (hard cut 2026-11-10; 1.231 history stays in git).
