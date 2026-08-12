# Upstream RFC #3768 — Rithmic adapter (closed)

- Issue: https://github.com/nautechsystems/nautilus_trader/issues/3768
- Related PR (closed, not merged): https://github.com/nautechsystems/nautilus_trader/pull/3767
- Closed by: cjdsellers (2026-04-02)

## Maintainer rationale (structural, not technical)

1. **Venue access model** — Rithmic is an intermediary between traders and exchanges; differs from direct exchange/broker APIs Nautilus official adapters assume.
2. **Gated API specification** — protobufs, SSL certs, and connection URIs sit behind a signed agreement; not freely inspectable/redistributable for OSS contributors.
3. **Per-application conformance** — Rithmic issues an application-scoped four-character prefix / `app_name`; material changes may require re-certification.
4. **Maintenance burden** — ongoing vendor coordination unlike public APIs.

## Implication for this project

Do **not** target official inclusion in `nautechsystems/nautilus_trader`. Ship as an **External** (optionally later **Community**-listed) adapter per Nautilus `ADAPTERS.md`.

Rejected RFC does **not** remove ecosystem legitimacy for an out-of-tree adapter.
