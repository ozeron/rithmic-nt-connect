# MY046 Rithmic access notes (quirk source)

Local research harness (sibling tree):

`../algotrading/quant-guild-work/projects/MY046_motivewave_simple_ls_demo`

Primary docs there: `HARNESS.md`, `FINDINGS.md`, `vendor/parbhatc-rithmic/README.MY046.md`, `scripts/rithmic_smoke.py`.

## Verified gateway profile

| Setting | Working value |
| --- | --- |
| System | `LucidTrading` |
| Gateway | `wss://rprotocol.rithmic.com:443` |
| Use case | Listed front, live ticks, short history |

## Hard-won quirks (carry into Rust client)

1. **Wrong plant/system looks like “permission denied”** — `rituz00100` / “Rithmic Test” fails for LucidTrading prop logins; use LucidTrading + production R|Protocol URL.
2. **One session per login** — close MotiveWave / R|Trader before API sessions.
3. **Expired months** — Rithmic history for expired contracts is empty; use lake/other venues for deep history; Rithmic for listed front / live-adjacent checks.
4. **Continuous roots** — `CME:NQ`-style roots behave like front aliases; roll days diverge from continuous research series (e.g. Databento `NQ.c.0`).
5. **Reference clients** — `async_rithmic` (Python) and vendored [parbhatc/Rithmic](https://github.com/parbhatc/Rithmic) (Node) both work for MD against this profile. Node client explicitly does **not** expose order routing (protos present).

## What MY046 did *not* prove

- Production-safe order routing / OCO / rejects
- Conformance-issued `app_name` (smoke names were used for MD)
- Official redistribution rights for gated protos (keep protos out of public git unless agreement allows)
