# Ops runbook (stub)

Filled in during U7. Until then:

1. Close MotiveWave / R|Trader (one session per login).
2. Copy `.env.example` → `.env` with LucidTrading credentials.
3. Set system to `LucidTrading` and gateway `wss://rprotocol.rithmic.com:443`.
4. Run unit tests without network; run `scripts/smoke_lucid_nq.py` only with real creds.

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
