# Scripts

| Script | Purpose | Live? |
|--------|---------|-------|
| `list_systems.py` | Gateway system-name discovery (no login) | No |
| `smoke_lucid_nq.py` | Phase 1 MD smoke (BBO/front) | Yes (MD only) |
| `verify_live_vs_history.py` | Front-month live↔history tick compare | Yes (MD only) |
| `verify_order_dry_run.py` | Phase 2 order-plant connect + subscribe | Dry-run by default; **never** places unless `--live-place` **and** `RITHMIC_ENABLE_TRADING=1` |

Do not run `--live-place` until order-plant / `app_name` conformance is confirmed.
