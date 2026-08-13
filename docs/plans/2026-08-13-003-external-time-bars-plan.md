# Plan: Advertise Rithmic EXTERNAL time bars (request + live)

Date: 2026-08-13  
Repo: `rithmic-nt-connect`  
Status: implementing  
Related: Phase 3.1–3.3 / 3.5, Data conventions, `docs/STATUS.md`

See session plan. `STATUS.md` is the only planned-vs-done table.

Live EXTERNAL time bars come from the **history plant** (`subscribe_time_bar_updates`), not the ticker. Request lookback stays `load_time_bars`. Do not advertise `1-SECOND-EXTERNAL`. Do not invent OHLCV. Lucid proof required before STATUS live-EXTERNAL = Done.
