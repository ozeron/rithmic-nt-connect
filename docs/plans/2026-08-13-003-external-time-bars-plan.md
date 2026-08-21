# Plan: Advertise Rithmic EXTERNAL time bars (request + live)

Date: 2026-08-13  
Repo: `rithmic-nt-connect`  
Status: implementing  
Related: Phase 3.1–3.3 / 3.5, Data conventions, `docs/STATUS.md`

See session plan. `STATUS.md` is the only planned-vs-done table.

Live EXTERNAL time bars come from the **history plant** (`subscribe_time_bar_updates`), not the ticker. Request lookback stays `load_time_bars`. Do not advertise `1-SECOND-EXTERNAL`. Do not invent OHLCV. Lucid proof required before STATUS live-EXTERNAL = Done.

## Findings: EXTERNAL fetch is symbol‑entitlement dependent (2026-08-19)

Empirical test (gateway mode sharing the live `rithmic‑gateway` login, RTH,
`LucidTrading` / `TEST001` account). Harness: raw `connect_market_data_session`
+ `subscribe_time_bars`/`poll_history_event`, and a Nautilus `TradingNode` +
`BarPrinter` with no exec clients.

| Fetch method | Symbol | Result |
|--------------|--------|--------|
| `subscribe_time_bars` (live) | **NQ** 1m EXT | streams (1 `time_bar` evt / 45s) |
| `subscribe_time_bars` (live) | **MNQ** 1m EXT | **0 events in 75s** — no live stream |
| `load_time_bars` / `request_bars` (history) | NQ 1m EXT | works |
| `load_time_bars` / `request_bars` (history) | **MNQ** 1m EXT | works — 30 bars, correct volume (1.6k–4.0k/1m) |

**Conclusion:** the connector's EXTERNAL mechanism is correct, but Rithmic only
streams EXTERNAL time bars **live for NQ** (and likely other liquid roots) — **not
for MNQ** — on this account. MNQ EXTERNAL is available **only via the history API**.

**History‑path freshness (measured, MNQ 1m EXT, 5 s poll, 28 samples):**
- Only **CLOSED** bars are returned (never an in‑progress / partial‑volume bar) → volume is always final.
- A 1m bar is available **~1–4 s after it closes**; with a 5 s poll, total delay ≈ **5–10 s** after each 1m bar closes.
- Volume is correct (e.g. 5199, 3608, 3017 per 1m bar) — unthrottled, unlike INTERNAL.

### How to fetch external data reliably
- **NQ** (and symbols that stream): `subscribe_time_bars` (live) + poll — works.
- **MNQ** (and symbols that do NOT stream live): use `load_time_bars` / `request_bars` on a **rolling poll** (every 3–5 s, last ~3 min) and consume only **NEW closed bars**. This yields correct‑volume EXTERNAL bars with ~5–10 s lag.

### Implication for the connector
`subscribe_bars(EXTERNAL)` is **not** a universal live source. Strategies needing
EXTERNAL for a non‑streaming symbol must fall back to the history‑poll path. The
`STATUS.md` "live‑EXTERNAL = Done" line should be scoped to streaming‑entitled
symbols (NQ proven live 2026‑08‑14 via `test_tc_d40`); **MNQ is history‑only**.
Proven via `qgw_book` MY043 reconciliation (doc `003 §13`/`§13.1`): MY043 cannot
use live EXTERNAL for MNQ and must roll `request_bars(m1_ext)`.
