# RPC inventory (task 1.1) — seed of the RPC table

Source: `#[pymethods] impl PySession` in `crates/rithmic-nt-connect/src/python/mod.rs`
(the direct surface), the `WireSession` protocol in `python/rithmic_nt_connect/session.py`,
and `GatewayWireSession` (the gateway surface). Gate column follows the
`session-backend` spec ("Trading gate parity"). Kind follows AGENTS.md "gateway RPC kinds".

Kinds: **OneShot** command, **Intent** (subscription intent: fan-out, `note_*` refcount,
plant on 0→1, rollback, restore), **Read** (idempotent query, no intent), **Life**
(lifecycle), **Poll** (event drain).
Gates: `-` none, `T` trading, `C` cancel-all (independent of `T`).

| Trait method (PyO3 name) | Kind | Gate | Notes |
| --- | --- | --- | --- |
| `connect` / `disconnect` | Life | - | `disconnect` takes the order-op gate (see design 11) |
| `request_plants(plants)` | Life | - | attaches extra plants while connected |
| `ensure_order_plant` / `ensure_pnl_plant` | Life | - | order plant implies account resolution |
| `disconnect_order_plant` / `disconnect_pnl_plant` | Life | - | order variant takes the order-op gate |
| `reset_ticker` (= plant `reset_ticker_plant`) | Life | - | refcount-blind; gateway re-issues intents |
| `resolved_account` | Read | - | local, no RPC; `None` until ensure |
| `subscribe` / `unsubscribe` (ticker) | Intent | - | |
| `subscribe_order_book_summary` / `unsubscribe_…` | Intent | - | |
| `subscribe_time_bars` / `unsubscribe_time_bars` | Intent | - | `(symbol, exchange, bar_type, period)` |
| `subscribe_pnl` | Intent | - | |
| `subscribe_order_updates` | Intent | T | rides the order-plant stream |
| `subscribe_bracket_updates` | Intent | T | rides the order-plant stream |
| `get_front_month` / `get_reference_data` | Read | - | |
| `load_ticks` / `load_time_bars` / `probe_time_bars` | Read | - | refused while live ticker intent (parent) |
| `load_orders(start, end)` | Read | T | working-order load |
| `load_product_rms_info` / `load_account_rms_info` | Read | T | order-plant RPC |
| `place_order` | OneShot | T | no retry; post-send failure = unknown |
| `place_bracket_order` | OneShot | T | `localid` / `stop_ticks` / `target_ticks` |
| `adjust_bracket_stop` / `adjust_bracket_target` | OneShot | T | `level` optional |
| `cancel_order` / `modify_order` | OneShot | T | no retry |
| `cancel_all_orders` | OneShot | C | plant-wide; own flag |
| `poll_event` | Poll | - | ticker/book/bbo/last-trade |
| `poll_history_event` | Poll | - | time bars |
| `poll_pnl_event` | Poll | - | |
| `poll_order_event` | Poll | - | order + bracket notifications |

Direct-only (`HistoryReplay` trait, never on `GatewaySession`):
`load_order_history(date)`, `load_fill_history(start, finish)`, `load_order_history_dates()`.

Not part of the trait (module level): `list_systems(url)`; the `Session.new(...)` constructor
(becomes backend selection from `connect_mode`).

Client-only on the gateway surface (no direct analog; owned by `GatewaySession`, not the trait):
`register_transport_generation_listener`, `subscribe_transport_events`,
`ensure_transport_live`, `trading_enabled`, `cancel_all_enabled`.

## Discrepancies found while inventorying

1. **Poll semantics differ.** Direct `poll_*` is non-blocking (`try_recv`, `now_or_never`);
   `_FlockedDirectSession.poll_event` discards `timeout_ms`. Gateway `poll_event(timeout_ms)`
   blocks up to the timeout. The trait must carry an explicit timeout (Direct honors 0 as
   today). See design decision 11.
2. **Order-op safety lives in the PyO3 class**, not in `rithmic-plants`: `session_ops` mutex,
   `disconnecting` flag, `acquire_order_handle` (clone the handle under a short lock so I/O
   does not block `poll_order_event`). It must move into `DirectSession`. See design decision 12.
3. **`reset_ticker` vs `reset_ticker_plant`**: PyO3 exposes one name, the gateway wire exposes
   both (they are now the same call after `simplify-gateway-modes`). The trait gets one method.
