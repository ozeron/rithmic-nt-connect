# Live execution adapter review checklist

Review checklist for the out-of-tree Rithmic/Nautilus 1.231.x connector. This is
an implementation and pre-live review surface, not a claim that every item is
currently complete.

The checklist is grounded in Nautilus adapter conventions and public live
adapter incidents:

- [Nautilus adapter conventions](nautilus-adapter-conventions.md)
- [Nautilus issue #3812: market-style stop `OrderTriggered`](https://github.com/nautechsystems/nautilus_trader/issues/3812)
- [Nautilus issue #4415: standardize adapter execution/WebSocket flows](https://github.com/nautechsystems/nautilus_trader/issues/4415)
- [Nautilus issue #3176: duplicate orders after restart/reconciliation](https://github.com/nautechsystems/nautilus_trader/issues/3176)
- [Nautilus issue #3476: partial fills double-counted by reconciliation](https://github.com/nautechsystems/nautilus_trader/issues/3476)
- [Nautilus issue #3450: duplicate fill `trade_id`](https://github.com/nautechsystems/nautilus_trader/issues/3450)
- [Nautilus issue #4682: algo trigger does not promote venue order ID](https://github.com/nautechsystems/nautilus_trader/issues/4682)
- [Nautilus issue #4215: retry after acceptance duplicates an order](https://github.com/nautechsystems/nautilus_trader/issues/4215)
- [Nautilus issue #4729: failed position query treated as empty success](https://github.com/nautechsystems/nautilus_trader/issues/4729)
- [Nautilus issue #4617: failed default-routed position query looks flat](https://github.com/nautechsystems/nautilus_trader/issues/4617)
- [Nautilus issue #4228: degraded connection data-integrity failures](https://github.com/nautechsystems/nautilus_trader/issues/4228)
- [Nautilus issue #3651: venue client-order-ID length limit](https://github.com/nautechsystems/nautilus_trader/issues/3651)
- [Nautilus issue #4564: duplicate broker orders after modify/connectivity loss](https://github.com/nautechsystems/nautilus_trader/issues/4564)
- [Nautilus issue #4440: filled market order with missing `avg_px`](https://github.com/nautechsystems/nautilus_trader/issues/4440)

## Review protocol

For each unchecked item, record:

- code path and file/line;
- venue fixture or captured payload;
- test name and command;
- known limitation or explicit deferment;
- operational mitigation if the item is not complete.

Do not mark an item complete from a unit test alone when the property depends on
wire ordering, reconnect, gateway mode, or a real venue response. Use a captured
fixture plus a mock-transport/integration test where possible.

## 1. Event semantics and order-state safety

### Triggerable order events

- [x] `STOP_MARKET + TRIGGER` does **not** emit Nautilus `OrderTriggered`.
      (`test_trigger_notification_emission_guarded_by_order_type`)
- [x] `MARKET_IF_TOUCHED + TRIGGER` does **not** emit Nautilus `OrderTriggered`.
      (`test_trigger_notification_emission_guarded_by_order_type`)
- [x] `STOP_LIMIT + TRIGGER` emits `OrderTriggered` exactly once.
      (`test_trigger_notification_emission_guarded_by_order_type`, `test_duplicate_trigger_emits_once`)
- [x] `TRAILING_STOP_LIMIT + TRIGGER` emits `OrderTriggered` exactly once.
      (`test_trigger_notification_emission_guarded_by_order_type`)
- [x] `LIMIT_IF_TOUCHED + TRIGGER` emits `OrderTriggered` exactly once.
      (`test_trigger_notification_emission_guarded_by_order_type`)
- [x] A market-style stop follows `ACCEPTED -> FILLED`; it does not require a
      `TRIGGERED` intermediate state. (`test_stop_market_trigger_then_fill_still_emits_fill`)
- [x] A trigger notification followed by a fill does not produce an execution
      error or position discrepancy. (`test_stop_market_trigger_then_fill_still_emits_fill`)
- [x] A fill without a preceding trigger is valid and processed.
      (`test_stop_market_trigger_then_fill_still_emits_fill`, `test_stale_trigger_after_fill_suppressed`)
- [x] A fill arriving before a trigger is valid; a later stale trigger is
      suppressed. (`test_stale_trigger_after_fill_suppressed`, `test_duplicate_trigger_emits_once`)
- [x] Trigger handling is guarded at every producer: live notification,
      reconciliation, query, and reconnect replay. (order-type + `is_closed` + already-`TRIGGERED` at the single emit point)

Reference: #3812 and commit
[`2f7d3947`](https://github.com/nautechsystems/nautilus_trader/commit/2f7d3947af983b707f7910d78c447ff4c8e493cb).

### Live events versus reports

- [ ] Tracked own-order notifications produce typed live order events:
      `OrderAccepted`, `OrderFilled`, `OrderCanceled`, `OrderUpdated`, or
      `OrderRejected`.
- [ ] Query/reconciliation responses use `OrderStatusReport`, `FillReport`, or
      `PositionStatusReport` rather than replaying normal live events blindly.
- [ ] Untracked/external orders are published as reports, not assigned invented
      strategy/client ownership.
- [ ] A report cannot cause a duplicate live event for the same venue execution.
- [ ] `OrderStatusReport.reconciliation` / external identity is preserved where
      the Nautilus version exposes it.

Reference: #4415 and `nautilus-adapter-conventions.md` execution ownership rules.

## 2. Identity, correlation, and deduplication

### Client and venue order identity

- [ ] Register client-order context before sending a command that can produce an
      immediate inbound notification.
- [ ] `user_tag`/client-order-ID round-trips through the venue and `load_orders`.
- [ ] Venue limits on client-order-ID length and allowed characters are tested.
- [ ] Client-order-ID mapping survives reconnect and process restart.
- [ ] An unresolved venue identity is not silently treated as a tracked order.
- [ ] A venue/algo child order can promote its actual venue order ID before later
      fills or cancels.
- [ ] A stale old venue-order incarnation cannot overwrite a newer working
      incarnation.
- [ ] Cancel and modify commands target the currently live venue order ID.

Reference: #3651 and #4682.

### Fill identity

- [ ] Prefer the venue execution/trade/match ID as the fill dedup key.
- [ ] Include account and instrument when the venue execution ID is not globally
      unique.
- [ ] Two distinct partial fills cannot receive the same `TradeId`.
- [ ] The same fill arriving through WebSocket, query, replay, and reconciliation
      is applied once.
- [ ] A dedup key is consumed only after parsing, routing, and event publication
      succeed.
- [ ] Duplicate fill notifications are harmless after order/position restart.
- [x] Fill quantity is capped or rejected safely when the venue reports an
      impossible overfill. (emitted at true size — never dropped/capped; Nautilus clamps `leaves_qty` and tracks `overfill_qty`; plant latched for a recon re-sync: `test_tracked_overfill_emits_latches_and_logs`)

References: #3450 and #3221.

## 3. Submit, cancel, modify, and retry behavior

- [ ] Pre-send validation failure emits `OrderDenied`, not `OrderSubmitted` plus
      a fake venue rejection.
- [ ] After transmission, timeout/disconnect/5xx/unknown response remains an
      unknown in-flight state, not a terminal rejection.
- [ ] A retry is canceled when `OrderAccepted`, `OrderFilled`, `OrderRejected`,
      or another definitive result arrives.
- [ ] State-changing commands are not blindly retried.
- [x] A timed-out submit can be recovered by client tag, venue order ID, or
      bounded status query. (bounded per-`user_tag` drain binds the venue id only after the row proves usable, carries the venue row's `ts_event`, and never regresses a live-resolved order: `test_unbound_submitted_query_recovers_by_tag_from_drain`, `test_unusable_drain_row_does_not_bind_or_disable_recovery`, `test_recovered_report_uses_venue_row_timestamp`, `test_stale_drain_row_does_not_regress_live_resolution`)
- [ ] A timed-out cancel does not cause the adapter to assume the order is gone.
- [ ] A timed-out modify cannot create two venue orders without detection.
- [ ] Cancel-rejected means the working order remains working unless the venue
      provides a definitive terminal state.
- [x] Cancel-after-fill and fill-after-cancel races are handled idempotently.
      (`test_cancel_after_fill_race_idempotent`, `test_fill_after_cancel_race_idempotent`)
- [ ] Stop/target OCO cancellation does not cancel the wrong leg.

References: #4215, #4564, and the three-evidence-class rules in
`nautilus-adapter-conventions.md`.

## 4. Reconciliation and position safety

### Query failure semantics

- [ ] Successful query with no positions returns an explicit empty snapshot.
- [ ] Failed query returns an exception or explicit failure, never `[]` as if the
      venue were flat.
- [ ] HTTP 5xx, timeout, disconnect, decode failure, and account failure are
      distinguishable from a valid empty response.
- [ ] The execution engine skips reconciliation for a failed venue cycle.
- [x] A position report failure cannot generate an inferred flattening fill.
      (`test_position_query_failure_emits_no_fills`)
- [ ] A missing order report cannot be interpreted as a cancellation when the
      venue query was incomplete.
- [x] Reconciliation failure leaves the strategy frozen or unarmed until a
      successful resync. (latch survives a failed re-arm drain, a newer latch raised during the drain, and a failed/dead order stream: `test_reconnect_ream_requires_successful_drain`, `test_reconnect_ream_requires_plant_stayed_connecting`)

References: #4729, #4617, and #4228.

### Partial fills and synthetic state

- [ ] Position reports and open-order partial fills are not double-counted.
- [ ] Synthetic reconciliation orders use deterministic, repeatable identities.
- [ ] Restarting does not create duplicate synthetic orders.
- [ ] The lookback window is sufficient for the venue's position history, or the
      limitation is explicit and the runner uses a fail-closed policy.
- [ ] Netting and hedging semantics are tested separately.
- [ ] A consistent venue-flat report converges the local state or escalates after
      bounded retries; it does not loop forever.
- [ ] Cross-zero repairs report failure when the repair did not actually apply.
- [ ] Position quantity, side, average price, and account are checked together.

References: #3176, #3476, #3104, #3622, and #4619.

### Filled-order reports

- [ ] Filled market orders have a usable execution price from venue average price,
      cumulative quote/quantity, or fill data.
- [x] `avg_px=None`, zero market price, and venue pending-price sentinels are
      handled without creating a fake price. (sentinel `-1.0` → `None` at the convert boundary)
- [ ] A filled report with missing price is held/rejected as incomplete rather
      than leaving a phantom accepted order.
- [ ] Commission is allowed to be temporarily unavailable without crashing the
      fill path.
- [x] `None` and venue sentinel values such as `-1.0` never reach `Money()`.
      (`test_sentinel_status_fields_are_none`, `test_sentinel_fill_price_suppressed_not_crashed`)

References: #4440 and #4228.

## 5. External and orphaned activity

- [ ] Manual/external orders are surfaced rather than silently dropped.
- [ ] External fills cannot be attributed to a strategy without evidence.
- [ ] An unmatched fill that changes exposure freezes new entries.
- [ ] Existing venue exposure is detected on startup before signals are armed.
- [ ] Working orders from a prior crashed process are discoverable and cancelable.
- [ ] Emergency flatten replays venue order state before cancel/flatten actions.
- [ ] Shared-login activity from another client is visible and does not corrupt
      tracked strategy ownership.
- [ ] Account ID and instrument ID are present before publishing an external fill.

## 6. Connectivity and lifecycle

- [ ] Partial connect failure tears down every resource that already started.
- [ ] Order/data/PnL streams have independent health states.
- [ ] A handler exception marks the affected plant unhealthy and stops unsafe
      command processing.
- [ ] Reconnect restores authentication, subscriptions, and required account or
      instrument context—not only the socket.
- [x] Reconnect resyncs working orders and positions before trading is re-armed.
      (bounded working-orders drain **and** a fresh account/position PnL observation — an activity gate set only after successful processing — gate the re-arm; the barrier clears the latch only if the plant stayed `CONNECTING` and the poll task is alive, so a newer latch or a failed/dead order stream survives it: `test_reconnect_ream_requires_successful_drain`, `test_reconnect_ream_requires_pnl_snapshot`, `test_reconnect_ream_requires_plant_stayed_connecting`, `test_pnl_marker_only_after_successful_account_processing`)
- [ ] Resubscription intent is restored after gateway reconnect.
- [ ] Reconnect does not replay old events as new fills without deduplication.
- [ ] Silent reconnect loops emit health/error diagnostics and have bounded retry
      behavior.
- [ ] `start`, `connect`, `disconnect`, `stop`, `reset`, and `dispose` are
      idempotent.
- [ ] No nested `block_on` or blocking plant call runs on the asyncio loop.
- [ ] Connect mode is explicit and direct/gateway behavior is capability-parity
      tested.

References: #4163, #4564, #4643, and the lifecycle section of
`nautilus-adapter-conventions.md`.

## 7. Rithmic-specific wire and plant checks

- [ ] Rithmic `TRIGGER` mapping is gated by the cached Nautilus order type.
- [ ] Rithmic `FILL` mapping uses the venue fill ID and preserves partial-fill
      identity.
- [ ] `price_type` 3/4 maps to the correct stop-limit/stop-market semantics.
- [ ] `trigger_type` is present in every status report with a trigger price.
- [ ] Plain market/limit reports use `TriggerType.NO_TRIGGER`.
- [ ] `is_reduce_only` is used; no obsolete `reduce_only` attribute access remains.
- [ ] `avg_px` is populated from Rithmic average fill data when available.
- [ ] Rithmic order notifications with no tracked client ID publish external
      reports or an explicit fail-closed warning; they are not silently lost.
- [ ] `load_orders` is bounded, serialized with order-stream handling, and its
      incomplete/empty result is labeled advisory.
- [ ] Direct and gateway plant paths have the same order notification, fill,
      cancel, modify, and reconnect semantics.
- [ ] Gateway order-stream subscription intent is restored after reconnect.
- [ ] One-login constraints and other-client/manual-order interference are tested
      operationally.

## 8. Minimum acceptance matrix before live use

- [ ] Market entry accepted and filled.
- [ ] Market entry rejected before send.
- [ ] Market entry send timeout with later acceptance.
- [ ] Stop-market accepted, triggered, and filled.
- [ ] Stop-market fills without a trigger notification.
- [ ] Stop-limit trigger followed by fill.
- [ ] Target fills while stop cancel is in flight.
- [ ] Stop fills while target cancel is in flight.
- [ ] Partial entry fill followed by protection placement.
- [ ] Partial exit fill followed by final exit fill.
- [ ] Duplicate fill notification.
- [ ] Unknown/untracked order notification.
- [ ] Restart with open position and working exits.
- [ ] Restart with an orphan working order.
- [ ] Failed order query.
- [ ] Failed position query.
- [ ] Reconnect during submit.
- [ ] Reconnect during modify.
- [ ] Reconnect during cancel.
- [ ] Gateway reconnect with subscription restoration.
- [ ] Manual venue flatten detected and trading remains blocked.

## Current known gap from MY043 review

The 2026-08-18 MY043 log exposed this gap:

```text
Rithmic exchange TRIGGER
→ rithmic_nt_connect._orders.notification_action()
→ execution.py generate_order_triggered()
→ Nautilus rejects OrderTriggered for STOP_MARKET
```

Required first remediation:

- [x] Add the #3812-style order-type guard to the Rithmic execution client.
      (order-type + `is_closed` + already-`TRIGGERED` at the single emit point, `execution.py` `_handle_order_notification`)
- [x] Add regression tests for `STOP_MARKET` and `MARKET_IF_TOUCHED` triggers.
      (`test_trigger_notification_emission_guarded_by_order_type`, `test_stop_market_trigger_then_fill_still_emits_fill`)
- [x] Verify the later fill still updates the order and position correctly.
      (`test_stop_market_trigger_then_fill_still_emits_fill`)
- [x] Run the full execution and gateway test suites. (253 passed / 75 live-gated
      skipped; `ty check` + `ruff` clean, 2026-08-19)
- [x] Run a dry-run/mock-transport acceptance sequence before any live place.
      (mock-transport acceptance: `tests/test_exec_transport_e2e.py`; a gated live dry-run stays a P5 item)
