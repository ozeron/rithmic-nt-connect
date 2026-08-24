# Rithmic Execution Client — Nautilus Exec Testing Spec Matrix

Maps each `TC-E*` test case from the
[NautilusTrader Execution Testing Spec](https://nautilustrader.io/docs/latest/developer_guide/spec_exec_testing/)
against our adapter's implementation. Mirror of
`nautilus-data-testing-matrix.md` for the execution client.

**Legend:** `[x]` live-proven · `[~]` wired / unit-tested, live unproven or partial ·
`[ ]` not implemented · `N/A` out of scope (venue/futures) · `MOCK` needs a fake
order-plant boundary (unit only, not live)

**Runnable suite (planned):** `tests/e2e/test_exec_client_live.py` (live, credentials +
`RITHMIC_ENABLE_TRADING=1` gated) + existing unit suites
(`test_orders.py`, `test_exec_recon.py`, `test_execution_trading.py`,
`test_execution_outcomes.py`, `test_exec_readonly.py`).

```bash
uv run pytest tests/ -m "not live"          # unit only (CI-safe)
uv run pytest tests/e2e/test_exec_client_live.py -v   # live exec sweep
```

Live execution needs `RITHMIC_ENABLE_TRADING=1` (or `enable_trading=True`). One Rithmic
login session only — never run while MotiveWave / R|Trader is open. `cancel_all_orders`
is plant-wide; never use it to clean up a single smoke order.

Last run: **2026-08-18** — CAN-set live on `Rithmic Test` (E01..E04, E06, E10..E16,
E19, E20..E23, E40, E42, E44, E84). `TC-E41` (plant-wide cancel on stop) is **not**
live-testable: it would cancel unrelated orders at the plant, so it stays
collection-skipped with a documented reason. Scaffolds (Groups 4/6/7/8/9) and
unsupported (`E05,E17,E18,E24..E27,E41,E43`) remain.

## Incident Replay Coverage

`tests/test_exec_recon.py` is the fake/replay regression suite for the known
incident chain. It covers:

- native `Order.is_closed` property semantics (never `is_closed()`);
- stop reports retaining `TriggerType.DEFAULT`, trigger price, and native
  reduce-only metadata, while market/limit reports retain `NO_TRIGGER`;
- recovered-fill ordering (`OrderStatusReport` before `FillReport`), tracked and
  external fills, partial fills, duplicate-fill suppression, and replay idempotency;
- unknown outcome handling and order-plant gate closure without fabricating a
  rejection or flat position;
- exact NQ/MNQ, account, and RITHMIC instrument identity;
- nonzero exposure on an unloaded instrument remaining visible as a warning and
  not being silently reconciled away.

Run only the replay/unit coverage with:

```bash
uv run pytest tests/test_config.py tests/test_exec_recon.py -q
```

Live tests use an explicit test-only env source and never fall back to the
repository-root `.env`:

```bash
RITHMIC_TEST_DOTENV=/secure/local/rithmic-test.env \
RITHMIC_ENABLE_TRADING=1 \
uv run pytest tests/e2e/test_exec_client_live.py -v
```

The source must resolve to a test/demo system. Production/LucidTrading systems,
missing sources, concurrent Rithmic sessions, and unsupported MIT/LIT mappings
fail closed.

---

## Group 1: Market orders

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E01 | Market BUY - submit & fill | [x] | live-proven `Rithmic Test` 2026-08-17 |
| TC-E02 | Market SELL - submit & fill | [x] | live-proven |
| TC-E03 | Market IOC | [x] | duration 3 wired; live-proven |
| TC-E04 | Market FOK | [x] | duration 4 wired; live-proven |
| TC-E05 | Market quote qty | [ ] | Rithmic contract-qty only, no quote qty |
| TC-E06 | Close position on stop | [x] | `OrderDriver.on_stop` flattens tracked positions during the post-stop window (no node-level auto-close in 1.231.x); live-proven |

## Group 2: Limit orders

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E10 | Limit BUY GTC | [x] | LIMIT type 1, GTC duration 2; live-proven |
| TC-E11 | Limit SELL GTC | [x] | live-proven |
| TC-E12 | Limit BUY+SELL pair | [x] | live-proven |
| TC-E13 | Limit IOC fill | [x] | aggressive limit IOC; live-proven |
| TC-E14 | Limit IOC no fill | [x] | `OrderCanceled`, not `Expired`; live-proven |
| TC-E15 | Limit FOK fill | [x] | live-proven |
| TC-E16 | Limit FOK no fill | [x] | live-proven |
| TC-E17 | Limit GTD | [ ] | no GTD in `_RITHMIC_DURATION_TO_TIF` (1/2/3/4 only) |
| TC-E18 | Limit GTD expiry | [ ] | no GTD |
| TC-E19 | Limit DAY | [x] | duration 1; live-proven |

## Group 3: Stop & conditional orders

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E20 | StopMarket BUY | [x] | merged live test `test_tc_e2x_stop` (trigger far from market; rests then cancel-on-accept); live-proven |
| TC-E21 | StopMarket SELL | [x] | |
| TC-E22 | StopLimit BUY | [x] | |
| TC-E23 | StopLimit SELL | [x] | |
| TC-E24 | MarketIfTouched BUY | [ ] | MIT not in rithmic price-type map |
| TC-E25 | MarketIfTouched SELL | [ ] | |
| TC-E26 | LimitIfTouched BUY | [ ] | LIT not mapped |
| TC-E27 | LimitIfTouched SELL | [ ] | |

## Group 4: Order modification

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E30 | Modify limit BUY | [~] | native amend support TBD; else cancel-replace |
| TC-E31 | Modify limit SELL | [~] | |
| TC-E32 | Cancel-replace BUY | [~] | universal path, always available |
| TC-E33 | Cancel-replace SELL | [~] | |
| TC-E34 | Modify stop trigger | [ ] | depends on E30 + stop modify |
| TC-E35 | Cancel-replace stop | [~] | if stop supported |
| TC-E36 | Modify rejected | [ ] | only if adapter has no native modify |

## Group 5: Order cancellation

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E40 | Cancel single limit | [x] | live-proven |
| TC-E41 | Cancel all on stop | [ ] | plant-wide — live-testing would cancel unrelated orders; collection-skipped (`tests/e2e/test_exec_client_live.py`), no safe unit boundary yet |
| TC-E42 | Individual cancels on stop | [x] | `test_tc_e42_individual_cancels_on_stop` — two resting orders individually canceled by `OrderDriver.on_stop`; live-proven |
| TC-E43 | Batch cancel on stop | [ ] | no batch-cancel API |
| TC-E44 | Cancel already-canceled | [x] | `test_tc_e44_cancel_already_canceled` — second cancel of a CANCELED order is refused locally (no venue command, no events); live-proven |

## Group 6: Bracket orders

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E50 | Bracket BUY | [~] | wire on direct+gateway (`execution.py:920`); Lucid accept/survival unproven |
| TC-E51 | Bracket SELL | [~] | |
| TC-E52 | Bracket entry activates TP/SL | [~] | unproven on Lucid |
| TC-E53 | Bracket post-only entry | [~] | |

Live bracket spike: `scripts/spike_bracket_order.py` (`RITHMIC_BRACKETS=1` +
`RITHMIC_ENABLE_TRADING=1`). Default entry: far LIMIT from BBO
(`BUY = bid - N*tick`, `--far-ticks`); optional `--limit-price` must still
clear that rule. Explicit `--market-entry` only.

## Group 7: Order flags

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E60 | PostOnly accepted | [~] | plant post-only support TBD |
| TC-E61 | ReduceOnly on close | [ ] | reduce-only support TBD |
| TC-E62 | Display qty (iceberg) | [ ] | no display/iceberg field |
| TC-E63 | Custom order params | [ ] | no adapter param pass-through |

## Group 8: Rejection handling

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E70 | PostOnly rejection | [~] | if post-only supported |
| TC-E71 | ReduceOnly rejection | [ ] | if reduce-only supported |
| TC-E72 | Unsupported order type | [x] | `test_unsupported_order_type_raises` (unit) |
| TC-E73 | Unsupported TIF | [~] | `test_side_type_tif_mapping` (unit) |
| TC-E74 | Ambiguous submit fail | MOCK | `test_resync_blocks_submit_allows_cancel`, `test_order_plant_policy_matrix` (unit) |
| TC-E75 | Ambiguous cancel fail | MOCK | no `OrderCancelRejected` on transport fail |
| TC-E76 | Ambiguous modify fail | MOCK | |
| TC-E77 | Ambiguous batch fail | [ ] | no batch |
| TC-E78 | Per-order batch reject | [ ] | no batch |

## Group 9: Lifecycle & reconciliation

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E80 | Open position on start | [~] | |
| TC-E81 | Cancel orders on stop | [~] | |
| TC-E82 | Close positions on stop | [~] | |
| TC-E83 | Unsubscribe on stop | [~] | mirrors data TC-D70 |
| TC-E84 | Reconcile open orders | [x] | `test_tc_e84_reconcile_resting_stop_preserves_trigger` (live, through `LiveExecutionEngine._check_orders_consistency` — the path that sends `GenerateOrderStatusReports`); `test_generate_order_status_reports_preserves_stop_trigger_type` (unit, same code path). Drain best-effort, NOT complete — the exec fixture pins `open_check_open_only=True` (the 1.231.x knob; `death_policy` no longer exists) so a cached order missing from an empty drain is advisory, not canceled |
| TC-E85 | Reconcile filled orders | [~] | fill query unavailable (`conventions.md:144`); `test_generate_fill_reports_*` |
| TC-E86 | Reconcile open long | [~] | `test_position_status_reports_*` |
| TC-E87 | Reconcile open short | [~] | |

Recon honesty caveat (trading): the `load_orders` drain is best-effort and *not*
provably complete — an empty result means "no working orders seen", not "venue has
none". The exec fixture therefore pins `open_check_open_only=True` (the 1.231.x
replacement for the removed `death_policy=trust_stop`): with it, a cached open
order missing from the drain is logged as advisory instead of canceled.
Live-venue proof that the drain returns the venue's working orders is still TODO.

## Group 10: Options trading

| TC | Name | Status | Where verified / gap |
|---|---|---|---|
| TC-E90..E101 | Options suite | N/A | futures only, no `CryptoOption` instrument |

---

## Summary

| Group | Total | [x] | [~] | [ ] | MOCK | N/A |
|---|---|---|---|---|---|---|
| 1. Market | 6 | 5 | 0 | 1 | 0 | 0 |
| 2. Limit | 10 | 8 | 0 | 2 | 0 | 0 |
| 3. Stop/cond | 8 | 4 | 0 | 4 | 0 | 0 |
| 4. Modify | 7 | 0 | 5 | 2 | 0 | 0 |
| 5. Cancel | 5 | 3 | 0 | 2 | 0 | 0 |
| 6. Brackets | 4 | 0 | 4 | 0 | 0 | 0 |
| 7. Flags | 4 | 0 | 1 | 3 | 0 | 0 |
| 8. Rejection | 9 | 1 | 2 | 3 | 3 | 0 |
| 9. Lifecycle/Recon | 8 | 1 | 7 | 0 | 0 | 0 |
| 10. Options | 12 | 0 | 0 | 0 | 0 | 12 |
| **Total** | **73** | **22** | **19** | **17** | **3** | **12** |

**Baseline-compliant (groups 1–5) achieved** — the CAN-set (`TC-E01..E04,E06,E10..E16,
E19,E20..E23,E40,E42,E44`) is `[x]` live-proven on `Rithmic Test` (2026-08-18), and
`TC-E84` (reconcile open orders, incl. the stop trigger_type report regression) is
live-proven through `LiveExecutionEngine._check_orders_consistency` (the open-order
consistency path) with a unit twin (`test_generate_order_status_reports_preserves_stop_trigger_type`)
that proves the same code path in CI. `TC-E41` (plant-wide cancel on stop) is **not**
claimed live-proven: exercising it would cancel unrelated orders, so it is
collection-skipped in the live suite. Remaining groups (4/6/7/8/9) are scaffolded and
await native modify / bracket / reduce-only / remaining reconciliation confirmation,
or are unsupported (`E05,E17,E18,E24..E27,E41,E43`) per this matrix.

**Open gaps (venue/futures capability, not adapter code):**
- E05 quote qty, E17/E18 GTD, E24–E27 MIT/LIT, E43/E77/E78 batch, E62 iceberg, E63
  custom, Group 10 options — all N/A or unsupported by Rithmic futures plant.
- E34/E36/E61/E70/E71 — depend on confirming native modify / reduce-only / post-only
  support; until then mark `[ ]` and skip.
- Reconciliation (E84–E87) is best-effort; the exec fixture pins `open_check_open_only=True`
  (the 1.231.x replacement for `death_policy=trust_stop`) so an empty drain never
  cancels tracked working orders.
- Recovered fills are published only after their prerequisite order status publishes; if
  that status publication fails the fill is suppressed (fail-closed, logged) rather than
  emitted without an order.

