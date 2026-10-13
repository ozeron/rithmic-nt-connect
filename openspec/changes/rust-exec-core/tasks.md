## 1. Crate and binding scaffold

- [x] 1.1 Add pure crate `crates/rithmic-exec` (no `rithmic-plants`, `tokio`, or Nautilus dependency) to the workspace and CI test/clippy lists
- [x] 1.2 Add the PyO3 registration seam in `rithmic-nt-connect` (one `exec` submodule of functions and classes) and `_lib.pyi` stubs

## 2. Stage A: order-plant policy

- [x] 2.1 Port `OrderPlantState` and `OrderPlantPolicy` with an exhaustive Rust test over every (state, transition) pair, taken from `tests/test_order_plant.py`
- [x] 2.2 Bind as a pyclass; `_order_plant.py` re-exports it with the Python `OrderPlantState` enum mapping; `tests/test_order_plant.py` passes unchanged

## 3. Stage B: exposure and commission rules

- [ ] 3.1 Port `product_root`, `matches_root`, `signed_net`, and snapshot `complete` (settle interval) with Rust tests from `tests/test_exposure.py`
- [ ] 3.2 Port the commission rate table (product rates, account default, per-fill lookup returning a decimal string) with Rust tests from `tests/test_commission_registry.py`
- [ ] 3.3 Switch `exposure.py` and `commission.py` call sites to the bindings; keep `Money`/`Cache` handling in Python

## 4. Stage C: convert boundary and dedup

- [ ] 4.1 Port instrument-id encoding/decoding, `format_price_str`, `_ts_ns`, and the closed-set coercions (`enum_int`, `sentinel_none`) with Rust tests from `tests/test_convert_ticks.py` and the mutation-corpus property test
- [ ] 4.2 Port `fill_dedup_key`, `trade_id_from_fill_fields`, `SeenKeyCache`, `FillDedupStore`, `UntrackedStatusBook` with Rust tests from `tests/test_notification_intake.py`
- [ ] 4.3 Port `order_notification_to_fields` and `kind_from_notify` (dict in, dict out) with venue-shaped fixtures
- [ ] 4.4 Switch `_convert.py` / `_orders.py` to re-exports of the bindings; Nautilus-typed helpers (`nautilus_*_to_rithmic`, `notification_action`) stay in Python

## 5. Stage D: drain decisions

- [ ] 5.1 Split `recon.py` at the decision/effect seam: row trust (`row_is_trustworthy`), identity choice, bind/skip decision become pure functions over dicts
- [ ] 5.2 Port those decisions with Rust tests from `tests/test_working_orders_drain.py` and `tests/test_exec_recon.py`; effects (publish, cache write, report building) stay in Python
- [ ] 5.3 Switch `recon.py` to the bindings; `test_exec_transport_e2e.py` stays green

## 6. Stage E: inventory

- [ ] 6.1 Port the inventory arithmetic (side from transaction type, gone-order test, open-position fold) with Rust tests; `inventory.py` keeps the session polling

## 7. Close-out

- [ ] 7.1 Confirm no ported rule has a second Python implementation (search for the removed function names)
- [ ] 7.2 Update AGENTS.md (review checklist rows now point at the Rust tests), `docs/STATUS.md`, and regenerate the scoreboard
- [ ] 7.3 Run the full verification list (fmt, clippy for workspace and `python`/`testing` features, cargo test, ruff, ty, pytest, qlty, status check) and record results
