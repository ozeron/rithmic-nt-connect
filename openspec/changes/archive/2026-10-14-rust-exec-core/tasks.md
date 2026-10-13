## 1. Crate and binding scaffold

- [x] 1.1 Add pure crate `crates/rithmic-exec` (no `rithmic-plants`, `tokio`, or Nautilus dependency) to the workspace and CI test/clippy lists
- [x] 1.2 Add the PyO3 registration seam in `rithmic-nt-connect` (one `exec` submodule of functions and classes) and `_lib.pyi` stubs

## 2. Stage A: order-plant policy

- [x] 2.1 Port `OrderPlantState` and `OrderPlantPolicy` with an exhaustive Rust test over every (state, transition) pair, taken from `tests/test_order_plant.py`
- [x] 2.2 Bind as a pyclass; `_order_plant.py` re-exports it with the Python `OrderPlantState` enum mapping; `tests/test_order_plant.py` passes unchanged

## 3. Stage B: exposure and commission rules

- [x] 3.1 Port `product_root`, `matches_root`, `signed_net`, and snapshot `complete` (settle interval) with Rust tests from `tests/test_exposure.py`
- [x] 3.2 Port the commission rate table (product rates, account default, per-fill lookup returning a decimal string) with Rust tests from `tests/test_commission_registry.py`
- [x] 3.3 Switch `exposure.py` and `commission.py` call sites to the bindings; keep `Money`/`Cache` handling in Python

## 4. Stage C: convert boundary and dedup

- [x] 4.1 Port the instrument-id codec and the event-time derivation with Rust tests (errors keep Python `repr` quoting) and a differential test against the original implementation. `enum_int`, `sentinel_none` and `format_price_str` stay Python by design (decision 5)
- [x] 4.2 Port `fill_dedup_key`, `trade_id_from_fill_fields` and the bounded recency cache (`SeenKeyCache`) with Rust tests and differential tests; `FillDedupStore`/`UntrackedStatusBook` stay thin Python wrappers over the Rust cache
- [x] 4.3 Move `kind_from_notify` to the crate and make the plant (`rithmic-plants`) use the same function, removing the duplicate mapping. `order_notification_to_fields` stays Python (heterogeneous dict pass-through, decision 5)
- [x] 4.4 `_convert.py` / `_orders.py` call the bindings; Nautilus-typed helpers (`nautilus_*_to_rithmic`, `notification_action`) stay in Python

## 5. Stage D: drain decisions

- [x] 5.1 Split `recon.py` at the decision/effect seam: row trust (`row_is_trustworthy`), freshest-row-per-order selection and the stale-row decision become pure functions over typed values; the Python-object coercions (`enum_int`, side) stay at the call site
- [x] 5.2 Port those decisions with Rust tests (from `tests/test_working_orders_drain.py`) and differential tests against the original Python over full term grids; effects (publish, cache write, report building, publish-before-bind ordering) stay in Python
- [x] 5.3 `recon.py` calls the bindings; `test_exec_transport_e2e.py` stays green. The empty-drain honesty branch stays in Python (it is a logging and raise decision tied to the host log)

## 6. Stage E: inventory

- [x] 6.1 Port the inventory rules (side from transaction type, gone-order test, position fold, PnL-live window, flatten side) with Rust tests and differential tests; `inventory.py` keeps the event reduction, session polling and Python-object parsing

## 7. Close-out

- [x] 7.1 Confirm no ported rule has a second Python implementation (search for the removed function names)
- [x] 7.2 Update AGENTS.md (review checklist rows now point at the Rust tests), `docs/STATUS.md`, and regenerate the scoreboard
- [x] 7.3 Run the full verification list (fmt, clippy for workspace and `python`/`testing` features, cargo test, ruff, ty, pytest, qlty, status check) and record results. **2026-10-14:** cargo fmt/clippy (workspace, `python`, `testing`) clean; cargo tests pass (rithmic-exec 53); ruff, ruff format, ty clean; pytest 670 passed / 91 skipped; qlty clean; status check OK
