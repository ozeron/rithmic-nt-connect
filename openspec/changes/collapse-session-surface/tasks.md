## 1. Delete the Python session wrappers

`unify-session-host` already moved the registry, holders, flock, idempotent `connect`, gates and session events into the extension, and added `Session.fake`. What is left in `session.py` is forwarding only.

- [ ] 1.1 Confirm the extension's `subscribe_session_events` supports several independent listeners per session, each cancellable on its own, in both modes. If it does not, add that in Rust (the dedicated delivery thread is unchanged) so Python needs no fan-out.
- [ ] 1.2 In `session.py`:
  - delete `_FlockedDirectSession`, `_RustGatewaySession`, `ensure_connected` and `_connect_once`
  - make `create_session` return `_lib.Session` directly (still calling `request_plants` for `plants="execution"`)
  - set `WireSession = _lib.Session`, and delete `TickerSession` / `PnlSession` / `OrderSession`
- [ ] 1.3 Make `_lib.Session.connect` succeed when the host is already connected (no `AlreadyConnectedError` to swallow), and call `asyncio.to_thread(self._session.connect)` from `data.py` / `execution.py`.
- [ ] 1.4 Update `tests/test_wire_session_parity.py` (no wrappers left to check) and any test that reaches into `_inner` / `_listeners`, using `Session.fake`.
- [ ] 1.5 Run the full verify gate: Rust fmt/clippy/tests, `ruff`, `ty check`, `pytest -q`.

## 2. One event model

- [ ] 2.1 Move `PlantPoller`'s `poll_fn` fallback, `inventory.py:265` and `verify.py:99` to `next_events`. Update `execution.py:716,725` to pass only the batch function.
- [ ] 2.2 Remove the four `poll_*` methods from the `Session` trait, `Venue` (`RithmicVenue`, `FakeVenue`), `LocalSession`, `RemoteSession`, PyO3, `_lib.pyi` and `WireSessionStub`. Update the scenario tests that polled.
- [ ] 2.3 Run the Rust tests, `tests/client_scenarios.rs`, `tests/test_gateway_scenarios.py` and the full pytest suite.

## 3. One argument name per RPC

- [ ] 3.1 Rename the trait, backend, PyO3 and stub arguments to the proto request field names (for example `load_orders(start_time_sec, end_time_sec)` and `load_fill_history(start_date, finish_date)`). Sweep the callers in `python/`, `scripts/`, `examples/` and `tests/`.
- [ ] 3.2 Add a test comparing the built extension's `inspect.signature` for each proto-backed method with its `session_pb2` request message fields.

## 4. Typed DTOs with shim

- [ ] 4.1 Record the FFI throughput baseline for both crossing and field access (add the access case to `tests/test_perf_ffi_throughput.py`), written to `docs/perf/`.
- [ ] 4.2 Add an optional `python` feature with an optional `pyo3` dependency to `rithmic-plants`. Put `cfg_attr(pyclass(frozen, get_all, eq))` and a `cfg_attr`'d `#[new]` kwargs constructor on every `dto.rs` struct returned to Python. Give each event struct a `type` class attribute. Enable the feature only from `rithmic-nt-connect`.
- [ ] 4.3 Add the read-only `__getitem__` / `get` / `__contains__` shim helper. Switch every PyO3 return to the pyclasses, and delete the `*_dict` / `set_opt_*` builders in `python/mod.rs`.
- [ ] 4.4 Make `wire_types.py` re-export the RMS pyclasses. Make `WireSessionStub` and `tests/gateway_fake.py` build records through the constructors.
- [ ] 4.5 Add a scenario test that one scripted event through direct and gateway yields the same class with equal fields, and that assigning a field raises.
- [ ] 4.6 Re-run the benchmark (no lower than the 4.1 baseline), plus the full pytest suite through the shim.

## 5. Generate PyO3 forwards and the stub

- [ ] 5.1 Add `session_forwards!` (D7) for the uniform RPCs. It emits the `#[pymethods]` forwards and `SESSION_PYI`. Move the hand-written methods to a second block (`multiple-pymethods`), or use the single-block fallback if `inventory` registration is unwanted in the wheel.
- [ ] 5.2 Add a stub test: `UPDATE_STUBS=1` writes the `Session` section of `_lib.pyi`, and otherwise the test fails on a diff. Keep the existing stub-vs-built-extension check.
- [ ] 5.3 Delete the hand-written forwards the macro replaced, and delete `tests/test_wire_trait_parity.py` checks that are now true by construction. Keep `test_wire_session_parity.py` and the `tests/rpc_table.rs` coverage tests.

## 6. Migrate Python to DTO attributes

- [ ] 6.1 Migrate `_convert.py`, `_orders.py`, `order_rows.py`, `notification_intake.py` and `_prices.py` from keys to attributes. Keep the Nautilus coercion and the closed-set rejection at the convert boundary.
- [ ] 6.2 Migrate `recon.py`, `inventory.py`, `exposure.py` and `commission.py`.
- [ ] 6.3 Migrate `data.py` and `execution.py`.
- [ ] 6.4 Migrate `providers.py`, `front_month.py`, `historical.py` and `verify.py`.
- [ ] 6.5 Migrate the test files that hand-build event dicts, with fixtures in the real venue payload shape (AGENTS.md "Venue-shaped fixtures").
- [ ] 6.6 Run `ty check` and the full pytest suite. Run `RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e -v` on the test account, with MotiveWave closed.

## 7. Remove the shim and update the docs

- [ ] 7.1 Delete the DTO dict shim and confirm that `ty check` is clean.
- [ ] 7.2 Update AGENTS.md "When you add or change a plant RPC" to the 8-step list in `design.md`, and update the parity-test list.
- [ ] 7.3 Update `crates/README.md` (the `python` feature on `rithmic-plants`, the forwards macro) and amend ADR 0002 for the transport event pyclasses and the listener set.
- [ ] 7.4 Update the `docs/STATUS.md` marks only if the advertised status changed, then run `python scripts/status_progress.py --check`.
- [ ] 7.5 Run the full verify gate from AGENTS.md, including `qlty check --fail-level=medium`.
