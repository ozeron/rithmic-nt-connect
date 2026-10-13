## 1. Pin the contract with tests first

- [x] 1.1 Add tests for connect-mode config: unset → error, unknown value → error, `direct` / `gateway` accepted (extend `tests/test_config.py`)
- [x] 1.2 Add tests for retired knobs per the design table: `CLIENT_MODE=dual`, `SPAWN_POLICY=if_missing|auto|default`, `AUTO_SPAWN` truthy → `ConfigError`; `mux`, `never`, `AUTO_SPAWN=0`, unset → accepted; invalid value → invalid-value error
- [x] 1.3 Add tests for listen URLs: `tcp://` / `tls://` rejected with not-implemented, relative `unix://` resolved absolute
- [x] 1.4 Add attach-only tests: no parent and no flock → `parent_not_running` with listen path and launcher command; flock held and socket appears → attach; flock released early → `parent_flock_held_no_socket`; flock held and wrong path → `listen_path_mismatch`; assert no `subprocess.Popen` is ever called
- [x] 1.5 Add no-password tests: gateway-mode `SessionConfig` builds without a password; direct-mode without a password fails; handshake/RPC frames and `repr` contain no password

## 2. Remove `dual`

- [x] 2.1 Delete the dual branch and `GatewayClientMode` plumbing in `python/rithmic_gateway/runtime.py`; keep mux as the only resolution path
- [x] 2.2 Remove dual fallbacks in `python/rithmic_nt_connect/gateway_wire.py` (`reset_ticker`, recovery demand, mux-up probe) and the dual mention in `session.py`
- [x] 2.3 Make `RITHMIC_GATEWAY_CLIENT_MODE=dual` raise the removal error; accept `mux`; add a one-time deprecation warning for the accepted no-op
- [x] 2.4 Delete or rewrite dual-only tests (`test_gateway_runtime.py`, `test_gateway_mux.py`, `test_gateway_wire.py`, `test_gateway_shared_consumers.py`) and confirm `pytest -q` is green

## 3. Remove client-side spawn

- [x] 3.1 Remove spawn from `GatewayAttachCoordinator.resolve_dial_failure`; replace `dial_failed_spawn_disabled` with `parent_not_running` (message: listen path + launcher command)
- [x] 3.2 Remove `auto_spawn`, `spawn_policy`, `spawn_environ`, `spawn_timeout_sec` from `GatewayConfig`; rename the bounded wait to `attach_timeout_sec` (default unchanged)
- [x] 3.3 Replace `parse_spawn_policy` with the retired-knob check from task 1.2 (accept `never`, reject spawn-requesting values)
- [x] 3.4 Remove `spawn_gateway` and the `IDLE_EXIT_SEC=5` injection from the library; keep `resolve_gateway_bin` for the launcher and tests
- [x] 3.5 Delete spawn-only tests (`test_gateway_spawn_resolve.py` spawn cases, spawn branches in `test_gateway_attach.py`, `test_rithmic_gateway_client.py`, `test_session_factory.py`, `test_packaging.py`) and keep the `resolve_gateway_bin` cases
- [x] 3.6 Remove `GatewayConfig`/`GatewayWireSession` password handoff; make `SessionConfig.password` optional when `connect_mode == gateway` and never read it on that path (direct mode still requires it)

## 4. Launcher and harness

- [x] 4.1 Add `python -m rithmic_gateway` (`__main__.py`): resolve binary, `exec` with the caller's environment, non-zero exit naming `RITHMIC_GATEWAY_BIN` when not found, no idle-exit or detach
- [x] 4.2 Add a harness-side parent helper (outside the shipped package) that starts the binary, waits for socket + flock, and terminates on exit
- [x] 4.3 Switch `scripts/smoke_gateway_shared_ticks.py`, `scripts/probe_hydrate_under_live_md.py`, `scripts/gateway_tick_consumer.py`, and `tests/e2e/test_rms_commission_live.py` to the helper
- [x] 4.4 Update `scripts/build_wheel.sh` comments; add a packaging test that the bundled binary is launchable via the launcher

## 5. Docs and status

- [x] 5.1 Update `.env.example`: remove `SPAWN_POLICY`, `AUTO_SPAWN`, `CLIENT_MODE`; note the parent needs `RITHMIC_PASSWORD` and clients do not
- [x] 5.2 Update `docs/references/ops-runbook.md`: replace auto-spawn guidance with launcher, systemd, launchd, and Kamal recipes; document `parent_not_running`
- [x] 5.3 Update `docs/STATUS.md` session-gateway notes (no auto-spawn, no dual, attach-only, credential-free clients); fix the existing scoreboard drift; run `python scripts/status_progress.py` and `--check`
- [x] 5.4 Update `AGENTS.md` safety notes (gateway clients hold no password; parent started explicitly)

## 6. Dependent change and verification

- [x] 6.1 Confirm `rust-unified-session` artifacts reflect attach-only and mux-only (done when this change is proposed; re-check before implementing it)
- [x] 6.2 Run `ruff check`, `ruff format --check`, `ty check`, `pytest -q`, `cargo clippy -D warnings`, `cargo test` (Rust untouched), and `status_progress.py --check`
- [x] 6.3 Live verification on Rithmic Test only: start the parent with the launcher, run `scripts/smoke_gateway_shared_ticks.py`; no live place, MotiveWave / R|Trader closed
- [ ] 6.4 Notify the market-data-lake owner that auto-spawn is gone and the parent must be started explicitly
