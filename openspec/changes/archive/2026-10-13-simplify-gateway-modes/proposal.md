## Why

The gateway client carries three behaviors that exist for convenience or rollback rather than for the product: `dual` client topology (a rollback for the Python mux), client-side auto-spawn of the gateway parent (spawn policy, auto-spawn alias, cold-start races, spawn timeouts, idle-exit injection), and passing credentials to the spawned child. Together they are ~150 spawn-related test references, a second recovery path, and a requirement that gateway-mode clients hold the Rithmic password (`SessionConfig.password` is required today even in gateway mode) so they can hand it to a child. The only supported topologies are `direct` and `gateway`, with one socket per process to an explicitly started parent; everything else is surface area to port, test, and misconfigure.

Landing this first, in Python where it is cheap, shrinks what `rust-unified-session` has to port (no spawn, no cold-start race, no `dual`) and turns the supported-mode matrix into a written, tested contract.

## What Changes

- Lock the supported mode matrix as a spec: `connect_mode` is `direct` or `gateway`, explicit, no default; gateway client topology is `mux` only; gateway transport is `unix` only (tcp/tls remain a clear not-implemented error); venue environment Live/Demo/Test, `enable_trading`, the independent cancel-all gate, and bar source INTERNAL/EXTERNAL are unchanged.
- **BREAKING**: the gateway client never spawns the parent. It attaches to an explicitly started `rithmic-gateway` or fails with a clear "parent not running" error. Removed: `spawn_gateway` from the client path, `GatewayConfig.auto_spawn` / `spawn_policy` / `spawn_environ` / `spawn_timeout_sec`, `RITHMIC_GATEWAY_SPAWN_POLICY`, `RITHMIC_GATEWAY_AUTO_SPAWN`, the cold-start spawn race handling, and the `IDLE_EXIT_SEC=5` injection.
- **BREAKING**: `dual` client topology and `RITHMIC_GATEWAY_CLIENT_MODE` are removed.
- Retired knobs fail fast: a value that requests removed behavior (`dual`, `if_missing`/`auto`, `AUTO_SPAWN=1`) raises a configuration error naming the removal; a value that equals the sole remaining behavior (`mux`, `never`, `AUTO_SPAWN=0`) is accepted as a no-op so existing env files do not break silently in either direction.
- Gateway-mode clients no longer require or read the Rithmic password. Credentials live only in the parent's environment; clients authenticate with the auth token and identify the parent by fingerprint (user, system, URL, env).
- Add an explicit parent launcher (`python -m rithmic_gateway` / documented `rithmic-gateway` path) that execs the bundled binary, so an installed wheel can start the parent without `cargo`.
- Keep the attach-time bounded wait for an already-starting parent (credential flock held, socket not yet listening); it is not a spawn.
- Scripts and live tests that pre-spawn a parent (`smoke_gateway_shared_ticks.py`, `probe_hydrate_under_live_md.py`, `tests/e2e/test_rms_commission_live.py`) use a small test-side launcher instead of the library spawn path.
- Update `.env.example`, `docs/references/ops-runbook.md`, and `docs/STATUS.md` to remove auto-spawn claims.
- Update the `rust-unified-session` change so it ports less and depends on this one.

Out of scope: renaming the gateway parent, collapsing duplicate env aliases, folding `RITHMIC_BRACKETS` into `enable_trading`, limiting advertised EXTERNAL bar periods, and the parent-side `RITHMIC_GATEWAY_IDLE_EXIT_SEC` knob (all recorded as open questions in the design).

## Capabilities

### New Capabilities
- `supported-modes`: the supported mode matrix (connect mode, client topology, transport, venue environment, trading and cancel-all gates, bar source) and the fail-fast treatment of retired configuration.
- `gateway-parent-lifecycle`: parent is started explicitly; the client only attaches; launcher; error codes; bounded wait for a starting parent; no credentials in gateway-mode clients.

### Modified Capabilities
<!-- None: openspec/specs/ is empty. -->

## Impact

- **Python**: `python/rithmic_gateway/{spawn.py,attach.py,config.py,runtime.py,client.py,__init__.py}`, `python/rithmic_nt_connect/{config.py,gateway_wire.py,session.py}`; `SessionConfig.password` becomes optional in gateway mode.
- **Tests**: remove or rewrite spawn and dual coverage in `test_gateway_spawn_resolve.py`, `test_gateway_attach.py`, `test_rithmic_gateway_client.py`, `test_gateway_shared_consumers.py`, `test_gateway_runtime.py`, `test_gateway_mux.py`, `test_gateway_wire.py`, `test_session_factory.py`; add attach-only, retired-knob, and no-password tests.
- **Scripts / live tests**: `scripts/smoke_gateway_shared_ticks.py`, `scripts/probe_hydrate_under_live_md.py`, `scripts/gateway_tick_consumer.py`, `tests/e2e/test_rms_commission_live.py`, `scripts/build_wheel.sh` wording.
- **Docs**: `.env.example`, `docs/references/ops-runbook.md`, `docs/STATUS.md` (session gateway row, scoreboard regenerated). Historical `docs/plans/*` are not rewritten.
- **Users**: anyone relying on auto-spawn (notably market-data-lake's auto-spawn transport) must start the parent explicitly (launcher, systemd, launchd, Kamal). This is the main ergonomics cost.
- **Rust**: no changes; the parent binary and its `IDLE_EXIT_SEC` knob are untouched.
- **rust-unified-session**: spawn/attach-race/`dual` port tasks and the spawn requirement are removed there.
