## Context

Current gateway client surface (Python, `python/rithmic_gateway`):

```
GatewayConfig: auto_spawn, spawn_policy (never|if_missing), spawn_environ (carries RITHMIC_PASSWORD),
               spawn_timeout_sec (90s), gateway_bin, attest_flock
GatewayAttachCoordinator.resolve_dial_failure:
    dial failed → flock held?  → wait_for_parent_socket (parent is starting)
                → policy never → AttachError(dial_failed_spawn_disabled)
                → else         → spawn_gateway(...) (injects IDLE_EXIT_SEC=5)
GatewayRuntime: mode mux | dual   (RITHMIC_GATEWAY_CLIENT_MODE; dual documented as "rollback")
```

Facts that matter:

- The Rithmic login belongs to the parent. Several client connections to one parent are normal and share one login, so `dual` is not unsafe for the one-login rule; it is simply a second client topology that exists as a rollback for the Python mux.
- `SessionConfig.password` is required unconditionally (`config.py`), including in gateway mode, largely so the client can hand it to a spawned child via `spawn_environ`. Gateway clients otherwise authenticate with the auth token and identify the parent by fingerprint (user, system, URL, env), and the parent attests with the credential flock.
- The wheel bundles the `rithmic-gateway` binary under `rithmic_gateway/bin/` but has no launcher; today the client spawns it, or users run the path by hand.
- `RITHMIC_PRICE_TYPE_TO_ORDER_TYPE` / `RITHMIC_DURATION_TO_TIF` are internal mapping tables in `_orders.py`, not environment variables; nothing to change.
- This change must land independently of `rust-unified-session` and should make that change smaller.

## Goals / Non-Goals

**Goals:**
- Write down the supported mode matrix as a testable spec.
- Remove client-side spawn, `dual`, and credential handoff, in Python first.
- Make gateway-mode clients credential-free (no password).
- Keep an installed wheel able to start the parent explicitly.
- Reduce what `rust-unified-session` ports.

**Non-Goals:**
- Any Rust change to the parent, proto, gates, fan-out, or restore.
- Renaming the gateway, collapsing duplicate env aliases, folding `RITHMIC_BRACKETS`, limiting EXTERNAL bar periods.
- Removing the parent-side `RITHMIC_GATEWAY_IDLE_EXIT_SEC` knob.
- Remote (tcp/tls) transport.

## Decisions

### 1. The client never spawns; it attaches or fails

`resolve_dial_failure` becomes: if the credential flock is held, wait (bounded) for the socket; otherwise raise `AttachError("parent_not_running", …)` with a message that names the launcher command and the expected listen path. The wait for a flock-held parent stays because that parent was started by someone else and is simply not listening yet; it is not a spawn and does not reintroduce a cold-start race (no client ever creates the parent, so there is no two-spawner contention).

Alternative: keep `if_missing` as an opt-in dev convenience. Rejected: it keeps the spawn code, the `spawn_environ` credential handoff, the idle-exit injection, and the cold-start race, which is exactly the surface being removed. Convenience for dev and tests moves to a test-side launcher (decision 5).

### 2. Retired knobs: fail fast on removed behavior, accept no-ops

| Variable | Value | Result |
| --- | --- | --- |
| `RITHMIC_GATEWAY_CLIENT_MODE` | `dual` | `ConfigError`: removed, `mux` is the only mode |
| `RITHMIC_GATEWAY_CLIENT_MODE` | `mux` / unset | accepted, no-op |
| `RITHMIC_GATEWAY_SPAWN_POLICY` | `if_missing` / `auto` / `default` | `ConfigError`: client spawn removed, start the parent explicitly |
| `RITHMIC_GATEWAY_SPAWN_POLICY` | `never` / unset | accepted, no-op |
| `RITHMIC_GATEWAY_AUTO_SPAWN` | truthy | `ConfigError` as above |
| `RITHMIC_GATEWAY_AUTO_SPAWN` | `0`/`false`/`no`/`off` / unset | accepted, no-op |

A silent downgrade (someone sets `if_missing`, nothing spawns, their lake job hangs) is the failure to avoid, so removed behavior errors loudly; values that already describe the sole remaining behavior do not break existing env files. The no-op acceptance is documented as deprecated and can be deleted in a later change.

### 3. Gateway-mode clients carry no password

`SessionConfig.password` becomes optional when `connect_mode == gateway` and is never read, logged, or sent. The parent needs `RITHMIC_PASSWORD` in its own environment. Direct mode still requires it. This is a security improvement as a side effect: the secret exists in one process (the parent) instead of every client.

Alternative: leave it required "to be safe". Rejected: it forces every lake and book process to hold a secret it never uses once spawn is gone.

### 4. Explicit launcher for the bundled parent binary

`python -m rithmic_gateway` resolves the bundled binary with the existing `resolve_gateway_bin` order and `exec`s it with the caller's environment (so the parent's credentials come from the launching shell, systemd unit, launchd plist, or Kamal accessory). It does not detach, set idle-exit, or edit environment. `resolve_gateway_bin` stays as a library function used only by the launcher and by tests.

Alternative: install a console script `rithmic-gateway`. Acceptable follow-up; `python -m` needs no packaging change and works in the existing wheel layout.

### 5. Test and script launcher lives outside the library

Smokes and live e2e that need a parent use a small helper under `scripts/` / `tests/` (Popen of the resolved binary, wait for socket + flock, terminate on exit). It is not importable from `rithmic_gateway`, so the library has no spawn path.

### 6. Parent lifetime is the operator's

With no spawner, nothing sets `RITHMIC_GATEWAY_IDLE_EXIT_SEC=5`. The parent's own knob is unchanged: unset or `-1` means stay until SIGTERM (the right default for a service). The knob stays available for ephemeral parents in CI and scripts. Whether to remove it too is an open question.

### 7. Error taxonomy

`dial_failed_spawn_disabled` is replaced by `parent_not_running`. Other attach codes (`parent_flock_held_no_socket`, `listen_path_mismatch`) are unchanged. Messages name the launcher and the resolved listen path.

### 8. Sequencing relative to rust-unified-session

This change lands first. `rust-unified-session` then ports attach-only (dial, flock check, bounded wait, handshake) and a single `mux` topology; tasks 4.1/4.2/6.3 and the spawn requirement there are reduced accordingly.

## Risks / Trade-offs

- [Users and the lake relied on auto-spawn] → Launcher plus runbook recipes (systemd, launchd, Kamal) and a clear `parent_not_running` message; release note marks it BREAKING; market-data-lake must start the parent explicitly (tracked as an external follow-up).
- [Forgotten parent start in dev feels like a regression] → Error message prints the exact launch command; test-side launcher covers our own smokes.
- [Making password optional hides a real misconfiguration in direct mode] → Required-ness is keyed on `connect_mode`; add tests for both directions.
- [Accepted no-op values drift into permanent config] → Document as deprecated, log a one-time warning, schedule removal.
- [Parent started without credentials fails late] → Parent already refuses to start without a login; add a runbook check and keep the Rust startup error unchanged.
- [Hung attach on a stale flock holder] → Bounded wait (existing 90s default) renamed `attach_timeout_sec`; the flock-held-but-no-socket case already fails fast.

## Migration Plan

1. Land spec and tests first (retired knobs, attach-only, no-password) so removal is test-driven.
2. Remove `dual`, then spawn, in separate commits; each keeps `pytest -q` green.
3. Switch scripts and e2e to the test-side launcher.
4. Update docs and STATUS; regenerate the scoreboard.
5. Update `rust-unified-session` artifacts.

Rollback is a revert of the removal commits; no data or wire changes are involved. Verification per AGENTS.md: `ruff`, `ty`, `pytest -q`, `cargo` gates untouched, `status_progress.py --check`; one shared-ticks smoke on Rithmic Test with a manually started parent (no live place, MotiveWave closed).

## Open Questions

- Duplicate env aliases: `RITHMIC_GATEWAY`/`RITHMIC_URL`, `RITHMIC_SYSTEM`/`RITHMIC_SYSTEM_NAME`, `RITHMIC_USER`/`RITHMIC_USERNAME`. Collapse to one canonical name each with fail-fast on the old names? (`env_keys.rs` records an incident where an alias mismatch sent a Test login to production.)
- Fold `RITHMIC_BRACKETS` (spike-only gate) into `enable_trading` now that brackets are proven in both modes?
- Rename the gateway parent? `RITHMIC_GATEWAY` already means Rithmic's venue endpoint, and `RITHMIC_GATEWAY_LISTEN` has a "never use as WSS URL" warning in the runbook.
- Limit advertised EXTERNAL bar periods to the live-proven 1m?
- Remove the parent-side `RITHMIC_GATEWAY_IDLE_EXIT_SEC` too, or keep it for ephemeral parents?
- Keep the deprecated no-op values for one release or remove them immediately?
