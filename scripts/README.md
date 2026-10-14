# Scripts

Run from the repo root. Live scripts read credentials from `.env`; exit code `2` means "credentials missing" (CI-safe skip). Close MotiveWave / R|Trader first (one Rithmic login per account).

## Live (need Rithmic credentials)

| Script | Purpose | Notes |
| --- | --- | --- |
| `smoke_lucid_nq.py` | LucidTrading acceptance smoke: connect, resolve the front month, subscribe, and require market-data events (plus PnL subscribe when an account is configured) | Never places orders; exit `0` ok, `2` no creds, `1` failure |
| `verify_live_vs_history.py` | Resolve the front month, record live trades, compare them to history, write a verify JSON (`--out`) | Market data |
| `verify_order_dry_run.py` | Order-plant connect + order-update subscribe, then exit | Dry-run by default; places only with `--live-place` **and** `RITHMIC_ENABLE_TRADING=1` and an explicit far `--price` / trigger; cancels only its own basket, never `cancel_all` |
| `smoke_gateway_shared_ticks.py` | Two tick consumers (NQ + MNQ) share one `rithmic-gateway` parent; both must report the same `gateway_pid` | Starts the parent through `_gateway_parent.py`; builds the debug binary if missing |
| `gateway_tick_consumer.py` | Single-symbol tick consumer attached to an already-running gateway parent | Normally launched by `smoke_gateway_shared_ticks.py`; never spawns a parent, holds no password |

## No login

| Script | Purpose |
| --- | --- |
| `list_systems.py` | List Rithmic system names for a gateway URL |

## Harness and tooling

| Script | Purpose |
| --- | --- |
| `_gateway_parent.py` | Library, not a CLI: starts a `rithmic-gateway` parent for smokes / live e2e, waits until attachable, terminates it on exit. The shipped client never spawns a parent; operators use `python -m rithmic_gateway` or a service manager |
| `build_wheel.sh` | Build the self-contained wheel: bundles the release `rithmic-gateway` binary and the `rithmic_gateway` package, then runs `maturin build` (`--install` also installs it) |
| `gen_gateway_proto.py` | Regenerate `python/rithmic_gateway/v1/session_pb2.py` from `session.proto` with the pinned protoc and restore the `5.29.6` marker |
| `gen_nautilus_stubs.py` | Regenerate the local `stubs/` for `nautilus_trader` and re-apply the known fixes |
| `status_progress.py` | Roll up `docs/STATUS.md` marks; `--check` fails if the At-a-glance table drifts |

Do not run `verify_order_dry_run.py --live-place` unless `RITHMIC_ENABLE_TRADING=1` is set and a far `--price` is passed (BUY below / SELL above market); see [`../AGENTS.md`](../AGENTS.md) and [`../docs/references/ops-runbook.md`](../docs/references/ops-runbook.md).
