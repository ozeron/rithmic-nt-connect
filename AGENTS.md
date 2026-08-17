# Agent notes — rithmic-nt-connect

Unofficial out-of-tree Rithmic adapter for NautilusTrader **1.231.x**.
Read this before changing adapter, wire, or docs.

## Must follow

Semantic rules for this adapter:

**[`docs/references/nautilus-adapter-conventions.md`](docs/references/nautilus-adapter-conventions.md)**

Read that file (or the sections you are touching) before implementing or reviewing data, execution, config, or plant code. In-tree Rust v2 machinery is N/A here; the **semantic** conventions still apply.

### Direct ↔ gateway plant-surface parity (hard)

`rithmic-plants` / PyO3 **direct** and **gateway** (`proto` → dispatch → `GatewayClient` → `gateway_wire`) must stay capability-compatible for every plant order/exec surface.

When you add or change a plant RPC (place / cancel / modify / brackets / ensure / subscribe / …):

1. Wire it on **both** paths in the same change (plants + PyO3 **and** `session.proto` + gateway dispatch + Python client + `gateway_wire`).
2. Do not ship “direct-only” for a new live capability unless STATUS explicitly marks gateway as deferred **and** book/runners refuse that mode.
3. Keep request field names and semantics aligned (e.g. bracket `localid` / `stop_ticks` / `target_ticks`).
4. Add or extend a test that would fail if one path drifts (proto framing, client method, or wire façade).

#### Learning: gateway RPC kinds (do not confuse)

Gateway plant RPCs are **not** all the same shape. Shipping a new stream as a bare `session.subscribe_*` + Ack is how bracket updates lost fan-out delivery and reconnect restore.

| Kind | Examples | Must include |
| --- | --- | --- |
| **One-shot command** | `place_*`, `adjust_*`, `cancel_*`, `modify_*` | Trading gate → plant call → map error. No fanout / `note_*` / restore. |
| **Subscription intent** | ticker, book, time bars, PnL, **order updates**, **bracket updates** | Gate → attach hub fanout → `note_*` refcount → plant only on 0→1 → rollback on fail → **`restore_intents` re-issues after plant reconnect**. |

Bracket notifications ride the **order-plant** stream. Use `subscribe_order_plant_stream` in `dispatch.rs` (order and/or brackets flags) — never a one-shot plant call. Extend `RestorePlan` + `restore_intents` and add a gate/reconnect test that asserts the new intent bit (see `gates` / `reconnect` tests). Proto string greps alone are not enough.

Related:

| Doc | Use |
| --- | --- |
| [`docs/references/nautilus-adapter-phases.md`](docs/references/nautilus-adapter-phases.md) | Upstream phase pattern (0–9) |
| [`docs/STATUS.md`](docs/STATUS.md) | Planned vs done; update marks when you close a gap |
| [`docs/references/nautilus-adapter-tiers.md`](docs/references/nautilus-adapter-tiers.md) | 1.231.x `TradingNode` seam; naming |
| [`docs/references/ops-runbook.md`](docs/references/ops-runbook.md) | How to run smokes |
| [`docs/references/gateway-remote.md`](docs/references/gateway-remote.md) | Gateway unix / tunnel / TLS roadmap |

Do not invent a second status table. Do not copy upstream DataTester / ExecTester case lists into the repo.

## Review against conventions

After a change that emits Nautilus data or execution events, check the rows that apply. If you cannot mark a row, say so and point at the convention section. Lasting marks live in [`docs/STATUS.md`](docs/STATUS.md), not here.

### Config / identity

- [ ] Secrets stay out of `repr`, errors, and logs
- [ ] Credentials resolved at construction, not in request methods
- [ ] Live / Demo / Test endpoints cannot mix accidentally
- [ ] Distinct venue instruments never share an `InstrumentId`
- [ ] `connect_mode` / `RITHMIC_CONNECT_MODE` is explicit (`ConnectMode.DIRECT` \| `GATEWAY`; no silent default)

### Data

- [ ] Wire → Nautilus at one convert boundary; unknown closed-set values rejected
- [ ] `ts_event` from venue when present; `ts_init` from adapter clock
- [ ] Subscribe is intent; do not confirm from local send success alone
- [ ] Requests complete (data, empty, or error) — never hang
- [ ] Book snapshots end `F_SNAPSHOT | F_LAST`; every logical group ends `F_LAST`
- [ ] Do not advertise `INTERNAL` bars as venue `EXTERNAL`, or L2 summary as L3

### Execution

- [ ] Pre-send local failure is deny (no `OrderSubmitted`), not a venue reject
- [ ] After send, transport / timeout / channel errors stay **unknown** (in flight), not `OrderRejected`
- [ ] Tracked orders → typed events; untracked → reports only; never drop exec events
- [ ] Fills deduped by venue trade / match id
- [ ] No `[]` meaning “venue empty” when there is no snapshot API
- [ ] State-changing place / cancel / modify is not blindly retried

### Lifecycle

- [ ] Partial connect failure tears down what started
- [ ] Reconnect restores auth **and** subscription intent, not only the socket
- [ ] No nested `block_on` on the asyncio loop (PyO3 `block_on` stays in the extension; Python uses `asyncio.to_thread`)

### Advertise / docs

- [ ] Only claim capabilities that are implemented **and** tested
- [ ] Checklist marks updated if advertised status changed
- [ ] Testers stay dry-run unless the user explicitly opts into live place

## Safety

- One Rithmic login session — do not run against LucidTrading while MotiveWave / R|Trader is open.
- Trading is off unless `enable_trading=True` / `RITHMIC_ENABLE_TRADING=1`.
- Do **not** run `scripts/verify_order_dry_run.py --live-place` unless `RITHMIC_ENABLE_TRADING=1` is set and an explicit far `--price` is passed (BUY below / SELL above market). Test-plant order routing with `DEFAULT_APP_NAME` is confirmed authorized (proven 2026-08-17: order routed to exchange); production `Rithmic 01` / LucidTrading place still needs a conformance `app_name`.
- Do not commit secrets, certs, or gated `.proto` sources.
- `cancel_all_orders` is plant-wide; do not use it to clean up a smoke order.
- Gateway `cancel_all` is an independent parent panic button (`RITHMIC_GATEWAY_CANCEL_ALL=1`): it does **not** require `RITHMIC_ENABLE_TRADING`. Still plant-wide — never use it to clean up a single smoke order.

## Verify

Gateway client / wire tests need the plants/gateway crates, a system `protoc`
(`protobuf-compiler` on Debian/Ubuntu; `brew install protobuf` on macOS), and
the protobuf Python package (core dep):

```bash
uv sync --extra dev
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect
uv run pytest -q
```

Live (creds in `.env`, MotiveWave closed):

```bash
python scripts/smoke_lucid_nq.py
python scripts/smoke_gateway_shared_ticks.py --seconds 25
python examples/live_nq_intraday_sandbox.py --seconds 90
python scripts/verify_order_dry_run.py --seconds 5
```
