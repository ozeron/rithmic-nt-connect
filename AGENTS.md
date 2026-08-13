# Agent notes — rithmic-nt-connect

Unofficial out-of-tree Rithmic adapter for NautilusTrader **1.231.x**.
Read this before changing adapter, wire, or docs.

## Must follow

Semantic rules for this adapter:

**[`docs/references/nautilus-adapter-conventions.md`](docs/references/nautilus-adapter-conventions.md)**

Read that file (or the sections you are touching) before implementing or reviewing data, execution, config, or plant code. In-tree Rust v2 machinery is N/A here; the **semantic** conventions still apply.

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
- Do **not** run `scripts/verify_order_dry_run.py --live-place` unless the user asked and `app_name` authorization is confirmed. `DEFAULT_APP_NAME` is not an authorization.
- Do not commit secrets, certs, or gated `.proto` sources.
- `cancel_all_orders` is plant-wide; do not use it to clean up a smoke order.

## Verify

```bash
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect
pytest -q
```

Live (creds in `.env`, MotiveWave closed):

```bash
python scripts/smoke_lucid_nq.py
python examples/live_nq_intraday_sandbox.py --seconds 90
python scripts/verify_order_dry_run.py --seconds 5
```
