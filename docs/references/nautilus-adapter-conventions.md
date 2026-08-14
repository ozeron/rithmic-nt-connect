# Nautilus adapter conventions

Extracted from the upstream
[Adapters developer guide](https://nautilustrader.io/docs/latest/developer_guide/adapters/),
plus the [data](https://nautilustrader.io/docs/latest/concepts/data/) and
[execution](https://nautilustrader.io/docs/latest/concepts/execution/) concept pages
the guide depends on.

This file is **how** to implement. Agents: start from [`AGENTS.md`](../../AGENTS.md)
and use its review list against these sections.

Phases: [`nautilus-adapter-phases.md`](nautilus-adapter-phases.md).
Status marks: [`../STATUS.md`](../STATUS.md).

The upstream guide distinguishes:

| Kind | Meaning |
| --- | --- |
| **Shared rules** | From common traits, network abstractions, hooks, or CI. Follow them. |
| **Common patterns** | Seen in several adapters; other designs are allowed. |
| **Examples** | One sound implementation; not mandatory. |
| **Exceptions** | Valid when venue semantics or protocol boundaries require them. |

Semantic rules apply even when in-tree Rust v2 machinery is **N/A** (this repo is out-of-tree 1.231.x).

---

## Config and credentials

- Typed fields; one source of truth for defaults. Closed sets (env, product, account mode) are enums, not free strings.
- `Option<T>` only when absence has a distinct meaning.
- Separate data and execution config when capabilities or credentials differ.
- Redact secrets in `Debug` / `repr`. Never put credentials, signatures, or secrets in errors, INFO, or DEBUG. Treat TRACE payloads as sensitive.
- Resolve credentials at credential / factory / client **construction**, not in request methods or Python wrappers.
- Authenticated clients reject an incomplete set before sending.
- Centralize environment / endpoint resolution so live and test cannot mix. Explicit URL overrides only for custom gateways or mocks.
- Document exact env var names in the integration / ops guide.

**Here:** `SessionConfig` / `Rithmic*ClientConfig`; password redacted; Live / Demo / Test on the config. No HMAC signing.

## Symbols and instruments

- Venue symbol ≠ `InstrumentId`. Map at one boundary. Never collapse distinct venue instruments onto one id.
- Round-trip every advertised family: venue → `InstrumentId` → venue. Reject missing or ambiguous product markers before cache.
- Build instruments from **current** venue definitions. Validate identity and precision before emit.
- Parsers stay deterministic and independent of live client state where practical.
- Test fixtures use distinct symbols, precisions, currencies, and contract fields so swaps fail visibly.

**Here:** `{symbol}.RITHMIC` plus `info["rithmic_symbol"]` / `rithmic_exchange`. Futures only.

## Payloads and timestamps

- Model the **wire**, not an imagined subset. Raw models stay separate from Nautilus types. Convert at one auditable boundary.
- Reject unknown values for **closed** sets that affect domain behavior. Classify (do not silently remap) open venue sets.
- Do not collapse missing / null / empty when they mean different things. No permissive fallback that turns a new venue value into an old semantic.
- Discrete prices, quantities, money: decimal / string constructors — not `f64` as the source of truth.
- Pass parse context explicitly (precision, currency, account, `ts_init`). Keep live client state out of parsers.
- `ts_event` = venue time when the payload has one. `ts_init` = adapter clock. Receipt time as `ts_event` only when the venue has no timestamp, and that fallback has a test.

**Here:** Rust DTOs + `python/rithmic_nt_connect/_convert.py` / `_orders.py`.

## Lifecycle

| Method | Successful postcondition |
| --- | --- |
| `start` | Local event paths exist before any task can publish |
| `connect` | Public commands can use the transport; required bootstrap state is observable |
| `disconnect` | No more venue send/receive |
| `stop` | Idempotent teardown |
| `reset` | Later start/connect does not inherit invalid session state |
| `dispose` | No client-owned resource remains |

- Do not report connected until commands work **and** required engine-side state is observable (account in cache when exec advertises account; instruments when MD requires them).
- Partial connect failure tears down what already started.
- Data bootstrap: env/creds → definitions → publish instruments → transport active → subscribe/replay **after** handler init → connected.
- Exec bootstrap: creds/account → private transport → streams that cannot lose acks → initial account → wait until engine can see it → connected.
- Reconnect restores **auth + subscription intent + required context**, not only the socket.

**Here:** Python `LiveMarketDataClient` / `LiveExecutionClient` on 1.231.x. Plants are the transports.

## Data

- **Subscribe** = ongoing intent. **Request** = finite current/historical window. Do not serve a new request from a stale private cache unless the API contract says so.
- Preserve request correlation id and original parameters on the response. Always complete a request (data, empty, or error) — never hang.
- Parse and validate before emit. Do not hold mutable adapter state across downstream dispatch.
- Never confirm a subscription from local send success alone; surface venue / plant reject.
- Order-book deltas ([data concepts](https://nautilustrader.io/docs/latest/concepts/data/)):
  - Every logical group **must** end with `F_LAST` (or buffered consumers stall forever).
  - Snapshot = `Clear` + `Add`s, last delta `F_SNAPSHOT | F_LAST`.
  - Empty snapshot = `Clear` with `F_SNAPSHOT | F_LAST`.
- Advertise book type honestly: L2 summary ≠ L3 MBO.
- Bars: `INTERNAL` (engine aggregates) vs `EXTERNAL` (venue aggregated). Do not advertise internal bars as venue external.
- Instrument status from a polled snapshot: emit **diffs**, not the full snapshot every time.

**Here:** live `TradeTick` / `QuoteTick` / L2 summary; history ticks and time bars on request. Live `EXTERNAL` time bars (1m / 15m / 1h / 1d) on the **history plant** subscribe + poll. 1s stays `INTERNAL`. No depth-by-order.

## Execution

Do not infer support from a venue API alone. Implement Nautilus command/event semantics, then advertise.

### Ownership routes

| Route | Evidence | Result |
| --- | --- | --- |
| Tracked | This client owns the order | Typed lifecycle events |
| External | No tracked / pending / terminal ownership | `OrderStatusReport` / `FillReport` only |
| Suppressed | Proven duplicate, stale, or superseded | Neither event nor report |

- Do not invent strategy or client identity for untracked orders.
- Missing metadata, parse failure, or unresolved venue binding does **not** prove external.
- Register order context **before** send/spawn that can produce an inbound update. Do not evict active context just to bound replay.

### Dedup

- Dedup by **stable venue identity**, not by which socket delivered the update.
- Fills: venue trade / match id (+ account/instrument if the venue id is not globally unique).
- Do not consume a dedup key before parse/route succeeds.
- Repeated acks / snapshots must be idempotent; they must not regress state.

### Three evidence classes

| Class | Meaning | Terminal event from this evidence |
| --- | --- | --- |
| Definitive local failure | Proven **not sent** | Submit: `OrderDenied` (no `OrderSubmitted`). Modify/cancel: matching reject only if attributable to that command |
| Definitive venue result | Structured accept / update / reject | Matching domain event |
| Unknown | May have reached the venue | **Never** a terminal event. Stay in flight; recover via stream / query / poll / reconcile |

After `OrderSubmitted`, emit `OrderRejected` only when local evidence proves **no transmission**. Transport errors, timeouts, disconnects, channel errors, 5xx without command proof, rate limits, missing acks, and parse failures **after** send are unknown.

On 1.231.x: use `generate_order_denied` when the API exists for pre-send failure. Otherwise document the closest honest event and do not claim a venue reject.

Retryability ≠ ambiguity. An error can be both, either, or neither. Do not retry a state-changing command unless repeating it cannot apply twice.

### Reports

| Variant | If the order is missing from cache |
| --- | --- |
| `OrderStatusReport` | Engine may create an external order |
| `FillReport` | Engine may create a market order then apply the fill |
| `PositionStatusReport` | Log; positions stay fill-derived |

If the venue has no snapshot API: do **not** return `[]` as “venue empty”. Cache-backed orders or `VenueQueryUnavailable` is honest when documented.

**Here:** `OmsType.NETTING`, margin, trading gated. Fill query is unavailable. Account FCM/IB/id auto-discovered when unset (optional env override / multi-account selector).

## HTTP / stream (when the venue has them)

- Sign the exact canonical bytes once per attempt. Never retry a signed state-changing request with a new identity unless the venue makes that safe.
- Retry only transient **and** idempotent operations. Interrupted state-changing requests stay unknown.
- Rate limiters follow venue quotas; in-flight gate if the venue caps concurrency.
- No public subscribe / order / control command may overtake handler initialization.
- Never drop execution events.

**Here:** no REST HMAC. Plants via `rithmic-rs`. Connect may retry; place/cancel/modify must not be blindly retried.

## Tasks and runtime

- Sync live-client methods: clone owned inputs and spawn. No nested `block_on` on the engine loop.
- Shared cancellation token; replace a canceled token on `reset` / reconnect before new work.

**Here:** PyO3 `block_on` stays inside the extension; Python clients use `asyncio.to_thread`.

## Testing and docs

- Canonical fixtures from venue docs or captured responses, not only hand-made happy paths.
- Layers: protocol + mock transport + Python boundary + acceptance, as applicable.
- Acceptance records environment, skipped spec cases, recovery, and cleanup of open orders/positions.
- Testers default to dry-run / no live place.
- Integration / ops docs: products, auth/env, config, limits, gaps, venue links.
- Link the testing specs; do not copy their case lists.

## In-tree only (N/A here)

- Workspace `ADAPTER_CRATES`, PyO3 crate feature, `get_global_pyo3_registry()`, `make py-stubs`
- `nautilus_trader.adapters.<name>` import path and `_libnautilus` module
- Rust `DataClient` / `ExecutionClient` traits, `CacheView`, `CommandFailure` enum, `ExecutionEventEmitter`
- `AHashMap` / `AtomicMap` / `DashMap` collection rules
- Official `ADAPTERS.md` listing (see [`nautilus-adapter-tiers.md`](nautilus-adapter-tiers.md))
