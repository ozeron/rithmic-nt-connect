# exec-core Specification

## Purpose
TBD - created by syncing change `rust-exec-core`. Update Purpose after archive. Scope: the Nautilus-independent execution rules (lifecycle policy, exposure, commission, kind mapping, fill identity, id codec, drain decisions, inventory) as one Rust crate.

## Requirements

### Requirement: One implementation of the execution rules
The system SHALL implement the Nautilus-independent execution rules (order-plant lifecycle transitions and policy, venue exposure derivation, commission lookup, notification field coercion, fill dedup keys and stores) once, in Rust, in a crate that does not depend on Nautilus, the plant machinery, or an async runtime. Python SHALL call that implementation and SHALL NOT keep a second copy.

#### Scenario: Rule changed in one place
- **WHEN** a transition, coercion, or dedup rule changes
- **THEN** exactly one source file SHALL change and the Python adapter SHALL pick it up through the extension

#### Scenario: Pure crate
- **WHEN** the rules crate is built
- **THEN** it SHALL compile without `nautilus_trader`, `tokio`, or `rithmic-plants`

### Requirement: Order-plant lifecycle transitions are exhaustive and pinned
The lifecycle machine SHALL have the states disconnected, connecting, live, resyncing and latched, and SHALL define every (state, transition) outcome. `latch` SHALL be the only entry to latched, `rearm` SHALL be the only arming transition and SHALL arm only from connecting with a live poll task, and a resync SHALL never clear a latch.

#### Scenario: Resync from a latched plant
- **WHEN** a resync starts while the plant is latched
- **THEN** the plant SHALL stay latched and commands SHALL stay blocked

#### Scenario: Resync during the re-arm barrier
- **WHEN** a resync starts while the plant is connecting
- **THEN** the plant SHALL become latched, and the subsequent `rearm` SHALL fail and leave it latched

#### Scenario: Re-arm with a dead poll task
- **WHEN** `rearm` is called from connecting with `poll_alive` false
- **THEN** the plant SHALL become disconnected and `rearm` SHALL return false

#### Scenario: Policy by state
- **WHEN** the plant is resyncing
- **THEN** cancel SHALL be allowed and submit and modify SHALL be blocked

### Requirement: Exposure never reads unknown as zero
Exposure derivation SHALL sum signed instrument PnL quantities over contracts whose product root equals the requested root exactly (never a prefix), and an exposure snapshot SHALL be complete only when net and open orders are both known, both timestamps are present, and the drain happened at least the PnL settle interval after the PnL stream was first observed.

#### Scenario: Prefix does not match
- **WHEN** the root is `ES` and a position is in `ESTX6`
- **THEN** that position SHALL NOT count toward `ES`

#### Scenario: Drain before the PnL burst settled
- **WHEN** the drain timestamp is less than the settle interval after the PnL timestamp
- **THEN** the snapshot SHALL NOT be complete

### Requirement: Commission lookup falls back, then zero
The commission for a fill SHALL be the per-contract rate for the fill's product code times quantity, else the account default times quantity, else zero. A failed account-default fetch SHALL NOT clear an already loaded product table, and the reverse.

#### Scenario: Product code, not contract symbol
- **WHEN** a fill's contract symbol is `MNQU6` and the rate table is keyed by product code `MNQ`
- **THEN** the lookup SHALL use the instrument's product code and SHALL NOT silently charge zero

### Requirement: One notification-kind mapping for plant and adapter
The mapping from a plant `notify_type_name` (with source, status and text) to a canonical action kind SHALL be a single Rust function used by the plant when it tags a notification and by the adapter for notifications that arrive without a kind. A name SHALL only be meaningful for its own source.

#### Scenario: Stop order working state
- **WHEN** a `rithmic` notification named `TRIGGER_PENDING` arrives
- **THEN** its kind SHALL be `accepted`

#### Scenario: Complete with text
- **WHEN** a `rithmic` `COMPLETE` arrives with a non-blank text and a non-cancel status
- **THEN** its kind SHALL be `rejected`, and with a cancel status it SHALL be `canceled`

### Requirement: Closed-set and price coercion stays at one Python boundary
Coercion of arbitrary Python objects (`enum_int`, the `-1.0` pending-price sentinel, price text formatting) depends on Python numeric semantics and SHALL stay a single function each at the converter boundary rather than being re-implemented in Rust. Booleans and non-integral numerics SHALL NOT survive as closed-set integers, and the venue sentinel `-1.0` SHALL become absent.

#### Scenario: Boolean in an enum field
- **WHEN** a notification carries `True` in a closed-set field
- **THEN** the coerced value SHALL be absent, never `1`

### Requirement: Instrument ids encode the exchange and reject ambiguous parts
The instrument-id codec SHALL be a single Rust implementation: `{symbol}-{EXCHANGE}.{VENUE}` when an exchange is present, else `{symbol}.{VENUE}`, trimming the symbol, uppercasing the exchange, and rejecting `-` or `.` inside either part with the adapter's historical error text.

#### Scenario: Round trip
- **WHEN** an id is built from `NQU6` and `cme` and then parsed
- **THEN** the parse SHALL return `("NQU6", "CME")`

#### Scenario: Ambiguous symbol
- **WHEN** the symbol contains `-`
- **THEN** building the id SHALL fail naming the symbol

### Requirement: Fills are deduplicated by venue identity
A fill's dedup key SHALL derive from the venue fill id when present, else from a stable composite of the fill's own fields, and SHALL NOT depend on the adapter clock. The seen-key stores SHALL be bounded and evict oldest first.

#### Scenario: Replayed fill
- **WHEN** the same venue fill arrives twice
- **THEN** the second SHALL be reported as already seen

#### Scenario: Fill without fill id
- **WHEN** a fill has no fill id and no timestamp
- **THEN** it SHALL still get a stable key, with any clock fallback confined to the report timestamp
