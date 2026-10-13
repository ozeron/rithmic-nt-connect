## Context

After `rust-unified-session` the plant surface is one Rust trait, but the rules about what an order or fill *means* are Python: `_order_plant.py` (lifecycle machine), `_orders.py` (notification mapping, closed-set coercion, dedup stores), `exposure.py`, `commission.py`, `_convert.py`, `recon.py` (drain), `notification_intake.py`, `inventory.py`. They were hardened over several review rounds (2026-08-19 exec-hardening), and their tests encode the behavior.

Not all of it is equally portable. Three layers are mixed in these files:

1. **Pure rules** over plain dicts and ints: transition table, `product_root`/`signed_net`/`complete`, closed-set coercion (`enum_int`, `sentinel_none`), fill dedup keys, rate lookup, instrument-id encoding, `_ts_ns`. No Nautilus import in the logic.
2. **Rules whose inputs or outputs are Nautilus types**: building `OrderStatusReport`/`FillReport`, `Price`/`Quantity` precision, `Money`, cache lookups, event publishing.
3. **Orchestration over the event loop**: the drain barrier, per-tag recovery, the poll task. It interleaves layer 1 decisions with `asyncio`, the cache, and the exec engine.

## Goals / Non-Goals

**Goals:**
- Every layer-1 rule has exactly one implementation, in Rust, tested without Nautilus and without a network.
- Python keeps only translation (layer 2) and the event-loop wiring (layer 3).
- Each stage is independently shippable and leaves the Python suite green; no flag day.

**Non-Goals:**
- Moving `asyncio` orchestration or Nautilus report construction into Rust.
- Changing any behavior: this is a port, the existing tests are the specification.
- A new wire protocol or `Session` trait change.

## Decisions

### 1. A new pure crate `rithmic-exec`, no dependency on plants or tokio
The rules take plain values and return plain values, so the crate depends on neither `rithmic-plants` nor the runtime. That keeps the tests as fast unit tests and lets the lake reuse them. Alternative: put them in `rithmic-plants`; rejected because that crate carries the plant machinery and a pure rule should not need it to compile.

### 2. Boundary is functions and small `#[pyclass]` state holders, not a general event stream
Rules are exposed as PyO3 functions over `dict`/ints/strings (`product_root`, `signed_net`, `exposure_complete`, `enum_int`, `fill_dedup_key`, ...) and stateful pieces as classes with the same method names the Python classes have today (`OrderPlantPolicy`, `SeenKeyCache`, `FillDedupStore`). Python call sites change one import line. Alternative: a Rust `ExecCore` that consumes `PlantEvent`s and emits normalized exec events; rejected for this change because it forces the drain barrier and the Nautilus publish path to move at the same time (layer 3). It stays a possible later step once layer 1 is in Rust.

### 3. Python classes survive as thin re-exports during migration
`rithmic_nt_connect._order_plant.OrderPlantPolicy` becomes `from rithmic_nt_connect._lib import OrderPlantPolicy` plus the Python-side `OrderPlantState` enum mapping. Existing imports and tests keep working; the Python test for a ported module is kept until the Rust test covers the same cases, then it shrinks to a binding smoke test.

### 4. Tests are ported with the code, not after it
For each module the Rust unit tests are written from the Python test's cases (same inputs, same expected outputs, venue-shaped fixtures) before the Python implementation is deleted. The transition table in `tests/test_order_plant.py` becomes an exhaustive Rust test over every (state, transition) pair. The mutation-corpus property test for "bindable implies real closed-set terms" becomes a Rust property test over the same corpus.

### 5. Order of work follows coupling, lowest first
A. order-plant policy; B. exposure and commission rules; C. convert boundary and dedup stores (`_convert.py`, `_orders.py` pure parts); D. drain row validation and recovery decisions (`recon.py`); E. inventory. Stage D splits `recon.py` at the point where a decision (is this row usable, which identity wins, may it bind) is separated from its effect (publish, cache write).

## Risks / Trade-offs

- [Behavior drift in the highest-churn code] → port module by module, keep the Python test until the Rust one covers it, run `test_exec_transport_e2e.py` after each stage.
- [Boundary conversion cost on the hot path (every notification crosses PyO3)] → rules take borrowed `dict` fields and return scalars; the notification path already crosses into Python per event, so one extra call per row is not a new order of magnitude. Measure before moving anything per-tick.
- [Two sources of truth while migrating] → a module is either Python or Rust, never both; the Python file keeps only the re-export.
- [Rust `f64`/`Decimal` differences] → prices stay strings or `Decimal` at the boundary (`format_price_str` is ported verbatim and tested against the Python outputs); no float arithmetic is introduced.
- [Stage D/E may not split cleanly] → they are the last stages and are explicitly allowed to stop at the decision/effect seam, leaving orchestration in Python.

## Migration Plan

One stage per commit series, each ending green on the full gate list. Rollback is reverting the stage's commits; the Python implementation is deleted only in the same commit that switches the call sites.

## Open Questions

- Does the lake want `rithmic-exec` as a published crate or a Python wheel module? Not needed for this change.
