# Handoff: Python Architecture Refactoring (Sandi Metz Approach)

## 1. Overview & Context

This handoff documents the architectural refactoring of the Python adapter layer (`rithmic_nt_connect`) within `rithmic-connect` following **Sandi Metz's object-oriented design principles** (*Practical Object-Oriented Design in Ruby (POODR)*, *99 Bottles of OOP*).

### The Objective
Address God class smells, feature envy, and primitive obsession in [`RithmicExecutionClient`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/execution.py#L372) and helper modules by extracting single-responsibility collaborators and domain value objects—**without** regressing NautilusTrader 1.231.x semantics, reconnect latches, or test suite assertions.

---

## 2. New Components & Files Created

### Value Objects & Caches
* **[`python/rithmic_nt_connect/_orders.py`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/_orders.py)**:
  * **[`SeenKeyCache`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/_orders.py#L66)**: Encapsulates bounded LRU cache operations (`mark`, `has_seen`, `get`, `clear`, `__getitem__`, `__setitem__`). Used to replace manual, ad-hoc `OrderedDict` manipulations in `execution.py`.
  * **[`VenueNotification`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/_orders.py#L110)**: Domain wrapper over normalized wire notifications implementing "Tell, Don't Ask", such as `is_benign_bare_complete(order)` and safe property accessors (`basket_id`, `kind`, `status`, `symbol`).
* **[`tests/test_domain_value_objects.py`](file:///private/mnt/DATA/code/rithmic-connect/tests/test_domain_value_objects.py)**:
  * Comprehensive test suite verifying LRU eviction, recency bumps, and notification classification.

### Domain Collaborators
* **[`python/rithmic_nt_connect/commission.py`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/commission.py)**:
  * **[`CommissionRegistry`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/commission.py#L18)**: Single-responsibility class handling venue commission loading from order-plant RMS info, contract-to-product mapping resolution, account default fallback, and fee calculation.
* **[`tests/test_commission_registry.py`](file:///private/mnt/DATA/code/rithmic-connect/tests/test_commission_registry.py)**:
  * Unit tests validating independent product and account rate fetches, cache lookups, and USD Money calculations.
* **[`python/rithmic_nt_connect/polling.py`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/polling.py)**:
  * **[`PlantPoller`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/polling.py#L20)**: Encapsulates asynchronous event polling loops, transient failure backoff, streak tracking, channel error recovery, and failure latches.
* **[`tests/test_plant_poller.py`](file:///private/mnt/DATA/code/rithmic-connect/tests/test_plant_poller.py)**:
  * Unit tests covering clean dispatching, transient streak escalation, and failure latch triggers.

---

## 3. Modified Components

* **[`python/rithmic_nt_connect/execution.py`](file:///private/mnt/DATA/code/rithmic-connect/python/rithmic_nt_connect/execution.py)**:
  * Replaced procedural `OrderedDict` management for `_seen_fill_keys` and `_untracked_status_keys` with `SeenKeyCache`.
  * Delegated commission math and RMS loading to `CommissionRegistry`, with backward-compatible property getters/setters (`_commission_rates`, `_default_commission`) preserving compatibility with test doubles.
  * Delegated `_plant_poll_loop` directly to `PlantPoller`.
  * Delegated `is_benign_bare_complete` directly to `VenueNotification`.
  * Trimmed ~165 lines of code from `execution.py`.

---

## 4. Current Verification State

All gates pass cleanly with zero warnings or failures:

| Check | Command | Status |
| :--- | :--- | :--- |
| **Pytest** | `uv run pytest -q` | **497 passed**, 83 skipped (17.8s) |
| **Type Check** | `uv run ty check python/rithmic_nt_connect tests` | **All checks passed!** |
| **Ruff Linter** | `uv run ruff check .` | **All checks passed!** |
| **Ruff Formatter** | `uv run ruff format --check .` | **130 files formatted** |
| **Rust Clippy** | `cargo clippy --workspace --all-targets -- -D warnings` | **Finished dev profile [0 warnings]** |
| **Rust Fmt** | `cargo fmt --all -- --check` | **Passed** |
| **STATUS Check** | `python scripts/status_progress.py --check` | **OK: STATUS.md rollup matches** |

---

## 5. Next Steps for Subsequent Phases

Plan `docs/plans/2026-09-14-1932-refactor-exec-drain-intake-stores-plan.md` landed on branch `feat/exec-drain-intake-stores`:

1. ~~**Extract Working-Orders Drain**~~ → `python/rithmic_nt_connect/recon.py` (`WorkingOrdersDrain`)
2. ~~**Extract Venue Notification Intake**~~ → `python/rithmic_nt_connect/notification_intake.py`
3. ~~**Name fill/status stores**~~ → `FillDedupStore` / `UntrackedStatusBook`

Remaining:

1. **Streamline Market Data (`data.py`)**:
   * Extract `BarSubscriptionRegistry` to replace ad-hoc dictionary lookups in `bar_types_for_event`.
2. **Commission shim cleanup** / command-path extract (deferred in the plan).
