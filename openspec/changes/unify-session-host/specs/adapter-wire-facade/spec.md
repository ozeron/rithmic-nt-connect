## MODIFIED Requirements

### Requirement: Recovery keeps PnL and lifecycle consistent
The recovery bridge SHALL run in both modes, driven by the session's `Interrupted` / `Resumed` / `RecoveryFailed` events. It SHALL NOT reset plants or replay market-data intents, and the adapter SHALL contain no `is_direct` branch.
- On `Interrupted`, the exec client SHALL disarm order handling.
- On `Resumed`, the exec client SHALL re-establish the PnL subscription (even with trading off), drain working orders, wait for the PnL snapshot barrier, and re-arm.
- An order-gap recovery and a PnL resync SHALL reset the PnL snapshot barrier.
- A failed exec connect SHALL cancel its poll tasks and detach its recovery listener.
- Recovery-bridge interrupted and failed callbacks SHALL run their non-latch work on the loop.

#### Scenario: Gateway re-dial with trading off
- **WHEN** the gateway connection is re-dialled while trading is disabled
- **THEN** the PnL subscription SHALL be restored

#### Scenario: Direct-mode plant recovery
- **WHEN** the host recovers a plant in direct mode
- **THEN** the exec client SHALL disarm on `Interrupted` and re-arm after reconciliation on `Resumed`, exactly as in gateway mode

#### Scenario: Data client after a host recovery
- **WHEN** the data client receives `Resumed`
- **THEN** it SHALL NOT call any reset or re-subscribe method, and its subscribed instruments SHALL keep receiving events

#### Scenario: Connect fails after the PnL poller started
- **WHEN** the order subscribe fails during connect
- **THEN** no poll task or recovery listener from that attempt SHALL remain
