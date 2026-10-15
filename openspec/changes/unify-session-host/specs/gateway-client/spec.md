## MODIFIED Requirements

### Requirement: Single-flight transport recovery with generation
On socket loss, `RemoteSession` SHALL emit `Interrupted` and perform one shared re-dial for all waiters. After re-attaching, it SHALL re-issue every subscription intent it has recorded, then increment its generation and emit `Resumed { generation }`. If the re-dial or the intent re-issue fails within its bounded window, it SHALL emit `RecoveryFailed`. The adapter SHALL NOT replay market-data intents itself. It SHALL use the events only to reconcile account and order state.

#### Scenario: Concurrent callers during a drop
- **WHEN** several threads issue RPCs while the socket is down
- **THEN** exactly one re-dial SHALL occur and the generation SHALL increase by one

#### Scenario: Intents re-joined after re-dial
- **WHEN** the socket drops while the session holds ticker, book and order intents, and the re-dial succeeds
- **THEN** the session SHALL re-issue those intents before emitting `Resumed`, and the consumer SHALL receive their events without re-subscribing

#### Scenario: Read-only RPC in flight at drop
- **WHEN** a read-only RPC was in flight when the socket dropped
- **THEN** the client MAY retry it once after the re-dial

#### Scenario: State-changing RPC in flight at drop
- **WHEN** a place, cancel or modify RPC was written and the socket dropped before the reply
- **THEN** the client SHALL return an unknown-outcome error and SHALL NOT retry

#### Scenario: Re-dial fails
- **WHEN** the re-dial cannot reach the parent within its bounded window
- **THEN** the session SHALL emit `RecoveryFailed`, and pending and subsequent RPCs SHALL fail with a transport error rather than hang

### Requirement: A plant reconnect keeps clients attached
When the parent's `SessionHost` recovers a plant, it SHALL restore the remembered subscription intents itself and SHALL keep attached clients connected. Each client SHALL receive `Interrupted` and then `Resumed` on its existing connection, instead of the connection being closed. A client SHALL NOT reset plants or re-issue market-data subscriptions in response. It MAY re-read its account state (working-order drain, PnL barrier) before trading resumes. Degraded market-data intents and account-intent escalation SHALL follow the host recovery rules.

#### Scenario: Reconnect during a flood
- **WHEN** the plants drop during a 10,000 events/s flood
- **THEN** each attached client SHALL receive `Interrupted` then `Resumed` on its existing connection, the venue SHALL see one subscribe per intent and no unsubscribe, and no event SHALL be lost without a gap

#### Scenario: One intent keeps failing
- **WHEN** a market-data intent fails on every restore pass
- **THEN** the gateway SHALL become ready with the other intents live, and a later subscribe for that key SHALL succeed once the venue accepts it
