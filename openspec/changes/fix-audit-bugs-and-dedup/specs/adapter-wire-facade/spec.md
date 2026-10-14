## MODIFIED Requirements

### Requirement: Packaging and lake client unaffected
The `rithmic-gateway` wheel SHALL carry the pure-Python `rithmic_gateway` package and the `rithmic-gateway` binary. The `rithmic-nt-connect` wheel SHALL depend on `rithmic-gateway` instead of bundling the binary. No wheel SHALL contain any fake-plant binary. The `rithmic_gateway` package SHALL remain importable without Nautilus or the extension.

#### Scenario: Wheel contents
- **WHEN** the wheels are built by the CI wheel workflow
- **THEN** the `rithmic-gateway` wheel SHALL contain the `rithmic-gateway` binary, and neither wheel SHALL contain `rithmic-gateway-fake`

#### Scenario: Lake import
- **WHEN** `rithmic_gateway.GatewayClient` is imported in an environment with neither Nautilus nor the extension installed
- **THEN** the import SHALL succeed

## ADDED Requirements

### Requirement: No blocking session call on the event loop
Every session call that may perform I/O SHALL run off the asyncio loop. The resolved account SHALL be fetched off-loop and cached; synchronous helpers SHALL read only the cache.

#### Scenario: Waiting for account registration in gateway mode
- **WHEN** the exec client waits for its account to register while the gateway command lane is slow
- **THEN** the event loop SHALL keep running other tasks

### Requirement: Position recon requires live PnL
Position status reports SHALL be produced only from positions observed on the current live PnL stream. When PnL is not live, the client SHALL return no position reports and mass status SHALL NOT add flat reports for open positions in the cache.

#### Scenario: Trading off, PnL not yet observed
- **WHEN** mass status is generated before any PnL snapshot on the current stream
- **THEN** it SHALL contain no position reports, including no fabricated flat reports

#### Scenario: Stale position after a resync
- **WHEN** a position row was last seen before the current PnL stream started
- **THEN** it SHALL NOT be reported

### Requirement: Book updates follow the venue update type
Order-book events SHALL be converted according to their `update_type`, and unknown values SHALL be rejected:

| `update_type` | Output |
|---|---|
| snapshot image | CLEAR plus ADDs flagged `F_SNAPSHOT`; the last delta also `F_LAST` |
| clear, no book | CLEAR with `F_LAST` |
| begin, middle | UPDATE/DELETE deltas without `F_LAST` |
| end | UPDATE/DELETE deltas; the last delta carries `F_LAST` |
| solo | UPDATE/DELETE deltas; the last delta carries `F_LAST` |

A level with size 0 SHALL be a DELETE.

#### Scenario: Solo update
- **WHEN** a book event with update type solo changes one bid level
- **THEN** one UPDATE delta with `F_LAST` SHALL be emitted and the rest of the book SHALL be kept

#### Scenario: Multi-part update
- **WHEN** begin, middle and end events arrive
- **THEN** only the last delta of the end event SHALL carry `F_LAST`

### Requirement: History requests always complete
A trade-tick or bar request SHALL always complete with data, empty data or an error log, including when the instrument has no route or a row fails conversion. A row that fails conversion SHALL be skipped and counted.

#### Scenario: Unloaded instrument
- **WHEN** `request_bars` is called for an instrument with no route
- **THEN** the request SHALL complete empty and an error SHALL be logged

### Requirement: Brackets are all-or-nothing
A bracket order list SHALL be recognised only through parent or linked order ids. All three legs SHALL receive the same submitted, rejected or denied events. The whole list SHALL be denied if either the stop or the target cannot be expressed in ticks.

#### Scenario: Stop ticks round to zero
- **WHEN** a bracket's stop distance rounds to zero ticks
- **THEN** all three orders SHALL be denied and nothing SHALL be sent

#### Scenario: Successful bracket
- **WHEN** a bracket is placed successfully
- **THEN** the entry, stop and target SHALL each receive `OrderSubmitted`

### Requirement: Fill identity does not depend on the adapter clock
The fill dedup key and trade id SHALL be computed from the fill's own fields, both live and in recon. The adapter clock SHALL be used only as the event timestamp when the venue gives none.

#### Scenario: Fill without fill id or timestamp
- **WHEN** a fill with no `fill_id` and no `ts_event` arrives live and is later seen in recon
- **THEN** it SHALL be emitted once

### Requirement: The order stream survives a bad notification
An order notification that fails conversion SHALL be logged with its payload and SHALL trigger an order re-drain. The order poll task SHALL keep running.

#### Scenario: Notification without a symbol
- **WHEN** one order notification lacks its symbol
- **THEN** later notifications in the batch and stream SHALL still be processed and a re-drain SHALL run

### Requirement: Recovery keeps PnL and lifecycle consistent
- A transport-up restore SHALL re-establish the PnL subscription even with trading off.
- An order-gap recovery and a PnL resync SHALL reset the PnL snapshot barrier.
- A failed exec connect SHALL cancel its poll tasks and detach its recovery listener.
- Recovery-bridge down and failed callbacks SHALL run their non-latch work on the loop.

#### Scenario: Gateway re-dial with trading off
- **WHEN** the gateway connection is re-dialled while trading is disabled
- **THEN** the PnL subscription SHALL be restored

#### Scenario: Connect fails after the PnL poller started
- **WHEN** the order subscribe fails during connect
- **THEN** no poll task or recovery listener from that attempt SHALL remain

### Requirement: Bar timestamps follow the configured convention
The data client config SHALL expose `bars_timestamp_on_close: bool`, default `True`. When `True`, EXTERNAL bars SHALL be stamped at the venue bar close; when `False`, at bar open. The client SHALL warn when the setting disagrees with the data engine's `time_bars_timestamp_on_close`. Unsubscribing a bar type SHALL unsubscribe its period at the venue when no other subscription for that period remains.

#### Scenario: Default close stamping
- **WHEN** a 1-minute bar with venue marker 10:01:00 arrives with the default config
- **THEN** its `ts_event` SHALL be 10:01:00

#### Scenario: Open stamping
- **WHEN** the same bar arrives with `bars_timestamp_on_close=False`
- **THEN** its `ts_event` SHALL be 10:00:00

#### Scenario: Unsubscribe one of two periods
- **WHEN** 1-minute and 5-minute bars are subscribed and 5-minute is unsubscribed
- **THEN** the venue 5-minute subscription SHALL be removed
