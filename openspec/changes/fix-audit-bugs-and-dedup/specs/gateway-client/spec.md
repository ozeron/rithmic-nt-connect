## ADDED Requirements

### Requirement: Intents are refcounted per facade
The Rust client runtime SHALL count each subscription intent (ticker, book, time bars, PnL, order, brackets) per facade. It SHALL send an unsubscribe only when no facade holds the intent any more (1→0). Every subscribe SHALL be sent (the parent treats a repeat as a no-op; a repeat is how a client re-joins a degraded or lost intent). A facade's disconnect or drop SHALL release only that facade's intents, including when the facade is dropped without disconnecting. Requests for one intent SHALL reach the parent in the order they were decided, and a slow request for one intent SHALL NOT hold up requests for another. The runtime key SHALL include `account_id`, `fcm_id` and `ib_id`.

#### Scenario: Two facades share a symbol
- **WHEN** facades A and B both subscribe ES and A unsubscribes
- **THEN** B SHALL keep receiving ES events

#### Scenario: Exec facade stops
- **WHEN** one exec facade calls `disconnect_order_plant` while another facade holds an order intent
- **THEN** the order stream SHALL continue for the other facade

#### Scenario: Different accounts
- **WHEN** two configs differ only in `account_id`
- **THEN** they SHALL NOT share a socket or handshake

### Requirement: Error classes for malformed and failed commands
A `bad_request` reply SHALL map to `NotSent`. For one-shot commands, any parent error other than a venue reject or not-sent SHALL map to `OutcomeUnknown`.

#### Scenario: Undecodable request
- **WHEN** the parent cannot decode a request frame
- **THEN** the client SHALL receive `NotSent` for that request id and the connection SHALL stay up

#### Scenario: Unclassified plant error on cancel
- **WHEN** a cancel fails with a plant error that is neither a reject nor not-sent
- **THEN** the client SHALL return `OutcomeUnknown`

### Requirement: History retries do not compound
A history slice SHALL be retried by the plant layer only. The client SHALL add at most one read retry, so one slice never costs more than two client attempts.

#### Scenario: Transport error on a slice
- **WHEN** a bar slice fails with a transport error
- **THEN** the client SHALL issue at most two attempts for that slice

### Requirement: The pure-Python client keeps frames aligned
The pure-Python `GatewayClient` SHALL buffer partial reads so a non-blocking poll never discards bytes. It SHALL bound pending replies, report a send failure as not sent, and report a failure after the request was written as unknown.

#### Scenario: Partial frame on a non-blocking poll
- **WHEN** a poll with `timeout_ms=0` reads half a frame
- **THEN** the next poll SHALL complete that frame and decode it
