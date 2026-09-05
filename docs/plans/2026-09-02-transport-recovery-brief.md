# TransportRecovery design brief (incident → module)

## Incident (2026-09-02 live MY043)

Mux drain EOF → notify generation → exec **only latched** → drain exited → polls silent `None` → MD stall. No L3 re-dial owner.

## Oracle (2026-09-02, `transport-recovery-module-v3`)

**APPROVE_WITH_FIXES.** Seam: `GatewayRuntime`-owned `TransportRecovery`. Fixes applied: gen bumps only after successful replace; typed `TransportFault`; DOWN→RECOVERING→UP/FAILED; listeners fire-and-forget; STOPPING ignores faults; poll-raises-eof KEEP as wake (not recovery).

## Landed module

- `python/rithmic_gateway/transport_recovery.py`
- `GatewayRuntime.transport` + mux `set_transport_fault_handler`
- Data/Exec: `TRANSPORT_DOWN` / `TRANSPORT_UP` → latch / plant restore / MD replay
- Dial ownership: `TransportRecovery.ensure_live` / flight only
