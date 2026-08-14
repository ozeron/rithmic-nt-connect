# Gateway remote access

How clients reach a `rithmic-gateway` parent that holds the single Rithmic login.

## v1 (local)

- Listen: `unix://$XDG_RUNTIME_DIR/rithmic-gateway-<hash>.sock` (mode `0600`)
- Auth: same UID + optional empty `auth_token`; Handshake fingerprint must match parent
- Only the gateway process (or `connect_mode=direct` with flock) opens Rithmic plants

## v1.5 — tunnel (no protocol change)

Keep the gateway on a host with Rithmic connectivity. Bind **unix** or **localhost-only** TCP when available.

From a VPS / laptop:

```bash
# Example: forward a future localhost TCP listen (v2) or use SSH remote unix
# forwarding / WireGuard so the client dials a local URL only.
ssh -N -L 7600:127.0.0.1:7600 user@gateway-host
```

Client config points at the **local** end of the tunnel (`unix://…` via remote socket forward, or `tls://127.0.0.1:7600` once v2 exists). Credentials stay on the gateway host; remotes carry at most a token (v2).

Docker: run one `rithmic-gateway` container/service with Rithmic env; app containers use the Compose service name only after v2 TLS listen lands — until then prefer host-network/unix mount or tunnel.

## v2 — native remote (not implemented yet)

| Item | Intent |
| --- | --- |
| Listen | `tls://0.0.0.0:PORT` (reject plaintext public TCP) |
| Auth | Non-empty `auth_token` required for non-unix |
| Scopes | Token / Ready scopes: `md`, `history`, `pnl`, `trade`, `cancel_all` |
| Compose | Gateway service holds secrets; apps get token + URL only |

Handshake already includes `auth_token` and Ready advertises scopes so v2 does not break the v1 codec.

## Never

- Second Rithmic login from a container/VPS for the same account
- Password on the gateway protobuf wire
- Advertising remote TLS before it is implemented and STATUS-marked
