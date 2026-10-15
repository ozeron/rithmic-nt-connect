# Let `SessionHost` own plant recovery and intent restore in both modes

**Status: Proposed** (accepted when `unify-session-host` lands).

`SessionHost` owns plant recovery and intent restore for both direct and gateway sessions. Consumers receive `Interrupted`, then `Resumed { generation }` after intents are restored, or `RecoveryFailed` when bounded recovery is exhausted. A plant failure under recovery is a session event, not a consumer stream error: `next_events` returns empty batches until recovery finishes. `RemoteSession` also owns socket re-attach and re-joins its own recorded intents before reporting `Resumed`. The adapter no longer replays market-data intents or calls `reset_ticker`. Keeping market-data replay in the adapter for the remote path alone was rejected because it would preserve two recovery paths.

This supersedes ADR 0002's adapter-side market-data replay and its deliberate direct-mode no-op ("direct mode has no L3 recovery"). Restore *policy* stays in the adapter's `TransportRecoveryBridge`: exec disarms on interruption, drains working orders and waits for the PnL barrier before re-arming; capability policy stays there too. Data still reconciles books and bars on a gap. The gateway parent never owns exec re-arm. Host gates remain separate from that policy, and a remote session cannot elevate them. One-shot commands interrupted after send remain unknown and are never retried.

Direct mode gains recovery through the same host as gateway mode. The only wire addition is `TransportInterrupted`; old clients ignore it. Mixed-version clients may replay twice, harmlessly: the parent treats repeat subscribes as no-ops, so the extra cost is only redundant frames.
