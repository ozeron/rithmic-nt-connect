"""LiveNode factories for Rithmic data and execution clients.

NautilusTrader 2 calls ``create`` synchronously with the owning node's context
(name, config, read-only cache, clock and, for execution, the trader id). The
factory builds the client and its session object only: no task, event loop or
network resource is touched before the client's ``_connect``.
"""

from __future__ import annotations

from typing import Any

from nautilus_trader.common import Clock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live import ClientCache
from nautilus_trader.live.clients import DataClientFactory, ExecutionClientFactory
from nautilus_trader.model import AccountId, TraderId

from rithmic_nt_connect.config import (
    RithmicDataClientConfig,
    RithmicExecClientConfig,
    RithmicLiveDataClientConfig,
    RithmicLiveExecClientConfig,
    SessionConfig,
)
from rithmic_nt_connect.constants import AUTO_ACCOUNT, VENUE
from rithmic_nt_connect.data import RithmicDataClient
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.providers import RithmicInstrumentProvider
from rithmic_nt_connect.session import (
    PLANTS_EXECUTION,
    PLANTS_MARKET_DATA,
    create_session,
)


def _session_config_from_data(config: object) -> SessionConfig:
    session = getattr(config, "session", None)
    if isinstance(session, SessionConfig):
        return session
    return SessionConfig.from_env()


def _pairs_from_session(session: SessionConfig) -> list[tuple[str, str]]:
    if session.symbol and session.exchange:
        return [(session.symbol, session.exchange)]
    return [("NQ", "CME")]


def nautilus_account_id(session: SessionConfig) -> AccountId:
    """The node's account identity for a Rithmic session.

    NautilusTrader 2 fixes an execution client's ``account_id`` at construction
    (every event and report must carry it), while Rithmic resolves the trading
    account at login. A configured ``RITHMIC_ACCOUNT_ID`` names the account
    (``RITHMIC-<id>``); without one the venue-resolved account is used under the
    stable ``RITHMIC-AUTO`` label and recorded in the AccountState ``info``.
    """
    raw = (session.account_id or "").strip()
    return AccountId(f"{VENUE}-{raw or AUTO_ACCOUNT}")


class RithmicLiveDataClientFactory(DataClientFactory):
    @staticmethod
    def create(
        *,
        name: str,
        config: Any,
        cache: ClientCache,
        clock: Clock,
    ) -> RithmicDataClient:
        session_cfg = _session_config_from_data(config)
        session = create_session(session_cfg, plants=PLANTS_MARKET_DATA)
        provider = RithmicInstrumentProvider(
            session=session,
            pairs=_pairs_from_session(session_cfg),
            config=InstrumentProviderConfig(load_all=True),
        )
        # The node-registered config is the Nautilus config (base fields such as
        # routing are read by the node); Rithmic knobs ride on it.
        if not isinstance(config, RithmicLiveDataClientConfig):
            config = RithmicLiveDataClientConfig(
                session=session_cfg,
                bars_timestamp_on_close=bool(
                    getattr(config, "bars_timestamp_on_close", True)
                ),
            )
        return RithmicDataClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            session=session,
        )


class RithmicLiveExecClientFactory(ExecutionClientFactory):
    @staticmethod
    def create(
        *,
        name: str,
        config: Any,
        cache: ClientCache,
        clock: Clock,
        trader_id: TraderId,
    ) -> RithmicExecutionClient:
        session_cfg = _session_config_from_data(config)
        session = create_session(session_cfg, plants=PLANTS_EXECUTION)
        provider = RithmicInstrumentProvider(
            session=session,
            pairs=_pairs_from_session(session_cfg),
            config=InstrumentProviderConfig(load_all=True),
        )
        # Pass the registered live config through unchanged: it carries
        # ``enable_trading`` (a bare ``ExecutionClientConfig`` never enables
        # trading).
        if not isinstance(config, RithmicLiveExecClientConfig):
            config = RithmicLiveExecClientConfig(session=session_cfg)
        return RithmicExecutionClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            trader_id=trader_id,
            account_id=nautilus_account_id(session_cfg),
            instrument_provider=provider,
            session=session,
        )


__all__ = [
    "RithmicDataClientConfig",
    "RithmicExecClientConfig",
    "RithmicLiveDataClientFactory",
    "RithmicLiveExecClientFactory",
    "nautilus_account_id",
]
