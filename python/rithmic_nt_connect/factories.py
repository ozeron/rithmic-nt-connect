"""TradingNode factories for Rithmic data and execution clients."""

from __future__ import annotations

import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataClientConfig,
    LiveExecClientConfig,
)
from nautilus_trader.live.factories import LiveDataClientFactory, LiveExecClientFactory

from rithmic_nt_connect.config import (
    RithmicDataClientConfig,
    RithmicExecClientConfig,
    RithmicLiveExecClientConfig,
    SessionConfig,
)
from rithmic_nt_connect.data import RithmicDataClient
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.pandas_compat import patch_nautilus_pandas
from rithmic_nt_connect.providers import RithmicInstrumentProvider
from rithmic_nt_connect.session import (
    PLANTS_EXECUTION,
    PLANTS_MARKET_DATA,
    create_session,
)

# The pandas shim only matters at runtime (BacktestEngine.run, INTERNAL bar
# aggregation) — same placement as historical.py, keeping all imports at top.
patch_nautilus_pandas()


def _session_config_from_data(config: object) -> SessionConfig:
    session = getattr(config, "session", None)
    if isinstance(session, SessionConfig):
        return session
    return SessionConfig.from_env()


def _pairs_from_session(session: SessionConfig) -> list[tuple[str, str]]:
    if session.symbol and session.exchange:
        return [(session.symbol, session.exchange)]
    return [("NQ", "CME")]


class RithmicLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: LiveDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> RithmicDataClient:
        session_cfg = _session_config_from_data(config)
        session = create_session(session_cfg, plants=PLANTS_MARKET_DATA)
        provider = RithmicInstrumentProvider(
            session=session,
            pairs=_pairs_from_session(session_cfg),
            config=InstrumentProviderConfig(load_all=True),
        )
        data_config = (
            config
            if isinstance(config, RithmicDataClientConfig)
            else RithmicDataClientConfig(session=session_cfg)
        )
        return RithmicDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=data_config,
            session=session,
            name=name,
        )


class RithmicLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: LiveExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> RithmicExecutionClient:
        session_cfg = _session_config_from_data(config)
        session = create_session(session_cfg, plants=PLANTS_EXECUTION)
        provider = RithmicInstrumentProvider(
            session=session,
            pairs=_pairs_from_session(session_cfg),
            config=InstrumentProviderConfig(load_all=True),
        )
        # Pass the registered live config through unchanged: it carries
        # ``enable_trading`` (the fallback only covers a bare nautilus
        # ``LiveExecClientConfig``, which never enables trading).
        client_config: RithmicExecClientConfig | RithmicLiveExecClientConfig
        if isinstance(config, (RithmicLiveExecClientConfig, RithmicExecClientConfig)):
            client_config = config
        else:
            client_config = RithmicExecClientConfig(session=session_cfg)
        return RithmicExecutionClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=client_config,
            session=session,
            name=name,
        )
