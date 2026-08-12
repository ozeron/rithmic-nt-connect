"""TradingNode factories for Rithmic data and read-only execution clients."""

from __future__ import annotations

import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory

from rithmic_connect.config import RithmicDataClientConfig
from rithmic_connect.config import RithmicExecClientConfig
from rithmic_connect.config import SessionConfig
from rithmic_connect.data import RithmicDataClient
from rithmic_connect.execution import RithmicReadOnlyExecutionClient
from rithmic_connect.providers import RithmicInstrumentProvider
from rithmic_connect.session import WireSession
from rithmic_connect.session import create_rust_session


def _pairs_from_session(session: SessionConfig) -> list[tuple[str, str]]:
    if session.symbol and session.exchange:
        return [(session.symbol, session.exchange)]
    return [("NQ", "CME")]


def _shared_session(
    config_session: SessionConfig,
    cache: dict[str, WireSession],
) -> WireSession:
    key = f"{config_session.user}:{config_session.system_name}:{config_session.url}"
    if key not in cache:
        cache[key] = create_rust_session(config_session)
    return cache[key]


_SESSION_CACHE: dict[str, WireSession] = {}


class RithmicLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: RithmicDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> RithmicDataClient:
        session = _shared_session(config.session, _SESSION_CACHE)
        provider = RithmicInstrumentProvider(
            session=session,
            pairs=_pairs_from_session(config.session),
            config=InstrumentProviderConfig(load_all=True),
        )
        return RithmicDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            session=session,
            name=name,
        )


class RithmicLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: RithmicExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> RithmicReadOnlyExecutionClient:
        session = _shared_session(config.session, _SESSION_CACHE)
        provider = RithmicInstrumentProvider(
            session=session,
            pairs=_pairs_from_session(config.session),
            config=InstrumentProviderConfig(load_all=True),
        )
        return RithmicReadOnlyExecutionClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            session=session,
            name=name,
        )
