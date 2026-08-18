from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider

class TemplateInstrumentProvider(InstrumentProvider):
    async def load_all_async(self, filters: dict | None = None) -> None: ...
