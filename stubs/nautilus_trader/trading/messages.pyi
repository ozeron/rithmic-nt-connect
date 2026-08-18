from _typeshed import Incomplete
from nautilus_trader.common.config import ImportableActorConfig as ImportableActorConfig
from nautilus_trader.core.message import Command as Command
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.model.identifiers import ComponentId as ComponentId, StrategyId as StrategyId
from nautilus_trader.trading.config import ImportableStrategyConfig as ImportableStrategyConfig

class CreateActor(Command):
    actor_config: Incomplete
    start: Incomplete
    def __init__(self, actor_config: ImportableActorConfig, start: bool = True, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class CreateStrategy(Command):
    strategy_config: Incomplete
    start: Incomplete
    def __init__(self, strategy_config: ImportableStrategyConfig, start: bool = True, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class StartActor(Command):
    actor_id: Incomplete
    def __init__(self, actor_id: ComponentId, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class StartStrategy(Command):
    strategy_id: Incomplete
    def __init__(self, strategy_id: StrategyId, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class StopActor(Command):
    actor_id: Incomplete
    def __init__(self, actor_id: ComponentId, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class StopStrategy(Command):
    strategy_id: Incomplete
    def __init__(self, strategy_id: StrategyId, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class RemoveActor(Command):
    actor_id: Incomplete
    def __init__(self, actor_id: ComponentId, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...

class RemoveStrategy(Command):
    strategy_id: Incomplete
    def __init__(self, strategy_id: StrategyId, command_id: UUID4 | None = None, ts_init: int = 0) -> None: ...
