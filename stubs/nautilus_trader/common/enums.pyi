from enum import Enum
from nautilus_trader.common.component import component_state_from_str as component_state_from_str, component_state_to_str as component_state_to_str, component_trigger_from_str as component_trigger_from_str, component_trigger_to_str as component_trigger_to_str, log_level_from_str as log_level_from_str, log_level_to_str as log_level_to_str
from nautilus_trader.core.rust.common import ComponentState as ComponentState, ComponentTrigger as ComponentTrigger, LogColor as LogColor, LogLevel as LogLevel

__all__ = ['ComponentState', 'ComponentTrigger', 'LogColor', 'LogLevel', 'component_state_from_str', 'component_state_to_str', 'component_trigger_from_str', 'component_trigger_to_str', 'log_level_from_str', 'log_level_to_str']

class ComponentState(Enum):
    PRE_INITIALIZED = 0
    READY = 1
    STARTING = 2
    RUNNING = 3
    STOPPING = 4
    STOPPED = 5
    RESUMING = 6
    RESETTING = 7
    DISPOSING = 8
    DISPOSED = 9
    DEGRADING = 10
    DEGRADED = 11
    FAULTING = 12
    FAULTED = 13

class ComponentTrigger(Enum):
    INITIALIZE = 1
    START = 2
    START_COMPLETED = 3
    STOP = 4
    STOP_COMPLETED = 5
    RESUME = 6
    RESUME_COMPLETED = 7
    RESET = 8
    RESET_COMPLETED = 9
    DISPOSE = 10
    DISPOSE_COMPLETED = 11
    DEGRADE = 12
    DEGRADE_COMPLETED = 13
    FAULT = 14
    FAULT_COMPLETED = 15

class LogLevel(Enum):
    OFF = 0
    TRACE = 1
    DEBUG = 2
    INFO = 3
    WARNING = 4
    ERROR = 5

class LogColor(Enum):
    NORMAL = 0
    GREEN = 1
    BLUE = 2
    MAGENTA = 3
    CYAN = 4
    YELLOW = 5
    RED = 6
