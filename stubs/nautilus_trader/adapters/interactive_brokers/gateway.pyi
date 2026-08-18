import types
from _typeshed import Incomplete
from enum import IntEnum
from nautilus_trader.adapters.interactive_brokers.config import DockerizedIBGatewayConfig
from typing import ClassVar

__all__ = ['DockerizedIBGateway']

class ContainerStatus(IntEnum):
    NO_CONTAINER = 1
    CONTAINER_CREATED = 2
    CONTAINER_STARTING = 3
    CONTAINER_STOPPED = 4
    NOT_LOGGED_IN = 5
    READY = 6
    UNKNOWN = 7

class DockerizedIBGateway:
    CONTAINER_NAME: ClassVar[str]
    PORTS_INTERNAL: ClassVar[dict[str, int]]
    PORTS_EXTERNAL: ClassVar[dict[str, int]]
    VNC_PORT_INTERNAL: ClassVar[int]
    log: Incomplete
    username: Incomplete
    password: Incomplete
    trading_mode: Incomplete
    read_only_api: Incomplete
    host: str
    port: Incomplete
    timeout: Incomplete
    container_image: Incomplete
    vnc_port: Incomplete
    def __init__(self, config: DockerizedIBGatewayConfig) -> None: ...
    @property
    def container_name(self) -> str: ...
    @property
    def container_status(self) -> ContainerStatus: ...
    @property
    def container(self): ...
    @staticmethod
    def is_logged_in(container) -> bool: ...
    def start(self, wait: int | None = None) -> None: ...
    def safe_start(self, wait: int | None = None) -> None: ...
    def stop(self) -> None: ...
    def __enter__(self) -> None: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None) -> None: ...

class ContainerExists(Exception): ...
class NoContainer(Exception): ...
class UnknownContainerStatus(Exception): ...
class GatewayLoginFailure(Exception): ...
