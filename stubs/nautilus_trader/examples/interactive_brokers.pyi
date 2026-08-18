from _typeshed import Incomplete

LOCALHOSTS: Incomplete
DEFAULT_IB_PORT_CANDIDATES: Incomplete

def is_ib_endpoint_reachable(host: str, port: int, timeout: float = 0.25) -> bool: ...
def resolve_ib_endpoint(host_env_var: str, port_env_var: str, *, default_host: str = '127.0.0.1', default_port: int = 7497, candidate_ports: tuple[int, ...] = ...) -> tuple[str, int]: ...
