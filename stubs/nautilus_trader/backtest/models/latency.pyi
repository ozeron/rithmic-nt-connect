from _typeshed import Incomplete
from typing import Any

__test__: dict

class LatencyModel:
    base_latency_nanos: Incomplete
    cancel_latency_nanos: Incomplete
    insert_latency_nanos: Incomplete
    update_latency_nanos: Incomplete
    def __init__(self, uint64_tbase_latency_nanos=..., uint64_tinsert_latency_nanos=..., uint64_tupdate_latency_nanos=..., uint64_tcancel_latency_nanos=..., config=...) -> None: ...
    def __reduce__(self): ...
    def __reduce_cython__(self) -> Any: ...
    def __setstate_cython__(self, __pyx_state) -> Any: ...
