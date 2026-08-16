"""Order-plant lifecycle state and trading policy."""

from __future__ import annotations

from enum import Enum


class OrderPlantState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    LIVE = "live"
    RESYNCING = "resyncing"


class OrderPlantPolicy:
    """Single place for submit / modify / cancel / report policy."""

    def __init__(self, state: OrderPlantState = OrderPlantState.DISCONNECTED) -> None:
        self.state = state

    def allow_submit(self) -> bool:
        return self.state is OrderPlantState.LIVE

    def allow_modify(self) -> bool:
        return self.state is OrderPlantState.LIVE

    def allow_cancel(self) -> bool:
        # Cancels remain available during resync (risk-reducing); blocked only when down.
        return self.state in {OrderPlantState.LIVE, OrderPlantState.RESYNCING}

    def load_orders_available(self) -> bool:
        return True

    def reject_reason(self, action: str) -> str:
        return f"order plant {self.state.value}; {action} blocked"
