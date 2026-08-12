"""Read-only live execution client for Rithmic account/PnL (Phase 1)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money

from rithmic_connect._convert import account_pnl_to_fields
from rithmic_connect.config import RithmicExecClientConfig
from rithmic_connect.constants import ADAPTER_NAME
from rithmic_connect.constants import VENUE
from rithmic_connect.providers import RithmicInstrumentProvider
from rithmic_connect.session import WireSession


class RithmicReadOnlyExecutionClient(LiveExecutionClient):
    """Phase 1 execution client: publish account/PnL only; never place orders."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: RithmicInstrumentProvider,
        config: RithmicExecClientConfig,
        session: WireSession,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or f"{ADAPTER_NAME}-EXEC"),
            venue=Venue(VENUE),
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=Currency.from_str("USD"),
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=None,
        )
        self._config_local = config
        self._session = session
        self._poll_task: asyncio.Task | None = None
        self._order_calls: list[str] = []

    async def _connect(self) -> None:
        try:
            await asyncio.to_thread(self._session.connect)
            if self._config_local.session.has_account():
                await asyncio.to_thread(self._session.subscribe_pnl)
            self._poll_task = self.create_task(self._poll_loop(), log_msg="rithmic_pnl_poll")
        except Exception as exc:  # noqa: BLE001
            if self._config_local.soft_fail_pnl:
                self._log.warning(f"PnL/account path soft-failed: {exc}")
            else:
                raise

    async def _disconnect(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        try:
            await asyncio.to_thread(self._session.disconnect)
        except Exception as exc:  # noqa: BLE001
            self._log.warning(f"disconnect warning: {exc}")

    async def _poll_loop(self) -> None:
        while True:
            event = await asyncio.to_thread(self._session.poll_event)
            if event is None:
                await asyncio.sleep(0.05)
                continue
            if event.get("type") == "account_pnl":
                self._publish_account(event)

    def _publish_account(self, event: dict[str, Any]) -> None:
        fields = account_pnl_to_fields(event)
        account_id = AccountId(f"{VENUE}-{fields['account_id']}")
        if self.account_id is None:
            self._set_account_id(account_id)
        currency = Currency.from_str(str(fields.get("currency", "USD")))
        free_raw = fields.get("cash_on_hand") or fields.get("account_balance") or "0"
        try:
            free_dec = Decimal(str(free_raw))
        except Exception:  # noqa: BLE001
            free_dec = Decimal("0")
        free = Money(free_dec, currency)
        locked = Money(Decimal("0"), currency)
        total = free
        balances = [AccountBalance(total, locked, free)]
        self.generate_account_state(
            balances=balances,
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info={"rithmic_account_id": fields["account_id"]},
        )

    def _reject_order(self, action: str) -> None:
        self._order_calls.append(action)
        self._log.error(f"Phase 1 read-only client rejects order action: {action}")

    async def _submit_order(self, command: SubmitOrder) -> None:
        _ = command
        self._reject_order("submit_order")

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        _ = command
        self._reject_order("submit_order_list")

    async def _modify_order(self, command: ModifyOrder) -> None:
        _ = command
        self._reject_order("modify_order")

    async def _cancel_order(self, command: CancelOrder) -> None:
        _ = command
        self._reject_order("cancel_order")

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        _ = command
        self._reject_order("cancel_all_orders")

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        _ = command
        return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        _ = command
        return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        _ = command
        return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        _ = command
        return []
