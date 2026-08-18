from nautilus_trader.execution.messages import CancelOrder as CancelOrder, ModifyOrder as ModifyOrder, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.model.identifiers import ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, VenueOrderId as VenueOrderId
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity
from nautilus_trader.model.orders import Order as Order, OrderList as OrderList
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs as TestIdStubs

class TestCommandStubs:
    @staticmethod
    def submit_order_command(order: Order) -> SubmitOrder: ...
    @staticmethod
    def submit_order_list_command(order_list: OrderList) -> SubmitOrderList: ...
    @staticmethod
    def modify_order_command(price: Price | None = None, quantity: Quantity | None = None, instrument_id: InstrumentId | None = None, client_order_id: ClientOrderId | None = None, venue_order_id: VenueOrderId | None = None, order: Order | None = None) -> ModifyOrder: ...
    @staticmethod
    def cancel_order_command(instrument_id: InstrumentId | None = None, client_order_id: ClientOrderId | None = None, venue_order_id: VenueOrderId | None = None, order: Order | None = None) -> CancelOrder: ...
