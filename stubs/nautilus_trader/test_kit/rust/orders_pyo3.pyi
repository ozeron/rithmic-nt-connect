from nautilus_trader.core.nautilus_pyo3 import ClientOrderId as ClientOrderId, ExecAlgorithmId as ExecAlgorithmId, InstrumentId as InstrumentId, LimitOrder as LimitOrder, MarketOrder as MarketOrder, OrderSide as OrderSide, Price as Price, Quantity as Quantity, StopLimitOrder as StopLimitOrder, StrategyId as StrategyId, TimeInForce as TimeInForce, TraderId as TraderId, TriggerType as TriggerType
from nautilus_trader.test_kit.rust.identifiers_pyo3 import TestIdProviderPyo3 as TestIdProviderPyo3

class TestOrderProviderPyo3:
    @staticmethod
    def market_order(instrument_id: InstrumentId | None = None, order_side: OrderSide | None = None, quantity: Quantity | None = None, trader_id: TraderId | None = None, strategy_id: StrategyId | None = None, client_order_id: ClientOrderId | None = None, time_in_force: TimeInForce | None = None) -> MarketOrder: ...
    @staticmethod
    def limit_order(instrument_id: InstrumentId, order_side: OrderSide, quantity: Quantity, price: Price, trader_id: TraderId | None = None, strategy_id: StrategyId | None = None, client_order_id: ClientOrderId | None = None, time_in_force: TimeInForce | None = None, exec_algorithm_id: ExecAlgorithmId | None = None) -> LimitOrder: ...
    @staticmethod
    def stop_limit_order(instrument_id: InstrumentId, order_side: OrderSide, quantity: Quantity, price: Price, trigger_price: Price, trigger_type: TriggerType = ..., trader_id: TraderId | None = None, strategy_id: StrategyId | None = None, client_order_id: ClientOrderId | None = None, time_in_force: TimeInForce | None = None, exec_algorithm_id: ExecAlgorithmId | None = None, tags: list[str] | None = None) -> StopLimitOrder: ...
