"""Shared NQ 4-bar rule: SMA20 (daily) + VWAP (1m) + 1s INTERNAL signals.

* SMA20 — ``1-DAY-LAST-EXTERNAL``: ``request_bars`` then ``subscribe_bars``.
* VWAP — one register on ``1-MINUTE-LAST-INTERNAL``. Live lookback applies
  venue ``1-MINUTE-LAST-EXTERNAL`` via ``handle_bar`` (fast); live 1m is INTERNAL.
* 4-bar rule stays on ``1-SECOND-LAST-INTERNAL``.

https://nautilustrader.io/docs/latest/concepts/strategies/
"""

from __future__ import annotations

from datetime import timedelta
from datetime import timezone
from zoneinfo import ZoneInfo

from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import SimpleMovingAverage
from nautilus_trader.indicators import VolumeWeightedAveragePrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import PositionChanged
from nautilus_trader.model.events import PositionClosed
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.trading.strategy import Strategy

from rithmic_nt_connect import VENUE

_CHI = ZoneInfo("America/Chicago")


class NqFourBarConfig(StrategyConfig, frozen=True):
    strategy_id: str = "NqFourBar-001"
    log_events: bool = False
    log_commands: bool = False
    venue: str = VENUE
    request_lookback: bool = False
    sma_period: int = 20
    sma_lookback_days: int = 40


class NqFourBarStrategy(Strategy):
    def __init__(self, config: NqFourBarConfig) -> None:
        super().__init__(config)
        self._instrument: Instrument | None = None
        self._last_close: Price | None = None
        self._down = 0
        self._up = 0
        self._sma = SimpleMovingAverage(int(config.sma_period))
        self._vwap = VolumeWeightedAveragePrice()
        self._daily_type: BarType | None = None
        self._m1_ext: BarType | None = None
        self._m1_int: BarType | None = None
        self._sec_type: BarType | None = None
        self._hist_day = 0
        self._hist_m1 = 0
        self._sma_ready = False
        self._vwap_ready = False
        self.bars = 0
        self.fills = 0

    def _note_ready(self) -> None:
        if self._sma.initialized and not self._sma_ready:
            self._sma_ready = True
            self.log.info(f"SMA20 READY  {self._sma.value:.2f}  daily_bars={self._hist_day}")
        if self._vwap.initialized and not self._vwap_ready:
            self._vwap_ready = True
            self.log.info(f"VWAP READY  {self._vwap.value:.2f}  m1_bars={self._hist_m1}")

    def _ctx(self) -> str:
        need = int(self.config.sma_period)
        sma = (
            f"{self._sma.value:.2f}"
            if self._sma.initialized
            else f"warming({self._hist_day}/{need})"
        )
        vwap = (
            f"{self._vwap.value:.2f}"
            if self._vwap.initialized
            else f"warming(m1={self._hist_m1})"
        )
        return f"sma{need}={sma} vwap={vwap}"

    def _rth_open_utc(self, now):
        chi = now.astimezone(_CHI)
        rth = chi.replace(hour=8, minute=30, second=0, microsecond=0).astimezone(timezone.utc)
        if now >= rth:
            return rth
        return rth - timedelta(days=1)

    def _ensure_indicator(self, bar_type: BarType, indicator) -> None:
        if indicator in list(self.registered_indicators):
            return
        self.register_indicator_for_bars(bar_type, indicator)

    def _after_lookback(self, bar_type: BarType, label: str):
        def _cb(_req: object) -> None:
            self._note_ready()
            self.log.info(f"{label} lookback done  {self._ctx()}")
            self.subscribe_bars(bar_type)

        return _cb

    def on_start(self) -> None:
        venue = self.config.venue
        instruments = [inst for inst in self.cache.instruments() if str(inst.id.venue) == venue]
        if not instruments:
            self.log.error(f"no {venue} instruments in cache")
            self.stop()
            return
        self._instrument = instruments[0]
        instrument_id = self._instrument.id
        self._daily_type = BarType.from_str(f"{instrument_id}-1-DAY-LAST-EXTERNAL")
        self._m1_ext = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")
        self._m1_int = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-INTERNAL")
        self._sec_type = BarType.from_str(f"{instrument_id}-1-SECOND-LAST-INTERNAL")
        self._ensure_indicator(self._daily_type, self._sma)
        # One registration only (Nautilus errors if the same indicator is
        # registered on EXTERNAL and INTERNAL). Warmup feeds EXTERNAL 1m via
        # handle_bar; live 1m INTERNAL uses this hook.
        self._ensure_indicator(self._m1_int, self._vwap)
        daily = self._daily_type
        m1_ext = self._m1_ext
        m1_int = self._m1_int
        self.subscribe_trade_ticks(instrument_id)
        self.subscribe_bars(self._sec_type)
        if self.config.request_lookback:
            now = self.clock.utc_now()
            self.request_bars(
                daily,
                start=now - timedelta(days=int(self.config.sma_lookback_days)),
                end=now,
                callback=self._after_lookback(daily, "SMA"),
            )
            self.request_bars(
                m1_ext,
                start=self._rth_open_utc(now),
                end=now,
                callback=self._after_lookback(m1_int, "VWAP"),
            )
        else:
            self.subscribe_bars(daily)
            self.subscribe_bars(m1_int)
        self.log.info(
            f"ready {instrument_id}  daily SMA{int(self.config.sma_period)} "
            f"+ 1m VWAP (EXTERNAL warmup / INTERNAL live) + 1s 4-bar  "
            f"(lookback={'on' if self.config.request_lookback else 'off'})"
        )

    def on_reset(self) -> None:
        self._sma.reset()
        self._vwap.reset()
        self._hist_day = 0
        self._hist_m1 = 0
        self._sma_ready = False
        self._vwap_ready = False

    def on_historical_data(self, data) -> None:
        if not isinstance(data, Bar):
            return
        spec = data.bar_type.spec
        if spec.aggregation == BarAggregation.DAY:
            self._hist_day += 1
            self._note_ready()
            self.log.info(f"HIST DAY #{self._hist_day} {data.close}  {self._ctx()}")
        elif spec.aggregation == BarAggregation.MINUTE and spec.step == 1:
            if self._m1_ext is not None and data.bar_type == self._m1_ext:
                self._vwap.handle_bar(data)
            self._hist_m1 += 1
            self._note_ready()
            if self._hist_m1 <= 3 or self._hist_m1 % 15 == 0:
                self.log.info(f"HIST M1 #{self._hist_m1} {data.close}  {self._ctx()}")

    def _has_working_order(self) -> bool:
        assert self._instrument is not None
        instrument_id = self._instrument.id
        return bool(
            self.cache.orders_inflight(instrument_id=instrument_id)
            or self.cache.orders_open(instrument_id=instrument_id)
        )

    def on_bar(self, bar: Bar) -> None:
        if self._instrument is None:
            return
        spec = bar.bar_type.spec
        if spec.aggregation == BarAggregation.DAY:
            self.log.info(f"DAY {bar.close}  {self._ctx()}")
            return
        if spec.aggregation == BarAggregation.MINUTE and spec.step == 1:
            self.log.info(f"M1 {bar.close}  vwap={self._vwap.value:.2f}  {self._ctx()}")
            return
        self.bars += 1
        prev = self._last_close
        self._last_close = bar.close
        if prev is None:
            self.log.info(f"BAR {bar.close}  first  {self._ctx()}")
            return
        if bar.close < prev:
            self._down += 1
            self._up = 0
        elif bar.close > prev:
            self._up += 1
            self._down = 0
        else:
            self._down = 0
            self._up = 0
        self.log.info(f"BAR {bar.close}  down={self._down} up={self._up}  {self._ctx()}")
        if self._down < 4 and self._up < 4:
            return
        if self._has_working_order():
            return
        side = OrderSide.BUY if self._down >= 4 else OrderSide.SELL
        self._down = 0
        self._up = 0
        net = self.portfolio.net_position(self._instrument.id)
        order = self.order_factory.market(
            self._instrument.id,
            side,
            self._instrument.make_qty(1),
            time_in_force=TimeInForce.IOC,
        )
        self.submit_order(order)
        self.log.info(f"SIGNAL {side.name} 1  BAR {bar.close}  net_was={net}")

    def on_order_filled(self, event: OrderFilled) -> None:
        self.fills += 1
        self.log.info(f"FILL  {event.order_side.name} {event.last_qty} @ {event.last_px}")

    def on_position_opened(self, event: PositionOpened) -> None:
        self.log.info(f"POS   {event.side.name} {event.quantity}  avg={event.avg_px_open}")

    def on_position_changed(self, event: PositionChanged) -> None:
        self.log.info(
            f"POS   {event.side.name} {event.quantity}  "
            f"avg={event.avg_px_open}  rPnL={event.realized_pnl}"
        )

    def on_position_closed(self, event: PositionClosed) -> None:
        self.log.info(
            f"POS   FLAT  rPnL={event.realized_pnl}  "
            f"open={event.avg_px_open} close={event.avg_px_close}"
        )

    def on_stop(self) -> None:
        if self._instrument is None:
            return
        if not self.portfolio.is_flat(self._instrument.id):
            self.log.info(f"FLATTEN net={self.portfolio.net_position(self._instrument.id)}")
            self.close_all_positions(self._instrument.id)
        self.cancel_all_orders(self._instrument.id)
