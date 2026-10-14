"""Plant-level gateway client over unix + length-delimited protobuf."""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any, cast

from google.protobuf.json_format import MessageToDict

from rithmic_gateway.attach import AttachError, GatewayAttachCoordinator
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framed_conn import (
    _CONNECT_FIRST,
    _NOT_CONNECTED,
    DEFAULT_RPC_TIMEOUT_SEC,
    FramedConnection,
    GatewayError,
)
from rithmic_gateway.types import AccountRmsInfo, ProductRmsInfo
from rithmic_gateway.v1 import session_pb2 as pb

# Per-chunk history RPC timeout — one plant slice can exceed the dial default.
DEFAULT_HISTORY_RPC_TIMEOUT_SEC = 300.0

_ORDER_EVENT_TYPES = frozenset({"order_notification"})
_PNL_EVENT_TYPES = frozenset({"account_pnl", "instrument_pnl"})
_HISTORY_EVENT_TYPES = frozenset({"time_bar"})
_NON_MD_EVENT_TYPES = _ORDER_EVENT_TYPES | _PNL_EVENT_TYPES | _HISTORY_EVENT_TYPES

_CAPABILITY_DENIED_WIRE_CODES = frozenset({"history_denied_live_md"})


def _normalize_error_code(code: str) -> str:
    if code in _CAPABILITY_DENIED_WIRE_CODES:
        return "capability_denied"
    return code


def _wire_error(code: str, message: str) -> GatewayError:
    return GatewayError(_normalize_error_code(code), message)


def _is_md_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") not in _NON_MD_EVENT_TYPES


def _is_history_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") in _HISTORY_EVENT_TYPES


def _is_pnl_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") in _PNL_EVENT_TYPES


def _is_order_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") in _ORDER_EVENT_TYPES


def _dial_with_attach(config: GatewayConfig, dial: Callable[[str], None]) -> None:
    """Dial the listen path; on failure wait for a starting parent or report none.

    The client never starts the parent: a held credential flock means someone
    else's parent is coming up (bounded wait), otherwise ``parent_not_running``.
    """
    path = config.socket_path
    try:
        dial(path)
        return
    except OSError:
        pass
    try:
        GatewayAttachCoordinator.resolve_dial_failure(config)
    except AttachError as attach_exc:
        # A parent may have bound its socket between the dial and the flock check.
        try:
            dial(path)
        except (OSError, RuntimeError, ValueError):
            raise attach_exc from None
        return
    try:
        dial(path)
    except OSError as retry_exc:
        raise AttachError(
            "listen_path_mismatch",
            "credential flock held but dial failed after wait — "
            "check RITHMIC_GATEWAY_LISTEN matches the running parent",
        ) from retry_exc


def _handshake_until_ready(
    config: GatewayConfig,
    *,
    dial: Callable[[str], None],
    handshake: Callable[[], None],
    require_flock: Callable[[], None],
    close_sock: Callable[[], None],
) -> None:
    """Handshake, retrying ``not_ready`` / silent-parent timeouts until deadline.

    Lucid plant cold-connect often exceeds a single RPC socket timeout while the
    parent is up but not yet ``Ready``. Treat ``TimeoutError`` like ``not_ready``
    and keep re-dialing until ``attach_timeout_sec`` (default 90s).
    """
    path = config.socket_path
    deadline = time.monotonic() + float(config.attach_timeout_sec)
    while True:
        try:
            handshake()
            return
        except GatewayError as exc:
            if exc.code != "not_ready" or time.monotonic() >= deadline:
                close_sock()
                raise
        except TimeoutError:
            if time.monotonic() >= deadline:
                close_sock()
                raise
        close_sock()
        time.sleep(0.1)
        dial(path)
        require_flock()


class GatewayClient(FramedConnection):
    """Attach to a parent ``rithmic-gateway`` and issue plant-semantic RPCs.

    Events and responses are plain dicts / structured protobuf MessageToDict-style
    maps — never Nautilus types.
    """

    def __init__(
        self,
        config: GatewayConfig,
        *,
        rpc_timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC,
        history_rpc_timeout_sec: float = DEFAULT_HISTORY_RPC_TIMEOUT_SEC,
    ) -> None:
        self._config = config
        self._rpc_timeout_sec = rpc_timeout_sec
        self._history_rpc_timeout_sec = history_rpc_timeout_sec
        self._sock: socket.socket | None = None
        self._next_id = 1
        self._scopes: list[str] = []
        self._trading_enabled = False
        self._cancel_all_enabled = False
        self._gateway_instance_id = 0
        self._transport_generation = 0
        self._pending: deque[dict[str, Any]] = deque()
        self._io_lock = threading.RLock()

    @property
    def scopes(self) -> list[str]:
        return list(self._scopes)

    @property
    def trading_enabled(self) -> bool:
        return self._trading_enabled

    @property
    def cancel_all_enabled(self) -> bool:
        return self._cancel_all_enabled

    @property
    def gateway_instance_id(self) -> int:
        return self._gateway_instance_id

    @property
    def transport_generation(self) -> int:
        return self._transport_generation

    def connect(self) -> None:
        if self._sock is not None:
            return
        with self._io_lock:
            cfg = self._config
            _dial_with_attach(cfg, self._dial)
            self._require_parent_flock()
            _handshake_until_ready(
                cfg,
                dial=self._dial,
                handshake=self._handshake,
                require_flock=self._require_parent_flock,
                close_sock=self._close_sock,
            )

    def disconnect(self) -> None:
        """Detach this client only — does not tear down parent plants for peers.

        The parent's lifetime is its operator's: it exits after the last client
        only when started with ``RITHMIC_GATEWAY_IDLE_EXIT_SEC`` (see ops-runbook).
        """
        with self._io_lock:
            if self._sock is None:
                return
            with contextlib.suppress(Exception):
                self._rpc_unlocked(pb.Frame(disconnect=pb.DisconnectRequest()))
            self._close_sock()
            self._pending.clear()

    def subscribe(self, symbol: str, exchange: str) -> None:
        self._rpc(
            pb.Frame(subscribe=pb.SubscribeRequest(symbol=symbol, exchange=exchange))
        )

    def unsubscribe(self, symbol: str, exchange: str) -> None:
        self._rpc(
            pb.Frame(
                unsubscribe=pb.UnsubscribeRequest(symbol=symbol, exchange=exchange)
            )
        )

    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        self._rpc(
            pb.Frame(
                subscribe_book=pb.SubscribeBookRequest(symbol=symbol, exchange=exchange)
            )
        )

    def unsubscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        self._rpc(
            pb.Frame(
                unsubscribe_book=pb.UnsubscribeBookRequest(
                    symbol=symbol, exchange=exchange
                )
            )
        )

    def request_plants(self, plants: str) -> None:
        self._rpc(pb.Frame(request_plants=pb.RequestPlantsRequest(plants=plants)))

    def get_front_month(self, symbol: str, exchange: str) -> dict[str, Any]:
        resp = self._rpc(
            pb.Frame(
                get_front_month=pb.GetFrontMonthRequest(
                    symbol=symbol, exchange=exchange
                )
            )
        )
        which = resp.WhichOneof("body")
        if which != "front_month_response":
            raise GatewayError(
                "protocol", f"expected front_month_response, got {which}"
            )
        return _message_to_dict(resp.front_month_response)

    def get_reference_data(self, symbol: str, exchange: str) -> dict[str, Any]:
        resp = self._rpc(
            pb.Frame(
                get_reference_data=pb.GetReferenceDataRequest(
                    symbol=symbol, exchange=exchange
                )
            )
        )
        which = resp.WhichOneof("body")
        if which != "reference_data_response":
            raise GatewayError(
                "protocol", f"expected reference_data_response, got {which}"
            )
        return _message_to_dict(resp.reference_data_response)

    def get_live_md_state(self) -> dict[str, Any]:
        """RC2.3 barrier snapshot (safe while live MD intents are active)."""
        resp = self._rpc(pb.Frame(get_live_md_state=pb.GetLiveMdStateRequest()))
        which = resp.WhichOneof("body")
        if which != "get_live_md_state_response":
            raise GatewayError(
                "protocol", f"expected get_live_md_state_response, got {which}"
            )
        return _message_to_dict(resp.get_live_md_state_response)

    def resolved_account(self) -> dict[str, Any] | None:
        """Resolved account triple, or None when the parent has not resolved one."""
        resp = self._rpc(pb.Frame(resolved_account=pb.ResolvedAccountRequest()))
        which = resp.WhichOneof("body")
        if which != "resolved_account_response":
            raise GatewayError(
                "protocol", f"expected resolved_account_response, got {which}"
            )
        d = _message_to_dict(resp.resolved_account_response)
        if not d.get("account_id"):
            return None
        return d

    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]:
        """Load order events (fills + cancels + rejects + working) over a window.

        Each event is a normalized ``OrderNotification``-shaped dict.
        """
        resp = self._rpc(
            pb.Frame(
                load_orders=pb.LoadOrdersRequest(
                    start_time_sec=start_ssboe, end_time_sec=end_ssboe
                )
            )
        )
        which = resp.WhichOneof("body")
        if which != "load_orders_response":
            raise GatewayError(
                "protocol", f"expected load_orders_response, got {which}"
            )
        return [_message_to_dict(e) for e in resp.load_orders_response.events]

    def load_product_rms_info(self) -> list[ProductRmsInfo]:
        """Product-level RMS info: per-product commission fill rates.

        Read-only venue config; requires parent trading enabled (order-plant
        login). Each row: ``{product_code, commission_fill_rate, presence_bits}``
        with unset fields omitted.
        """
        resp = self._rpc(pb.Frame(load_product_rms_info=pb.LoadProductRmsInfoRequest()))
        which = resp.WhichOneof("body")
        if which != "load_product_rms_info_response":
            raise GatewayError(
                "protocol", f"expected load_product_rms_info_response, got {which}"
            )
        return [
            cast(ProductRmsInfo, _message_to_dict(r))
            for r in resp.load_product_rms_info_response.rows
        ]

    def load_account_rms_info(self) -> list[AccountRmsInfo]:
        """Account-level RMS info: default commission rate.

        Read-only venue config; requires parent trading enabled (order-plant
        login). Each row: ``{account_id, default_commission, presence_bits}``
        with unset fields omitted.
        """
        resp = self._rpc(pb.Frame(load_account_rms_info=pb.LoadAccountRmsInfoRequest()))
        which = resp.WhichOneof("body")
        if which != "load_account_rms_info_response":
            raise GatewayError(
                "protocol", f"expected load_account_rms_info_response, got {which}"
            )
        return [
            cast(AccountRmsInfo, _message_to_dict(r))
            for r in resp.load_account_rms_info_response.rows
        ]

    def load_fill_history(
        self, start_date: int, finish_date: int
    ) -> list[dict[str, Any]]:
        """Fills for trade dates ``start_date..=finish_date`` (CCYYMMDD).

        Read-only; requires parent trading enabled (order-plant login). Fails
        closed with ``GatewayError`` code ``reconciliation_unavailable`` when the
        venue's 10,000-row cap is reached: split the date range and retry.
        """
        resp = self._rpc(
            pb.Frame(
                load_fill_history=pb.LoadFillHistoryRequest(
                    start_date=start_date, finish_date=finish_date
                )
            )
        )
        which = resp.WhichOneof("body")
        if which != "load_fill_history_response":
            raise GatewayError(
                "protocol", f"expected load_fill_history_response, got {which}"
            )
        return [
            _normalize_fill_history(_message_to_dict(r))
            for r in resp.load_fill_history_response.rows
        ]

    def load_order_history_dates(self) -> list[str]:
        """Trade dates (CCYYMMDD) the venue keeps order history for. Read-only."""
        resp = self._rpc(
            pb.Frame(load_order_history_dates=pb.LoadOrderHistoryDatesRequest())
        )
        which = resp.WhichOneof("body")
        if which != "load_order_history_dates_response":
            raise GatewayError(
                "protocol", f"expected load_order_history_dates_response, got {which}"
            )
        return list(resp.load_order_history_dates_response.dates)

    def load_order_history(self, date: str) -> list[dict[str, Any]]:
        """Replay one trade date's order history (CCYYMMDD) as order notifications.

        Read-only; best-effort drain like ``load_orders`` (same serialization).
        """
        resp = self._rpc(
            pb.Frame(load_order_history=pb.LoadOrderHistoryRequest(date=date))
        )
        which = resp.WhichOneof("body")
        if which != "load_order_history_response":
            raise GatewayError(
                "protocol", f"expected load_order_history_response, got {which}"
            )
        return [
            _normalize_order_notification(_message_to_dict(e))
            for e in resp.load_order_history_response.events
        ]

    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int
    ) -> list[dict[str, Any]]:
        resp = self._rpc(
            pb.Frame(
                load_ticks=pb.LoadTicksRequest(
                    symbol=symbol,
                    exchange=exchange,
                    start_time_sec=start_ssboe,
                    end_time_sec=end_ssboe,
                )
            )
        )
        which = resp.WhichOneof("body")
        if which != "load_ticks_response":
            raise GatewayError("protocol", f"expected load_ticks_response, got {which}")
        return [
            _normalize_history_tick(_message_to_dict(t))
            for t in resp.load_ticks_response.ticks
        ]

    def load_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
        *,
        rpc_timeout_sec: float | None = None,
    ) -> list[dict[str, Any]]:
        """Single-slice history RPC (prefer :meth:`load_time_bars_range` for
        wide windows).
        """
        resp = self._rpc(
            pb.Frame(
                load_time_bars=pb.LoadTimeBarsRequest(
                    symbol=symbol,
                    exchange=exchange,
                    start_time_sec=start_ssboe,
                    end_time_sec=end_ssboe,
                    bar_type=bar_type,
                    period=period,
                )
            ),
            timeout_sec=rpc_timeout_sec,
        )
        which = resp.WhichOneof("body")
        if which != "load_time_bars_response":
            raise GatewayError(
                "protocol", f"expected load_time_bars_response, got {which}"
            )
        return [_message_to_dict(b) for b in resp.load_time_bars_response.bars]

    def load_time_bars_range(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
        *,
        rpc_timeout_sec: float | None = None,
    ) -> list[dict[str, Any]]:
        """Load a wide window via client-side calendar chunks + merge.

        Slice lengths match Rust ``bar_slice_secs``. Slices are fetched
        sequentially over this connection: the parent holds a session mutex for
        each history RPC, so extra sockets would not overlap plant work.
        """
        from rithmic_gateway.history_window import (
            bar_slice_secs,
            dedupe_bars_by_marker,
            window_slices,
        )

        step = bar_slice_secs(bar_type, period)
        timeout = (
            self._history_rpc_timeout_sec
            if rpc_timeout_sec is None
            else float(rpc_timeout_sec)
        )
        merged: list[dict[str, Any]] = []
        for slice_start, slice_end in window_slices(
            int(start_ssboe), int(end_ssboe), step
        ):
            merged.extend(
                self.load_time_bars(
                    symbol,
                    exchange,
                    slice_start,
                    slice_end,
                    bar_type=bar_type,
                    period=period,
                    rpc_timeout_sec=timeout,
                )
            )
        return dedupe_bars_by_marker(merged)

    async def load_time_bars_range_async(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
        *,
        rpc_timeout_sec: float | None = None,
    ) -> list[dict[str, Any]]:
        """Async wrapper around :meth:`load_time_bars_range` (thread offload)."""
        import asyncio

        return await asyncio.to_thread(
            self.load_time_bars_range,
            symbol,
            exchange,
            start_ssboe,
            end_ssboe,
            bar_type,
            period,
            rpc_timeout_sec=rpc_timeout_sec,
        )

    def probe_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]:
        resp = self._rpc(
            pb.Frame(
                probe_time_bars=pb.ProbeTimeBarsRequest(
                    symbol=symbol,
                    exchange=exchange,
                    bar_type=bar_type,
                    period=period,
                    start_time_sec=start_ssboe,
                    end_time_sec=end_ssboe,
                )
            )
        )
        which = resp.WhichOneof("body")
        if which != "probe_time_bars_response":
            raise GatewayError(
                "protocol", f"expected probe_time_bars_response, got {which}"
            )
        return [_message_to_dict(r) for r in resp.probe_time_bars_response.rows]

    def subscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        self._rpc(
            pb.Frame(
                subscribe_time_bars=pb.SubscribeTimeBarsRequest(
                    symbol=symbol, exchange=exchange, bar_type=bar_type, period=period
                )
            )
        )

    def unsubscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        self._rpc(
            pb.Frame(
                unsubscribe_time_bars=pb.UnsubscribeTimeBarsRequest(
                    symbol=symbol, exchange=exchange, bar_type=bar_type, period=period
                )
            )
        )

    def subscribe_pnl(self) -> None:
        self._rpc(pb.Frame(subscribe_pnl=pb.SubscribePnlRequest()))

    def ensure_pnl_plant(self) -> None:
        self._rpc(pb.Frame(ensure_pnl=pb.EnsurePnlRequest()))

    def ensure_order_plant(self) -> None:
        self._rpc(pb.Frame(ensure_order=pb.EnsureOrderRequest()))

    def disconnect_pnl_plant(self) -> None:
        self._rpc(pb.Frame(disconnect_pnl=pb.DisconnectPnlRequest()))

    def subscribe_order_updates(self) -> None:
        self._rpc(pb.Frame(subscribe_order_updates=pb.SubscribeOrderUpdatesRequest()))

    def subscribe_bracket_updates(self) -> None:
        self._rpc(
            pb.Frame(subscribe_bracket_updates=pb.SubscribeBracketUpdatesRequest())
        )

    def disconnect_order_plant(self) -> None:
        self._rpc(pb.Frame(disconnect_order=pb.DisconnectOrderRequest()))

    def reset_ticker_plant(self) -> None:
        self._rpc(pb.Frame(reset_ticker_plant=pb.ResetTickerPlantRequest()))

    def place_order(
        self,
        symbol: str,
        exchange: str,
        side: str,
        price_type: str,
        quantity: int,
        user_tag: str = "",
        price: float | None = None,
        trigger_price: float | None = None,
        duration: str = "DAY",
        trail_by_ticks: int | None = None,
        trail_by_price_id: int | None = None,
    ) -> None:
        req = pb.PlaceOrderRequest(
            symbol=symbol,
            exchange=exchange,
            side=side,
            price_type=price_type,
            quantity=quantity,
            user_tag=user_tag,
            duration=duration,
        )
        if price is not None:
            req.price = price
        if trigger_price is not None:
            req.trigger_price = trigger_price
        if trail_by_ticks is not None:
            req.trail_by_ticks = trail_by_ticks
        if trail_by_price_id is not None:
            req.trail_by_price_id = trail_by_price_id
        self._rpc(pb.Frame(place_order=req))

    def place_bracket_order(
        self,
        symbol: str,
        exchange: str,
        side: str,
        price_type: str,
        quantity: int,
        localid: str,
        price: float | None = None,
        trigger_price: float | None = None,
        duration: str = "DAY",
        stop_ticks: int | None = None,
        target_ticks: int | None = None,
    ) -> None:
        req = pb.PlaceBracketOrderRequest(
            symbol=symbol,
            exchange=exchange,
            side=side,
            price_type=price_type,
            quantity=quantity,
            localid=localid,
            duration=duration,
        )
        if price is not None:
            req.price = price
        if trigger_price is not None:
            req.trigger_price = trigger_price
        if stop_ticks is not None:
            req.stop_ticks = stop_ticks
        if target_ticks is not None:
            req.target_ticks = target_ticks
        self._rpc(pb.Frame(place_bracket_order=req))

    def adjust_bracket_stop(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None:
        req = pb.AdjustBracketStopRequest(basket_id=basket_id, ticks=ticks)
        if level is not None:
            req.level = level
        self._rpc(pb.Frame(adjust_bracket_stop=req))

    def adjust_bracket_target(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None:
        req = pb.AdjustBracketTargetRequest(basket_id=basket_id, ticks=ticks)
        if level is not None:
            req.level = level
        self._rpc(pb.Frame(adjust_bracket_target=req))

    def cancel_order(self, basket_id: str) -> None:
        self._rpc(pb.Frame(cancel_order=pb.CancelOrderRequest(basket_id=basket_id)))

    def modify_order(
        self,
        basket_id: str,
        symbol: str,
        exchange: str,
        quantity: int,
        price_type: str,
        price: float | None = None,
        trigger_price: float | None = None,
        trail_by_ticks: int | None = None,
    ) -> None:
        req = pb.ModifyOrderRequest(
            basket_id=basket_id,
            symbol=symbol,
            exchange=exchange,
            quantity=quantity,
            price_type=price_type,
        )
        if price is not None:
            req.price = price
        if trigger_price is not None:
            req.trigger_price = trigger_price
        if trail_by_ticks is not None:
            req.trail_by_ticks = trail_by_ticks
        self._rpc(pb.Frame(modify_order=req))

    def cancel_all_orders(self) -> None:
        self._rpc(pb.Frame(cancel_all_orders=pb.CancelAllOrdersRequest()))

    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        """Next MD event (last_trade / bbo / order_book / other), or None."""
        return self._poll_filtered(timeout_ms, _is_md_event)

    def poll_history_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return self._poll_filtered(timeout_ms, _is_history_event)

    def poll_pnl_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return self._poll_filtered(timeout_ms, _is_pnl_event)

    def poll_order_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return self._poll_filtered(timeout_ms, _is_order_event)

    # --- internals ---------------------------------------------------------

    def _handshake(self) -> None:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        cfg = self._config
        hs = pb.Handshake(
            user=cfg.user,
            system_name=cfg.system_name,
            url=cfg.url,
            env=cfg.env,
            account_id=cfg.account_id or "",
            fcm_id=cfg.fcm_id or "",
            ib_id=cfg.ib_id or "",
            auth_token=cfg.auth_token or "",
        )
        self._write_frame(pb.Frame(handshake=hs))
        ready_frame = self._read_frame()
        which = ready_frame.WhichOneof("body")
        if which != "ready":
            if which == "error":
                raise _wire_error(ready_frame.error.code, ready_frame.error.message)
            raise GatewayError("protocol", f"expected Ready, got {which}")
        ready = ready_frame.ready
        self._scopes = list(ready.scopes)
        self._trading_enabled = bool(ready.trading_enabled)
        self._cancel_all_enabled = bool(ready.cancel_all_enabled)
        self._gateway_instance_id = int(ready.gateway_instance_id)
        self._transport_generation = int(ready.transport_generation)

    def _rpc(self, frame: pb.Frame, *, timeout_sec: float | None = None) -> pb.Frame:
        with self._io_lock:
            return self._rpc_unlocked(frame, timeout_sec=timeout_sec)

    def _rpc_unlocked(
        self, frame: pb.Frame, *, timeout_sec: float | None = None
    ) -> pb.Frame:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        rid = self._next_id
        self._next_id += 1
        frame.request_id = rid
        self._write_frame(frame)
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        effective = self._rpc_timeout_sec if timeout_sec is None else float(timeout_sec)
        self._sock.settimeout(effective)
        try:
            while True:
                resp = self._read_frame()
                which = resp.WhichOneof("body")
                if which == "event":
                    self._pending.append(_event_to_dict(resp.event))
                    continue
                if which == "error":
                    if resp.request_id != rid:
                        raise GatewayError(
                            "protocol",
                            f"error for request_id={resp.request_id} while "
                            f"waiting for {rid}: "
                            f"{resp.error.code}: {resp.error.message}",
                        )
                    raise _wire_error(resp.error.code, resp.error.message)
                if resp.request_id == rid:
                    return resp
                # Uncorrelated non-event frame — treat as protocol error.
                raise GatewayError(
                    "protocol",
                    f"unexpected frame {which!r} request_id={resp.request_id} "
                    f"while waiting for {rid}",
                )
        except GatewayError as exc:
            # Fatal wire errors close the fd inside _recv_exact; null self._sock
            # so the next call is not_connected (unknown) instead of OSError.
            if exc.code in {"desync", "eof", "frame_too_large"}:
                self._close_sock()
            raise
        except (TimeoutError, BlockingIOError) as exc:
            # ``settimeout(0)`` switches the socket to non-blocking mode, so
            # the read raises BlockingIOError (not TimeoutError) after the
            # request was sent. Treat it as a timeout and close the socket:
            # leaving it open with a queued response would let the next RPC
            # consume it and fail with a request-ID protocol error.
            self._close_sock()
            raise GatewayError(
                "timeout", f"RPC {rid} timed out after {effective}s"
            ) from exc

    def _poll_filtered(
        self,
        timeout_ms: int,
        predicate: Any,
    ) -> dict[str, Any] | None:
        with self._io_lock:
            return self._poll_filtered_unlocked(timeout_ms, predicate)

    def _poll_filtered_unlocked(
        self,
        timeout_ms: int,
        predicate: Any,
    ) -> dict[str, Any] | None:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        for _ in range(len(self._pending)):
            evt = self._pending.popleft()
            if predicate(evt):
                return evt
            self._pending.append(evt)
        timeout = max(timeout_ms, 0) / 1000.0 if timeout_ms else 0.0
        self._sock.settimeout(timeout)
        try:
            while True:
                frame = self._read_frame()
                which = frame.WhichOneof("body")
                if which == "event":
                    evt = _event_to_dict(frame.event)
                    if predicate(evt):
                        return evt
                    self._pending.append(evt)
                    if not timeout:
                        return None
                    continue
                if which == "error":
                    raise GatewayError(frame.error.code, frame.error.message)
                # Unexpected Ack/response while polling — queue nothing, keep
                # going if timed.
                if not timeout:
                    return None
        except GatewayError as exc:
            if exc.code in {"desync", "eof", "frame_too_large"}:
                self._close_sock()
            raise
        except (TimeoutError, BlockingIOError):
            return None
        finally:
            if self._sock is not None:
                self._sock.settimeout(self._rpc_timeout_sec)


def _message_to_dict(msg: Any) -> dict[str, Any]:
    """Proto → dict including proto3 default scalars (parity with PyO3 dicts)."""
    # Pass via kwargs to stay self-documenting; ``protobuf>=5`` supports both,
    # but the bundled ``.pyi`` stubs lag behind the runtime, so Sonar's S930
    # analyzer reports "unknown kwarg". Indirection via ``**flags`` keeps the
    # call site simple and bypasses the false-positive.
    flags: dict[str, Any] = {
        "preserving_proto_field_name": True,
        "always_print_fields_with_no_presence": True,
    }
    return MessageToDict(msg, **flags)


def _normalize_fill_history(d: dict[str, Any]) -> dict[str, Any]:
    """Match PyO3 ``fill_history_dict``: type tag, and ``fill_size`` as an int
    (proto-JSON renders uint64 as a string)."""
    out = dict(d)
    out["type"] = "fill_history"
    if "fill_size" in out:
        out["fill_size"] = int(out["fill_size"])
    return out


def _normalize_order_notification(d: dict[str, Any]) -> dict[str, Any]:
    """Match PyO3 ``order_notification_dict``: type tag, and ``ts_event_ns`` as
    an int (proto-JSON renders uint64 as a string)."""
    out = dict(d)
    out["type"] = "order_notification"
    if "ts_event_ns" in out:
        out["ts_event_ns"] = int(out["ts_event_ns"])
    return out


def _normalize_history_tick(d: dict[str, Any]) -> dict[str, Any]:
    """Match PyO3 history_tick_dict aliases required by payloads_to_trade_ticks."""
    out = dict(d)
    if "trade_price" not in out and "close_price" in out:
        out["trade_price"] = out["close_price"]
    if "trade_size" not in out and "num_trades" in out:
        try:
            out["trade_size"] = int(out["num_trades"])
        except (TypeError, ValueError):
            out["trade_size"] = out["num_trades"]
    out.setdefault("type", "history_tick")
    return out


def _event_to_dict(event: pb.Event) -> dict[str, Any]:
    which = event.WhichOneof("body")
    if which is None:
        return {"type": "empty"}
    out = _message_to_dict(getattr(event, which))
    out["type"] = which
    # Always include empty book sides (MessageToDict may omit empty repeated).
    if which == "order_book":
        for key in ("bid_price", "bid_size", "ask_price", "ask_size"):
            out.setdefault(key, [])
    return out
