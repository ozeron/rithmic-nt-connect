"""Plant-level gateway client over unix + length-delimited protobuf."""

from __future__ import annotations

import contextlib
import socket
import struct
import threading
import time
from collections import deque
from typing import Any, cast

from google.protobuf.json_format import MessageToDict

from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import session_flock_held
from rithmic_gateway.framing import MAX_FRAME_LEN, encode_frame
from rithmic_gateway.spawn import SpawnError, spawn_gateway
from rithmic_gateway.types import AccountRmsInfo, ProductRmsInfo
from rithmic_gateway.v1 import session_pb2 as pb

# Default dial + RPC socket timeout (seconds). Stuck parent must not hang forever.
DEFAULT_RPC_TIMEOUT_SEC = 30.0
# Per-chunk history RPC timeout — one plant slice can exceed the dial default.
DEFAULT_HISTORY_RPC_TIMEOUT_SEC = 120.0

_ORDER_EVENT_TYPES = frozenset({"order_notification"})
_PNL_EVENT_TYPES = frozenset({"account_pnl", "instrument_pnl"})
_HISTORY_EVENT_TYPES = frozenset({"time_bar"})
_NON_MD_EVENT_TYPES = _ORDER_EVENT_TYPES | _PNL_EVENT_TYPES | _HISTORY_EVENT_TYPES


def _is_md_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") not in _NON_MD_EVENT_TYPES


def _is_history_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") in _HISTORY_EVENT_TYPES


def _is_pnl_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") in _PNL_EVENT_TYPES


def _is_order_event(evt: dict[str, Any]) -> bool:
    return evt.get("type") in _ORDER_EVENT_TYPES


class GatewayError(RuntimeError):
    """Gateway RPC or protocol error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class GatewayClient:
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
        self._spawned = None
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

    def connect(self) -> None:
        if self._sock is not None:
            return
        with self._io_lock:
            path = self._config.socket_path
            try:
                self._dial(path)
            except (FileNotFoundError, ConnectionRefusedError, OSError):
                if not self._config.auto_spawn:
                    raise
                try:
                    self._spawned = spawn_gateway(self._config)
                except SpawnError as spawn_exc:
                    # Lost race: another process may have bound the socket.
                    try:
                        self._dial(path)
                    except Exception:
                        raise spawn_exc from None
                else:
                    self._dial(path)
            self._require_parent_flock()
            # Parent may bind before plants are Ready (path claim); retry not_ready.
            deadline = time.monotonic() + float(self._config.spawn_timeout_sec)
            while True:
                try:
                    self._handshake()
                    return
                except GatewayError as exc:
                    if exc.code != "not_ready" or time.monotonic() >= deadline:
                        self._close_sock()
                        raise
                    self._close_sock()
                    time.sleep(0.1)
                    self._dial(path)
                    self._require_parent_flock()

    def _require_parent_flock(self) -> None:
        """Refuse Ready from a dialable impostor that does not hold the flock."""
        if not self._config.attest_flock:
            return
        cfg = self._config
        if not session_flock_held(cfg.user, cfg.system_name, cfg.url, cfg.env):
            self._close_sock()
            raise GatewayError(
                "parent_unattested",
                "listen path is up but credential flock is free — refusing "
                "impostor parent",
            )

    def disconnect(self) -> None:
        """Detach this client only — does not tear down parent plants for peers.

        When this was the last Ready peer, an auto-spawned parent may idle-exit
        after ``RITHMIC_GATEWAY_IDLE_EXIT_SEC`` grace (see ops-runbook).
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
        max_workers: int = 1,
    ) -> list[dict[str, Any]]:
        """Load a wide window via client-side calendar chunks + merge.

        Slice lengths match Rust ``bar_slice_secs``. Default ``max_workers=1``
        (sequential): the parent holds a session mutex for each history RPC, so
        extra dials do not overlap plant work. ``max_workers`` > 1 remains for
        experiments once the plant allows concurrent history.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

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
        slices = window_slices(int(start_ssboe), int(end_ssboe), step)
        if not slices:
            return []
        if len(slices) == 1 or max_workers <= 1:
            merged: list[dict[str, Any]] = []
            for slice_start, slice_end in slices:
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

        # Ensure parent is up on this client first (auto-spawn / warm attach).
        if self._sock is None:
            self.connect()

        def _fetch_slice(window: tuple[int, int]) -> list[dict[str, Any]]:
            slice_start, slice_end = window
            peer = GatewayClient(
                self._config,
                rpc_timeout_sec=self._rpc_timeout_sec,
                history_rpc_timeout_sec=self._history_rpc_timeout_sec,
            )
            peer.connect()
            try:
                return peer.load_time_bars(
                    symbol,
                    exchange,
                    slice_start,
                    slice_end,
                    bar_type=bar_type,
                    period=period,
                    rpc_timeout_sec=timeout,
                )
            finally:
                with contextlib.suppress(Exception):
                    peer.disconnect()

        merged_map: dict[tuple[int, int], list[dict[str, Any]]] = {}
        workers = min(max_workers, len(slices))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_fetch_slice, window): window for window in slices}
            for fut in as_completed(futs):
                window = futs[fut]
                merged_map[window] = fut.result()
        ordered: list[dict[str, Any]] = []
        for window in slices:
            ordered.extend(merged_map[window])
        return dedupe_bars_by_marker(ordered)

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
        max_workers: int = 1,
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
            max_workers=max_workers,
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

    def _dial(self, path: str) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self._rpc_timeout_sec)
        sock.connect(path)
        self._sock = sock

    def _handshake(self) -> None:
        assert self._sock is not None
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
                raise GatewayError(ready_frame.error.code, ready_frame.error.message)
            raise GatewayError("protocol", f"expected Ready, got {which}")
        ready = ready_frame.ready
        self._scopes = list(ready.scopes)
        self._trading_enabled = bool(ready.trading_enabled)
        self._cancel_all_enabled = bool(ready.cancel_all_enabled)

    def _rpc(self, frame: pb.Frame, *, timeout_sec: float | None = None) -> pb.Frame:
        with self._io_lock:
            return self._rpc_unlocked(frame, timeout_sec=timeout_sec)

    def _rpc_unlocked(
        self, frame: pb.Frame, *, timeout_sec: float | None = None
    ) -> pb.Frame:
        if self._sock is None:
            raise GatewayError("not_connected", "call connect() first")
        rid = self._next_id
        self._next_id += 1
        frame.request_id = rid
        self._write_frame(frame)
        assert self._sock is not None
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
                    raise GatewayError(resp.error.code, resp.error.message)
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
            raise GatewayError("not_connected", "call connect() first")
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
                    if timeout == 0.0:
                        return None
                    continue
                if which == "error":
                    raise GatewayError(frame.error.code, frame.error.message)
                # Unexpected Ack/response while polling — queue nothing, keep
                # going if timed.
                if timeout == 0.0:
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

    def _close_sock(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.close()

    def _write_frame(self, frame: pb.Frame) -> None:
        assert self._sock is not None
        payload = frame.SerializeToString()
        self._sock.sendall(encode_frame(payload))

    def _read_frame(self) -> pb.Frame:
        assert self._sock is not None
        header = _recv_exact(self._sock, 4)
        (length,) = struct.unpack("!I", header)
        if length > MAX_FRAME_LEN:
            raise GatewayError("frame_too_large", f"{length} bytes")
        payload = _recv_exact(self._sock, length)
        frame = pb.Frame()
        frame.ParseFromString(payload)
        return frame


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    try:
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise GatewayError("eof", "gateway closed connection")
            buf.extend(chunk)
    except TimeoutError as exc:
        if buf:
            with contextlib.suppress(Exception):
                sock.close()
            raise GatewayError(
                "desync",
                f"timed out after {len(buf)}/{n} bytes; connection closed",
            ) from exc
        raise
    return bytes(buf)


def _message_to_dict(msg: Any) -> dict[str, Any]:
    """Proto → dict including proto3 default scalars (parity with PyO3 dicts)."""
    return MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )


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
