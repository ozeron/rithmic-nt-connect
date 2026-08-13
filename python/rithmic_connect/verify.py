"""Record live ticks and compare against history for the resolved front month.

Produces a small JSON-friendly verify report that a frontend / API can display.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

from rithmic_connect.front_month import resolve_front_month
from rithmic_connect.session import WireSession


@dataclass
class RecordedTick:
    ts_event_ns: int
    price: float
    size: float | None = None
    source: str = "live"  # live | history


@dataclass
class VerifyReport:
    ok: bool
    summary: str
    root: str
    exchange: str
    front: dict[str, Any]
    window: dict[str, int]
    live: dict[str, Any]
    history: dict[str, Any]
    compare: dict[str, Any]
    recorded_live_path: str | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def write_json(self, path: str | Path) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(self.to_json() + "\n", encoding="utf-8")
        return out


def _tick_key(ts_event_ns: int, price: float) -> tuple[int, float]:
    # Round price lightly to absorb float noise across plants.
    return int(ts_event_ns), round(float(price), 6)


def _event_to_recorded(event: Mapping[str, Any], *, source: str) -> RecordedTick | None:
    etype = event.get("type")
    if etype not in ("last_trade", "history_tick"):
        return None
    price = event.get("trade_price")
    size = event.get("trade_size")
    ts = event.get("ts_event_ns")
    if ts is None and event.get("ssboe") is not None:
        usecs = event.get("usecs")
        if usecs is None:
            usecs = 0
        ts = int(event["ssboe"]) * 1_000_000_000 + int(usecs) * 1_000
    if price is None or size is None or ts is None:
        raise ValueError(
            f"{source} {etype} missing trade_price/trade_size/timestamp: {dict(event)!r}"
        )
    return RecordedTick(
        ts_event_ns=int(ts),
        price=float(price),
        size=float(size),
        source=source,
    )


def record_live_trades(
    session: WireSession,
    symbol: str,
    exchange: str,
    *,
    duration_sec: float = 10.0,
    max_events: int = 500,
) -> list[RecordedTick]:
    """Subscribe and record last-trade events for a short window."""
    if duration_sec <= 0:
        raise ValueError(f"duration_sec must be > 0, got {duration_sec}")
    session.subscribe(symbol, exchange)
    out: list[RecordedTick] = []
    deadline = time.time() + duration_sec
    while time.time() < deadline and len(out) < max_events:
        event = session.poll_event()
        if event is None:
            time.sleep(0.01)
            continue
        tick = _event_to_recorded(event, source="live")
        if tick is not None:
            out.append(tick)
    session.unsubscribe(symbol, exchange)
    out.sort(key=lambda t: t.ts_event_ns)
    return out


def load_history_trades(
    session: WireSession,
    symbol: str,
    exchange: str,
    start_sec: int,
    end_sec: int,
) -> list[RecordedTick]:
    """Load history ticks for [start_sec, end_sec] and normalize to RecordedTick."""
    raw = session.load_ticks(symbol, exchange, start_sec, end_sec)
    out: list[RecordedTick] = []
    for item in raw:
        tick = _event_to_recorded(item, source="history")
        if tick is None:
            raise ValueError(f"unexpected history payload type: {item!r}")
        out.append(tick)
    out.sort(key=lambda t: t.ts_event_ns)
    return out


def compare_ticks(
    live: list[RecordedTick],
    history: list[RecordedTick],
    *,
    price_tol: float = 1e-6,
) -> dict[str, Any]:
    """Compare live vs history on (timestamp, price) keys within the live window."""
    if not live:
        return {
            "matched": 0,
            "mismatched": 0,
            "live_only": 0,
            "history_only": 0,
            "live_count": 0,
            "history_in_window": 0,
            "unique_live_keys": 0,
            "unique_history_keys": 0,
            "max_price_diff": None,
            "overlap_ratio": 0.0,
            "live_only_samples": [],
            "history_only_samples": [],
            "notes": [],
        }

    live_start = min(t.ts_event_ns for t in live)
    live_end = max(t.ts_event_ns for t in live)
    hist_in_window = [h for h in history if live_start <= h.ts_event_ns <= live_end]

    live_map: dict[tuple[int, float], RecordedTick] = {}
    for t in live:
        live_map[_tick_key(t.ts_event_ns, t.price)] = t

    hist_map: dict[tuple[int, float], RecordedTick] = {}
    for t in hist_in_window:
        hist_map[_tick_key(t.ts_event_ns, t.price)] = t

    matched_keys = live_map.keys() & hist_map.keys()
    live_only_keys = live_map.keys() - hist_map.keys()
    hist_only_keys = hist_map.keys() - live_map.keys()
    matched = len(matched_keys)

    # Nearest-price diff only when the same timestamp exists on both sides but
    # a live price is missing from history at that timestamp (not cross-product).
    max_diff = 0.0
    mismatched = 0
    live_by_ts: dict[int, set[float]] = {}
    hist_by_ts: dict[int, set[float]] = {}
    for t in live:
        live_by_ts.setdefault(t.ts_event_ns, set()).add(round(float(t.price), 6))
    for t in hist_in_window:
        hist_by_ts.setdefault(t.ts_event_ns, set()).add(round(float(t.price), 6))
    for ts, live_prices in live_by_ts.items():
        hist_prices = hist_by_ts.get(ts)
        if not hist_prices:
            continue
        for lp in live_prices:
            if any(abs(lp - hp) <= price_tol for hp in hist_prices):
                continue
            mismatched += 1
            nearest = min(abs(lp - hp) for hp in hist_prices)
            max_diff = max(max_diff, nearest)

    # Fuzzy: same second + same price often means live truncated usecs (ssboe-only).
    fuzzy_matched = 0
    for ts, price in live_only_keys:
        sec = ts // 1_000_000_000
        if any(
            (hts // 1_000_000_000) == sec and abs(price - hprice) <= price_tol
            for hts, hprice in hist_only_keys
        ):
            fuzzy_matched += 1

    notes: list[str] = []
    dup_extra = len(live) - len(live_map)
    if dup_extra > 0:
        notes.append(
            f"{dup_extra} duplicate live rows collapsed by (ts,price) key "
            f"({len(live)} events → {len(live_map)} unique)"
        )
    if fuzzy_matched:
        notes.append(
            f"{fuzzy_matched} live_only key(s) match history_only on same "
            "UTC second+price (likely live usecs truncated to 0)"
        )

    denom = max(len(live_map), 1)
    overlap_ratio = matched / denom

    def _sample(keys: set[tuple[int, float]], src: dict[tuple[int, float], RecordedTick]) -> list[dict]:
        out = []
        for key in sorted(keys)[:5]:
            t = src[key]
            out.append(
                {
                    "ts_event_ns": t.ts_event_ns,
                    "price": t.price,
                    "size": t.size,
                    "source": t.source,
                }
            )
        return out

    return {
        "matched": matched,
        "mismatched": mismatched,
        "live_only": len(live_only_keys),
        "history_only": len(hist_only_keys),
        "fuzzy_second_matches": fuzzy_matched,
        "live_count": len(live),
        "history_in_window": len(hist_in_window),
        "unique_live_keys": len(live_map),
        "unique_history_keys": len(hist_map),
        "max_price_diff": max_diff if mismatched else 0.0,
        "overlap_ratio": round(overlap_ratio, 4),
        "live_window_ns": {"start": live_start, "end": live_end},
        "live_only_samples": _sample(live_only_keys, live_map),
        "history_only_samples": _sample(hist_only_keys, hist_map),
        "notes": notes,
    }


def run_front_month_verify(
    session: WireSession,
    *,
    root: str = "NQ",
    exchange: str = "CME",
    record_sec: float = 10.0,
    history_pad_sec: int = 2,
    min_live_trades: int = 1,
    min_overlap_ratio: float = 0.0,
    record_dir: str | Path | None = None,
) -> VerifyReport:
    """Resolve front month, record live trades, reload history, compare.

    Designed so a frontend can call this (or shell out to the CLI) and render
    ``report.to_dict()`` / ``report.summary``.
    """
    errors: list[str] = []
    front = resolve_front_month(session, root, exchange)
    symbol = str(front["trading_symbol"])
    xch = str(front["trading_exchange"])

    live = record_live_trades(session, symbol, xch, duration_sec=record_sec)
    if not live:
        errors.append("no live last_trade events recorded")
        start_sec = int(time.time()) - int(record_sec) - history_pad_sec
        end_sec = int(time.time()) + history_pad_sec
    else:
        start_sec = int(live[0].ts_event_ns // 1_000_000_000) - history_pad_sec
        end_sec = int(live[-1].ts_event_ns // 1_000_000_000) + history_pad_sec

    history = load_history_trades(session, symbol, xch, start_sec, end_sec)
    compare = compare_ticks(live, history)

    recorded_path: str | None = None
    if record_dir is not None:
        out_dir = Path(record_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time())
        path = out_dir / f"live_{symbol}_{stamp}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for tick in live:
                fh.write(json.dumps(asdict(tick)) + "\n")
        recorded_path = str(path)

    ok = (
        len(errors) == 0
        and len(live) >= min_live_trades
        and float(compare.get("overlap_ratio") or 0.0) >= min_overlap_ratio
    )
    if len(live) < min_live_trades:
        errors.append(f"live trades {len(live)} < min_live_trades {min_live_trades}")

    summary = (
        f"VERIFY {'OK' if ok else 'FAIL'}: front={symbol}.{xch} "
        f"live={len(live)} hist_window={compare.get('history_in_window')} "
        f"matched={compare.get('matched')} overlap={compare.get('overlap_ratio')}"
    )
    if errors:
        summary += f" errors={';'.join(errors)}"

    return VerifyReport(
        ok=ok,
        summary=summary,
        root=root,
        exchange=exchange,
        front=front,
        window={"start_sec": start_sec, "end_sec": end_sec},
        live={
            "count": len(live),
            "first_ts_ns": live[0].ts_event_ns if live else None,
            "last_ts_ns": live[-1].ts_event_ns if live else None,
            "sample": [asdict(t) for t in live[:3]],
        },
        history={
            "count": len(history),
            "first_ts_ns": history[0].ts_event_ns if history else None,
            "last_ts_ns": history[-1].ts_event_ns if history else None,
            "sample": [asdict(t) for t in history[:3]],
        },
        compare=compare,
        recorded_live_path=recorded_path,
        errors=errors,
    )
