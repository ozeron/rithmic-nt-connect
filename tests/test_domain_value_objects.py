"""Tests for SeenKeyCache, FillDedupStore, UntrackedStatusBook, VenueNotification."""

from unittest.mock import Mock

from nautilus_trader.model.enums import OrderStatus
from rithmic_nt_connect._orders import (
    FillDedupStore,
    SeenKeyCache,
    UntrackedStatusBook,
    VenueNotification,
)


def test_seen_key_cache_lru_and_bounds() -> None:
    cache = SeenKeyCache(max_size=3)
    assert len(cache) == 0
    assert not cache.has_seen("k1")

    cache.mark("k1", "v1")
    cache.mark("k2", "v2")
    cache.mark("k3", "v3")
    assert len(cache) == 3
    assert cache.has_seen("k1")
    assert cache.get("k1") == "v1"

    # Accessing k1 refreshes recency in LRU
    assert "k1" in cache

    # Adding k4 should evict the oldest (k2 was not refreshed, k1 was)
    cache.mark("k4", "v4")
    assert len(cache) == 3
    assert "k4" in cache
    assert "k1" in cache
    assert "k3" in cache
    assert "k2" not in cache
    assert not cache.has_seen("k2")

    cache.clear()
    assert len(cache) == 0


def test_seen_key_cache_get_refreshes_recency() -> None:
    """Untracked-status suppress uses get(); it must keep hot keys alive."""
    cache = SeenKeyCache(max_size=2)
    cache.mark("hot", ("OPEN",))
    cache.mark("a", ("X",))
    assert cache.get("hot") == ("OPEN",)
    cache.mark("b", ("Y",))
    assert cache.get("hot") == ("OPEN",)
    assert "a" not in cache
    assert "b" in cache


def test_fill_dedup_store_mark_and_seen() -> None:
    store = FillDedupStore(max_size=2)
    assert not store.has_seen("f1")
    store.mark("f1")
    assert store.has_seen("f1")
    store.mark("f2")
    assert store.has_seen("f1")  # refresh
    store.mark("f3")
    assert not store.has_seen("f2")
    assert store.has_seen("f1")


def test_untracked_status_book_record_and_get() -> None:
    book = UntrackedStatusBook(max_size=2)
    key = ("B-HOT", "OPEN", "1")
    book.record("B-HOT", key)
    assert book.get("B-HOT") == key
    book.record("B-OTHER", ("B-OTHER", "OPEN", "1"))
    assert book.get("B-HOT") == key  # touch
    book.record("B-CHURN", ("B-CHURN", "OPEN", "1"))
    assert book.get("B-HOT") == key
    assert book.get("B-OTHER") is None


def test_venue_notification_properties_and_tell_dont_ask() -> None:
    raw = {
        "source": "rithmic",
        "basket_id": "B-123",
        "symbol": "MNQU6",
        "account_id": "ACT-1",
        "kind": "filled",
        "status": "complete",
        "ts_event": "1700000000000",
    }
    notif = VenueNotification(raw)
    assert notif.basket_id == "B-123"
    assert notif.symbol == "MNQU6"
    assert notif.account_id == "ACT-1"
    assert notif.kind == "filled"
    assert notif.status == "complete"
    assert notif.ts_event == 1700000000000
    assert notif.is_fill is True

    bare_raw = {
        "source": "rithmic",
        "notify_type_name": "COMPLETE",
        "status": "complete",
    }
    bare_notif = VenueNotification(bare_raw)

    order_open = Mock(is_closed=False, status=OrderStatus.ACCEPTED)
    assert bare_notif.is_benign_bare_complete(order_open) is False

    order_filled = Mock(is_closed=True, status=OrderStatus.FILLED)
    assert bare_notif.is_benign_bare_complete(order_filled) is True

    order_canceled = Mock(is_closed=True, status=OrderStatus.CANCELED)
    assert bare_notif.is_benign_bare_complete(order_canceled) is True
