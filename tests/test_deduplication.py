"""Tests for deduplication service."""

import os
import tempfile
import pytest
from datetime import datetime, timedelta

from apartment_finder.models.apartment import Amenities, Apartment
from apartment_finder.services.deduplication import DeduplicationService


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def dedup_service(temp_db):
    """Create a deduplication service with temp database."""
    return DeduplicationService(db_path=temp_db)


@pytest.fixture
def make_apartment():
    """Factory for creating test apartments."""
    def _make(source_id: str, city: str = "NYC", price: float = 3000.0) -> Apartment:
        return Apartment(
            source_id=source_id,
            source_name="test",
            title=f"Apartment {source_id}",
            url=f"https://example.com/{source_id}",
            price_local=price,
            currency="USD",
            price_usd=price,
            bedrooms=2,
            bathrooms=1.0,
            sqft=800,
            city=city,
            country="USA",
            amenities=Amenities(),
        )
    return _make


class TestDeduplicationService:
    """Tests for DeduplicationService."""

    def test_filter_new_listings_all_new(self, dedup_service, make_apartment):
        apartments = [make_apartment("apt1"), make_apartment("apt2")]
        result = dedup_service.filter_new_listings(apartments)

        assert len(result) == 2
        assert result[0].source_id == "apt1"
        assert result[1].source_id == "apt2"

    def test_filter_new_listings_empty_list(self, dedup_service):
        result = dedup_service.filter_new_listings([])
        assert result == []

    def test_filter_new_listings_already_seen_not_sent(
        self, dedup_service, make_apartment
    ):
        apt = make_apartment("apt1")

        # First time - should be new
        result1 = dedup_service.filter_new_listings([apt])
        assert len(result1) == 1

        # Second time - seen but not sent, should still appear
        result2 = dedup_service.filter_new_listings([apt])
        assert len(result2) == 1

    def test_filter_new_listings_already_sent(self, dedup_service, make_apartment):
        apt = make_apartment("apt1")

        # Add and mark as sent
        dedup_service.filter_new_listings([apt])
        dedup_service.mark_as_sent([apt])

        # Should be filtered out now
        result = dedup_service.filter_new_listings([apt])
        assert len(result) == 0

    def test_mark_as_sent(self, dedup_service, make_apartment):
        apt = make_apartment("apt1")
        dedup_service.filter_new_listings([apt])

        dedup_service.mark_as_sent([apt])

        stats = dedup_service.get_stats()
        assert stats["total_sent"] == 1

    def test_mark_as_sent_empty_list(self, dedup_service):
        # Should not raise
        dedup_service.mark_as_sent([])

    def test_cleanup_old_listings(self, dedup_service, make_apartment, temp_db):
        apt = make_apartment("old_apt")
        dedup_service.filter_new_listings([apt])

        # Manually backdate the listing in the database
        import sqlite3
        from datetime import datetime, timedelta
        old_date = datetime.utcnow() - timedelta(days=31)
        conn = sqlite3.connect(temp_db)
        conn.execute(
            "UPDATE seen_listings SET last_seen_at = ? WHERE source_id = ?",
            (old_date, "old_apt")
        )
        conn.commit()
        conn.close()

        # Cleanup with 30 days should remove the old listing
        removed = dedup_service.cleanup_old_listings(days=30)
        assert removed == 1

        stats = dedup_service.get_stats()
        assert stats["total_tracked"] == 0

    def test_cleanup_keeps_recent_listings(self, dedup_service, make_apartment):
        apt = make_apartment("recent_apt")
        dedup_service.filter_new_listings([apt])

        # Cleanup with 30 days should keep recent listings
        removed = dedup_service.cleanup_old_listings(days=30)
        assert removed == 0

        stats = dedup_service.get_stats()
        assert stats["total_tracked"] == 1

    def test_get_stats(self, dedup_service, make_apartment):
        apt1 = make_apartment("apt1", city="NYC")
        apt2 = make_apartment("apt2", city="Dubai")

        dedup_service.filter_new_listings([apt1, apt2])
        dedup_service.mark_as_sent([apt1])

        stats = dedup_service.get_stats()

        assert stats["total_tracked"] == 2
        assert stats["total_sent"] == 1
        assert stats["by_city"]["NYC"] == 1
        assert stats["by_city"]["Dubai"] == 1
        assert stats["by_source"]["test"] == 2

    def test_reset(self, dedup_service, make_apartment):
        apartments = [make_apartment("apt1"), make_apartment("apt2")]
        dedup_service.filter_new_listings(apartments)

        assert dedup_service.get_stats()["total_tracked"] == 2

        dedup_service.reset()

        assert dedup_service.get_stats()["total_tracked"] == 0

    def test_database_persists(self, temp_db, make_apartment):
        # Create service and add listing
        service1 = DeduplicationService(db_path=temp_db)
        apt = make_apartment("persistent")
        service1.filter_new_listings([apt])
        service1.mark_as_sent([apt])

        # Create new service instance with same DB
        service2 = DeduplicationService(db_path=temp_db)

        # Should still be marked as sent
        result = service2.filter_new_listings([apt])
        assert len(result) == 0

        stats = service2.get_stats()
        assert stats["total_tracked"] == 1
        assert stats["total_sent"] == 1
