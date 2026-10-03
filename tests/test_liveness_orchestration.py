"""ApartmentFinder._check_liveness and the get_listings `active` flag (need DATABASE_URL)."""

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from apartment_finder.db import get_connection, init_db
from apartment_finder.main import ApartmentFinder
from apartment_finder.models.apartment import Amenities, Apartment
from apartment_finder.services.deduplication import DeduplicationService


@pytest.fixture(autouse=True)
def clean_db():
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — cannot run PostgreSQL tests")
    init_db()
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM ratings; DELETE FROM comments; DELETE FROM seen_listings")
    yield
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM ratings; DELETE FROM comments; DELETE FROM seen_listings")


def _apt(source_id, source_name="balder"):
    return Apartment(
        source_id=source_id, source_name=source_name, title=source_id,
        url=f"https://example.com/{source_id}", price_local=1000.0, currency="DKK",
        price_usd=140.0, bedrooms=2, bathrooms=None, sqft=None,
        city="Copenhagen", country="Denmark", amenities=Amenities(),
    )


def _set(source_id, **cols):
    sets = ", ".join(f"{k} = %s" for k in cols)
    with get_connection() as conn:
        conn.cursor().execute(
            f"UPDATE seen_listings SET {sets} WHERE source_id = %s", (*cols.values(), source_id)
        )


def _status(source_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT status, status_checked_at FROM seen_listings WHERE source_id = %s", (source_id,))
        return cur.fetchone()


def _finder():
    finder = ApartmentFinder.__new__(ApartmentFinder)
    finder.config = {"sources": {}}
    finder.dedup_service = DeduplicationService()
    return finder


def test_check_liveness_applies_statuses_and_isolates_failing_sources():
    DeduplicationService().filter_new_listings(
        [_apt("b_gone"), _apt("b_live"), _apt("b_unchecked"), _apt("l_1", "lejebolig")]
    )
    city_config = {"display_name": "Copenhagen", "sources": ["lejebolig", "balder", "nope"]}

    balder = MagicMock()
    balder.check_status.return_value = {
        "https://example.com/b_gone": "gone",
        "https://example.com/b_live": "active",
    }
    broken = MagicMock()
    broken.check_status.side_effect = RuntimeError("boom")

    def fake_get_adapter(name, *_):
        return {"balder": balder, "lejebolig": broken}[name]

    with patch("apartment_finder.main.get_adapter", side_effect=fake_get_adapter):
        _finder()._check_liveness(city_config, None, datetime.utcnow() + timedelta(seconds=1))

    assert _status("b_gone")["status"] == "gone"
    assert _status("b_live")["status"] == "active"
    assert _status("b_live")["status_checked_at"] is not None
    # Omitted from the result = not checked: stays first in line for next run
    assert _status("b_unchecked")["status_checked_at"] is None
    assert _status("l_1")["status_checked_at"] is None


def test_check_liveness_respects_only_source():
    DeduplicationService().filter_new_listings([_apt("b_1")])
    with patch("apartment_finder.main.get_adapter") as get_adapter:
        _finder()._check_liveness(
            {"display_name": "Copenhagen", "sources": ["balder"]}, "lejebolig",
            datetime.utcnow() + timedelta(seconds=1),
        )
    get_adapter.assert_not_called()


def test_get_listings_active_flag_is_relative_to_source():
    from apartment_finder.web.app import get_listings

    DeduplicationService().filter_new_listings(
        [_apt("fresh"), _apt("stale"), _apt("gone"),
         _apt("dead_src_old", "deadsrc"), _apt("dead_src_older", "deadsrc")]
    )
    now = datetime.utcnow()
    _set("stale", last_seen_at=now - timedelta(days=4))
    _set("gone", status="gone")
    # A source whose scraper stopped weeks ago: its newest rows stay active
    _set("dead_src_old", last_seen_at=now - timedelta(days=20))
    _set("dead_src_older", last_seen_at=now - timedelta(days=25))

    active = {row["source_id"]: row["active"] for row in get_listings()}
    assert active == {
        "fresh": True, "stale": False, "gone": False,
        "dead_src_old": True, "dead_src_older": False,
    }
