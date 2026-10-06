"""Car storage, liveness orchestration, and /api/cars (need DATABASE_URL)."""

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from apartment_finder.cars.models import Car
from apartment_finder.db import get_connection, init_db


@pytest.fixture(autouse=True)
def clean_cars():
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — cannot run PostgreSQL tests")
    init_db()
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM car_listings")
    yield
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM car_listings")


@pytest.fixture
def service():
    from apartment_finder.cars.service import CarListingService
    return CarListingService()


def _car(source_id, **overrides):
    fields = dict(
        source_id=source_id, source_name="dba", city="Copenhagen",
        url=f"https://www.dba.dk/mobility/item/{source_id}", make="Toyota", model="Aygo",
        year=2016, mileage_km=136000, fuel="Petrol", gearbox="manual",
        price_local=65000.0, price_usd=9100.0, location="Hedehusene", seller_type="private",
    )
    fields.update(overrides)
    return Car(**fields)


def _row(source_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM car_listings WHERE source_id = %s", (source_id,))
        return cur.fetchone()


def _set(source_id, **cols):
    sets = ", ".join(f"{k} = %s" for k in cols)
    with get_connection() as conn:
        conn.cursor().execute(
            f"UPDATE car_listings SET {sets} WHERE source_id = %s", (*cols.values(), source_id)
        )


# --- CarListingService -----------------------------------------------------

def test_upsert_inserts_new_and_returns_them(service):
    new = service.upsert_listings([_car("a"), _car("b", is_promoted=True)])
    assert [c.source_id for c in new] == ["a", "b"]
    row = _row("b")
    assert row["is_promoted"] is True
    assert row["status"] == "active"
    assert row["listing_type"] == "buy"
    assert row["first_seen_at"] == row["last_seen_at"]


def test_upsert_refreshes_seen_listing(service):
    service.upsert_listings([_car("a", variant=None)])
    old = datetime.utcnow() - timedelta(days=5)
    _set("a", last_seen_at=old, first_seen_at=old, status="gone", is_promoted=True)

    new = service.upsert_listings([_car("a", price_local=59000.0, variant="VVT-i", is_promoted=False)])

    assert new == []
    row = _row("a")
    assert row["status"] == "active"  # re-seen means live again
    assert row["price_local"] == 59000.0  # price cut picked up
    assert row["variant"] == "VVT-i"  # backfilled
    assert row["is_promoted"] is False
    assert row["first_seen_at"] == old
    assert row["last_seen_at"] > old


def test_liveness_queue_and_status_recording(service):
    service.upsert_listings([_car("seen"), _car("old1"), _car("old2"), _car("old3")])
    past = datetime.utcnow() - timedelta(days=1)
    for sid in ("old1", "old2", "old3"):
        _set(sid, last_seen_at=past)
    _set("old3", status_checked_at=datetime.utcnow() - timedelta(hours=1))

    rows = service.get_listings_to_check("dba", datetime.utcnow() - timedelta(hours=2), 10)
    # Never-checked first; the listing seen this run isn't queued
    queued = [r["source_id"] for r in rows]
    assert len(queued) == 3  # "seen" (scraped this run) isn't queued
    assert set(queued[:2]) == {"old1", "old2"} and queued[2] == "old3"

    service.record_status_check(["old1", "old2"], gone_ids=["old1"], live_ids=["old2"])
    assert _row("old1")["status"] == "gone"
    assert _row("old2")["status"] == "active"
    assert _row("old2")["last_seen_at"] > past
    assert _row("old1")["status_checked_at"] is not None


def test_cleanup_deletes_only_expired(service):
    service.upsert_listings([_car("fresh"), _car("stale")])
    _set("stale", last_seen_at=datetime.utcnow() - timedelta(days=31))
    assert service.cleanup_old_listings() == 1
    assert _row("stale") is None
    assert _row("fresh") is not None


# --- run_car_pipeline ------------------------------------------------------

def test_run_car_pipeline_stores_converts_and_checks_liveness(service):
    from apartment_finder.main import run_car_pipeline

    service.upsert_listings([_car("old_gone"), _car("old_unknown")])
    for sid in ("old_gone", "old_unknown"):
        _set(sid, last_seen_at=datetime.utcnow() - timedelta(days=1))

    adapter = MagicMock()
    adapter.fetch_listings.return_value = [_car("fresh", price_usd=None, price_local=100000.0)]
    adapter.check_status.return_value = {
        "https://www.dba.dk/mobility/item/old_gone": "gone",
        "https://www.dba.dk/mobility/item/old_unknown": "unknown",
    }
    config = {"cars": {"copenhagen": {"display_name": "Copenhagen", "currency": "DKK",
                                      "sources": ["dba"]}}}

    with patch("apartment_finder.main.get_car_adapter", return_value=adapter), \
         patch("apartment_finder.main.CurrencyService") as currency:
        currency.return_value.convert_to_usd.return_value = 14000.0
        result = run_car_pipeline(config, "copenhagen")

    assert result == {"Copenhagen": 1}
    assert _row("fresh")["price_usd"] == 14000.0
    assert _row("old_gone")["status"] == "gone"
    assert _row("old_unknown")["status"] == "active"
    assert _row("old_unknown")["status_checked_at"] is not None
    checked_urls = adapter.check_status.call_args.args[0]
    assert "https://www.dba.dk/mobility/item/fresh" not in checked_urls


def test_run_car_pipeline_raises_when_a_source_fails(service):
    from apartment_finder.main import CarPipelineError, run_car_pipeline

    service.upsert_listings([_car("stale")])
    _set("stale", last_seen_at=datetime.utcnow() - timedelta(days=31))
    empty = MagicMock()
    empty.fetch_listings.return_value = []  # e.g. blocked, or markup changed
    config = {"cars": {
        "copenhagen": {"display_name": "Copenhagen", "sources": ["dba"]},
        "aarhus": {"display_name": "Aarhus", "sources": ["dba"]},
    }}

    with patch("apartment_finder.main.get_car_adapter",
               side_effect=[RuntimeError("bad config"), empty]):
        with pytest.raises(CarPipelineError) as exc:
            run_car_pipeline(config)

    assert "copenhagen/dba: RuntimeError (see server log)" in str(exc.value)
    assert "bad config" not in str(exc.value)  # raw error text stays in the server log
    assert "aarhus/dba: 0 listings" in str(exc.value)
    # A failing first city doesn't skip later cities or cleanup
    empty.fetch_listings.assert_called_once()
    assert _row("stale") is None


def test_run_car_pipeline_fails_on_unknown_or_unmatched_source(service):
    from apartment_finder.main import CarPipelineError, run_car_pipeline

    config = {"cars": {"copenhagen": {"display_name": "Copenhagen", "sources": ["dbaa"]}}}
    with pytest.raises(CarPipelineError, match="dbaa: unknown source"):
        run_car_pipeline(config, "copenhagen")

    config = {"cars": {"copenhagen": {"display_name": "Copenhagen", "sources": ["dba"]}}}
    with pytest.raises(CarPipelineError, match="no car sources ran"):
        run_car_pipeline(config, "copenhagen", only_source="foo")


def test_run_car_pipeline_rejects_mostly_incomplete_listings(service):
    from apartment_finder.main import CarPipelineError, run_car_pipeline

    service.upsert_listings([_car("priced")])
    adapter = MagicMock()
    adapter.page_errors = []
    # Markup drift: cards still parse but price/year selectors no longer match
    adapter.fetch_listings.return_value = [
        _car("priced", price_local=None, year=None),
        _car("other", price_local=None),
        _car("ok"),
    ]
    config = {"cars": {"copenhagen": {"display_name": "Copenhagen", "sources": ["dba"]}}}

    with patch("apartment_finder.main.get_car_adapter", return_value=adapter):
        with pytest.raises(CarPipelineError, match="missing core fields"):
            run_car_pipeline(config, "copenhagen")

    assert _row("priced")["price_local"] == 65000.0  # not nulled out
    assert _row("ok") is None


def test_full_catalog_source_marks_unseen_gone_without_checks(service):
    from apartment_finder.main import run_car_pipeline

    lease = dict(listing_type="lease", price_local=None, monthly_price_local=3000.0,
                 term_months=36, source_name="findleasing")
    kept = [f"still_there_{n}" for n in range(6)]
    service.upsert_listings([_car(sid, **lease) for sid in kept + ["dropped"]])
    for sid in kept + ["dropped"]:
        _set(sid, last_seen_at=datetime.utcnow() - timedelta(days=1))
    adapter = MagicMock(full_catalog=True, page_errors=[])
    adapter.fetch_listings.return_value = [_car(sid, **lease) for sid in kept]
    config = {"cars": {"copenhagen": {"display_name": "Copenhagen", "sources": ["findleasing"]}}}

    with patch("apartment_finder.main.get_car_adapter", return_value=adapter), \
         patch("apartment_finder.main.CAR_ADAPTER_REGISTRY", {"findleasing": object}):
        run_car_pipeline(config, "copenhagen")

    assert _row("dropped")["status"] == "gone"
    assert _row("still_there_0")["status"] == "active"
    adapter.check_status.assert_not_called()


def test_mark_unseen_gone_refuses_mass_removal_and_scopes_city(service):
    service.upsert_listings([_car(f"c{n}") for n in range(10)] + [_car("other_city", city="Aarhus")])
    old = datetime.utcnow() - timedelta(days=1)
    for n in range(3):
        _set(f"c{n}", last_seen_at=old)
    _set("other_city", last_seen_at=old)
    cutoff = datetime.utcnow() - timedelta(hours=1)

    # 3 of 10 unseen = 30% > 20% cap: nothing changes
    assert service.mark_unseen_gone("dba", "Copenhagen", cutoff) is None
    assert _row("c0")["status"] == "active"

    _set("c1", last_seen_at=datetime.utcnow())
    _set("c2", last_seen_at=datetime.utcnow())
    assert service.mark_unseen_gone("dba", "Copenhagen", cutoff) == 1
    assert _row("c0")["status"] == "gone"
    assert _row("other_city")["status"] == "active"  # other city untouched


def test_lease_without_year_is_not_incomplete(service):
    from apartment_finder.main import _is_incomplete

    lease = dict(listing_type="lease", price_local=None, monthly_price_local=3000.0)
    assert not _is_incomplete(_car("new_order", year=None, term_months=36, **lease))
    assert _is_incomplete(_car("no_term", term_months=None, **lease))
    assert _is_incomplete(_car("buy_no_year", year=None))


def test_run_car_pipeline_reports_partial_page_failures(service):
    from apartment_finder.main import CarPipelineError, run_car_pipeline

    adapter = MagicMock()
    adapter.page_errors = ["page 2: HTTP 429"]
    adapter.fetch_listings.return_value = [_car("p1")]
    adapter.check_status.return_value = {}
    config = {"cars": {"copenhagen": {"display_name": "Copenhagen", "sources": ["dba"]}}}

    with patch("apartment_finder.main.get_car_adapter", return_value=adapter):
        with pytest.raises(CarPipelineError, match="page 2: HTTP 429"):
            run_car_pipeline(config, "copenhagen")

    assert _row("p1") is not None  # page 1 still stored


def test_upsert_skips_bad_row_without_losing_batch(service):
    new = service.upsert_listings([_car("good1"), _car("bad", mileage_km=10**12), _car("good2")])

    assert [c.source_id for c in new] == ["good1", "good2"]
    assert _row("bad") is None
    assert _row("good2") is not None


def test_upsert_stores_vat_flag(service):
    service.upsert_listings([_car("van", price_local=18750.0, vat_added=True)])
    assert _row("van")["vat_added"] is True


def test_upsert_takes_listing_type_and_prices_as_scraped(service):
    service.upsert_listings([_car("flip", listing_type="lease", price_local=None,
                                  monthly_price_local=2999.0)])
    service.upsert_listings([_car("flip", price_local=120000.0)])
    row = _row("flip")
    assert (row["listing_type"], row["price_local"], row["monthly_price_local"]) == \
        ("buy", 120000.0, None)


def test_run_car_pipeline_rejects_unknown_city():
    from apartment_finder.main import run_car_pipeline

    with pytest.raises(ValueError):
        run_car_pipeline({"cars": {"copenhagen": {}}}, "oslo")


# --- /api/cars ---------------------------------------------------------------

@pytest.fixture
def client():
    from apartment_finder.web.app import app
    return app.test_client()


def test_api_cars_filters_sorts_and_flags(service, client):
    now = datetime.utcnow()
    service.upsert_listings([
        _car("cheap_old", price_local=20000.0, year=2005, mileage_km=250000, fuel="Diesel"),
        _car("mid_auto", price_local=90000.0, year=2017, gearbox="automatic", seller_type="dealer"),
        _car("pricey_ev", price_local=200000.0, year=2022, mileage_km=30000, fuel="Electric", gearbox=None),
        _car("sold", price_local=50000.0),
    ])
    _set("cheap_old", listed_at=now - timedelta(days=10))
    _set("mid_auto", listed_at=now - timedelta(hours=2))
    _set("pricey_ev", listed_at=now - timedelta(days=2))
    _set("sold", status="gone")

    data = client.get("/api/cars").get_json()
    assert data["total"] == 3  # gone listing hidden by default
    assert [c["source_id"] for c in data["cars"]] == ["mid_auto", "pricey_ev", "cheap_old"]  # newest first
    assert data["fuels"] == ["Diesel", "Electric", "Petrol"]

    def ids(query):
        return sorted(c["source_id"] for c in client.get("/api/cars?" + query).get_json()["cars"])

    assert ids("max_price=100000") == ["cheap_old", "mid_auto"]
    assert ids("min_year=2015") == ["mid_auto", "pricey_ev"]
    assert ids("max_km=100000") == ["pricey_ev"]
    assert ids("fuel=Electric") == ["pricey_ev"]
    assert ids("gearbox=automatic") == ["mid_auto"]
    assert ids("seller=dealer") == ["mid_auto"]
    assert ids("added_days=3") == ["mid_auto", "pricey_ev"]
    assert ids("show_unavailable=1") == ["cheap_old", "mid_auto", "pricey_ev", "sold"]
    assert ids("max_price=abc") == ["cheap_old", "mid_auto", "pricey_ev"]  # bad input ignored

    sold = next(c for c in client.get("/api/cars?show_unavailable=1").get_json()["cars"]
                if c["source_id"] == "sold")
    assert sold["active"] is False

    by_price = client.get("/api/cars?sort=price-asc").get_json()["cars"]
    assert [c["source_id"] for c in by_price] == ["cheap_old", "mid_auto", "pricey_ev"]

    page = client.get("/api/cars?limit=2&offset=2").get_json()
    assert page["total"] == 3 and [c["source_id"] for c in page["cars"]] == ["cheap_old"]


def test_api_cars_active_flag_is_relative_to_source(service, client):
    service.upsert_listings([_car("recent"), _car("stale")])
    _set("stale", last_seen_at=datetime.utcnow() - timedelta(days=15))
    data = client.get("/api/cars?show_unavailable=1").get_json()
    assert {c["source_id"]: c["active"] for c in data["cars"]} == {"recent": True, "stale": False}


def test_api_cars_lease_fields_filters_and_total_sort(service, client):
    lease = dict(listing_type="lease", price_local=None, seller_type="dealer")
    service.upsert_listings([
        # 2.000/mo x 12 + 60.000 down = 84.000 total
        _car("cheap_monthly", monthly_price_local=2000.0, down_payment_local=60000,
             term_months=12, lease_kind="financial", **lease),
        # 2.500/mo x 12 + 0 down = 30.000 total
        _car("cheap_total", monthly_price_local=2500.0, down_payment_local=0,
             term_months=12, lease_kind="operational", km_per_year=15000, **lease),
        _car("for_sale"),
    ])

    # Real monthly cost: 2.000 + 60.000/12 = 7.000 vs 2.500 + 0 = 2.500
    data = client.get("/api/cars?type=lease&sort=effective-asc").get_json()
    assert [c["source_id"] for c in data["cars"]] == ["cheap_total", "cheap_monthly"]
    first = data["cars"][0]
    assert (first["term_months"], first["km_per_year"], first["lease_kind"]) == (12, 15000, "operational")

    ids = lambda q: [c["source_id"] for c in client.get(f"/api/cars?type=lease&{q}").get_json()["cars"]]
    assert ids("max_down=10000") == ["cheap_total"]
    service.upsert_listings([_car("dba_monthly", monthly_price_local=1500.0, **lease)])
    assert "dba_monthly" not in ids("max_down=100000")  # unknown down payment fails the filter
    assert ids("lease_kind=financial") == ["cheap_monthly"]
    assert sorted(ids("max_price=2200")) == ["cheap_monthly", "dba_monthly"]  # monthly price for leases
    assert client.get("/api/cars?type=buy").get_json()["total"] == 1


def test_api_cars_lease_empty_and_bad_type(service, client):
    service.upsert_listings([_car("a")])
    assert client.get("/api/cars?type=lease").get_json()["total"] == 0
    assert client.get("/api/cars?type=rent").status_code == 400
    # Huge numbers are clamped, not a 500
    assert client.get("/api/cars?added_days=99999999999&max_km=99999999999").status_code == 200


def test_cars_page_served(client):
    response = client.get("/cars")
    assert response.status_code == 200
    assert b"Max down payment" in response.data
    response.close()


class _InlineThread:
    """Stands in for threading.Thread: runs the fetch job immediately."""
    def __init__(self, target, args=(), **kwargs):
        self._target, self._args = target, args

    def start(self):
        self._target(*self._args)


def _clear_fetch_state():
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM fetch_locks; DELETE FROM fetch_runs")


def test_fetch_cars_endpoint_starts_job_throttles_and_reports(client):
    _clear_fetch_state()
    assert client.post("/api/fetch-cars", json={"source": "bilbasen"}).status_code == 400
    assert client.post("/api/fetch-cars", data="x").status_code == 400  # no default source

    log = "12:00 | INFO | apartment_finder.cars.service | Stored 150 car listings (2 new, 0 skipped)"
    done = MagicMock(returncode=0, stdout=log, stderr="")
    with patch("apartment_finder.web.app.subprocess.run", return_value=done) as run, \
         patch("apartment_finder.web.app.threading.Thread", _InlineThread):
        first = client.post("/api/fetch-cars", json={"source": "dba"})
        second = client.post("/api/fetch-cars", json={"source": "dba"})
        other = client.post("/api/fetch-cars", json={"source": "findleasing"})

    assert first.status_code == 202 and first.get_json()["started"] is True
    assert run.call_args_list[0].args[0][-3:] == ["--source", "dba", "--no-email"]
    assert second.status_code == 429  # cooldown is per source, stored in Postgres
    assert other.status_code == 202
    assert run.call_count == 2

    status = client.get("/api/fetch-status?job=cars:dba").get_json()
    assert status["running"] is False
    assert status["last"]["ok"] is True
    assert status["last"]["summary"] == ["Stored 150 car listings (2 new, 0 skipped)"]
    assert client.get("/api/fetch-status?job=bad name").status_code == 400


def test_fetch_cars_failure_does_not_leak_logs_and_allows_retry(client):
    _clear_fetch_state()
    crash = MagicMock(returncode=1, stdout="Traceback (most recent call last):\n  File \"/home/runner/workspace/x.py\"",
                      stderr='psycopg2.OperationalError: connection to server at "db.internal" failed')
    with patch("apartment_finder.web.app.subprocess.run", return_value=crash), \
         patch("apartment_finder.web.app.threading.Thread", _InlineThread):
        client.post("/api/fetch-cars", json={"source": "dba"})

    status = client.get("/api/fetch-status?job=cars:dba")
    body = status.get_data(as_text=True)
    assert status.get_json()["last"]["ok"] is False
    assert status.get_json()["running"] is False
    assert "Traceback" not in body and "/home/runner" not in body and "db.internal" not in body

    # With a long cooldown, a failed run frees the slot after 30 min instead
    _clear_fetch_state()
    with patch("apartment_finder.web.app.subprocess.run", return_value=crash), \
         patch("apartment_finder.web.app.threading.Thread", _InlineThread), \
         patch("apartment_finder.web.app.CAR_FETCH_COOLDOWN_MINUTES", 600):
        client.post("/api/fetch-cars", json={"source": "dba"})
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT started_at FROM fetch_locks WHERE name = 'cars:dba'")
        started = cur.fetchone()["started_at"]
    expires_in = started + timedelta(minutes=600) - datetime.utcnow()
    assert timedelta(minutes=29) < expires_in <= timedelta(minutes=30)


def test_fetch_status_reports_running(client):
    _clear_fetch_state()
    with patch("apartment_finder.web.app.threading.Thread") as thread:  # never runs
        assert client.post("/api/fetch-cars", json={"source": "dba"}).status_code == 202
    thread.return_value.start.assert_called_once()
    status = client.get("/api/fetch-status?job=cars:dba").get_json()
    assert status["running"] is True and status["last"] is None


def test_failed_long_cooldown_run_cannot_be_restarted_at_once(client):
    _clear_fetch_state()
    crash = MagicMock(returncode=1, stdout="", stderr="boom")
    with patch("apartment_finder.web.app.subprocess.run", return_value=crash), \
         patch("apartment_finder.web.app.threading.Thread", _InlineThread):
        assert client.post("/api/fetch-homes").status_code == 202  # fails
        # Failure frees the slot after 30 min, not immediately
        assert client.post("/api/fetch-homes").status_code == 429

