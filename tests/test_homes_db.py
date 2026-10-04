"""Home storage, price history, liveness, the homes pipeline, /api/homes and
/api/fetch-homes (need DATABASE_URL)."""

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from apartment_finder.db import get_connection, init_db
from apartment_finder.homes.models import HomeListing


@pytest.fixture(autouse=True)
def clean_homes():
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — cannot run PostgreSQL tests")
    init_db()
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM home_listings")
    yield
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM home_listings")


@pytest.fixture
def service():
    from apartment_finder.homes.service import HomeListingService
    return HomeListingService()


@pytest.fixture
def client():
    from apartment_finder.web import app as web_app
    # Never call Frankfurter from tests
    web_app._usd_rate_cache.update(rate=0.15, at=10 ** 12)
    return web_app.app.test_client()


def _home(source_id, **overrides):
    fields = dict(
        source_id=source_id, source_name="homedk",
        url=f"https://home.dk/salg/lejligheder/x/sag-{source_id}/",
        address="Malttorvet 16, 2. mf., 1799 København V", street="Malttorvet 16, 2. mf.",
        postcode="1799", city="København V", municipality="København",
        property_type="flat", price_dkk=4_795_000, sqm=47,
        latitude=55.665, longitude=12.530, broker="home",
    )
    fields.update(overrides)
    return HomeListing(**fields)


def _row(source_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM home_listings WHERE source_id = %s", (source_id,))
        return cur.fetchone()


def _set(source_id, **cols):
    sets = ", ".join(f"{k} = %s" for k in cols)
    with get_connection() as conn:
        conn.cursor().execute(
            f"UPDATE home_listings SET {sets} WHERE source_id = %s", (*cols.values(), source_id)
        )


# --- HomeListingService ------------------------------------------------------

def test_init_db_is_idempotent():
    init_db()
    init_db()


def test_upsert_inserts_new_with_first_price_and_estimate(service):
    new = service.upsert_listings([_home("a"), _home("b", is_external=True, broker=None)])
    assert [h.source_id for h in new] == ["a", "b"]
    row = _row("a")
    assert row["status"] == "active"
    assert row["first_price_dkk"] == row["price_dkk"] == 4_795_000
    assert row["previous_price_dkk"] is None and row["price_changed_at"] is None
    assert row["est_monthly_cash_dkk"] > 20_000
    assert row["first_seen_at"] == row["last_seen_at"]
    assert _row("b")["is_external"] is True


def test_price_drop_is_recorded_and_reseen_listing_revives(service):
    service.upsert_listings([_home("a", sqm=None)])
    old = datetime.utcnow() - timedelta(days=5)
    _set("a", first_seen_at=old, last_seen_at=old, status="gone")
    estimate_before = _row("a")["est_monthly_cash_dkk"]

    assert service.upsert_listings([_home("a", price_dkk=4_495_000, sqm=47)]) == []
    row = _row("a")
    assert row["status"] == "active"  # back in the catalog = for sale again
    assert row["price_dkk"] == 4_495_000
    assert row["previous_price_dkk"] == 4_795_000
    assert row["first_price_dkk"] == 4_795_000
    assert row["price_changed_at"] is not None
    assert row["sqm"] == 47  # backfilled
    assert row["est_monthly_cash_dkk"] < estimate_before
    changed_at = row["price_changed_at"]

    # Same price again: no new change recorded
    service.upsert_listings([_home("a", price_dkk=4_495_000)])
    row = _row("a")
    assert row["previous_price_dkk"] == 4_795_000 and row["price_changed_at"] == changed_at


def test_one_bad_row_does_not_sink_the_batch(service):
    new = service.upsert_listings([_home("ok"), _home("bad", price_dkk=10 ** 20), _home("ok2")])
    assert [h.source_id for h in new] == ["ok", "ok2"]
    assert _row("bad") is None
    assert service.last_skipped == 1
    service.upsert_listings([_home("ok")])
    assert service.last_skipped == 0


def test_mark_unseen_gone_needs_two_missed_runs(service):
    service.upsert_listings([_home("seen"), _home("unseen"), _home("other_source", source_name="x")])
    past = datetime.utcnow() - timedelta(hours=3)
    _set("unseen", last_seen_at=past)
    _set("other_source", last_seen_at=past)
    cutoff = datetime.utcnow() - timedelta(hours=1)

    assert service.mark_unseen_gone("homedk", cutoff) == 0  # first miss: could be a paging hiccup
    assert _row("unseen")["status"] == "active" and _row("unseen")["missed_runs"] == 1
    assert service.mark_unseen_gone("homedk", cutoff) == 1
    assert _row("unseen")["status"] == "gone"
    assert _row("seen")["status"] == "active" and _row("seen")["missed_runs"] == 0
    assert _row("other_source")["status"] == "active"  # other sources untouched

    service.upsert_listings([_home("unseen")])  # back in the catalog
    assert _row("unseen")["status"] == "active" and _row("unseen")["missed_runs"] == 0

    _set("unseen", last_seen_at=datetime.utcnow() - timedelta(days=31))
    assert service.cleanup_old_listings() == 1
    assert _row("unseen") is None


def test_details_queue_and_recording(service):
    service.upsert_listings([_home("own_new"), _home("ext", is_external=True), _home("own_sold")])
    queued = {r["source_id"] for r in service.get_listings_needing_details("homedk", 10)}
    assert queued == {"own_new", "own_sold"}  # external listings are never queued

    before = _row("own_new")["est_monthly_cash_dkk"]
    service.record_details({
        "own_new": {"rooms": 2, "year_built": 2021, "monthly_owner_expenses_dkk": 2411,
                    "energy_label": "A2015", "listed_at": datetime(2026, 10, 3, 6, 28)},
        "own_sold": {"gone": True},
    })
    row = _row("own_new")
    assert (row["rooms"], row["year_built"], row["energy_label"]) == (2, 2021, "A2015")
    assert row["monthly_owner_expenses_dkk"] == 2411
    assert row["est_monthly_cash_dkk"] != before  # owner expenses replace the tax estimate
    assert _row("own_sold")["status"] == "gone"
    assert service.get_listings_needing_details("homedk", 10) == []


def test_under_offer_stays_active_and_sold_goes(service):
    service.upsert_listings([_home("offer"), _home("sold")])
    service.record_details({"offer": {"rooms": 3, "under_offer": True, "off_market": False},
                            "sold": {"under_offer": False, "off_market": True}})
    assert (_row("offer")["status"], _row("offer")["under_offer"]) == ("active", True)
    assert _row("sold")["status"] == "gone"


def test_unreadable_detail_pages_leave_the_queue(service):
    service.upsert_listings([_home("broken"), _home("good")])
    service.record_details({"broken": {"failed": True}})
    row = _row("broken")
    assert row["details_checked_at"] is not None and row["status"] == "active" and row["rooms"] is None
    assert [r["source_id"] for r in service.get_listings_needing_details("homedk", 10)] == ["good"]


# --- run_home_pipeline -------------------------------------------------------

def _config():
    return {"homes": {"copenhagen": {"display_name": "Copenhagen", "sources": ["homedk"]}}}


def _mock_adapter(homes, page_errors=()):
    adapter = MagicMock()
    adapter.fetch_listings.return_value = homes
    adapter.page_errors = list(page_errors)
    adapter.full_catalog = True
    adapter.max_detail_pages = 40
    adapter.request_count = 7
    adapter.detail_errors = 0
    adapter.rate_limited = False
    adapter.fetch_details.return_value = {}
    return adapter


def test_pipeline_clean_run_marks_unseen_gone_and_reads_details(service):
    from apartment_finder.main import run_home_pipeline

    service.upsert_listings([_home("sold")])
    # Already missed one clean run; this run is its second
    _set("sold", last_seen_at=datetime.utcnow() - timedelta(days=1), details_checked_at=datetime.utcnow(),
         missed_runs=1)
    adapter = _mock_adapter([_home("fresh")])
    adapter.fetch_details.return_value = {"fresh": {"rooms": 3}}

    with patch("apartment_finder.main.get_home_adapter", return_value=adapter):
        assert run_home_pipeline(_config(), "copenhagen") == {"Copenhagen": 1}

    assert _row("sold")["status"] == "gone"
    assert _row("fresh")["rooms"] == 3
    asked = adapter.fetch_details.call_args.args[0]
    assert [r["source_id"] for r in asked] == ["fresh"]


def test_pipeline_with_page_errors_stores_but_does_not_mark_gone(service):
    from apartment_finder.main import HomePipelineError, run_home_pipeline

    service.upsert_listings([_home("maybe_sold")])
    _set("maybe_sold", last_seen_at=datetime.utcnow() - timedelta(days=1))
    adapter = _mock_adapter([_home("fresh")], page_errors=["rate limited (429) after 40 requests"])

    with patch("apartment_finder.main.get_home_adapter", return_value=adapter):
        with pytest.raises(HomePipelineError, match="429"):
            run_home_pipeline(_config(), "copenhagen")

    assert _row("fresh") is not None
    assert _row("maybe_sold")["status"] == "active"
    assert _row("maybe_sold")["missed_runs"] == 0  # a failed run doesn't count as a miss


def test_pipeline_skipped_rows_block_gone_marking(service):
    from apartment_finder.main import HomePipelineError, run_home_pipeline

    service.upsert_listings([_home("listed")])
    _set("listed", last_seen_at=datetime.utcnow() - timedelta(days=1), missed_runs=1)
    # Still in the catalog, but this run can't store it (BIGINT overflow)
    adapter = _mock_adapter([_home("listed", price_dkk=10 ** 20), _home("fresh")])

    with patch("apartment_finder.main.get_home_adapter", return_value=adapter):
        with pytest.raises(HomePipelineError, match="could not be stored"):
            run_home_pipeline(_config(), "copenhagen")
    assert _row("listed")["status"] == "active"


def test_pipeline_flags_failing_detail_pages_and_hides_exception_text(service):
    from apartment_finder.main import HomePipelineError, run_home_pipeline

    adapter = _mock_adapter([_home("fresh")])
    adapter.detail_errors = 1  # the only detail page failed
    with patch("apartment_finder.main.get_home_adapter", return_value=adapter):
        with pytest.raises(HomePipelineError, match="1 of 1 detail pages failed"):
            run_home_pipeline(_config(), "copenhagen")

    broken = _mock_adapter([])
    broken.fetch_listings.side_effect = RuntimeError('connection to server at "db.internal" failed')
    with patch("apartment_finder.main.get_home_adapter", return_value=broken):
        with pytest.raises(HomePipelineError) as exc:
            run_home_pipeline(_config(), "copenhagen")
    assert "RuntimeError" in str(exc.value) and "db.internal" not in str(exc.value)


def test_pipeline_fails_on_empty_unknown_or_missing_city(service):
    from apartment_finder.main import HomePipelineError, run_home_pipeline

    with patch("apartment_finder.main.get_home_adapter", return_value=_mock_adapter([])):
        with pytest.raises(HomePipelineError, match="0 listings"):
            run_home_pipeline(_config(), "copenhagen")
    config = {"homes": {"copenhagen": {"display_name": "Copenhagen", "sources": ["boliga"]}}}
    with pytest.raises(HomePipelineError, match="unknown source"):
        run_home_pipeline(config, "copenhagen")
    with pytest.raises(ValueError):
        run_home_pipeline(_config(), "aarhus")


# --- /api/homes --------------------------------------------------------------

def _seed(service):
    now = datetime.utcnow()
    service.upsert_listings([
        # 102,021 kr/m2: pricey flat in København (avg 75,120)
        _home("kbh_flat", price_dkk=4_795_000, sqm=47),
        # 50,000 kr/m2: cheap big flat, price will drop
        _home("frb_flat", price_dkk=6_000_000, sqm=120, municipality="Frederiksberg",
              postcode="2000", city="Frederiksberg"),
        _home("villa", property_type="villa", price_dkk=9_000_000, sqm=180,
              municipality="Gentofte", postcode="2900", city="Hellerup", is_external=True, broker=None),
        _home("terraced", property_type="terraced", price_dkk=3_500_000, sqm=None,
              municipality="Hvidovre", postcode="2650", city="Hvidovre"),
        _home("sold", price_dkk=2_000_000, sqm=60),
    ])
    service.upsert_listings([_home("frb_flat", price_dkk=5_400_000, sqm=120, municipality="Frederiksberg",
                                   postcode="2000", city="Frederiksberg")])  # 10% drop
    _set("sold", status="gone")
    _set("kbh_flat", rooms=2, energy_label="A2015", listed_at=now - timedelta(days=10))
    _set("frb_flat", rooms=4, energy_label="C")
    _set("villa", rooms=6, energy_label="E", listed_at=now - timedelta(days=2))
    # A home from the first import, a month ago: the others then count as new
    service.upsert_listings([_home("seed_import", price_dkk=8_000_000, sqm=90)])
    _set("seed_import", first_seen_at=now - timedelta(days=30), status="gone")


def test_api_homes_added_at_unknown_for_first_import(service, client):
    service.upsert_listings([_home("a"), _home("b")])  # both from the first import
    homes = client.get("/api/homes").get_json()["homes"]
    assert [h["added_at"] for h in homes] == [None, None]
    assert client.get("/api/homes?added_days=1").get_json()["total"] == 0


def test_api_homes_filters(service, client):
    _seed(service)
    data = client.get("/api/homes").get_json()
    assert data["total"] == 4  # sold home hidden by default
    assert data["usd_per_dkk"] == 0.15
    assert data["municipalities"] == ["Frederiksberg", "Gentofte", "Hvidovre", "København"]

    def ids(query):
        response = client.get("/api/homes?" + query)
        assert response.status_code == 200, response.get_json()
        return sorted(h["source_id"] for h in response.get_json()["homes"])

    assert ids("max_price=5500000") == ["frb_flat", "kbh_flat", "terraced"]
    assert ids("min_sqm=100") == ["frb_flat", "villa"]
    assert ids("max_sqm=100") == ["kbh_flat"]  # unknown size fails size filters
    assert ids("min_rooms=4") == ["frb_flat", "villa"]
    assert ids("type=villa") == ["villa"]
    assert ids("municipality=Frederiksberg") == ["frb_flat"]
    assert ids("postcode=2650") == ["terraced"]
    assert ids("max_ppsqm=60000") == ["frb_flat", "villa"]
    assert ids("energy=C") == ["frb_flat", "kbh_flat"]  # C or better
    assert ids("energy=a") == ["kbh_flat"]
    assert ids("added_days=5") == ["frb_flat", "terraced", "villa"]
    assert ids("show_unavailable=1") == ["frb_flat", "kbh_flat", "seed_import", "sold", "terraced", "villa"]
    assert ids("max_price=abc") == ["frb_flat", "kbh_flat", "terraced", "villa"]  # bad input ignored

    for bad in ("type=castle", "energy=Z", "postcode=12a4", "postcode=' OR 1=1"):
        assert client.get("/api/homes?" + bad).status_code == 400


def test_api_homes_sorts(service, client):
    _seed(service)

    def order(sort):
        return [h["source_id"] for h in client.get(f"/api/homes?sort={sort}").get_json()["homes"]]

    assert order("price-asc") == ["terraced", "kbh_flat", "frb_flat", "villa"]
    assert order("price-desc") == ["villa", "frb_flat", "kbh_flat", "terraced"]
    assert order("ppsqm-asc") == ["frb_flat", "villa", "kbh_flat", "terraced"]  # unknown m2 last
    assert order("monthly-asc") == ["terraced", "kbh_flat", "frb_flat", "villa"]
    assert order("drop-desc")[0] == "frb_flat"
    newest = order("newest")
    assert newest.index("villa") < newest.index("kbh_flat")  # listed 2 vs 10 days ago

    page = client.get("/api/homes?sort=price-asc&limit=2&offset=2").get_json()
    assert page["total"] == 4 and [h["source_id"] for h in page["homes"]] == ["frb_flat", "villa"]


def test_api_homes_money_fields(service, client):
    _seed(service)
    homes = {h["source_id"]: h for h in client.get("/api/homes").get_json()["homes"]}

    flat = homes["kbh_flat"]
    assert flat["price_per_sqm"] == 102_021
    assert flat["sqft"] == 506  # 47 m2 x 10.7639
    assert flat["municipality_avg_per_sqm"] == 75_120
    assert flat["vs_municipality_pct"] == pytest.approx(35.8, abs=0.1)
    assert flat["monthly"]["running_costs_estimated"] is True
    assert flat["monthly"]["cash_out"] == flat["est_monthly_cash_dkk"]
    assert flat["cash_needed"] > 0.07 * 4_795_000
    assert flat["price_dropped"] is False

    dropped = homes["frb_flat"]
    assert dropped["price_dropped"] is True
    assert (dropped["price_drop_dkk"], dropped["price_drop_pct"]) == (600_000, 10.0)
    assert dropped["previous_price_dkk"] == 6_000_000
    assert dropped["vs_municipality_pct"] == pytest.approx(-46.1, abs=0.1)  # 45,000 vs 83,530

    assert homes["villa"]["vs_municipality_pct"] is not None  # Gentofte house average
    assert homes["terraced"]["price_per_sqm"] is None and homes["terraced"]["sqft"] is None


def test_api_homes_active_flag_is_relative_to_source(service, client):
    service.upsert_listings([_home("recent"), _home("stale")])
    _set("stale", last_seen_at=datetime.utcnow() - timedelta(days=4))
    data = client.get("/api/homes?show_unavailable=1").get_json()
    assert {h["source_id"]: h["active"] for h in data["homes"]} == {"recent": True, "stale": False}


def test_homes_page_served_with_nav(client):
    response = client.get("/homes")
    assert response.status_code == 200
    assert b"Buy a home" in response.data and b"estimate" in response.data
    response.close()
    for page in ("/", "/cars"):
        response = client.get(page)
        assert b'href="/homes"' in response.data
        response.close()


# --- /api/fetch-homes --------------------------------------------------------

def _clear_fetch_locks():
    with get_connection() as conn:
        conn.cursor().execute("DELETE FROM fetch_locks")


def test_fetch_homes_endpoint_throttles_and_summarizes(client):
    _clear_fetch_locks()
    log = ("12:00 | INFO | apartment_finder.homes.service | Stored 4380 home listings (12 new, 0 skipped)\n"
           "12:00 | INFO | apartment_finder.main | Homes run: 4380 listings, 412 requests")
    done = MagicMock(returncode=0, stdout=log, stderr="")
    with patch("apartment_finder.web.app.subprocess.run", return_value=done) as run:
        first = client.post("/api/fetch-homes")
        second = client.post("/api/fetch-homes")

    assert first.status_code == 200
    assert first.get_json()["summary"] == ["Stored 4380 home listings (12 new, 0 skipped)",
                                           "Homes run: 4380 listings, 412 requests"]
    args = run.call_args_list[0].args[0]
    assert args[-6:] == ["--homes", "--city", "copenhagen", "--source", "homedk", "--no-email"]
    assert run.call_args_list[0].kwargs["timeout"] == 3000
    assert second.status_code == 429  # cooldown stored in Postgres
    assert run.call_count == 1


def test_fetch_homes_failure_does_not_leak_logs(client):
    _clear_fetch_locks()
    crash = MagicMock(returncode=1, stdout="Traceback (most recent call last):\n  File \"/home/runner/x.py\"",
                      stderr='psycopg2.OperationalError: connection to server at "db.internal" failed')
    with patch("apartment_finder.web.app.subprocess.run", return_value=crash):
        response = client.post("/api/fetch-homes")
    body = response.get_data(as_text=True)
    assert response.status_code == 502
    assert "Traceback" not in body and "/home/runner" not in body and "db.internal" not in body
