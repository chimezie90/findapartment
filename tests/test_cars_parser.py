"""DBA car parser and adapter tests (no network, no database)."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from apartment_finder.cars.models import Car
from apartment_finder.cars.dba import (
    DbaCarAdapter,
    parse_danish_int,
    parse_listing_age,
    parse_search_page,
    parse_spec_line,
    split_make_model,
)

FIXTURE = Path(__file__).parent / "fixtures" / "dba_cars_search.html"
NOW = datetime(2026, 10, 3, 12, 0, 0)


@pytest.fixture(scope="module")
def cars():
    return {c.source_id: c for c in parse_search_page(FIXTURE.read_text(), "Copenhagen", now=NOW)}


def test_parses_every_card_in_fixture(cars):
    assert len(cars) == 10
    assert all(c.url == f"https://www.dba.dk/mobility/item/{c.source_id[4:]}" for c in cars.values())
    assert all(c.city == "Copenhagen" and c.currency == "DKK" for c in cars.values())
    assert all(c.listing_type == "buy" and c.monthly_price_local is None for c in cars.values())


def test_paid_placement_card(cars):
    audi = cars["dba_25373599"]
    assert (audi.make, audi.model, audi.variant) == ("Audi", "A1", "TFSi 125 Sportback")
    assert (audi.year, audi.mileage_km, audi.fuel, audi.gearbox) == (2018, 108000, "Petrol", "manual")
    assert audi.price_local == 124900
    assert audi.location == "Frederiksberg"
    assert audi.seller_type == "private"
    assert audi.is_promoted is True
    assert audi.listed_at == NOW - timedelta(days=4)
    # Downsized thumbnail for low-bandwidth phones
    assert audi.thumbnail_url.startswith("https://images.dbastatic.dk/dynamic/240w/item/25373599/")


def test_ex_vat_price_gets_moms_added(cars):
    van = cars["dba_25478260"]  # Citroen Jumper, "15.000 kr. ekskl. moms"
    assert van.vat_added is True
    assert van.price_local == 18750
    assert sum(c.vat_added for c in cars.values()) == 1


def test_only_one_card_is_promoted(cars):
    assert [c.source_id for c in cars.values() if c.is_promoted] == ["dba_25373599"]


def test_dealer_card_keeps_town_and_skips_engine_size(cars):
    peugeot = next(c for c in cars.values() if c.model == "208")
    assert peugeot.seller_type == "dealer"
    assert peugeot.location == "Herlev"  # not "Herlev ∙ Herlev Biler"
    assert (peugeot.year, peugeot.mileage_km, peugeot.fuel) == (2014, 74000, "Petrol")
    assert peugeot.price_local == 59995


def test_ev_and_plugin_have_no_gearbox(cars):
    kona = next(c for c in cars.values() if c.model == "Kona")
    kuga = next(c for c in cars.values() if c.model == "Kuga")
    assert (kona.fuel, kona.gearbox) == ("Electric", None)
    assert (kuga.fuel, kuga.gearbox) == ("Plug-in hybrid", None)


def test_multi_word_make_and_missing_variant(cars):
    rover = next(c for c in cars.values() if c.make == "Land Rover")
    assert rover.model == "Range Rover"
    assert rover.gearbox == "automatic"
    rio = next(c for c in cars.values() if c.model == "Rio")
    assert rio.variant is None


def test_cards_without_photo(cars):
    no_photo = sorted(c.title for c in cars.values() if c.thumbnail_url is None)
    assert no_photo == ["Audi A3", "Citroen Jumper"]


@pytest.mark.parametrize("text,expected", [("65.000", 65000), ("1.234.567", 1234567), ("2.500 kr.", 2500), ("", None)])
def test_parse_danish_int(text, expected):
    assert parse_danish_int(text) == expected


def test_parse_spec_line_handles_cc_and_range():
    assert parse_spec_line("2016 ∙ 136.000 km ∙ Benzin ∙ Manuelt") == {
        "year": 2016, "mileage_km": 136000, "fuel": "Petrol", "gearbox": "manual"}
    assert parse_spec_line("2020 ∙ 164.000 km ∙ El ∙ 449 km rækkevide") == {
        "year": 2020, "mileage_km": 164000, "fuel": "Electric", "gearbox": None}
    assert parse_spec_line("2011 ∙ 233.000 km ∙ 1400 cc ∙ Diesel ∙ Automatisk")["fuel"] == "Diesel"


@pytest.mark.parametrize("text,delta", [
    ("8 min.", timedelta(minutes=8)), ("5 t.", timedelta(hours=5)), ("1 dag", timedelta(days=1)),
    ("4 dage", timedelta(days=4)), ("2 uger", timedelta(weeks=2)),
])
def test_parse_listing_age(text, delta):
    assert parse_listing_age(text, NOW) == NOW - delta


def test_parse_listing_age_unknown_text():
    assert parse_listing_age("i går", NOW) is None


def test_split_make_model():
    assert split_make_model("Toyota Aygo") == ("Toyota", "Aygo")
    assert split_make_model("Alfa Romeo GTV") == ("Alfa Romeo", "GTV")
    assert split_make_model("Tesla") == ("Tesla", None)


def _resp(status, text=""):
    return MagicMock(status_code=status, text=text)


def _cars(*ids):
    return [Car(source_id=f"dba_{i}", source_name="dba", city="Copenhagen",
                url=f"https://www.dba.dk/mobility/item/{i}") for i in ids]


@patch("apartment_finder.cars.dba.DbaCarAdapter._segment_total", side_effect=[3, 2])
@patch("apartment_finder.cars.dba.FULL_PAGE_SIZE", 2)
@patch("apartment_finder.cars.dba.parse_search_page")
@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_fetch_walks_both_segments_to_the_end(mock_get, _sleep, parse, _total):
    mock_get.return_value = _resp(200, "<html>")
    parse.side_effect = [
        _cars(1, 2), _cars(3),        # dealers: full page, then a short last page
        _cars(4, 5), _cars(4, 5),     # private: page 2 repeats page 1 (DBA wrapped)
    ]
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen"})

    result = adapter.fetch_listings()

    assert sorted(c.source_id for c in result) == ["dba_1", "dba_2", "dba_3", "dba_4", "dba_5"]
    assert adapter.full_catalog is True
    params = [call.kwargs["params"] for call in mock_get.call_args_list]
    assert params[0] == {"location": "0.200001", "dealer_segment": "2", "page": 1}
    assert params[2] == {"location": "0.200001", "dealer_segment": "3", "page": 1}
    assert all("sort" not in p for p in params)  # robots.txt disallows sort=


@patch("apartment_finder.cars.dba.FULL_PAGE_SIZE", 2)
@patch("apartment_finder.cars.dba.parse_search_page")
@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_fetch_stops_on_block_and_is_not_full(mock_get, _sleep, parse):
    mock_get.side_effect = [_resp(200, "<html>"), _resp(202, "")]
    parse.return_value = _cars(1, 2)
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen"})

    result = adapter.fetch_listings()

    assert len(result) == 2
    assert mock_get.call_count == 2  # private segment never requested
    assert adapter.page_errors == ["dealer page 2: HTTP 202"]
    assert adapter.full_catalog is False


@patch("apartment_finder.cars.dba.DbaCarAdapter._segment_total", return_value=100)
@patch("apartment_finder.cars.dba.FULL_PAGE_SIZE", 2)
@patch("apartment_finder.cars.dba.parse_search_page")
@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_page_cap_means_not_full_catalog(mock_get, _sleep, parse, _total):
    mock_get.return_value = _resp(200, "<html>")
    parse.side_effect = [_cars(1, 2), _cars(3, 4), _cars(5, 6), _cars(7, 8)]
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen", "dba": {"max_pages": 2}})

    adapter.fetch_listings()

    assert adapter.page_errors == []
    assert adapter.full_catalog is False  # both segments stopped at the cap


@patch("apartment_finder.cars.dba.DbaCarAdapter._segment_total", return_value=10)
@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_real_fixture_page_parses_through_fetch(mock_get, _sleep, _total):
    mock_get.return_value = _resp(200, FIXTURE.read_text())
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen"})
    assert len(adapter.fetch_listings()) == 10  # 10-card page = short last page


@patch("apartment_finder.cars.dba.DbaCarAdapter._segment_total", side_effect=[50, 2])
@patch("apartment_finder.cars.dba.FULL_PAGE_SIZE", 2)
@patch("apartment_finder.cars.dba.parse_search_page")
@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_early_end_below_dba_total_is_not_full(mock_get, _sleep, parse, _total):
    # Page 2 is an error/consent page that parses to nothing, or a full page
    # where one card failed to parse: looks like the end, but DBA says 50 cars
    mock_get.return_value = _resp(200, "<html>")
    parse.side_effect = [_cars(1, 2), [], _cars(3), ]
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen"})

    adapter.fetch_listings()

    assert adapter.page_errors == []
    assert adapter.full_catalog is False


@patch("apartment_finder.cars.dba.DbaCarAdapter._segment_total", return_value=None)
@patch("apartment_finder.cars.dba.FULL_PAGE_SIZE", 2)
@patch("apartment_finder.cars.dba.parse_search_page")
@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_unknown_total_is_not_full(mock_get, _sleep, parse, _total):
    mock_get.return_value = _resp(200, "<html>")
    parse.side_effect = [_cars(1), _cars(2)]
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen"})
    adapter.fetch_listings()
    assert adapter.full_catalog is False


@patch("apartment_finder.cars.dba.requests.get")
def test_segment_total_reads_match_count(mock_get):
    mock_get.return_value = MagicMock(status_code=200)
    mock_get.return_value.raise_for_status.return_value = None
    mock_get.return_value.json.return_value = {"metadata": {"result_size": {"match_count": 1300}}}
    assert DbaCarAdapter({}, {})._segment_total("3") == 1300
    mock_get.return_value.json.return_value = {"metadata": {}}
    assert DbaCarAdapter({}, {})._segment_total("3") is None


def test_max_pages_is_capped_at_dba_limit():
    adapter = DbaCarAdapter({}, {"dba": {"max_pages": 500}})
    assert adapter.max_pages == 50


def _ctx_response(status_code):
    resp = MagicMock(status_code=status_code)
    resp.__enter__.return_value = resp
    return resp


@patch("apartment_finder.adapters.base.time.sleep")
@patch("apartment_finder.adapters.base.requests.get")
def test_check_status_404_gone_200_unknown(mock_get, _sleep):
    codes = {"live": 200, "removed": 404, "blocked": 403}
    mock_get.side_effect = lambda url, **kw: _ctx_response(codes[url])

    assert DbaCarAdapter({}, {}).check_status(list(codes)) == {
        "live": "unknown", "removed": "gone", "blocked": "unknown"}
