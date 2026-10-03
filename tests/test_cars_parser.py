"""DBA car parser and adapter tests (no network, no database)."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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


@patch("apartment_finder.cars.dba.time.sleep")
@patch("apartment_finder.cars.dba.requests.get")
def test_fetch_stops_on_block_and_dedupes(mock_get, _sleep):
    page = FIXTURE.read_text()
    # Page 2 repeats page 1 (paid placements repeat), page 3 is a bot wall
    mock_get.side_effect = [_resp(200, page), _resp(200, page), _resp(202, "")]
    adapter = DbaCarAdapter({}, {"display_name": "Copenhagen", "dba": {"max_pages": 3}})

    result = adapter.fetch_listings()

    assert len(result) == 10
    assert mock_get.call_count == 3
    assert adapter.page_errors == ["page 3: HTTP 202"]
    params = mock_get.call_args_list[0].kwargs["params"]
    assert params == {"location": "0.200001", "sort": "PUBLISHED_DESC", "page": 1}


@patch("apartment_finder.cars.dba.requests.get")
def test_max_pages_is_capped_at_three(mock_get):
    adapter = DbaCarAdapter({}, {"dba": {"max_pages": 10}})
    assert adapter.max_pages == 3


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
