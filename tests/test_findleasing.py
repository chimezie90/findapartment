"""findleasing.nu adapter: parsing against a saved API page, paging, liveness (no network)."""

import copy
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from apartment_finder.cars.findleasing import FindleasingCarAdapter, parse_listing

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "findleasing_listings.json").read_text())
ITEMS = FIXTURE["results"]


def _adapter(max_dealer_zip=4999):
    return FindleasingCarAdapter({}, {"display_name": "Copenhagen",
                                      "findleasing": {"max_dealer_zip": max_dealer_zip}})


def _resp(status=200, json_data=None):
    resp = MagicMock(status_code=status)
    resp.json.return_value = json_data
    resp.raise_for_status.side_effect = (
        requests.HTTPError(f"HTTP {status}") if status >= 400 else None
    )
    return resp


def test_parses_lease_fields():
    car = parse_listing(ITEMS[0], "Copenhagen", 4999)
    assert car.source_id == "findleasing_243545"
    assert car.listing_type == "lease"
    assert car.price_local is None
    assert car.monthly_price_local == 2737
    assert car.down_payment_local == 50000
    assert car.term_months == 12
    assert car.lease_kind == "financial"
    assert (car.make, car.year, car.mileage_km, car.fuel) == ("Audi", 2024, 110000, "Hybrid")
    assert car.gearbox == "automatic"
    assert car.location == "Albertslund"
    assert car.seller_type == "dealer"
    assert car.url.endswith("/243545/")
    # +02:00 converted to naive UTC
    assert car.listed_at == datetime(2026, 10, 2, 15, 17, 46)


def test_operational_manual_and_km_per_year():
    car = parse_listing(ITEMS[1], "Copenhagen", 4999)
    assert car.lease_kind == "operational"
    assert car.gearbox == "manual"  # API says "Manuel", DBA says "Manuelt"
    assert car.km_per_year == 50000


def test_filters_out_of_area_inactive_and_business_only():
    far = ITEMS[4]  # dealer in Randers (8940)
    assert parse_listing(far, "Copenhagen", 4999) is None
    assert parse_listing(far, "Copenhagen", 9999) is not None

    inactive = copy.deepcopy(ITEMS[0]); inactive["active"] = False
    deleted = copy.deepcopy(ITEMS[0]); deleted["deleted"] = True
    business = copy.deepcopy(ITEMS[0]); business["ownership"] = 1
    no_zip = copy.deepcopy(ITEMS[0]); no_zip["dealer"]["address"]["zip_code"] = ""
    for item in (inactive, deleted, business, no_zip):
        assert parse_listing(item, "Copenhagen", 4999) is None


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_fetch_pages_until_next_is_empty(mock_get, _sleep):
    page1 = {"next": "https://backend.findleasing.nu/api/v2/listings/?page=2", "results": ITEMS[:2]}
    page2 = {"next": None, "results": ITEMS[2:]}
    mock_get.side_effect = [_resp(json_data=page1), _resp(json_data=page2)]
    adapter = _adapter()

    cars = adapter.fetch_listings()

    assert len(cars) == 4  # Randers dealer filtered out
    assert adapter.page_errors == []
    assert mock_get.call_args_list[0].kwargs["params"] == {"ownership": 0, "page_size": 100, "page": 1}


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_fetch_stops_and_reports_on_error(mock_get, _sleep):
    page1 = {"next": "x", "results": ITEMS[:2]}
    mock_get.side_effect = [_resp(json_data=page1), _resp(status=429)]
    adapter = _adapter()

    cars = adapter.fetch_listings()

    assert len(cars) == 2
    assert len(adapter.page_errors) == 1 and "page 2" in adapter.page_errors[0]


@patch("apartment_finder.cars.findleasing.MAX_PAGES", 2)
@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_fetch_flags_page_cap(mock_get, _sleep):
    mock_get.return_value = _resp(json_data={"next": "more", "results": []})
    adapter = _adapter()
    adapter.fetch_listings()
    assert "page cap" in adapter.page_errors[0]


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_check_status_uses_detail_api(mock_get, _sleep):
    base = "https://www.findleasing.nu/x-leasing/x/"
    live_item = copy.deepcopy(ITEMS[0])
    business_only = copy.deepcopy(ITEMS[0]); business_only["ownership"] = 1
    responses = {
        "1": _resp(json_data=live_item),
        "2": _resp(status=404),
        "3": _resp(json_data={"active": False, "deleted": False}),
        "4": _resp(json_data={"active": True, "deleted": True}),
        "5": _resp(status=503),
        "6": _resp(json_data={"detail": "maintenance"}),  # 200 without 'active'
        "7": _resp(json_data=business_only),  # live, but no longer a private deal
    }
    mock_get.side_effect = lambda url, **kw: responses[url.rstrip("/").rsplit("/", 1)[-1]]
    urls = [f"{base}{n}/" for n in "1234567"] + ["https://www.findleasing.nu/no-id-here"]

    result = _adapter().check_status(urls)

    assert result == {
        f"{base}1/": "active", f"{base}2/": "gone", f"{base}3/": "gone",
        f"{base}4/": "gone", f"{base}5/": "unknown",
        f"{base}6/": "unknown", f"{base}7/": "gone",
        "https://www.findleasing.nu/no-id-here": "unknown",
    }
    assert mock_get.call_args_list[0].args[0] == "https://backend.findleasing.nu/api/v2/listings/1/"


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_check_status_stops_on_429(mock_get, _sleep):
    mock_get.return_value = _resp(status=429)
    urls = ["https://www.findleasing.nu/a/1/", "https://www.findleasing.nu/a/2/"]
    assert _adapter().check_status(urls) == {}
    assert mock_get.call_count == 1


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_fetch_rejects_unexpected_shape(mock_get, _sleep):
    mock_get.return_value = _resp(json_data={"items": []})
    adapter = _adapter()
    assert adapter.fetch_listings() == []
    assert "unexpected response shape" in adapter.page_errors[0]


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_fetch_flags_paging_that_ends_early(mock_get, _sleep):
    # 'next' missing on page 1 although the API says 500 deals exist
    mock_get.return_value = _resp(json_data={"count": 500, "results": ITEMS})
    adapter = _adapter()
    adapter.fetch_listings()
    assert "paging ended early" in adapter.page_errors[0]


@patch("apartment_finder.cars.findleasing.time.sleep")
@patch("apartment_finder.cars.findleasing.requests.get")
def test_fetch_retries_once_on_server_error(mock_get, _sleep):
    mock_get.side_effect = [_resp(status=502), _resp(json_data={"next": None, "results": ITEMS[:1]})]
    adapter = _adapter()
    assert len(adapter.fetch_listings()) == 1
    assert adapter.page_errors == []
