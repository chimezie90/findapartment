"""home.dk adapter: NUXT payload resolver, catalog/detail parsing against saved
pages, CSS fallback, paging and politeness (no network)."""

import copy
import json
import math
import re
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from apartment_finder.homes.homedk import (
    HomeDkAdapter, parse_catalog_cards, parse_catalog_page, parse_detail_page, parse_result,
)
from apartment_finder.homes.nuxt import NuxtDataError, extract_payload, parse_nuxt_data, resolve

FIXTURES = Path(__file__).parent / "fixtures"
CATALOG_HTML = (FIXTURES / "homedk_catalog_villa_hvidovre.html").read_text()
DETAIL_HTML = (FIXTURES / "homedk_detail_1110006588.html").read_text()
RESULTS = parse_nuxt_data(CATALOG_HTML)["data"]["case-catalog-search-p-1"]["results"]


# --- NUXT payload resolver -------------------------------------------------

def test_resolver_handles_indices_wrappers_and_special_forms():
    payload = [
        ["ShallowReactive", 1],                      # 0: root, wrapped
        {"data": 2, "list": 5, "tags": 8, "when": 9, "nums": 10, "empty": 11, "same": 3},
        ["Reactive", 3],                             # 2
        {"name": 4, "price": 12},                    # 3: shared object
        "Malttorvet",                                # 4
        [4, 4, -1],                                  # 5: indices + undefined
        "unused",                                    # 6
        "x",                                         # 7
        ["Set", 4, 7],                               # 8
        ["Date", "2026-10-03T06:28:05Z"],            # 9: literal, not an index
        [-3, -4, -6],                                # 10: NaN, inf, -0
        ["EmptyRef", 13],                            # 11
        4795000,                                     # 12
        "null",                                      # 13: EmptyRef payload is JSON text
    ]
    root = resolve(payload)
    assert root["data"] == {"name": "Malttorvet", "price": 4795000}
    assert root["same"] is root["data"]  # shared values are stored once
    assert root["list"] == ["Malttorvet", "Malttorvet", None]
    assert root["tags"] == ["Malttorvet", "x"]
    assert root["when"] == "2026-10-03T06:28:05Z"
    assert math.isnan(root["nums"][0]) and root["nums"][1] == math.inf
    assert root["empty"] is None


def test_resolver_survives_cycles_and_rejects_garbage():
    cyclic = resolve([{"self": 0}])
    assert cyclic["self"] is cyclic
    with pytest.raises(NuxtDataError):
        resolve([{"a": 99}])  # index out of range
    with pytest.raises(NuxtDataError):
        extract_payload("<html><body>no payload</body></html>")
    with pytest.raises(NuxtDataError):
        extract_payload('<script id="__NUXT_DATA__" type="application/json">{oops</script>')


def test_fixture_payload_resolves_to_the_search_results():
    search = parse_nuxt_data(CATALOG_HTML)["data"]["case-catalog-search-p-1"]
    assert search["total"] == 86 and search["hasNextPage"] is True
    assert len(search["results"]) == 12


# --- Catalog parsing ---------------------------------------------------------

def test_parses_own_and_external_listings_from_saved_page():
    page = parse_catalog_page(CATALOG_HTML, "villa", "Hvidovre")
    assert (page["results"], page["total"], page["has_next"], page["fallback"]) == (12, 86, True, False)
    homes = page["homes"]
    assert len(homes) == 12

    own = homes[0]
    assert own.source_id == "homedk_1040001956"
    assert own.url == "https://home.dk/salg/huse-villaer/ketilstorp-alle-56-2650-hvidovre/sag-1040001956/"
    assert own.address == "Ketilstorp Alle 56, 2650 Hvidovre"
    assert (own.street, own.postcode, own.city, own.municipality) == ("Ketilstorp Alle 56", "2650", "Hvidovre", "Hvidovre")
    assert (own.price_dkk, own.sqm, own.property_type) == (7495000, 147, "villa")
    assert own.latitude == pytest.approx(55.63265, abs=1e-4) and own.longitude == pytest.approx(12.47352, abs=1e-4)
    assert own.is_external is False and own.broker == "home"
    assert own.thumbnail_url.startswith("https://home.mindworking.eu/") and own.thumbnail_url.endswith("?w=240")
    assert own.rooms is None and own.year_built is None  # catalog doesn't have them

    external = [h for h in homes if h.is_external]
    assert len(external) == 4
    ext = next(h for h in external if h.address.startswith("Borrisvej 31"))
    assert ext.source_id == "homedk_x_bf73e52b-0f72-431f-be4e-890759f0d77f"
    assert ext.url.startswith("https://www.boligsiden.dk/viderestillingekstern/")
    assert ext.broker is None and ext.headline is None
    assert ext.thumbnail_url.startswith("https://images.boligsiden.dk/")


def test_excludes_rentals_plots_unpriced_holiday_and_cooperative_homes():
    base = RESULTS[0]
    assert parse_result(base, "villa", "Hvidovre") is not None
    variants = []
    for path, value in ((("isRentalCase",), True), (("isBusinessCase",), True), (("isPlot",), True),
                        (("type",), "CooperativeHousing"), (("type",), "HolidayHome"),
                        (("offer", "price", "amount"), 0), (("offer", "price"), None),
                        (("url",), "javascript:alert(1)"), (("address", "full"), "")):
        item = copy.deepcopy(base)
        target = item
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        variants.append(item)
    for item in variants:
        assert parse_result(item, "villa", "Hvidovre") is None


def test_css_fallback_matches_payload_ids_and_core_fields():
    cards = parse_catalog_cards(CATALOG_HTML, "villa", "Hvidovre")
    payload = {h.source_id: h for h in parse_catalog_page(CATALOG_HTML, "villa", "Hvidovre")["homes"]}
    assert len(cards) == 12
    for home in cards:
        match = payload[home.source_id]  # same ids in both modes: no duplicates
        assert (home.price_dkk, home.sqm, home.postcode) == (match.price_dkk, match.sqm, match.postcode)
        assert home.is_external == match.is_external


def test_page_without_payload_falls_back_to_cards_and_flags_it():
    no_payload = re.sub(r'<script[^>]*id="__NUXT_DATA__".*?</script>', "", CATALOG_HTML, flags=re.S)
    page = parse_catalog_page(no_payload, "villa", "Hvidovre")
    assert page["fallback"] is True and len(page["homes"]) == 12 and page["total"] is None
    with pytest.raises(NuxtDataError):
        parse_catalog_page("<html><body>Access denied</body></html>", "villa", "Hvidovre")


# --- Detail page -------------------------------------------------------------

def test_parses_detail_page_fields():
    details = parse_detail_page(DETAIL_HTML, "1110006588")
    assert details == {
        "rooms": 2,
        "year_built": 2021,
        "monthly_owner_expenses_dkk": 2411,
        "energy_label": "A2015",
        "listed_at": datetime(2026, 10, 3, 6, 28, 5, 499731),
        "under_offer": False,
        "off_market": False,
    }


def test_detail_page_sale_states():
    from apartment_finder.homes import homedk

    def states(**flags):
        with patch.object(homedk, "parse_nuxt_data", return_value={"data": {"case-1": flags}}):
            details = parse_detail_page("", "1")
        return details["under_offer"], details["off_market"]

    assert states(isForSale=True, isUnderSale=False, isSold=False) == (False, False)
    # "Solgt med forbehold": still in the catalog, so not gone
    assert states(isForSale=False, isUnderSale=True, isSold=False) == (True, False)
    assert states(isForSale=False, isUnderSale=False, isSold=True) == (False, True)
    assert states(isForSale=False, isUnderSale=False, isSold=False) == (False, True)
    with pytest.raises(NuxtDataError):
        parse_detail_page(DETAIL_HTML, "999")


# --- Adapter: paging, politeness ---------------------------------------------

def _resp(status=200, text=""):
    resp = MagicMock(status_code=status, text=text)
    resp.raise_for_status.side_effect = (
        requests.HTTPError(f"HTTP {status}", response=resp) if status >= 400 else None
    )
    return resp


def _page_html(results, total, has_next):
    """A minimal catalog page in the real payload format (values are indices)."""
    payload = [["ShallowReactive", 1], {"data": 2}, {"case-catalog-search-p-1": 3},
               {"total": 4, "hasNextPage": 5, "results": 6}, total, has_next]
    payload.append([])
    for item in results:
        payload[6].append(_encode(item, payload))
    return f'<html><script type="application/json" id="__NUXT_DATA__">{json.dumps(payload)}</script></html>'


def _encode(value, payload):
    payload.append(None)
    idx = len(payload) - 1
    if isinstance(value, dict):
        payload[idx] = {k: _encode(v, payload) for k, v in value.items()}
    elif isinstance(value, list):
        payload[idx] = [_encode(v, payload) for v in value]
    else:
        payload[idx] = value
    return idx


def _adapter(**overrides):
    config = {"municipalities": ["hvidovre-kommune"], "categories": ["villa"], **overrides}
    return HomeDkAdapter({}, {"homedk": config})


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_fetch_walks_pages_until_has_next_is_false(mock_get, _sleep):
    mock_get.side_effect = [
        _resp(text=_page_html(RESULTS[:12], 14, True)),
        _resp(text=_page_html(RESULTS[:2] + [dict(RESULTS[2], id="dup")], 14, False)),
    ]
    adapter = _adapter()
    homes = adapter.fetch_listings()

    assert len(homes) == 12  # duplicates across pages collapse by id
    assert adapter.page_errors == []
    assert adapter.request_count == 2
    first, second = (call.args[0] for call in mock_get.call_args_list)
    assert first == second == "https://home.dk/til-salg/villa/region-hovedstaden/hvidovre-kommune/"
    assert mock_get.call_args_list[1].kwargs["params"] == {"page": 2}
    assert "Chrome" in mock_get.call_args_list[0].kwargs["headers"]["User-Agent"]


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_short_paging_and_page_cap_are_page_errors(mock_get, _sleep):
    mock_get.return_value = _resp(text=_page_html(RESULTS[:12], 86, False))
    adapter = _adapter()
    adapter.fetch_listings()
    assert any("saw 12 of 86" in e for e in adapter.page_errors)

    mock_get.reset_mock()
    mock_get.return_value = _resp(text=_page_html(RESULTS[:12], 86, True))
    adapter = _adapter(max_pages=3)
    adapter.fetch_listings()
    assert mock_get.call_count == 3
    assert any("cap" in e for e in adapter.page_errors)


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_retries_once_on_5xx_and_stops_everything_on_429(mock_get, _sleep):
    mock_get.side_effect = [_resp(503), _resp(text=_page_html(RESULTS[:3], 3, False))]
    adapter = _adapter()
    assert len(adapter.fetch_listings()) == 3
    assert adapter.page_errors == [] and adapter.request_count == 2

    mock_get.reset_mock()
    mock_get.side_effect = [_resp(429)]
    adapter = _adapter(municipalities=["hvidovre-kommune", "ballerup-kommune"])
    assert adapter.fetch_listings() == []
    assert mock_get.call_count == 1  # second municipality never requested
    assert any("429" in e for e in adapter.page_errors)
    assert adapter.fetch_details([{"source_id": "homedk_1", "url": "https://home.dk/salg/x/sag-1/"}]) == {}
    assert mock_get.call_count == 1


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_waits_between_requests(mock_get, mock_sleep):
    mock_get.side_effect = [_resp(text=_page_html(RESULTS[:12], 24, True)),
                            _resp(text=_page_html(RESULTS[:12], 24, False))]
    with patch("apartment_finder.homes.homedk.time.monotonic", return_value=100.0):
        _adapter().fetch_listings()
    assert mock_sleep.call_args_list[0].args[0] == pytest.approx(1.5)


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_details_never_fetch_external_links_and_respect_the_cap(mock_get, _sleep):
    mock_get.return_value = _resp(text=DETAIL_HTML)
    rows = [
        {"source_id": "homedk_x_abc", "url": "https://www.boligsiden.dk/viderestillingekstern/abc"},
        {"source_id": "homedk_1110006588",
         "url": "https://home.dk/salg/lejligheder/malttorvet-16-2mf-1799-koebenhavn-v/sag-1110006588/"},
        {"source_id": "homedk_1110006589", "url": "https://home.dk/salg/lejligheder/y/sag-1110006589/"},
    ]
    adapter = _adapter(max_detail_pages=2)
    details = adapter.fetch_details(rows)

    requested = [call.args[0] for call in mock_get.call_args_list]
    assert not any("boligsiden" in url for url in requested)
    assert requested == [rows[1]["url"]]  # cap of 2 rows; the external one is skipped
    assert details["homedk_1110006588"]["rooms"] == 2

    mock_get.return_value = _resp(404)
    gone = _adapter().fetch_details([rows[2]])
    assert gone == {"homedk_1110006589": {"gone": True}}


def test_rejects_bad_config_slugs_and_categories():
    with pytest.raises(ValueError):
        _adapter(municipalities=["../../etc"])
    with pytest.raises(ValueError):
        _adapter(categories=["andelsbolig"])


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_small_paging_slack_passes_but_three_failing_areas_stop_the_run(mock_get, _sleep):
    mock_get.return_value = _resp(text=_page_html(RESULTS[:12], 13, False))  # 1 listing moved mid-walk
    adapter = _adapter()
    adapter.fetch_listings()
    assert adapter.page_errors == []

    mock_get.reset_mock()
    mock_get.return_value = _resp(text="<html><body>maintenance</body></html>")
    slugs = ["hvidovre-kommune", "ballerup-kommune", "herlev-kommune", "glostrup-kommune", "dragoer-kommune"]
    adapter = _adapter(municipalities=slugs)
    adapter.fetch_listings()
    assert mock_get.call_count == 3
    assert any("3 failing areas in a row" in e for e in adapter.page_errors)


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_404_on_page_one_is_an_empty_area_unless_every_category_404s(mock_get, _sleep):
    # Real home.dk behaviour: no villa flats in Herlev -> 404, not an empty page
    mock_get.side_effect = [_resp(text=_page_html(RESULTS[:3], 3, False)), _resp(404)]
    adapter = _adapter(categories=["villa", "villalejlighed"])
    assert len(adapter.fetch_listings()) == 3
    assert adapter.page_errors == []

    mock_get.side_effect = [_resp(404), _resp(404)]
    adapter = _adapter(categories=["villa", "villalejlighed"], municipalities=["herlev-kommun"])
    adapter.fetch_listings()
    assert adapter.page_errors == ["herlev-kommun: not found in any category (wrong slug?)"]

    mock_get.side_effect = [_resp(text=_page_html(RESULTS[:12], 24, True)), _resp(404)]
    adapter = _adapter()
    adapter.fetch_listings()
    assert any("page 2" in e for e in adapter.page_errors)  # a 404 mid-walk is still an error


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_403_stops_the_run_and_off_site_redirects_are_not_followed(mock_get, _sleep):
    mock_get.return_value = _resp(403)
    adapter = _adapter(municipalities=["hvidovre-kommune", "ballerup-kommune"])
    adapter.fetch_listings()
    assert mock_get.call_count == 1 and adapter.rate_limited

    mock_get.reset_mock()
    redirect = _resp(302)
    redirect.url = "https://home.dk/salg/x/sag-1/"
    redirect.headers = {"Location": "https://www.boligsiden.dk/viderestillingekstern/abc"}
    mock_get.return_value = redirect
    details = _adapter().fetch_details([{"source_id": "homedk_1", "url": "https://home.dk/salg/x/sag-1/"}])
    assert mock_get.call_count == 1  # boligsiden never requested
    assert details == {}
    assert mock_get.call_args.kwargs["allow_redirects"] is False

    mock_get.reset_mock()
    internal = _resp(301)
    internal.url = "https://home.dk/salg/x/sag-1"
    internal.headers = {"Location": "/salg/x/sag-1/"}
    mock_get.side_effect = [internal, _resp(text=DETAIL_HTML.replace("case-1110006588", "case-1"))]
    details = _adapter().fetch_details([{"source_id": "homedk_1", "url": "https://home.dk/salg/x/sag-1"}])
    assert mock_get.call_args_list[1].args[0] == "https://home.dk/salg/x/sag-1/"
    assert details["homedk_1"]["rooms"] == 2


@patch("apartment_finder.homes.homedk.time.sleep")
@patch("apartment_finder.homes.homedk.requests.get")
def test_unparseable_detail_page_is_marked_failed_not_retried(mock_get, _sleep):
    mock_get.return_value = _resp(text="<html>no payload</html>")
    adapter = _adapter()
    details = adapter.fetch_details([{"source_id": "homedk_1", "url": "https://home.dk/salg/x/sag-1/"}])
    assert details == {"homedk_1": {"failed": True}} and adapter.detail_errors == 1

    mock_get.return_value = _resp(503)
    adapter = _adapter()
    assert adapter.fetch_details([{"source_id": "homedk_1", "url": "https://home.dk/salg/x/sag-1/"}]) == {}
    assert adapter.detail_errors == 1  # 5xx: retried next run, so not marked


def test_odd_numbers_and_timestamps_do_not_crash_parsing():
    from apartment_finder.homes.homedk import _naive_utc, _to_int

    assert _to_int(float("inf")) is None and _to_int(float("nan")) is None
    huge = copy.deepcopy(RESULTS[0])
    huge["stats"]["floorArea"] = 1557  # real home.dk data error (built-up area was 60 m2)
    assert parse_result(huge, "terraced", "Gentofte").sqm is None
    weird = copy.deepcopy(RESULTS[0])
    weird["stats"] = ["not", "a", "dict"]
    page = parse_catalog_page(_page_html([weird, RESULTS[1]], 2, False), "villa", "Hvidovre")
    assert [h.source_id for h in page["homes"]] == ["homedk_1040002000"]  # odd one skipped, page kept
    for text, micro in (("2026-10-03T06:28:05.4Z", 400000), ("2026-10-03T06:28:05.4997Z", 499700),
                        ("2026-10-03T06:28:05.4997314Z", 499731), ("2026-10-03T08:28:05+02:00", 0)):
        parsed = _naive_utc(text)
        assert parsed is not None and parsed.microsecond == micro and parsed.hour == 6
