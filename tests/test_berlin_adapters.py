"""Berlin apartment adapter tests (no network, no database).

Fixtures are trimmed copies of real pages fetched 2026-10-25.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from apartment_finder.adapters.base import SearchCriteria
from apartment_finder.adapters.inberlinwohnen import InBerlinWohnenAdapter
from apartment_finder.adapters.kleinanzeigen import SKIP_TITLE, KleinanzeigenAdapter
from apartment_finder.adapters.wggesucht import WGGesuchtAdapter
from apartment_finder.utils.numbers import parse_german_number

FIXTURES = Path(__file__).parent / "fixtures"
CITY = {"display_name": "Berlin"}
NO_BUDGET = SearchCriteria(0, 0, 0, 0, 0, [])


def fetch(adapter_cls, fixture, criteria=NO_BUDGET):
    """Run fetch_listings with every page request serving the fixture. The
    second page repeats the first, which must end paging."""
    html = (FIXTURES / fixture).read_text()
    response = MagicMock(text=html)
    with patch("requests.get", return_value=response) as get, patch("time.sleep"):
        listings = adapter_cls({}, CITY).fetch_listings(criteria)
    return listings, get.call_count


@pytest.mark.parametrize("text, expected", [
    ("1.047,25", 1047.25),
    ("4,0", 4.0),
    ("2.685 €", 2685.0),
    ("1920 €", 1920.0),
    (867.25, 867.25),
    ("VB", None),
    (None, None),
])
def test_parse_german_number(text, expected):
    assert parse_german_number(text) == expected


class TestInBerlinWohnen:
    @pytest.fixture(scope="class")
    def result(self):
        return fetch(InBerlinWohnenAdapter, "inberlinwohnen_page1.html")

    def test_parses_every_card_and_stops_on_repeat_page(self, result):
        listings, calls = result
        assert len(listings) == 10
        assert calls == 2
        assert all(a.currency == "EUR" and a.city == "Berlin" and a.latitude for a in listings)

    def test_maps_warm_rent_and_breakdown(self, result):
        flat = next(a for a in result[0] if a.source_id == "inberlinwohnen_23360")
        assert flat.price_local == 1047.25  # Gesamtmiete, incl. heating
        assert flat.bedrooms == 4
        assert flat.sqft == 890  # 82.76 m²
        assert flat.neighborhood == "Reinickendorf"
        assert flat.url.startswith("https://www.gewobag.de/")
        assert "heating €180.00" in flat.description
        assert "WBS (social-housing permit): nicht erforderlich" in flat.description

    def test_budget_filter(self):
        listings, _ = fetch(InBerlinWohnenAdapter, "inberlinwohnen_page1.html", SearchCriteria(900, 1100, 0, 0, 0, []))
        assert sorted(a.price_local for a in listings) == [951.56, 955.47, 1001.64, 1047.25]


class TestKleinanzeigen:
    @pytest.fixture(scope="class")
    def listings(self):
        return {a.source_id: a for a in fetch(KleinanzeigenAdapter, "kleinanzeigen_berlin_page1.html")[0]}

    def test_parses_cards_and_drops_swaps_and_rooms(self, listings):
        assert len(listings) == 22
        assert "kleinanzeigen_3551668485" not in listings  # Privatzimmer
        assert "kleinanzeigen_3551585406" not in listings  # Tauschwohnung

    def test_card_fields(self, listings):
        flat = listings["kleinanzeigen_3549159472"]
        assert flat.price_local == 2685.0
        assert flat.bedrooms == 5
        assert flat.sqft == 1560  # 145 m²
        assert (flat.address, flat.neighborhood) == ("14169 Dahlem", "Dahlem")
        assert flat.url.startswith("https://www.kleinanzeigen.de/s-anzeige/")

    @pytest.mark.parametrize("title, skipped", [
        ("Wohnungsswap - 4 Zimmer, 109 m²", True),
        ("Tausche ruhige 4-Raum-Wohnung", True),
        ("1000€ Belohnung: suche Wohnung in Berlin", True),
        ("Suche Nachmieter für 2-Zi-Wohnung", False),
        ("Nachmieter gesucht, 90 m²", False),
        ("2 Zi, Bad ausgetauscht", False),
    ])
    def test_skip_titles(self, title, skipped):
        assert bool(SKIP_TITLE.search(title)) is skipped

    def test_page_url_applies_cold_rent_floor(self):
        adapter = KleinanzeigenAdapter({}, CITY)
        assert adapter._page_url(3, SearchCriteria(1200, 3500, 0, 0, 0, [])) == (
            "https://www.kleinanzeigen.de/s-wohnung-mieten/berlin/preis:900:3500/seite:3/c203l3331"
        )

    @pytest.mark.parametrize("status, location, expected", [
        (301, "https://www.kleinanzeigen.de/s-wohnung-mieten/friedrichshain/c203l3350", "gone"),
        (301, "https://www.kleinanzeigen.de/s-anzeige/new-slug/123-203-3353", "unknown"),
        (302, "https://www.kleinanzeigen.de/consent", "unknown"),
        (404, "", "gone"),
        (200, "", "unknown"),  # a 200 isn't proof of life
        (403, "", "unknown"),
    ])
    def test_check_status(self, status, location, expected):
        response = MagicMock(status_code=status, headers={"Location": location})
        response.__enter__.return_value = response
        with patch("requests.get", return_value=response), patch("time.sleep"):
            result = KleinanzeigenAdapter({}, CITY).check_status(["https://www.kleinanzeigen.de/s-anzeige/x/1-203-1"])
        assert result == {"https://www.kleinanzeigen.de/s-anzeige/x/1-203-1": expected}


class TestWGGesucht:
    @pytest.fixture(scope="class")
    def listings(self):
        return {a.source_id: a for a in fetch(WGGesuchtAdapter, "wggesucht_berlin_page0.html")[0]}

    def test_skips_promoted_cards_linking_to_search_page(self, listings):
        assert len(listings) == 26
        assert "wggesucht_14244449" not in listings
        assert all(".html?" not in a.url for a in listings.values())

    def test_card_fields_and_sublet_flag(self, listings):
        flat = listings["wggesucht_11962680"]
        assert flat.price_local == 1950.0
        assert flat.bedrooms == 2
        assert flat.sqft == 990  # 92 m²
        assert (flat.neighborhood, flat.address) == ("Mitte", "Bergstr 18, Mitte")
        assert flat.description == "Temporary sublet: 01.12.2026 to 30.06.2027"
        assert listings["wggesucht_10724830"].description == "Available from 01.12.2026"
