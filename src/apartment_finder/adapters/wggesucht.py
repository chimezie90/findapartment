"""WG-Gesucht.de adapter for whole-flat listings in Berlin.

WG-Gesucht is best known for shared-flat rooms, but its "Wohnungen"
category (category 2) lists whole flats. Many are fixed-term sublets, so
any ad with an end date gets "temporary" in its description, which the
scoring service treats as a red flag. Search pages are server-rendered,
~28 cards per page; robots.txt allows them (it blocks /api/ and forms).
"""

import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from ..models.apartment import Amenities, Apartment
from ..utils.numbers import SQM_TO_SQFT, parse_german_number
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)

BASE_URL = "https://www.wg-gesucht.de"
PAGE_DELAY_SECONDS = 2.0
# A real ad link: /wohnungen-in-Berlin-Mitte.11962680.html. Promoted cards
# link to the search page (?asset_id=...) and must not win dedup.
AD_HREF = re.compile(r"/wohnungen-in-[^/?]+\.\d+\.html")
DATE_RANGE = re.compile(r"(\d{2}\.\d{2}\.\d{4})(?:\s*-\s*(\d{2}\.\d{2}\.\d{4}))?")


@register_adapter("wggesucht")
class WGGesuchtAdapter(BaseAdapter):
    """
    Config (city_config["wggesucht"]):
        city_slug: e.g. "Berlin"
        city_id: WG-Gesucht city id, e.g. 8 (Berlin)
        max_pages: Result pages per run
    """

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        settings = city_config.get("wggesucht", {})
        self.city_slug = settings.get("city_slug", "Berlin")
        self.city_id = int(settings.get("city_id", 8))
        self.max_pages = int(settings.get("max_pages", 15))
        self.city_name = city_config.get("display_name", "Berlin")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        }

    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        apartments = []
        seen_ids = set()

        for page in range(self.max_pages):
            if page:
                time.sleep(PAGE_DELAY_SECONDS)
            # wohnungen-in-Berlin.8.2.1.{page}.html: city 8, category 2 (flats), rent type 1, page (0-based)
            url = f"{BASE_URL}/wohnungen-in-{self.city_slug}.{self.city_id}.2.1.{page}.html"
            try:
                response = requests.get(url, headers=self._headers, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"WG-Gesucht page {page} failed: {e}")
                break

            cards = BeautifulSoup(response.text, "html.parser").select(".wgg-list-card.offer_list_item[data-id]")
            if page == 0 and not cards:
                logger.warning("WG-Gesucht: no listing cards on page 1 -- markup changed or blocked?")
            new_on_page = 0
            for card in cards:
                ad_id = card.get("data-id")
                if ad_id in seen_ids:
                    continue
                apartment = self._parse_card(card)
                if not apartment:
                    continue
                seen_ids.add(ad_id)
                new_on_page += 1
                if criteria.min_price_local and apartment.price_local < criteria.min_price_local:
                    continue
                if criteria.max_price_local and apartment.price_local > criteria.max_price_local:
                    continue
                apartments.append(apartment)

            if not new_on_page:
                break

        logger.info(f"Fetched {len(apartments)} listings from WG-Gesucht ({len(seen_ids)} scanned)")
        return apartments

    def _parse_card(self, card) -> Optional[Apartment]:
        try:
            href = card.get("href") or ""
            # Promoted cards can point at other cities, categories, or the search page
            if not href.startswith(f"/wohnungen-in-{self.city_slug}") or not AD_HREF.fullmatch(href):
                return None

            price_el = card.select_one(".price-row .card-main-text")
            price = parse_german_number(price_el.get_text()) if price_el else None
            title_el = card.select_one(".ad-title")
            title = title_el.get_text(" ", strip=True) if title_el else card.get("aria-label")
            if not price or not title:
                return None

            # "2-Zimmer-Wohnung · Berlin Mitte · Bergstr 18"
            subtitle_el = card.select_one(".ad-subtitle")
            subtitle = re.sub(r"\s+", " ", subtitle_el.get_text(" ", strip=True)) if subtitle_el else ""
            parts = [p.strip() for p in subtitle.split("·")]
            rooms_match = re.match(r"(\d+)-Zimmer", parts[0]) if parts else None
            district = parts[1].replace(self.city_slug, "", 1).strip() if len(parts) > 1 else None
            street = parts[2] if len(parts) > 2 else None

            size_el = card.select_one(".size-value")
            sqm = parse_german_number(size_el.get_text()) if size_el else None

            description = None
            middle = card.select_one(".middle-row")
            dates = DATE_RANGE.search(middle.get_text(" ", strip=True)) if middle else None
            if dates and dates.group(2):
                description = f"Temporary sublet: {dates.group(1)} to {dates.group(2)}"
            elif dates:
                description = f"Available from {dates.group(1)}"

            image = card.select_one("img.top-image-card")
            return Apartment(
                source_id=f"wggesucht_{card.get('data-id')}",
                source_name="wggesucht",
                title=title,
                url=f"{BASE_URL}{href}",
                price_local=price,
                currency="EUR",
                price_usd=None,
                bedrooms=int(rooms_match.group(1)) if rooms_match else None,  # total "Zimmer"
                bathrooms=None,
                sqft=int(sqm * SQM_TO_SQFT) if sqm else None,
                address=f"{street}, {district}" if street and district else None,
                neighborhood=district or None,
                city=self.city_name,
                country="Germany",
                amenities=Amenities(),
                description=description,
                images=[],
                thumbnail_url=image.get("src") if image else None,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Failed to parse WG-Gesucht card: {e}")
            return None

    def _normalize(self, raw_listing: Dict[str, Any]) -> Optional[Apartment]:
        """Not used; cards are parsed directly in _parse_card."""
        return None
