"""Kleinanzeigen.de adapter for Berlin apartment listings.

Kleinanzeigen (formerly eBay Kleinanzeigen) is Germany's largest classifieds
site. The "Mietwohnungen" category is server-rendered, 27 cards per page,
newest first, and paging stops at 50 pages. The price on a card is
whatever the advertiser entered -- usually cold rent, sometimes warm.
Private ads include rooms in shared flats and flat swaps; both are dropped.
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
from .base import LIVENESS_DELAY_SECONDS, LIVENESS_TIME_BUDGET_SECONDS, BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)

BASE_URL = "https://www.kleinanzeigen.de"
PAGE_DELAY_SECONDS = 2.0
SITE_MAX_PAGES = 50
# Cold rent runs ~20-25% under warm rent, so a warm budget floor of X still
# wants cold-rent ads from about 0.75X
COLD_RENT_FLOOR_FACTOR = 0.75
# Swaps, rooms in shared flats, and "looking for a flat" ads. "Suche
# Nachmieter" / "Nachmieter gesucht" -- a tenant handing over their lease --
# is a real rental, so "suche" only counts when not followed by Nachmieter.
SKIP_TITLE = re.compile(
    r"\b(?:wohnungs)?tausch|tauschwohnung|swap|wg-zimmer|wg zimmer|privatzimmer|zimmer in (?:einer )?wg"
    r"|\bsuche\b(?!\s+(?:einen?\s+)?nachmieter)",
    re.IGNORECASE,
)
# Removed ads 301 to a category search page, e.g. /s-wohnung-mieten/c203
REMOVED_AD_REDIRECT = re.compile(r"/s-[a-z-]+(?:/[^/]+)*/c\d+")


@register_adapter("kleinanzeigen")
class KleinanzeigenAdapter(BaseAdapter):
    """
    Config (city_config["kleinanzeigen"]):
        location: Kleinanzeigen location slug, e.g. "berlin"
        location_id: Category+location code, e.g. "c203l3331" (203 = Mietwohnungen, l3331 = Berlin)
        max_pages: Result pages per run (site caps at 50)
    """

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        settings = city_config.get("kleinanzeigen", {})
        self.location = settings.get("location", "berlin")
        self.location_id = settings.get("location_id", "c203l3331")
        self.max_pages = min(int(settings.get("max_pages", SITE_MAX_PAGES)), SITE_MAX_PAGES)
        self.city_name = city_config.get("display_name", "Berlin")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        }

    def _page_url(self, page: int, criteria: SearchCriteria) -> str:
        parts = [BASE_URL, "s-wohnung-mieten", self.location]
        if criteria.min_price_local or criteria.max_price_local:
            low = int((criteria.min_price_local or 0) * COLD_RENT_FLOOR_FACTOR)
            high = int(criteria.max_price_local) if criteria.max_price_local else ""
            parts.append(f"preis:{low or ''}:{high}")
        if page > 1:
            parts.append(f"seite:{page}")
        parts.append(self.location_id)
        return "/".join(parts)

    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        apartments = []
        seen_ids = set()

        for page in range(1, self.max_pages + 1):
            if page > 1:
                time.sleep(PAGE_DELAY_SECONDS)
            url = self._page_url(page, criteria)
            try:
                response = requests.get(url, headers=self._headers, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Kleinanzeigen page {page} failed: {e}")
                break

            cards = BeautifulSoup(response.text, "html.parser").select("article[data-adid]")
            if page == 1 and not cards:
                logger.warning("Kleinanzeigen: no listing cards on page 1 -- markup changed or blocked?")
            new_on_page = 0
            for card in cards:
                ad_id = card.get("data-adid")
                if ad_id in seen_ids:
                    continue
                seen_ids.add(ad_id)
                new_on_page += 1
                apartment = self._parse_card(card)
                # The URL's price filter already applies; this catches ads
                # with a buy-out ("Ablöse") or sale price entered as rent
                if apartment and not (criteria.max_price_local and apartment.price_local > criteria.max_price_local):
                    apartments.append(apartment)

            # Past the last page the site repeats the final page
            if not new_on_page:
                break

        logger.info(f"Fetched {len(apartments)} listings from Kleinanzeigen ({len(seen_ids)} scanned)")
        return apartments

    def _parse_card(self, card) -> Optional[Apartment]:
        try:
            href = card.get("data-href") or ""
            if not href.startswith("/s-anzeige/"):
                return None

            # The markup is utility-class Tailwind with no stable hooks, so
            # pick fields out of the card's text lines by shape:
            # "14169 Dahlem" / title link / teaser / "145 m² · 5 Zi." / "2.685 €"
            title = price = sqm = rooms = postcode = district = description = None
            for node in card.find_all(string=True):
                text = node.strip()
                if not text or node.parent.name == "script":
                    continue
                if title is None and node.parent.name == "a" and node.parent.get("href") == href:
                    title = text
                elif postcode is None and re.fullmatch(r"\d{5}\s+.+", text):
                    postcode, district = text.split(None, 1)
                elif price is None and re.fullmatch(r"[\d.]+(?:,\d+)?\s*€(?:\s*VB)?", text):
                    price = parse_german_number(text)
                elif "m²" in text and "Zi." in text and len(text) < 40:
                    sqm_match = re.search(r"(\d+(?:,\d+)?)\s*m²", text)
                    rooms_match = re.search(r"(\d+(?:,\d+)?)\s*Zi\.", text)
                    sqm = parse_german_number(sqm_match.group(1)) if sqm_match else None
                    rooms = parse_german_number(rooms_match.group(1)) if rooms_match else None
                elif title is not None and description is None and node.parent.name == "p":
                    description = text

            if not title or not price:
                return None  # no price means "VB"/"Zu verschenken" etc.
            if SKIP_TITLE.search(title):
                return None

            image = card.select_one("img")
            thumbnail = (image.get("src") or image.get("data-src")) if image else None

            return Apartment(
                source_id=f"kleinanzeigen_{card.get('data-adid')}",
                source_name="kleinanzeigen",
                title=title,
                url=f"{BASE_URL}{href}",
                price_local=price,
                currency="EUR",
                price_usd=None,
                bedrooms=int(rooms) if rooms else None,  # total "Zimmer"
                bathrooms=None,
                sqft=int(sqm * SQM_TO_SQFT) if sqm else None,
                address=f"{postcode} {district}" if postcode else None,
                neighborhood=district,
                city=self.city_name,
                country="Germany",
                amenities=Amenities(),
                description=description,
                images=[],
                thumbnail_url=thumbnail,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Failed to parse Kleinanzeigen card: {e}")
            return None

    def check_status(self, urls: List[str]) -> Dict[str, str]:
        """
        Removed ads 301 to a category search page instead of 404ing, so
        don't follow redirects. A 200 isn't proof of life (block and consent
        pages are 200 too), so live ads come back "unknown", per the
        BaseAdapter.check_status contract.
        """
        results = {}
        deadline = time.monotonic() + LIVENESS_TIME_BUDGET_SECONDS
        for i, url in enumerate(urls):
            if time.monotonic() > deadline:
                logger.info(f"Kleinanzeigen liveness time budget hit after {i}/{len(urls)} URLs")
                break
            if i:
                time.sleep(LIVENESS_DELAY_SECONDS)
            results[url] = "unknown"
            try:
                with requests.get(
                    url, headers=self._headers, timeout=(5, 10), allow_redirects=False, stream=True
                ) as response:
                    status_code = response.status_code
                    location = response.headers.get("Location", "")
            except requests.RequestException:
                continue
            if status_code in (404, 410) or (
                status_code in (301, 302) and REMOVED_AD_REDIRECT.search(location)
            ):
                results[url] = "gone"
        return results

    def _normalize(self, raw_listing: Dict[str, Any]) -> Optional[Apartment]:
        """Not used; cards are parsed directly in _parse_card."""
        return None
