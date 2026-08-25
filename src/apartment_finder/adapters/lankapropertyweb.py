"""LankaPropertyWeb adapter for Sri Lanka house listings."""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from ..models.apartment import Amenities, Apartment
from ..utils.retry import retry_with_backoff
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)

# LKR to USD conversion rate (approximate, matches services/currency.py fallback)
LKR_TO_USD = 0.00303

BASE_URL = "https://www.lankapropertyweb.com"


@register_adapter("lankapropertyweb")
class LankaPropertyWebAdapter(BaseAdapter):
    """
    Adapter for LankaPropertyWeb.com — Sri Lanka's largest property portal.

    Uses plain HTTP requests to scrape listing cards from server-rendered HTML.
    """

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.property_type = city_config.get("lankapropertyweb", {}).get("property_type", "House")
        self.city_name = city_config.get("display_name", "Sri Lanka")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch house listings from LankaPropertyWeb."""
        apartments = []
        url = f"{BASE_URL}/rentals/lease-all-{self.property_type}.html"

        try:
            logger.info("Fetching LankaPropertyWeb listings")

            response = requests.get(url, headers=self._headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            cards = soup.select("article.listing-item")
            logger.debug(f"Found {len(cards)} raw listings on page")

            for card in cards:
                apartment = self._parse_listing(card)
                if apartment:
                    if criteria.min_price_local and apartment.price_local < criteria.min_price_local:
                        continue
                    if criteria.max_price_local and apartment.price_local > criteria.max_price_local:
                        continue
                    apartments.append(apartment)

        except requests.RequestException as e:
            logger.error(f"Error fetching from LankaPropertyWeb: {e}")

        logger.info(f"Fetched {len(apartments)} listings from LankaPropertyWeb")
        return apartments

    def _parse_listing(self, card) -> Optional[Apartment]:
        """Parse a single listing card."""
        try:
            ad_id = card.get("data-ad-id")
            title_link = card.select_one("h4.listing-title a")
            if not title_link:
                return None

            href = title_link.get("href", "")
            url = f"{BASE_URL}{href}" if href.startswith("/") else href
            title = title_link.get_text(strip=True) or "Sri Lanka House"

            location_elem = card.select_one("span.location")
            neighborhood = location_elem.get_text(strip=True) if location_elem else None

            price_lkr = 0.0
            price_elem = card.select_one(".listing-price")
            if price_elem:
                # Prices appear either as "225,000" or abbreviated "1.5M"/"850K"
                price_match = re.search(r"([\d,]+(?:\.\d+)?)\s*([MK])?", price_elem.get_text())
                if price_match:
                    price_lkr = float(price_match.group(1).replace(",", ""))
                    suffix = price_match.group(2)
                    if suffix == "M":
                        price_lkr *= 1_000_000
                    elif suffix == "K":
                        price_lkr *= 1_000
            price_usd = price_lkr * LKR_TO_USD

            counts = card.select(".listing-summery .count")
            bedrooms = None
            sqft = None
            if counts:
                # Bedroom count can appear as "9+" for very large houses
                bed_match = re.match(r"\d+", counts[0].get_text(strip=True))
                if bed_match:
                    bedrooms = int(bed_match.group())
                if len(counts) > 1:
                    units = card.select(".listing-summery .unit")
                    unit_text = units[0].get_text(strip=True).lower() if units else ""
                    size_match = re.match(r"[\d,]+", counts[1].get_text(strip=True))
                    if size_match:
                        size_value = int(size_match.group().replace(",", ""))
                        if "sqft" in unit_text:
                            sqft = size_value
                        elif "perch" in unit_text:
                            # 1 perch = 272.25 sqft (land size, not floor area)
                            sqft = int(size_value * 272.25)

            img = card.select_one("img.lozad")
            thumbnail_url = None
            if img:
                raw_src = img.get("data-src") or img.get("src")
                if raw_src:
                    # Strip PageSpeed image-optimization query suffix
                    thumbnail_url = raw_src.split(",qv=")[0]

            return Apartment(
                source_id=f"lankapropertyweb_{ad_id or hash(url)}",
                source_name="lankapropertyweb",
                title=title,
                url=url,
                price_local=price_lkr,
                currency="LKR",
                price_usd=price_usd,
                bedrooms=bedrooms,
                bathrooms=None,
                sqft=sqft,
                address=None,
                neighborhood=neighborhood,
                city=self.city_name,
                country="Sri Lanka",
                latitude=None,
                longitude=None,
                amenities=Amenities(),
                description=None,
                images=[],
                thumbnail_url=thumbnail_url,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Failed to parse LankaPropertyWeb listing: {e}")
            return None

    def _normalize(self, raw: Dict[str, Any]) -> Optional[Apartment]:
        """Not used in direct scraping approach."""
        return None
