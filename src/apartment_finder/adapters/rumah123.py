"""Rumah123 adapter for Bali apartment listings."""

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

# IDR to USD conversion rate (approximate)
IDR_TO_USD = 0.000063

# Square meters to square feet
SQM_TO_SQFT = 10.764


@register_adapter("rumah123")
class Rumah123Adapter(BaseAdapter):
    """
    Adapter for Rumah123.com — Indonesia's largest property portal.

    Uses plain HTTP requests to scrape listing cards from static HTML.
    """

    BASE_URL = "https://www.rumah123.com"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.region = city_config.get("rumah123", {}).get("region", "bali")
        self.city_name = city_config.get("display_name", "Bali")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from Rumah123."""
        apartments = []
        url = f"{self.BASE_URL}/en/rent/{self.region}/apartment/"

        try:
            logger.info(f"Fetching Rumah123 listings for {self.region}")

            response = requests.get(url, headers=self._headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            listings = self._parse_listing_cards(soup)

            logger.debug(f"Found {len(listings)} listing cards")

            for item in listings:
                apartment = self._normalize(item, criteria)
                if apartment:
                    apartments.append(apartment)

        except Exception as e:
            logger.error(f"Error fetching from Rumah123: {e}")

        logger.info(f"Fetched {len(apartments)} listings from Rumah123")
        return apartments

    def _parse_listing_cards(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract listing data from HTML cards."""
        listings = []

        # Rumah123 listing cards contain links to /en/property/...
        cards = soup.select('a[href*="/en/property/"]')

        seen_urls = set()
        for card in cards:
            href = card.get("href", "")
            if not href or "/property/" not in href:
                continue

            full_url = href if href.startswith("http") else self.BASE_URL + href
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Walk up to find the card container
            container = card
            for _ in range(5):
                parent = container.parent
                if parent and parent.name in ('div', 'article', 'section'):
                    container = parent
                else:
                    break

            listing = {"url": full_url, "container": container, "link": card}
            listings.append(listing)

        return listings[:50]

    def _parse_price_idr(self, text: str) -> float:
        """Parse IDR price text like 'IDR 6,5 Million monthly' or 'IDR 241 Million yearly'."""
        text = text.strip().lower()

        # Remove "idr" prefix and "rp" prefix
        text = re.sub(r'^(idr|rp\.?)\s*', '', text, flags=re.IGNORECASE)

        # Extract the numeric part and multiplier
        match = re.search(r'([\d.,]+)\s*(million|billion|juta|miliar)?', text, re.IGNORECASE)
        if not match:
            nums = re.findall(r'[\d.,]+', text)
            if nums:
                return float(nums[0].replace(',', '.').replace('..', '.'))
            return 0.0

        num_str = match.group(1)
        # Indonesian/European format: comma as decimal separator
        # e.g., "6,5" = 6.5, "17,2" = 17.2
        if ',' in num_str and '.' not in num_str:
            num_str = num_str.replace(',', '.')
        elif '.' in num_str:
            parts = num_str.split('.')
            if len(parts) == 2 and len(parts[1]) <= 2:
                pass  # Already decimal
            else:
                num_str = num_str.replace('.', '')

        value = float(num_str)
        multiplier_text = (match.group(2) or "").lower()

        if multiplier_text in ("million", "juta"):
            value *= 1_000_000
        elif multiplier_text in ("billion", "miliar"):
            value *= 1_000_000_000

        # Check if yearly → convert to monthly
        if "year" in text or "tahun" in text:
            value /= 12

        return value

    def _normalize(
        self, raw: Dict[str, Any], criteria: SearchCriteria = None
    ) -> Optional[Apartment]:
        """Convert a scraped listing card to normalized Apartment model."""
        try:
            url = raw.get("url", "")
            container = raw.get("container")
            link = raw.get("link")

            # Extract listing ID from URL
            id_match = re.search(r'/([a-z0-9-]+?)(?:\?|#|$)', url.rstrip('/').split('/')[-1])
            listing_id = id_match.group(1) if id_match else str(abs(hash(url)))[-8:]

            # Get all text from the container
            container_text = container.get_text(separator=" ", strip=True) if container else ""

            # Title: from first heading in container or link text
            title = ""
            heading = container.select_one("h2, h3, h4") if container else None
            if heading:
                title = heading.get_text(strip=True)
            if not title:
                title = link.get_text(strip=True) if link else ""
            if not title:
                title = "Bali Apartment"

            # Price: look for "IDR X Million monthly" pattern in container text
            price_idr = 0.0
            price_match = re.search(
                r'(?:IDR|Rp\.?)\s*([\d.,]+)\s*(Million|Billion|Juta|Miliar)?\s*(?:/\s*)?(monthly|yearly|month|year|tahun|bulan)?',
                container_text, re.IGNORECASE
            )
            if price_match:
                price_idr = self._parse_price_idr(price_match.group(0))

            # Apply price filter
            if criteria:
                if criteria.min_price_local and price_idr < criteria.min_price_local:
                    return None
                if criteria.max_price_local and price_idr > criteria.max_price_local:
                    return None

            price_usd = price_idr * IDR_TO_USD

            # Bedrooms
            bedrooms = None
            bed_match = re.search(r'(\d+)\s*(?:bed|bedroom|BR|kamar)', container_text, re.IGNORECASE)
            if bed_match:
                bedrooms = int(bed_match.group(1))
            elif re.search(r'\bstudio\b', container_text, re.IGNORECASE):
                bedrooms = 0

            # Bathrooms
            bathrooms = None
            bath_match = re.search(r'(\d+)\s*(?:bath|bathroom|BA)', container_text, re.IGNORECASE)
            if bath_match:
                bathrooms = int(bath_match.group(1))

            # Area in sqm → sqft
            sqft = None
            area_match = re.search(r'(?:BA|LB|LA)?:?\s*(\d+)\s*m[²2]', container_text)
            if area_match:
                sqm = int(area_match.group(1))
                sqft = int(sqm * SQM_TO_SQFT)

            # Location from text
            neighborhood = None
            loc_match = re.search(r'(?:in\s+)?(\w+(?:\s+\w+)?),?\s*(?:Bali|Denpasar|Badung)', container_text, re.IGNORECASE)
            if loc_match:
                neighborhood = loc_match.group(1)

            # Thumbnail image
            thumbnail_url = None
            images = []
            if container:
                # Prefer 720x420 crop images
                img = container.select_one('img[src*="rumah123"], img[src*="r123"]')
                if img:
                    thumbnail_url = img.get("src") or img.get("data-src")
                if not thumbnail_url:
                    img = container.select_one('img[src*="http"]')
                    if img:
                        thumbnail_url = img.get("src") or img.get("data-src")
            if thumbnail_url:
                images = [thumbnail_url]

            return Apartment(
                source_id=f"rumah123_{listing_id}",
                source_name="rumah123",
                title=title,
                url=url,
                price_local=price_idr,
                currency="IDR",
                price_usd=price_usd,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                sqft=sqft,
                address=None,
                neighborhood=neighborhood,
                city=self.city_name,
                country="Indonesia",
                latitude=None,
                longitude=None,
                amenities=Amenities(),
                description=None,
                images=images,
                thumbnail_url=thumbnail_url,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Error normalizing Rumah123 listing: {e}")
            return None
