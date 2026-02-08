"""Lejebolig.dk adapter for Copenhagen apartment listings."""

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

# DKK to USD conversion rate (approximate)
DKK_TO_USD = 0.14


@register_adapter("lejebolig")
class LejeboligAdapter(BaseAdapter):
    """
    Adapter for Lejebolig.dk Danish apartment listings.

    Lejebolig.dk is one of the largest rental platforms in Denmark
    with server-rendered listings that are easy to scrape.
    """

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.city = city_config.get("lejebolig", {}).get("city", "koebenhavn")
        self.rate_limit = config.get("rate_limit", 2)
        self.city_name = city_config.get("display_name", "Copenhagen")

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from Lejebolig.dk."""
        apartments = []

        try:
            logger.info(f"Fetching Lejebolig listings for {self.city}")

            listings = self._scrape_listings(criteria)
            apartments.extend(listings)

        except Exception as e:
            logger.error(f"Error fetching from Lejebolig: {e}")

        logger.info(f"Fetched {len(apartments)} listings from Lejebolig")
        return apartments

    def _scrape_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Scrape listings from Lejebolig search page."""
        base_url = "https://en.lejebolig.dk"

        # Lejebolig search URL for Copenhagen apartments
        search_url = f"{base_url}/lejligheder/{self.city}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,da;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
        }

        try:
            response = requests.get(search_url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Lejebolig listings: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        apartments = []

        # Find all listing links with class="lease-info"
        listing_links = soup.select('a.lease-info[href*="/lejebolig/"]')
        logger.debug(f"Found {len(listing_links)} raw listings on page")

        for link in listing_links[:50]:  # Limit to 50 listings
            apartment = self._parse_listing(link, base_url)
            if apartment:
                # Apply price filter
                if criteria.min_price_local and apartment.price_local < criteria.min_price_local:
                    continue
                if criteria.max_price_local and apartment.price_local > criteria.max_price_local:
                    continue
                apartments.append(apartment)

        return apartments

    def _parse_listing(self, link, base_url: str) -> Optional[Apartment]:
        """Parse a single listing element."""
        try:
            # Extract URL and ID
            href = link.get("href", "")
            if not href:
                return None

            # Make URL absolute
            if href.startswith("/"):
                url = base_url + href
            else:
                url = href

            # Extract listing ID from href or id attribute
            listing_id = link.get("id", "").replace("lease-", "")
            if not listing_id:
                id_match = re.search(r"/lejebolig/(\d+)/", href)
                listing_id = id_match.group(1) if id_match else str(hash(url))[-8:]

            # Extract title from h2
            title_elem = link.select_one("h2, .lease-description h2")
            title = title_elem.get_text(strip=True) if title_elem else "Copenhagen Apartment"

            # Extract location from lease-sub-header
            location_elem = link.select_one(".lease-sub-header")
            neighborhood = None
            if location_elem:
                location_text = location_elem.get_text(strip=True)
                # Remove "Apartment in " prefix
                neighborhood = re.sub(r"^(Apartment|Room|House) in ", "", location_text)

            # Extract price from rent div
            price_dkk = 0.0
            price_elem = link.select_one(".rent, .rent div")
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                # Parse "8,985,-" format
                price_match = re.search(r"([\d,]+)", price_text)
                if price_match:
                    price_str = price_match.group(1).replace(",", "")
                    price_dkk = float(price_str)

            # Convert to USD
            price_usd = price_dkk * DKK_TO_USD

            # Extract size and rooms from lease-spec divs
            sqm = None
            rooms = None
            spec_divs = link.select(".lease-spec")
            for i, spec in enumerate(spec_divs):
                span = spec.select_one("span")
                if span:
                    value = span.get_text(strip=True)
                    if i == 0:  # First is usually size in sqm
                        try:
                            sqm = int(value)
                        except ValueError:
                            pass
                    elif i == 1:  # Second is usually rooms
                        try:
                            rooms = int(value)
                        except ValueError:
                            pass

            # Convert sqm to sqft
            sqft = int(sqm * 10.764) if sqm else None

            # Look for thumbnail image
            thumbnail_url = None
            # Check parent for images (they're usually in a script tag before the link)
            parent = link.find_parent()
            if parent:
                img = parent.select_one("img[src*='lejeboligdata']")
                if img:
                    thumbnail_url = img.get("src")

            return Apartment(
                source_id=f"lejebolig_{listing_id}",
                source_name="lejebolig",
                title=title,
                url=url,
                price_local=price_dkk,
                currency="DKK",
                price_usd=price_usd,
                bedrooms=rooms,
                bathrooms=None,
                sqft=sqft,
                address=None,
                neighborhood=neighborhood,
                city=self.city_name,
                country="Denmark",
                latitude=55.6761,  # Copenhagen center
                longitude=12.5683,
                amenities=Amenities(),
                description=None,
                images=[],
                thumbnail_url=thumbnail_url,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Failed to parse listing: {e}")
            return None

    def _normalize(self, raw: Dict[str, Any]) -> Optional[Apartment]:
        """Not used in direct scraping approach."""
        return None
