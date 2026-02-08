"""Craigslist adapter for NYC apartment listings."""

import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from ..models.apartment import Amenities, Apartment
from ..utils.retry import retry_with_backoff
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)


@register_adapter("craigslist")
class CraigslistAdapter(BaseAdapter):
    """
    Adapter for Craigslist apartment listings using direct scraping.

    Free, no API key required.
    Rate limited to be respectful to the service.
    """

    DETAIL_DELAY = 0.5  # seconds between detail page fetches

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.site = city_config.get("craigslist", {}).get("site", "newyork")
        self.areas = city_config.get("craigslist", {}).get("areas", [])
        self.rate_limit = config.get("rate_limit", 2)
        self.city_name = city_config.get("display_name", "Unknown")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from Craigslist."""
        apartments = []
        areas_to_search = self.areas if self.areas else [None]

        for area in areas_to_search:
            try:
                logger.info(f"Fetching Craigslist listings for {self.site}, area: {area or 'all'}")

                listings = self._scrape_listings(area, criteria)
                apartments.extend(listings)

                if area != areas_to_search[-1]:
                    time.sleep(self.rate_limit)

            except Exception as e:
                logger.error(f"Error fetching from Craigslist area {area}: {e}")
                continue

        logger.info(f"Fetched {len(apartments)} listings from Craigslist")
        return apartments

    def _scrape_listings(self, area: Optional[str], criteria: SearchCriteria) -> List[Apartment]:
        """Scrape listings from Craigslist search page."""
        base_url = f"https://{self.site}.craigslist.org"
        if area:
            search_url = f"{base_url}/search/{area}/apa"
        else:
            search_url = f"{base_url}/search/apa"

        params = {
            "min_price": int(criteria.min_price_local),
            "max_price": int(criteria.max_price_local),
            "min_bedrooms": criteria.min_bedrooms,
            "max_bedrooms": criteria.max_bedrooms,
            "minSqft": criteria.min_sqft,
        }

        response = requests.get(search_url, params=params, headers=self._headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        apartments = []

        # Parse JSON-LD for coordinates (indexed by position matching HTML listing order)
        jsonld_coords = self._parse_jsonld_coords(soup)

        # Find listing items - Craigslist uses different structures
        # Try the gallery view first
        listings = soup.select("li.cl-static-search-result, li.cl-search-result, div.cl-search-result")

        if not listings:
            # Try older structure
            listings = soup.select("li.result-row")

        if not listings:
            # Try newest structure with gallery cards
            listings = soup.select("ol.cl-static-search-results > li, div.results > div.result")

        logger.debug(f"Found {len(listings)} raw listings on page")

        for idx, listing in enumerate(listings[:50]):
            apartment = self._parse_listing(listing, base_url, jsonld_coords.get(idx))
            if apartment:
                apartments.append(apartment)

        return apartments

    def _parse_jsonld_coords(self, soup) -> Dict[int, Tuple[float, float]]:
        """Extract lat/lng from JSON-LD structured data on the search page.

        Returns dict mapping position index to (latitude, longitude).
        """
        coords = {}
        script = soup.find("script", id="ld_searchpage_results")
        if not script or not script.string:
            return coords

        try:
            data = json.loads(script.string)
            for item in data.get("itemListElement", []):
                pos = int(item.get("position", -1))
                apt_data = item.get("item", {})
                lat = apt_data.get("latitude")
                lng = apt_data.get("longitude")
                if lat is not None and lng is not None:
                    coords[pos] = (float(lat), float(lng))
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.debug(f"Failed to parse JSON-LD coordinates: {e}")

        return coords

    def _fetch_detail_page(self, url: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """Fetch detail page for coordinates and thumbnail image.

        Returns (latitude, longitude, thumbnail_url).
        """
        try:
            time.sleep(self.DETAIL_DELAY)
            response = requests.get(url, headers=self._headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # --- Coordinates ---
            lat, lng = None, None

            # Primary: <div id="map" data-latitude="..." data-longitude="...">
            map_div = soup.find("div", id="map")
            if map_div:
                lat_str = map_div.get("data-latitude")
                lng_str = map_div.get("data-longitude")
                if lat_str and lng_str:
                    lat, lng = float(lat_str), float(lng_str)

            # Fallback: <meta name="geo.position" content="lat;lng">
            if lat is None:
                geo_meta = soup.find("meta", attrs={"name": "geo.position"})
                if geo_meta:
                    content = geo_meta.get("content", "")
                    parts = content.split(";")
                    if len(parts) == 2:
                        lat, lng = float(parts[0].strip()), float(parts[1].strip())

            # --- Thumbnail ---
            thumbnail_url = None
            # Find first full-size image from Craigslist CDN
            for img in soup.select("img[src*='images.craigslist.org']"):
                src = img.get("src", "")
                if "600x450" in src or "1200x900" in src:
                    thumbnail_url = src
                    break

            # Fallback: any craigslist image
            if not thumbnail_url:
                img_urls = re.findall(
                    r'https://images\.craigslist\.org/[^\s"\'<>]+\.jpg',
                    response.text,
                )
                for img_url in img_urls:
                    if "50x50c" not in img_url:
                        thumbnail_url = img_url
                        break

            return lat, lng, thumbnail_url

        except Exception as e:
            logger.debug(f"Failed to fetch detail page {url}: {e}")

        return None, None, None

    def _parse_listing(self, listing, base_url: str, jsonld_coords: Optional[Tuple[float, float]] = None) -> Optional[Apartment]:
        """Parse a single listing element."""
        try:
            # Try to find the link and title
            link = listing.select_one("a.cl-app-anchor, a.titlestring, a.result-title, a[href*='/apa/']")
            if not link:
                # Current structure: the whole li wraps an <a>
                link = listing.select_one("a")
            if not link:
                href = listing.get("data-url")
                title = listing.get("title") or "Untitled"
            else:
                href = link.get("href", "")
                title = link.get_text(strip=True) or listing.get("title", "Untitled")

            if not href:
                return None

            # Make URL absolute
            if href.startswith("/"):
                url = base_url + href
            elif not href.startswith("http"):
                url = base_url + "/" + href
            else:
                url = href

            # Extract price
            price = 0.0
            price_elem = listing.select_one(".priceinfo, .result-price, .price, span.cl-static-search-result-price")
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_match = re.search(r"\$?([\d,]+)", price_text)
                if price_match:
                    price = float(price_match.group(1).replace(",", ""))

            # Also check title for price
            if price == 0:
                price_match = re.search(r"\$\s*([\d,]+)", title)
                if price_match:
                    price = float(price_match.group(1).replace(",", ""))

            # Extract bedrooms
            bedrooms = None
            meta_text = listing.get_text()
            br_match = re.search(r"(\d+)\s*br\b", meta_text, re.IGNORECASE)
            if br_match:
                bedrooms = int(br_match.group(1))

            # Extract sqft
            sqft = None
            sqft_match = re.search(r"(\d+)\s*ft", meta_text, re.IGNORECASE)
            if sqft_match:
                sqft = int(sqft_match.group(1))

            # Extract neighborhood
            neighborhood = None
            hood_elem = listing.select_one(".location, .result-hood, .meta .nearby")
            if hood_elem:
                neighborhood = hood_elem.get_text(strip=True).strip("()")

            # Extract listing ID from URL
            id_match = re.search(r"/(\d+)\.html", url)
            listing_id = id_match.group(1) if id_match else url

            # Use JSON-LD coordinates if available (from search page, no extra request)
            latitude, longitude, thumbnail_url = None, None, None
            if jsonld_coords:
                latitude, longitude = jsonld_coords

            # Fetch detail page for thumbnail (and coords if JSON-LD didn't have them)
            detail_lat, detail_lng, detail_thumb = self._fetch_detail_page(url)

            if latitude is None and detail_lat is not None:
                latitude, longitude = detail_lat, detail_lng
            if detail_thumb:
                thumbnail_url = detail_thumb

            return Apartment(
                source_id=f"craigslist_{listing_id}",
                source_name="craigslist",
                title=title,
                url=url,
                price_local=price,
                currency="USD",
                price_usd=price,
                bedrooms=bedrooms,
                bathrooms=None,
                sqft=sqft,
                address=None,
                neighborhood=neighborhood,
                city=self.city_name,
                country="USA",
                latitude=latitude,
                longitude=longitude,
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
