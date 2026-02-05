"""CASA SAPO adapter for Lisbon apartment listings."""

import json
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

# EUR to USD conversion rate (approximate)
EUR_TO_USD = 1.08


@register_adapter("casasapo")
class CasaSapoAdapter(BaseAdapter):
    """
    Adapter for CASA SAPO (casa.sapo.pt) — Portugal's property portal.

    Parses JSON-LD Offer objects embedded in page HTML.
    """

    BASE_URL = "https://casa.sapo.pt"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.city_name = city_config.get("display_name", "Lisbon")

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from CASA SAPO."""
        apartments = []

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9,pt;q=0.8",
        }

        # Scrape first 2 pages
        for page in range(1, 3):
            url = f"{self.BASE_URL}/en-gb/rent-apartments/lisboa/"
            if page > 1:
                url += f"?pn={page}"

            try:
                logger.info(f"Fetching CASA SAPO page {page} for Lisbon")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                page_listings = self._extract_listings(soup)

                logger.debug(f"Page {page}: found {len(page_listings)} listings")

                for item in page_listings:
                    apartment = self._normalize(item, criteria)
                    if apartment:
                        apartments.append(apartment)

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching CASA SAPO page {page}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on page {page}: {e}")

        logger.info(f"Fetched {len(apartments)} listings from CASA SAPO")
        return apartments

    def _extract_listings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract Offer objects from JSON-LD and pair with detail URLs."""
        offers = []

        # Collect detail URLs from listing links
        detail_urls = []
        for a in soup.select('a[href*="/en-gb/rent-apartments/"]'):
            href = a.get("href", "")
            if href and "/detail/" in href.lower() or re.search(r'/\d+/?$', href):
                full_url = href if href.startswith("http") else self.BASE_URL + href
                if full_url not in detail_urls:
                    detail_urls.append(full_url)

        # Parse JSON-LD script tags
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
            except (json.JSONDecodeError, TypeError):
                continue

            items = data if isinstance(data, list) else [data]
            for item in items:
                item_type = item.get("@type", "")
                if isinstance(item_type, list):
                    item_type = item_type[0] if item_type else ""
                if item_type == "Offer":
                    offers.append(item)

        # Pair offers with detail URLs by index
        for i, offer in enumerate(offers):
            if i < len(detail_urls):
                offer["_detail_url"] = detail_urls[i]

        return offers

    def _normalize(
        self, raw: Dict[str, Any], criteria: SearchCriteria = None
    ) -> Optional[Apartment]:
        """Convert a JSON-LD Offer to normalized Apartment model."""
        try:
            title = raw.get("name", "Lisbon Apartment")

            # Extract listing ID from detail URL or generate one
            detail_url = raw.get("_detail_url", "")
            id_match = re.search(r'/(\d+)/?$', detail_url)
            listing_id = id_match.group(1) if id_match else str(abs(hash(title)))[-8:]

            url = detail_url or self.BASE_URL

            # Parse price: "3.000 €" → 3000
            price_eur = 0.0
            price_raw = raw.get("price", [])
            if isinstance(price_raw, list):
                price_text = price_raw[0] if price_raw else "0"
            else:
                price_text = str(price_raw)

            # Strip dots (thousands separator), spaces, and currency symbol
            price_cleaned = re.sub(r'[€\s]', '', price_text)
            price_cleaned = price_cleaned.replace('.', '').replace(',', '.')
            try:
                price_eur = float(price_cleaned)
            except ValueError:
                price_eur = 0.0

            # Apply price filter
            if criteria:
                if criteria.min_price_local and price_eur < criteria.min_price_local:
                    return None
                if criteria.max_price_local and price_eur > criteria.max_price_local:
                    return None

            price_usd = price_eur * EUR_TO_USD

            # Bedrooms from title: "Apartment 2 Bedrooms ..."
            bedrooms = None
            bed_match = re.search(r'(\d+)\s*Bedroom', title, re.IGNORECASE)
            if bed_match:
                bedrooms = int(bed_match.group(1))
            elif re.search(r'\bstudio\b', title, re.IGNORECASE):
                bedrooms = 0

            # Geo coordinates
            available_from = raw.get("availableAtOrFrom", {})
            geo = available_from.get("geo", {})
            latitude = geo.get("latitude")
            longitude = geo.get("longitude")

            # Thumbnail
            thumbnail_url = raw.get("image")
            images = [thumbnail_url] if thumbnail_url else []

            # Extract neighborhood from title (after bedrooms part)
            neighborhood = None
            loc_match = re.search(r'Bedroom[s]?\s+(.+?)(?:,\s*Lisboa)?$', title, re.IGNORECASE)
            if loc_match:
                neighborhood = loc_match.group(1).strip()

            return Apartment(
                source_id=f"casasapo_{listing_id}",
                source_name="casasapo",
                title=title,
                url=url,
                price_local=price_eur,
                currency="EUR",
                price_usd=price_usd,
                bedrooms=bedrooms,
                bathrooms=None,
                sqft=None,
                address=None,
                neighborhood=neighborhood,
                city=self.city_name,
                country="Portugal",
                latitude=latitude,
                longitude=longitude,
                amenities=Amenities(),
                description=None,
                images=images,
                thumbnail_url=thumbnail_url,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Error normalizing CASA SAPO listing: {e}")
            return None
