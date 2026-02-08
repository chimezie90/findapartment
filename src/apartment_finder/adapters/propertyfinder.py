"""PropertyFinder.ae adapter for Dubai apartment listings."""

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

# AED to USD conversion rate (approximate)
AED_TO_USD = 0.27


@register_adapter("propertyfinder")
class PropertyFinderAdapter(BaseAdapter):
    """
    Adapter for PropertyFinder.ae (UAE's #1 property portal).

    Parses __NEXT_DATA__ JSON embedded in the static HTML.
    """

    BASE_URL = "https://www.propertyfinder.ae"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.emirate = city_config.get("propertyfinder", {}).get("emirate", "dubai")
        self.city_name = city_config.get("display_name", "Dubai")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
        }

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from PropertyFinder.ae."""
        apartments = []
        url = f"{self.BASE_URL}/en/rent/{self.emirate}/apartments-for-rent.html"

        try:
            logger.info(f"Fetching PropertyFinder listings for {self.emirate}")

            response = requests.get(url, headers=self._headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            properties = self._extract_properties(soup)

            logger.debug(f"Found {len(properties)} properties")

            for prop in properties:
                apartment = self._normalize(prop, criteria)
                if apartment:
                    apartments.append(apartment)

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing PropertyFinder JSON: {e}")
        except Exception as e:
            logger.error(f"Error fetching from PropertyFinder: {e}")

        logger.info(f"Fetched {len(apartments)} listings from PropertyFinder")
        return apartments

    def _extract_properties(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract property listings from __NEXT_DATA__ script tag."""
        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            logger.error("Could not find __NEXT_DATA__ in response")
            return []

        try:
            data = json.loads(script.string)
            properties = (
                data.get("props", {})
                .get("pageProps", {})
                .get("searchResult", {})
                .get("properties", [])
            )
            return properties
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to parse __NEXT_DATA__: {e}")
            return []

    def _normalize(
        self, raw: Dict[str, Any], criteria: SearchCriteria = None
    ) -> Optional[Apartment]:
        """Convert a PropertyFinder property to normalized Apartment model."""
        try:
            property_id = raw.get("id", "")
            if not property_id:
                return None

            title = raw.get("title", "Dubai Apartment")

            # Detail URL
            details_path = raw.get("details_path", "")
            detail_url = f"{self.BASE_URL}{details_path}" if details_path else self.BASE_URL

            # Price: value is in AED, period can be yearly/monthly
            price_data = raw.get("price", {})
            price_value = float(price_data.get("value", 0))
            period = price_data.get("period", "yearly").lower()

            if "year" in period:
                price_aed_monthly = price_value / 12
            else:
                price_aed_monthly = price_value

            # Apply price filter
            if criteria:
                if criteria.min_price_local and price_aed_monthly < criteria.min_price_local:
                    return None
                if criteria.max_price_local and price_aed_monthly > criteria.max_price_local:
                    return None

            price_usd = price_aed_monthly * AED_TO_USD

            # Bedrooms and bathrooms
            bedrooms = raw.get("bedrooms")
            if bedrooms is not None:
                bedrooms = int(bedrooms)
            bathrooms = raw.get("bathrooms")
            if bathrooms is not None:
                bathrooms = int(bathrooms)

            # Floor size in sqft
            sqft = None
            size = raw.get("size")
            if isinstance(size, dict):
                try:
                    sqft = int(float(size.get("value", 0)))
                except (ValueError, TypeError):
                    pass
            elif size:
                try:
                    sqft = int(float(size))
                except (ValueError, TypeError):
                    pass

            # Geo coordinates
            location = raw.get("location", {})
            coords = location.get("coordinates", {})
            latitude = coords.get("lat")
            longitude = coords.get("lon")

            # Images
            images_data = raw.get("images", [])
            images = []
            thumbnail_url = None
            for img in images_data:
                if isinstance(img, dict):
                    url = img.get("medium") or img.get("small") or ""
                    if url:
                        images.append(url)
                elif isinstance(img, str):
                    images.append(img)
            if images:
                thumbnail_url = images[0]

            # Neighborhood from location
            neighborhood = location.get("full_name") or location.get("name")

            # Description
            description = raw.get("description")

            return Apartment(
                source_id=f"propertyfinder_{property_id}",
                source_name="propertyfinder",
                title=title,
                url=detail_url,
                price_local=price_aed_monthly,
                currency="AED",
                price_usd=price_usd,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                sqft=sqft,
                address=location.get("full_name", "Dubai"),
                neighborhood=neighborhood,
                city=self.city_name,
                country="UAE",
                latitude=float(latitude) if latitude else None,
                longitude=float(longitude) if longitude else None,
                amenities=Amenities(),
                description=description,
                images=images,
                thumbnail_url=thumbnail_url,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Error normalizing listing: {e}")
            return None
