"""FindProperties.ae adapter for Dubai apartment listings."""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from ..models.apartment import Amenities, Apartment
from ..utils.retry import retry_with_backoff
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)


@register_adapter("findproperties")
class FindPropertiesAdapter(BaseAdapter):
    """
    Adapter for FindProperties.ae (UAE real estate).

    No API key required - data is embedded in page as JSON.
    """

    BASE_URL = "https://findproperties.ae"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.emirate = city_config.get("findproperties", {}).get("emirate", "dubai")

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from FindProperties.ae."""
        apartments = []

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # Build URL - site uses AED prices
        url = f"{self.BASE_URL}/for-rent/apartments/{self.emirate}"

        try:
            logger.info(f"Fetching FindProperties listings for {self.emirate}")

            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            # Extract JSON data from __NEXT_DATA__
            match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', response.text)
            if not match:
                logger.error("Could not find __NEXT_DATA__ in response")
                return []

            data = json.loads(match.group(1))
            properties = data.get("props", {}).get("pageProps", {}).get("initialProperties", [])

            for prop in properties:
                apartment = self._normalize(prop, criteria)
                if apartment:
                    apartments.append(apartment)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from FindProperties: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing FindProperties JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        logger.info(f"Fetched {len(apartments)} listings from FindProperties")
        return apartments

    def _normalize(self, raw: Dict[str, Any], criteria: SearchCriteria) -> Optional[Apartment]:
        """Convert FindProperties listing to normalized Apartment model."""
        try:
            # Get price in AED
            price_aed = float(raw.get("price", 0))

            # Skip if no price
            if price_aed <= 0:
                return None

            # Detect if price is yearly (very high values) and convert to monthly
            # Typical monthly rent in Dubai: 2,000 - 50,000 AED
            # Yearly would be 24,000 - 600,000 AED
            if price_aed > 100000:
                price_aed = price_aed / 12  # Convert yearly to monthly

            # Get bedrooms (-1 means studio)
            beds_str = raw.get("beds", "0")
            if beds_str == "-1":
                bedrooms = 0  # Studio
            else:
                try:
                    bedrooms = int(beds_str)
                except (ValueError, TypeError):
                    bedrooms = None

            # Get bathrooms
            baths_str = raw.get("baths", "0")
            try:
                bathrooms = float(baths_str)
            except (ValueError, TypeError):
                bathrooms = None

            # Get area in sqft
            area_str = raw.get("area", "0")
            try:
                sqft = int(float(area_str))
            except (ValueError, TypeError):
                sqft = None

            # Build URL
            property_id = raw.get("id", "")
            title_slug = raw.get("title_en", "").lower().replace(" ", "-").replace("/", "-")[:50]
            url = f"{self.BASE_URL}/property/{property_id}/{title_slug}"

            # Get coordinates
            lat = raw.get("lat")
            lng = raw.get("lng")

            return Apartment(
                source_id=f"findproperties_{property_id}",
                source_name="findproperties",
                title=raw.get("title_en", "Dubai Apartment"),
                url=url,
                price_local=price_aed,
                currency="AED",
                price_usd=None,  # Will be filled by currency service
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                sqft=sqft,
                address=None,
                neighborhood=raw.get("emirate_en", "Dubai"),
                city="Dubai",
                country="UAE",
                latitude=float(lat) if lat else None,
                longitude=float(lng) if lng else None,
                amenities=Amenities(),
                description=None,
                images=[raw.get("image")] if raw.get("image") else [],
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.warning(f"Failed to normalize FindProperties listing: {e}")
            return None
