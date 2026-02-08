"""PropertyFinder.ae adapter for Dubai apartment listings."""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

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

    Parses JSON-LD structured data embedded in page HTML.
    """

    BASE_URL = "https://www.propertyfinder.ae"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.emirate = city_config.get("propertyfinder", {}).get("emirate", "dubai")
        self.city_name = city_config.get("display_name", "Dubai")

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from PropertyFinder.ae."""
        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            logger.error("Camoufox not installed. Run: pip install camoufox && python -m camoufox fetch")
            return []

        apartments = []
        url = f"{self.BASE_URL}/en/rent/{self.emirate}/apartments-for-rent.html"

        try:
            logger.info(f"Fetching PropertyFinder listings for {self.emirate}")

            with Camoufox(headless=True) as browser:
                page = browser.new_page()
                page.goto(url, timeout=60000)
                page.wait_for_timeout(3000)
                html = page.content()

            soup = BeautifulSoup(html, "html.parser")
            listings = self._extract_jsonld_listings(soup)

            logger.debug(f"Found {len(listings)} JSON-LD listings")

            for item in listings:
                apartment = self._normalize(item, criteria)
                if apartment:
                    apartments.append(apartment)

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing PropertyFinder JSON: {e}")
        except Exception as e:
            logger.error(f"Error fetching from PropertyFinder: {e}")

        logger.info(f"Fetched {len(apartments)} listings from PropertyFinder")
        return apartments

    def _extract_jsonld_listings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract RealEstateListing objects from JSON-LD script tags."""
        listings = []

        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
            except (json.JSONDecodeError, TypeError):
                continue

            # Handle @graph wrapper
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                if "@graph" in data:
                    items = data["@graph"]
                else:
                    items = [data]

            for item in items:
                item_types = item.get("@type", [])
                if isinstance(item_types, str):
                    item_types = [item_types]
                if "RealEstateListing" in item_types:
                    listings.append(item)

        return listings

    def _normalize(
        self, raw: Dict[str, Any], criteria: SearchCriteria = None
    ) -> Optional[Apartment]:
        """Convert a JSON-LD RealEstateListing to normalized Apartment model."""
        try:
            # Extract ID from @id URL
            at_id = raw.get("@id", "")
            id_match = re.search(r'(\d+)', at_id.split('/')[-1]) if at_id else None
            listing_id = id_match.group(1) if id_match else str(hash(at_id))[-8:]
            if not listing_id:
                return None

            # Detail URL
            detail_url = at_id or self.BASE_URL

            # Title
            title = raw.get("name", "Dubai Apartment")

            # Price: AED/year → monthly → USD
            offers = raw.get("offers", {})
            price_spec = offers.get("priceSpecification", offers)
            price_value = float(price_spec.get("price", 0))
            unit_text = price_spec.get("unitText", "YEAR").upper()

            if "YEAR" in unit_text:
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
            bedrooms = raw.get("numberOfBedrooms")
            if bedrooms is not None:
                bedrooms = int(bedrooms)
            bathrooms = raw.get("numberOfBathroomsTotal")
            if bathrooms is not None:
                bathrooms = int(bathrooms)

            # Floor size in sqft
            sqft = None
            floor_size = raw.get("floorSize", {})
            if floor_size:
                size_value = floor_size.get("value")
                unit_code = floor_size.get("unitCode", "SQF").upper()
                if size_value:
                    if "SQM" in unit_code or "MTK" in unit_code:
                        sqft = int(float(size_value) * 10.764)
                    else:
                        sqft = int(float(size_value))

            # Geo coordinates
            geo = raw.get("geo", {})
            latitude = geo.get("latitude")
            longitude = geo.get("longitude")

            # Thumbnail from images
            images = raw.get("image", [])
            if isinstance(images, str):
                images = [images]
            thumbnail_url = images[0] if images else None

            # Address from geo or title
            address_data = raw.get("address", {})
            if isinstance(address_data, dict):
                address = address_data.get("streetAddress") or address_data.get("addressLocality", "Dubai")
            else:
                address = "Dubai"

            return Apartment(
                source_id=f"propertyfinder_{listing_id}",
                source_name="propertyfinder",
                title=title,
                url=detail_url,
                price_local=price_aed_monthly,
                currency="AED",
                price_usd=price_usd,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                sqft=sqft,
                address=address,
                neighborhood=None,
                city=self.city_name,
                country="UAE",
                latitude=latitude,
                longitude=longitude,
                amenities=Amenities(),
                description=None,
                images=images if isinstance(images, list) else [],
                thumbnail_url=thumbnail_url,
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Error normalizing listing: {e}")
            return None
