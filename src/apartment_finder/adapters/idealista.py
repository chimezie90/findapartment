"""Idealista adapter for Lisbon/Portugal apartment listings."""

import base64
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from ..models.apartment import Amenities, Apartment
from ..utils.retry import retry_with_backoff
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)


@register_adapter("idealista")
class IdealistaAdapter(BaseAdapter):
    """
    Adapter for Idealista (Spain/Portugal/Italy real estate).

    API docs: https://developers.idealista.com
    Requires API key and secret from developers.idealista.com
    """

    BASE_URL = "https://api.idealista.com"
    TOKEN_URL = f"{BASE_URL}/oauth/token"
    SEARCH_URL = f"{BASE_URL}/3.5"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.api_key = os.getenv("IDEALISTA_API_KEY")
        self.api_secret = os.getenv("IDEALISTA_SECRET")
        self._access_token: Optional[str] = None

        # Get Idealista-specific config from city config
        idealista_config = city_config.get("idealista", {})
        self.country = idealista_config.get("country", "pt")
        self.center = idealista_config.get("center", "38.7223,-9.1393")  # Lisbon default
        self.distance = idealista_config.get("distance", 10000)  # meters

        if not self.api_key or not self.api_secret:
            logger.warning("IDEALISTA_API_KEY or IDEALISTA_SECRET not set - adapter disabled")

    def is_available(self) -> bool:
        """Check if API credentials are configured."""
        return bool(self.api_key and self.api_secret)

    def _get_access_token(self) -> Optional[str]:
        """Get OAuth2 access token using client credentials."""
        if self._access_token:
            return self._access_token

        credentials = f"{self.api_key}:{self.api_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}

        try:
            response = requests.post(self.TOKEN_URL, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            self._access_token = token_data.get("access_token")
            return self._access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get Idealista access token: {e}")
            return None

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch rental listings from Idealista API."""
        if not self.is_available():
            logger.warning("Idealista adapter not available - skipping")
            return []

        token = self._get_access_token()
        if not token:
            logger.error("Failed to authenticate with Idealista API")
            return []

        apartments = []
        headers = {"Authorization": f"Bearer {token}"}

        try:
            logger.info(f"Fetching Idealista listings for {self.country}")

            params = {
                "country": self.country,
                "operation": "rent",
                "propertyType": "homes",
                "center": self.center,
                "distance": self.distance,
                "minPrice": int(criteria.min_price_local),
                "maxPrice": int(criteria.max_price_local),
                "minSize": self._sqft_to_sqm(criteria.min_sqft),
                "bedrooms": f"{criteria.min_bedrooms},{criteria.max_bedrooms}",
                "maxItems": 50,
                "numPage": 1,
                "language": "en",
                "order": "publicationDate",
                "sort": "desc",
            }

            url = f"{self.SEARCH_URL}/{self.country}/search"
            response = requests.post(url, headers=headers, data=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            for listing in data.get("elementList", []):
                apartment = self._normalize(listing)
                if apartment:
                    apartments.append(apartment)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Idealista API authentication failed - check credentials")
                self._access_token = None
            elif e.response.status_code == 429:
                logger.error("Idealista API rate limit exceeded")
            else:
                logger.error(f"Idealista API error: {e}")
        except Exception as e:
            logger.error(f"Error fetching from Idealista: {e}")

        logger.info(f"Fetched {len(apartments)} listings from Idealista")
        return apartments

    def _normalize(self, raw: Dict[str, Any]) -> Optional[Apartment]:
        """Convert Idealista listing to normalized Apartment model."""
        try:
            # Extract amenities from features
            amenities = self._extract_amenities(raw)

            # Get price (in EUR for Portugal)
            price = float(raw.get("price", 0))

            # Size is in sqm, convert to sqft
            sqm = raw.get("size")
            sqft = int(sqm * 10.764) if sqm else None

            # Build full URL
            property_code = raw.get("propertyCode", "")
            url = raw.get("url", f"https://www.idealista.pt/imovel/{property_code}/")

            return Apartment(
                source_id=f"idealista_{property_code}",
                source_name="idealista",
                title=self._build_title(raw),
                url=url,
                price_local=price,
                currency="EUR",
                price_usd=None,
                bedrooms=raw.get("rooms"),
                bathrooms=raw.get("bathrooms"),
                sqft=sqft,
                address=raw.get("address"),
                neighborhood=raw.get("neighborhood"),
                city=raw.get("municipality", "Lisbon"),
                country="Portugal",
                latitude=raw.get("latitude"),
                longitude=raw.get("longitude"),
                amenities=amenities,
                description=raw.get("description"),
                images=raw.get("multimedia", {}).get("images", [])[:5],
                posted_date=self._parse_date(raw.get("modificationDate")),
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.warning(f"Failed to normalize Idealista listing: {e}")
            return None

    def _build_title(self, raw: Dict[str, Any]) -> str:
        """Build a descriptive title from listing data."""
        property_type = raw.get("propertyType", "Apartment")
        rooms = raw.get("rooms", "")
        neighborhood = raw.get("neighborhood", "")

        if rooms and neighborhood:
            return f"{rooms} bedroom {property_type} in {neighborhood}"
        elif rooms:
            return f"{rooms} bedroom {property_type}"
        elif neighborhood:
            return f"{property_type} in {neighborhood}"
        return property_type

    def _extract_amenities(self, raw: Dict[str, Any]) -> Amenities:
        """Parse Idealista features into Amenities."""
        return Amenities(
            laundry_in_unit=raw.get("hasLift", False),  # Approximation
            laundry_in_building=False,
            dishwasher=False,  # Not typically in API response
            parking=raw.get("hasParkingSpace", False) or raw.get("parkingSpace", False),
            gym=False,
            pool=raw.get("hasSwimmingPool", False),
            doorman=False,
            elevator=raw.get("hasLift", False),
            pets_allowed=False,
            air_conditioning=raw.get("hasAirConditioning", False),
        )

    def _sqft_to_sqm(self, sqft: int) -> int:
        """Convert square feet to square meters."""
        return int(sqft * 0.092903)

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse Idealista date format."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
