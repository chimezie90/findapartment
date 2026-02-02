"""Bayut adapter for Dubai apartment listings via RapidAPI."""

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


@register_adapter("bayut")
class BayutAdapter(BaseAdapter):
    """
    Adapter for Bayut (Dubai real estate) via RapidAPI.

    Free tier: 750 calls/month
    API docs: https://rapidapi.com/apidojo/api/bayut
    """

    BASE_URL = "https://bayut.p.rapidapi.com"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.api_key = os.getenv("BAYUT_RAPIDAPI_KEY")
        self.location_ids = city_config.get("bayut", {}).get("location_ids", ["5002"])

        if not self.api_key:
            logger.warning("BAYUT_RAPIDAPI_KEY not set - Bayut adapter disabled")

    def is_available(self) -> bool:
        """Check if API key is configured."""
        return bool(self.api_key)

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch rental listings from Bayut API."""
        if not self.is_available():
            logger.warning("Bayut adapter not available - skipping")
            return []

        apartments = []
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "bayut.p.rapidapi.com",
        }

        for location_id in self.location_ids:
            try:
                logger.info(f"Fetching Bayut listings for location: {location_id}")

                params = {
                    "purpose": "for-rent",
                    "locationExternalIDs": location_id,
                    "categoryExternalID": "4",  # Apartments
                    "priceMin": int(criteria.min_price_local),
                    "priceMax": int(criteria.max_price_local),
                    "roomsMin": criteria.min_bedrooms,
                    "roomsMax": criteria.max_bedrooms,
                    "areaMin": self._sqft_to_sqm(criteria.min_sqft),
                    "rentFrequency": "monthly",
                    "sort": "date-desc",
                    "hitsPerPage": 25,
                }

                response = requests.get(
                    f"{self.BASE_URL}/properties/list",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                for hit in data.get("hits", []):
                    apartment = self._normalize(hit)
                    if apartment:
                        apartments.append(apartment)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.error("Bayut API rate limit exceeded (750 calls/month)")
                else:
                    logger.error(f"Bayut API error: {e}")
            except Exception as e:
                logger.error(f"Error fetching from Bayut location {location_id}: {e}")
                continue

        logger.info(f"Fetched {len(apartments)} listings from Bayut")
        return apartments

    def _normalize(self, raw: Dict[str, Any]) -> Optional[Apartment]:
        """Convert Bayut listing to normalized Apartment model."""
        try:
            # Extract amenities from the amenities array
            raw_amenities = raw.get("amenities", [])
            amenity_texts = [a.get("text", "").lower() for a in raw_amenities]
            amenities = self._extract_amenities(amenity_texts)

            # Get location info
            location = raw.get("location", [])
            neighborhood = location[-1].get("name") if location else None

            # Price is in AED
            price_aed = float(raw.get("price", 0))

            # Area - Bayut returns in sqft
            sqft = raw.get("area")
            if sqft:
                sqft = int(sqft)

            # Build URL
            url_path = raw.get("externalID", "")
            url = f"https://www.bayut.com/property/details-{url_path}.html"

            # Get coordinates
            geography = raw.get("geography", {})

            return Apartment(
                source_id=f"bayut_{raw.get('id', raw.get('externalID', ''))}",
                source_name="bayut",
                title=raw.get("title", ""),
                url=url,
                price_local=price_aed,
                currency="AED",
                price_usd=None,  # Will be filled by currency service
                bedrooms=raw.get("rooms"),
                bathrooms=raw.get("baths"),
                sqft=sqft,
                address=raw.get("location", [{}])[-1].get("name"),
                neighborhood=neighborhood,
                city="Dubai",
                country="UAE",
                latitude=geography.get("lat"),
                longitude=geography.get("lng"),
                amenities=amenities,
                description=raw.get("description"),
                images=[p.get("url") for p in raw.get("photos", [])[:5]],
                posted_date=self._parse_timestamp(raw.get("createdAt")),
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.warning(f"Failed to normalize Bayut listing: {e}")
            return None

    def _extract_amenities(self, amenity_texts: List[str]) -> Amenities:
        """Parse Bayut amenities array."""
        joined = " ".join(amenity_texts)
        return Amenities(
            laundry_in_unit="washing machine" in joined or "laundry" in joined,
            laundry_in_building=False,
            dishwasher="dishwasher" in joined,
            parking="parking" in joined,
            gym="gym" in joined or "fitness" in joined,
            pool="pool" in joined or "swimming" in joined,
            doorman="concierge" in joined or "security" in joined or "24 hour" in joined,
            elevator="elevator" in joined or "lift" in joined,
            pets_allowed="pets allowed" in joined,
            air_conditioning="central a/c" in joined or "air condition" in joined or "a/c" in joined,
        )

    def _sqft_to_sqm(self, sqft: int) -> int:
        """Convert square feet to square meters for API query."""
        return int(sqft * 0.092903)

    def _parse_timestamp(self, timestamp: Optional[int]) -> Optional[datetime]:
        """Parse Unix timestamp in milliseconds."""
        if not timestamp:
            return None
        try:
            return datetime.fromtimestamp(timestamp / 1000)
        except (ValueError, OSError):
            return None
