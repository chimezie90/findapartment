"""Balder.dk adapter for Copenhagen apartment listings.

Balder is one of the largest private landlords in Denmark, with buildings
concentrated in Ørestad and Østerbro (plus Copenhagen suburbs). Their site
is a client-rendered Next.js app; listings are loaded from a public
Meilisearch instance at api.balder.dk rather than server-rendered HTML, so
this adapter talks to that search API directly instead of scraping HTML.

The bearer token below is Balder's public, search-only frontend key -- it
ships in their JS bundle to every visitor and only allows read access to
the "leases" index (confirmed via 401 on unauthenticated requests). It can
be overridden via config in case Balder rotates it.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from ..models.apartment import Amenities, Apartment
from ..utils.retry import retry_with_backoff
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)

# DKK to USD conversion rate (approximate)
DKK_TO_USD = 0.14

SEARCH_URL = "https://api.balder.dk/multi-search"
LISTING_BASE_URL = "https://www.balder.dk/lejeboliger"

# Public, search-only Meilisearch key shipped in Balder's frontend bundle.
DEFAULT_API_KEY = "9ea6b13db73f347472ae0555fd5cd5c1c043246f20f83640abada39e5e4b132d"


@register_adapter("balder")
class BalderAdapter(BaseAdapter):
    """
    Adapter for Balder.dk rental listings via their public Meilisearch API.

    Config (city_config["balder"]):
        areas: Optional list of Balder area names to filter to
               (e.g. ["Ørestad", "Østerbro"]). Omit to fetch all Copenhagen
               & Omegn listings.
    """

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.areas: List[str] = city_config.get("balder", {}).get("areas", [])
        self.api_key = config.get("api_key", DEFAULT_API_KEY)
        self.city_name = city_config.get("display_name", "Copenhagen")

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from Balder's search API."""
        apartments = []

        try:
            logger.info(f"Fetching Balder listings (areas={self.areas or 'all'})")
            hits = self._search(criteria)
            for hit in hits:
                apartment = self._normalize(hit)
                if apartment:
                    apartments.append(apartment)
        except Exception as e:
            logger.error(f"Error fetching from Balder: {e}")

        logger.info(f"Fetched {len(apartments)} listings from Balder")
        return apartments

    def _search(self, criteria: SearchCriteria) -> List[Dict[str, Any]]:
        """Query the Meilisearch 'leases' index for available units."""
        min_rent = int(criteria.min_price_local) if criteria.min_price_local else 0
        max_rent = int(criteria.max_price_local) if criteria.max_price_local else 100000

        filter_clauses = [f"status = 'Ledig'", f"(rent >= {min_rent} AND rent <= {max_rent})"]
        if self.areas:
            area_clause = " OR ".join(f"area='{area}'" for area in self.areas)
            filter_clauses.append(f"({area_clause})")

        payload = {
            "queries": [
                {
                    "indexUid": "leases",
                    "filter": [" AND ".join(filter_clauses)],
                    "attributesToHighlight": [],
                    "limit": 100,
                    "offset": 0,
                    "sort": ["acquisition_date:asc"],
                }
            ]
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        try:
            response = requests.post(SEARCH_URL, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Balder listings: {e}")
            return []

        data = response.json()
        results = data.get("results", [])
        if not results:
            return []
        return results[0].get("hits", [])

    def _normalize(self, hit: Dict[str, Any]) -> Optional[Apartment]:
        """Convert a raw Meilisearch hit into a normalized Apartment."""
        try:
            slug = hit.get("slug")
            if not slug:
                return None
            url = f"{LISTING_BASE_URL}/{slug}"

            title = hit.get("headline_en") or hit.get("headline_da") or hit.get("headline") or "Copenhagen Apartment"

            price_dkk = float(hit.get("rent") or 0)
            price_usd = price_dkk * DKK_TO_USD

            gross_area = hit.get("gross_area")
            sqft = int(gross_area * 10.764) if gross_area else None

            geo = hit.get("_geo") or {}

            # Note: acquisition_date is the lease's move-in availability date,
            # not when it was listed, so it can't be used as posted_date
            # (freshness scoring assumes posted_date <= now).
            posted_date = None

            description = hit.get("apartment_text_en") or hit.get("apartment_text_da") or hit.get("apartment_text")

            images = hit.get("images") or []

            amenities = Amenities(
                dishwasher=bool(hit.get("has_dishwasher")),
                parking=bool(hit.get("has_parking")),
                elevator=bool(hit.get("has_elevator")),
                laundry_in_unit=bool(hit.get("has_washing_machine")),
            )

            return Apartment(
                source_id=f"balder_{hit.get('id')}",
                source_name="balder",
                title=title,
                url=url,
                price_local=price_dkk,
                currency="DKK",
                price_usd=price_usd,
                bedrooms=hit.get("number_of_rooms"),
                bathrooms=None,
                sqft=sqft,
                address=hit.get("street"),
                neighborhood=hit.get("area") or hit.get("property_name"),
                city=self.city_name,
                country="Denmark",
                latitude=geo.get("lat", 55.6761),
                longitude=geo.get("lng", 12.5683),
                amenities=amenities,
                description=description,
                images=images,
                thumbnail_url=images[0] if images else None,
                posted_date=posted_date,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Failed to parse Balder listing: {e}")
            return None
