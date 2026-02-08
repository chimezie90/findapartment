"""CASA SAPO adapter for Lisbon apartment listings."""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

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

    Extracts listing cards from HTML (detail URLs from <a> tags with
    "See Apartment" titles) and enriches with JSON-LD Offer data
    (price, geo, description) matched via property image ID.
    """

    BASE_URL = "https://casa.sapo.pt"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.city_name = city_config.get("display_name", "Lisbon")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
        }

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from CASA SAPO."""
        apartments = []

        for pg in range(1, 3):
            url = f"{self.BASE_URL}/en-gb/rent-apartments/lisboa/"
            if pg > 1:
                url += f"?pn={pg}"

            try:
                logger.info(f"Fetching CASA SAPO page {pg} for Lisbon")
                response = requests.get(url, headers=self._headers, timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                page_listings = self._extract_listings(soup)

                logger.debug(f"Page {pg}: found {len(page_listings)} listings")

                for item in page_listings:
                    apartment = self._normalize(item, criteria)
                    if apartment:
                        apartments.append(apartment)

            except Exception as e:
                logger.error(f"Error fetching CASA SAPO page {pg}: {e}")

        logger.info(f"Fetched {len(apartments)} listings from CASA SAPO")
        return apartments

    def _extract_listings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract listings by combining HTML detail links with JSON-LD data.

        Strategy:
        1. Find <a> tags with title containing "See " and href containing ".html"
           — these wrap listing cards and contain the real detail URL (inside a
           gespub.casa.sapo.pt redirect wrapper).
        2. Extract the property ID (PID) from the <img> inside each card.
        3. Parse JSON-LD Offer objects and index them by PID.
        4. Merge: each card gets the detail URL from HTML + data from JSON-LD.
        """
        # Step 1+2: Extract listing cards with detail URLs and PIDs
        cards = {}  # pid -> {url, title, thumbnail}
        for a_tag in soup.select('a[title*="See "][href*=".html"]'):
            href = a_tag.get("href", "")
            title = a_tag.get("title", "")

            # Extract real URL from redirect wrapper:
            # gespub...?...&l=https://casa.sapo.pt/en-gb/rent-apartment-...-UUID.html
            real_url = None
            url_match = re.search(
                r'l=(https?://casa\.sapo\.pt/[^\s&]+\.html)', href
            )
            if url_match:
                real_url = unquote(url_match.group(1))
            elif href.startswith("https://casa.sapo.pt/") and href.endswith(".html"):
                real_url = href

            if not real_url:
                continue

            # Extract UUID from URL for dedup
            uuid_match = re.search(
                r'-([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\.html',
                real_url,
            )
            if not uuid_match:
                continue
            uuid = uuid_match.group(1)

            if uuid in cards:
                # Already have this listing, but check if we can grab the PID
                if not cards[uuid].get("pid"):
                    img = a_tag.select_one('img[src*="casasapo"]')
                    if img:
                        pid_match = re.search(r'/P(\d+)/', img.get("src", ""))
                        if pid_match:
                            cards[uuid]["pid"] = pid_match.group(1)
                            cards[uuid]["thumbnail"] = img.get("src", "")
                continue

            # Get property image PID
            pid = None
            thumbnail = None
            img = a_tag.select_one('img[src*="casasapo"]')
            if img:
                pid_match = re.search(r'/P(\d+)/', img.get("src", ""))
                if pid_match:
                    pid = pid_match.group(1)
                    thumbnail = img.get("src", "")

            # Clean title: "See Apartment 2 Bedrooms for rent in Lisboa, ..."
            clean_title = re.sub(r'^See\s+', '', title)
            clean_title = re.sub(r'\s+for rent in\s+', ' in ', clean_title)

            cards[uuid] = {
                "url": real_url,
                "title": clean_title,
                "pid": pid,
                "thumbnail": thumbnail,
            }

        logger.debug(f"Found {len(cards)} unique listing cards from HTML")

        # Step 3: Parse JSON-LD Offers and index by PID
        offers_by_pid = {}
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
                if item_type != "Offer":
                    continue

                image_url = item.get("image", "")
                pid_match = re.search(r'/P(\d+)/', image_url)
                if pid_match:
                    offers_by_pid[pid_match.group(1)] = item

        logger.debug(f"Found {len(offers_by_pid)} JSON-LD offers")

        # Step 4: Merge cards with JSON-LD data
        merged = []
        for uuid, card in cards.items():
            entry = {
                "_detail_url": card["url"],
                "_card_title": card["title"],
                "_thumbnail": card["thumbnail"],
                "_pid": card["pid"],
            }

            # Enrich with JSON-LD data if we have a matching PID
            if card["pid"] and card["pid"] in offers_by_pid:
                offer = offers_by_pid[card["pid"]]
                entry["name"] = offer.get("name", card["title"])
                entry["price"] = offer.get("price", [])
                entry["description"] = offer.get("description")
                entry["image"] = offer.get("image")
                entry["availableAtOrFrom"] = offer.get("availableAtOrFrom", {})
            else:
                entry["name"] = card["title"]

            merged.append(entry)

        return merged

    def _normalize(
        self, raw: Dict[str, Any], criteria: SearchCriteria = None
    ) -> Optional[Apartment]:
        """Convert a merged listing to normalized Apartment model."""
        try:
            title = raw.get("name") or raw.get("_card_title", "Lisbon Apartment")

            # Use PID for stable IDs, fall back to URL hash
            listing_id = raw.get("_pid")
            if not listing_id:
                listing_id = str(abs(hash(raw.get("_detail_url", title))))[-10:]

            url = raw.get("_detail_url", self.BASE_URL)

            # Parse price from JSON-LD: ["3.000 €"] → 3000
            price_eur = 0.0
            price_raw = raw.get("price", [])
            if isinstance(price_raw, list):
                price_text = price_raw[0] if price_raw else "0"
            else:
                price_text = str(price_raw)

            price_cleaned = re.sub(r'[€EUR\s]', '', price_text)
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

            # Bedrooms from title
            bedrooms = None
            bed_match = re.search(r'(\d+)\s*Bedroom', title, re.IGNORECASE)
            if bed_match:
                bedrooms = int(bed_match.group(1))
            elif re.search(r'\bstudio\b', title, re.IGNORECASE):
                bedrooms = 0

            # Geo coordinates from JSON-LD
            available_from = raw.get("availableAtOrFrom", {})
            geo = available_from.get("geo", {}) if isinstance(available_from, dict) else {}
            latitude = geo.get("latitude")
            longitude = geo.get("longitude")

            # Thumbnail: prefer JSON-LD image, fall back to card image
            thumbnail_url = raw.get("image") or raw.get("_thumbnail")
            images = [thumbnail_url] if thumbnail_url else []

            # Description from JSON-LD
            description = raw.get("description")

            # Neighborhood from address or title
            neighborhood = None
            if isinstance(available_from, dict):
                address_data = available_from.get("address", {})
                if isinstance(address_data, dict):
                    neighborhood = address_data.get("addressRegion")
            if not neighborhood:
                loc_match = re.search(
                    r'Bedroom[s]?\s+(?:in\s+)?(.+?)(?:,\s*Lisboa)?$', title, re.IGNORECASE
                )
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
            logger.debug(f"Error normalizing CASA SAPO listing: {e}")
            return None
