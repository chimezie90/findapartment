"""inberlinwohnen.de adapter for Berlin apartment listings.

inberlinwohnen.de is the shared portal of Berlin's state-owned landlords
(berlinovo, degewo, GESOBAU, Gewobag, HOWOGE, STADT UND LAND, WBM). The
"Wohnungsfinder" is a Laravel Livewire page: each listing card carries its
full data (rents, size, coordinates, WBS requirement) as JSON in a
`wire:snapshot` attribute, and `?page=N` serves the catalog 10 at a time.
robots.txt allows everything. Each run walks the whole catalog (~50 pages).
"""

import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from ..models.apartment import Amenities, Apartment
from ..utils.numbers import SQM_TO_SQFT, parse_german_number
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)

BASE_URL = "https://www.inberlinwohnen.de"
FINDER_URL = f"{BASE_URL}/wohnungsfinder/"
PAGE_DELAY_SECONDS = 1.5
MAX_PAGES = 80  # catalog is ~48 pages of 10; cap guards against a paging loop


def _unwrap(value: Any) -> Any:
    """Livewire snapshots wrap arrays as [value, {"s": "arr"}]; strip that."""
    if isinstance(value, list) and len(value) == 2 and isinstance(value[1], dict) and set(value[1]) == {"s"}:
        return _unwrap(value[0])
    if isinstance(value, list):
        return [_unwrap(v) for v in value]
    if isinstance(value, dict):
        return {k: _unwrap(v) for k, v in value.items()}
    return value


def _details(item: Dict[str, Any]) -> Dict[str, str]:
    """Flatten the nested 'details' list into {label: value}."""
    found = {}

    def walk(node):
        if isinstance(node, dict) and "label" in node:
            found[node["label"]] = node.get("value")
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(item.get("details"))
    return found


@register_adapter("inberlinwohnen")
class InBerlinWohnenAdapter(BaseAdapter):
    """Adapter for the state-owned landlords' portal inberlinwohnen.de."""

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.city_name = city_config.get("display_name", "Berlin")
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        }

    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        apartments = []
        seen_ids = set()
        total = None

        for page in range(1, MAX_PAGES + 1):
            if page > 1:
                time.sleep(PAGE_DELAY_SECONDS)
            try:
                response = requests.get(FINDER_URL, params={"page": page}, headers=self._headers, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"inberlinwohnen page {page} failed: {e}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            if total is None:
                match = re.search(r"von ([\d.]+) Angeboten", soup.get_text())
                total = int(match.group(1).replace(".", "")) if match else None

            cards = soup.select('[id^="apartment-"][wire\\:snapshot]')
            if page == 1 and not cards:
                logger.warning("inberlinwohnen: no listing cards on page 1 -- markup changed or blocked?")
            new_on_page = 0
            for card in cards:
                try:
                    item = _unwrap(json.loads(card["wire:snapshot"])["data"]["item"])
                except (KeyError, ValueError, TypeError) as e:
                    logger.debug(f"Unreadable inberlinwohnen card: {e}")
                    continue
                if item.get("id") in seen_ids:
                    continue
                seen_ids.add(item.get("id"))
                new_on_page += 1

                apartment = self._normalize(item)
                if not apartment:
                    continue
                if criteria.min_price_local and apartment.price_local < criteria.min_price_local:
                    continue
                if criteria.max_price_local and apartment.price_local > criteria.max_price_local:
                    continue
                apartments.append(apartment)

            # Past the last page Livewire serves the last page again (or nothing)
            if not new_on_page or (total and len(seen_ids) >= total):
                break

        logger.info(
            f"Fetched {len(apartments)} listings from inberlinwohnen "
            f"({len(seen_ids)} scanned, site total {total})"
        )
        return apartments

    def check_status(self, urls: List[str]) -> Dict[str, str]:
        # Listings deep-link to seven different landlord sites with their own
        # removal behaviour. Every run walks the whole catalog, so a listing
        # that drops off it ages out of "active" via last_seen instead.
        return {}

    def _normalize(self, item: Dict[str, Any]) -> Optional[Apartment]:
        details = _details(item)
        # Gesamtmiete = warm rent (cold + service charges + heating), the
        # figure a tenant actually pays each month
        # (item["rentGross"] leaves heating out, so it's no fallback)
        rent = parse_german_number(details.get("Gesamtmiete"))
        url = item.get("deeplink")
        if not rent or not url:
            return None

        address = item.get("address") or {}
        street = " ".join(p for p in (address.get("street"), address.get("number")) if p)
        district = address.get("district")
        rooms = parse_german_number(item.get("rooms"))
        area_sqm = parse_german_number(item.get("area"))
        company = (item.get("company") or {}).get("name", "").strip()
        wbs = details.get("WBS") or ""

        lines = [f"Landlord: {company}" if company else None]
        cold = parse_german_number(item.get("rentNet"))
        extra = parse_german_number(item.get("extraCosts")) or 0
        if cold:
            heating = rent - cold - extra
            lines.append(
                f"Warm rent €{rent:,.2f} = cold €{cold:,.2f} + service charges €{extra:,.2f}"
                + (f" + heating €{heating:,.2f}" if heating > 0.5 else "")
            )
        # A WBS (Wohnberechtigungsschein) is an income-tested social-housing
        # permit -- without one, "erforderlich" listings can't be rented
        if wbs:
            lines.append(f"WBS (social-housing permit): {wbs}")
        for label in ("Bezugsfertig ab", "Etage", "Baujahr", "Heizung"):
            if details.get(label):
                lines.append(f"{label}: {re.sub(r'<[^>]+>', '', str(details[label]))}")

        posted = None
        if item.get("createdAt"):
            try:
                posted = datetime.fromisoformat(item["createdAt"].replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                pass

        image = item.get("imagePath")

        return Apartment(
            source_id=f"inberlinwohnen_{item.get('id')}",
            source_name="inberlinwohnen",
            title=item.get("title") or f"{rooms or ''} Zimmer, {district or 'Berlin'}",
            url=url,
            price_local=rent,
            currency="EUR",
            price_usd=None,
            # Total "Zimmer" incl. living room, like Balder's Danish "værelser"
            bedrooms=int(rooms) if rooms else None,
            bathrooms=float(item["bathrooms"]) if item.get("bathrooms") else None,
            sqft=int(area_sqm * SQM_TO_SQFT) if area_sqm else None,
            address=", ".join(p for p in (street, address.get("zipCode"), district) if p) or None,
            neighborhood=district,
            city=self.city_name,
            country="Germany",
            latitude=float(address["lat"]) if address.get("lat") else None,
            longitude=float(address["lon"]) if address.get("lon") else None,
            amenities=Amenities(),
            description="\n".join(line for line in lines if line),
            images=[],
            thumbnail_url=f"{BASE_URL}/img/{image}" if image else None,
            posted_date=posted,
            fetched_at=datetime.utcnow(),
        )
