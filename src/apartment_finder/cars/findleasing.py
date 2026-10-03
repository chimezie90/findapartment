"""findleasing.nu adapter (Danish car leasing marketplace, many dealers).

findleasing.nu's frontend loads listings from a public JSON API at
backend.findleasing.nu/api/v2/listings/ (no auth). `ownership=0` is the
site's own "Privatleasing" filter (1 = business); a listing's own
`ownership` field is 0/1/2 (private / business / both).

Politeness: their robots.txt disallows crawling the search pages
(`/listings?`) and blocks AI crawlers entirely. The API path isn't
disallowed, but we keep the footprint small: run once a day (see .replit),
1.5s between requests, private deals only, and stop at the first error.

Coverage: the API is national. Dealers are filtered by postcode
(`max_dealer_zip`, default 4999 = Copenhagen + the rest of Zealand), since a
lease is picked up and serviced at the dealer.

Liveness: the per-listing endpoint returns 404 for removed listings and
`active`/`deleted` flags otherwise, so it can confirm both gone and live.
"""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from . import register_car_adapter
from .dba import normalize_fuel
from .models import Car

logger = logging.getLogger(__name__)

API_URL = "https://backend.findleasing.nu/api/v2/listings/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "da,en;q=0.8",
}
PRIVATE_OWNERSHIP = 0
PAGE_SIZE = 100  # API maximum
MAX_PAGES = 60  # ~4,200 private deals today; stop well before runaway paging
REQUEST_DELAY_SECONDS = 1.5
RETRY_DELAY_SECONDS = 5
LIVENESS_TIME_BUDGET_SECONDS = 90
DEFAULT_MAX_DEALER_ZIP = 4999

LISTING_ID_RE = re.compile(r"/(\d+)/?$")
GEARBOX_MAP = {"manuel": "manual", "manuelt": "manual", "automatisk": "automatic"}
LEASE_KIND_MAP = {"finansiel": "financial", "operationel": "operational", "månedsleje": "monthly rental"}


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _naive_utc(timestamp: Optional[str]) -> Optional[datetime]:
    """'2026-10-03T14:16:49+02:00' -> naive UTC datetime (the DB stores naive UTC)."""
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_listing(item: Dict[str, Any], city: str, max_dealer_zip: int) -> Optional[Car]:
    """One API result -> Car, or None if inactive, not private, or out of area."""
    if not item.get("active") or item.get("deleted"):
        return None
    if item.get("ownership") not in (0, 2):  # business-only deal
        return None
    if not item.get("id") or not item.get("listing_url") or item.get("price_monthly") is None:
        return None

    address = (item.get("dealer") or {}).get("address") or {}
    zip_code = _to_int(address.get("zip_code"))
    if zip_code is None or zip_code > max_dealer_zip:
        return None

    fuel = item.get("fuel_type")
    gear = (item.get("gear_type") or "").strip().lower()
    funding = (item.get("funding") or "").strip().lower()
    return Car(
        source_id=f"findleasing_{item['id']}",
        source_name="findleasing",
        city=city,
        url=item["listing_url"],
        make=item.get("make"),
        model=item.get("model"),
        variant=item.get("header") or None,
        year=_to_int(item.get("year")),
        mileage_km=_to_int(item.get("mileage")),
        fuel=normalize_fuel(fuel) if fuel else None,
        gearbox=GEARBOX_MAP.get(gear),
        listing_type="lease",
        monthly_price_local=float(item["price_monthly"]),
        down_payment_local=_to_int(item.get("down_payment")),
        term_months=_to_int(item.get("period")),
        km_per_year=_to_int(item.get("kilometers")),
        lease_kind=LEASE_KIND_MAP.get(funding, funding or None),
        currency="DKK",
        location=address.get("city") or None,
        seller_type="dealer",
        thumbnail_url=item.get("thumbnail_image") or None,
        listed_at=_naive_utc(item.get("pub_date")),
    )


@register_car_adapter("findleasing")
class FindleasingCarAdapter:
    """
    Private car leasing deals from findleasing.nu dealers near the city.

    Config (cars.<city>.findleasing):
        max_dealer_zip: highest dealer postcode to include (default 4999)
    """

    source_name = "findleasing"
    full_catalog = True  # every run returns all matching deals (see run_car_pipeline)

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        fl_config = city_config.get("findleasing", {}) or {}
        self.max_dealer_zip = int(fl_config.get("max_dealer_zip", DEFAULT_MAX_DEALER_ZIP))
        self.city_name = city_config.get("display_name", "Copenhagen")
        self.page_errors: List[str] = []  # set by fetch_listings; pipeline treats as failure

    def _get_page(self, page: int) -> Dict[str, Any]:
        """One catalog page. Retries once on timeouts/5xx; never on 429."""
        params = {"ownership": PRIVATE_OWNERSHIP, "page_size": PAGE_SIZE, "page": page}
        for attempt in (1, 2):
            try:
                response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
                if response.status_code >= 500 and attempt == 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                response.raise_for_status()
                data = response.json()
            except requests.Timeout:
                if attempt == 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                raise
            if not isinstance(data, dict) or not isinstance(data.get("results"), list):
                raise ValueError("unexpected response shape (no 'results' list)")
            return data
        raise RuntimeError("unreachable")

    def fetch_listings(self) -> List[Car]:
        cars: Dict[str, Car] = {}
        seen = 0
        expected = None
        for page in range(1, MAX_PAGES + 1):
            if page > 1:
                time.sleep(REQUEST_DELAY_SECONDS)
            try:
                data = self._get_page(page)
            except (requests.RequestException, ValueError) as e:
                logger.error(f"findleasing page {page} failed: {e}")
                self.page_errors.append(f"page {page}: {e}")
                break
            if expected is None:
                expected = data.get("count")
            results = data["results"]
            seen += len(results)
            for item in results:
                car = parse_listing(item, self.city_name, self.max_dealer_zip)
                if car:
                    cars.setdefault(car.source_id, car)
            if not data.get("next"):
                break
        else:
            self.page_errors.append(f"stopped at the {MAX_PAGES}-page cap; catalog may be truncated")
        # Paging that ends early (e.g. a renamed 'next') must not pass as a full catalog
        if not self.page_errors and isinstance(expected, int) and seen < 0.95 * expected:
            self.page_errors.append(f"saw {seen} of {expected} deals; paging ended early")
        logger.info(
            f"Fetched {len(cars)} findleasing deals (of {seen} private deals nationwide)"
        )
        return list(cars.values())

    def check_status(self, urls: List[str]) -> Dict[str, str]:
        """Ask the per-listing API: 404, inactive or deleted -> gone; active -> active."""
        results: Dict[str, str] = {}
        deadline = time.monotonic() + LIVENESS_TIME_BUDGET_SECONDS
        for i, url in enumerate(urls):
            if time.monotonic() > deadline:
                logger.info(f"findleasing liveness budget hit after {i}/{len(urls)} URLs")
                break
            if i:
                time.sleep(REQUEST_DELAY_SECONDS)
            results[url] = "unknown"
            match = LISTING_ID_RE.search(url)
            if not match:
                continue
            try:
                response = requests.get(f"{API_URL}{match.group(1)}/", headers=HEADERS, timeout=(5, 10))
                if response.status_code == 429:
                    logger.warning("findleasing liveness got 429; stopping")
                    del results[url]  # not checked; stays first in line
                    break
                if response.status_code in (404, 410):
                    results[url] = "gone"
                    continue
                if response.status_code != 200:
                    continue
                item = response.json()
            except (requests.RequestException, ValueError) as e:
                logger.debug(f"findleasing liveness check failed for {url}: {e}")
                continue
            # Only trust an explicit boolean; an error page served as 200 stays unknown
            if not isinstance(item, dict) or not isinstance(item.get("active"), bool):
                continue
            if not item["active"] or item.get("deleted"):
                results[url] = "gone"
            else:
                # Live, but must still match our filters (private, nearby, priced)
                matches = parse_listing(item, self.city_name, self.max_dealer_zip) is not None
                results[url] = "active" if matches else "gone"
        return results
