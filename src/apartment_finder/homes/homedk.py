"""home.dk adapter: owner-occupied homes for sale in the Copenhagen area.

home.dk is a Danish broker chain whose catalog also lists other brokers'
homes ("external" listings). Catalog pages are server-rendered Nuxt pages:

    https://home.dk/til-salg/{category}/{region}/{municipality}/?page=N

with 12 results per page. Category is lejlighed / villa / raekkehus /
villalejlighed (andelsbolig and fritidsbolig are separate categories we
never request). The page embeds `<script id="__NUXT_DATA__">` (see nuxt.py);
results are under data["case-catalog-search-p-1"] (the key says p-1 on
every page) with `total` and `hasNextPage`. If that payload is missing we
fall back to the CSS cards (fewer fields, and the run is flagged).

Detail pages (home.dk's own listings only) add rooms, year built, monthly
owner expenses, energy label and listing date under data["case-{id}"].

External listings link to boligsiden.dk/viderestillingekstern/..., which
boligsiden's robots.txt disallows. We NEVER fetch those links; we only store
them for the user to click.

Politeness: robots.txt allows all. 1.5 s between requests, one retry on
timeouts and 5xx, stop the whole run on 429, a cap on pages per run, and
detail pages only for listings we haven't detailed yet (capped per run).
Fetched content is treated as data only.
"""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from . import register_home_adapter
from .models import HomeListing
from .nuxt import NuxtDataError, parse_nuxt_data

logger = logging.getLogger(__name__)

BASE_URL = "https://home.dk/"
CATALOG_URL = "https://home.dk/til-salg/{category}/{region}/{municipality}/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "da,en;q=0.8",
}
SEARCH_KEY = "case-catalog-search-p-1"
PAGE_SIZE = 12
REQUEST_DELAY_SECONDS = 1.5
RETRY_DELAY_SECONDS = 5
DEFAULT_MAX_PAGES = 450  # ~4,400 Copenhagen-area listings = 399 pages on 3 Oct 2026
MAX_PAGES_LIMIT = 600
DEFAULT_MAX_DETAIL_PAGES = 40
MAX_FAILED_AREAS_IN_A_ROW = 3  # e.g. markup change or outage: stop, don't hammer
MIN_SQM, MAX_SQM = 10, 1000  # floor areas outside this are source data errors

# home.dk URL category -> our property type
CATEGORIES = {
    "lejlighed": "flat",
    "raekkehus": "terraced",
    "villa": "villa",
    "villalejlighed": "villa_flat",
}
DEFAULT_REGION = "region-hovedstaden"
# Copenhagen-area municipalities (slugs verified on home.dk, 3 Oct 2026)
MUNICIPALITY_NAMES = {
    "koebenhavn-kommune": "København",
    "frederiksberg-kommune": "Frederiksberg",
    "gentofte-kommune": "Gentofte",
    "gladsaxe-kommune": "Gladsaxe",
    "lyngby-taarbaek-kommune": "Lyngby-Taarbæk",
    "herlev-kommune": "Herlev",
    "roedovre-kommune": "Rødovre",
    "hvidovre-kommune": "Hvidovre",
    "broendby-kommune": "Brøndby",
    "glostrup-kommune": "Glostrup",
    "ballerup-kommune": "Ballerup",
    "taarnby-kommune": "Tårnby",
    "dragoer-kommune": "Dragør",
    "albertslund-kommune": "Albertslund",
}
# Result `type` values we never want, whatever category page they show up on
EXCLUDED_TYPE_WORDS = ("cooperative", "andel", "holiday", "fritid", "leisure", "summer")

CASE_NUMBER_RE = re.compile(r"/sag-(\d+)/?$")
EXTERNAL_ID_RE = re.compile(r"/viderestillingekstern/([0-9a-fA-F-]{16,})/?$")
POSTCODE_RE = re.compile(r",\s*(\d{4})\s+([^,]+)$")
SLUG_RE = re.compile(r"^[a-z0-9-]+$")
SQM_RE = re.compile(r"^([\d.]+)\s*m(?:2|²)$")
MINDWORKING_HOST = "https://home.mindworking.eu/"


class RateLimited(Exception):
    """home.dk answered 429 (slow down) or 403 (blocked): stop the whole run."""


def _to_int(value: Any) -> Optional[int]:
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError, OverflowError):  # NaN/inf from the payload too
        return None
    return number


def _plausible_sqm(sqm: Optional[int]) -> Optional[int]:
    """Floor area, or None if implausible. home.dk has listed a terraced house
    at 1,557 m2 (built-up area 60 m2), which would wreck price-per-m2 sorting."""
    return sqm if sqm is not None and MIN_SQM <= sqm <= MAX_SQM else None


def _amount(money: Any) -> Optional[int]:
    """{"amount": 4795000, "displayValue": "..."} -> 4795000 (None if missing or 0)."""
    if isinstance(money, dict):
        amount = _to_int(money.get("amount"))
        return amount if amount else None
    return None


def _naive_utc(timestamp: Any) -> Optional[datetime]:
    """'2026-10-03T06:28:05.4997314Z' -> naive UTC datetime."""
    if not isinstance(timestamp, str) or not timestamp:
        return None
    text = timestamp.strip().replace("Z", "+00:00")
    # Python 3.9's fromisoformat takes exactly 3 or 6 fractional digits
    text = re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), text, count=1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _listing_url(raw: Any) -> Optional[str]:
    """Absolute http(s) URL for a result's `url`; anything else is rejected."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    url = raw.strip()
    if url.startswith(("https://", "http://")):
        return url
    if "://" in url or url.startswith(("//", "javascript:", "data:")):
        return None
    return BASE_URL + url.lstrip("/")


def _source_id(url: str, fallback_id: Any = None) -> Optional[str]:
    """Stable id from the listing URL, the same in NUXT and CSS mode."""
    path = url.split("?")[0]
    match = CASE_NUMBER_RE.search(path)
    if match:
        return f"homedk_{match.group(1)}"
    match = EXTERNAL_ID_RE.search(path)
    if match:
        return f"homedk_x_{match.group(1).lower()}"
    cleaned = re.sub(r"[^A-Za-z0-9-]", "", str(fallback_id or ""))
    return f"homedk_{cleaned}" if cleaned else None


def _thumbnail(media: Any) -> Optional[str]:
    """First catalog photo; home's own image host is asked for a 240 px copy (~15 KB)."""
    if not isinstance(media, list):
        return None
    for item in media:
        url = item.get("url") if isinstance(item, dict) else None
        if isinstance(url, str) and url.startswith("https://"):
            if url.startswith(MINDWORKING_HOST) and "?" not in url:
                url += "?w=240"
            return url
    return None


def _split_address(full: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """'Malttorvet 16, 2. mf., 1799 København V' -> (street part, '1799', 'København V')."""
    match = POSTCODE_RE.search(full or "")
    if not match:
        return None, None, None
    return full[:match.start()].strip() or None, match.group(1), match.group(2).strip()


def _excluded_type(raw_type: Any) -> bool:
    lowered = str(raw_type or "").lower()
    return any(word in lowered for word in EXCLUDED_TYPE_WORDS)


def parse_result(item: Dict[str, Any], property_type: str, municipality: Optional[str]) -> Optional[HomeListing]:
    """One catalog result -> HomeListing, or None if it isn't an owner-occupied
    home for sale with a price."""
    if not isinstance(item, dict):
        return None
    if item.get("isRentalCase") or item.get("isBusinessCase") or item.get("isPlot"):
        return None
    if _excluded_type(item.get("type")):
        return None
    price = _amount((item.get("offer") or {}).get("price"))
    url = _listing_url(item.get("url"))
    address = item.get("address") or {}
    full = (address.get("full") or "").strip() if isinstance(address, dict) else ""
    if not price or not url or not full:
        return None
    source_id = _source_id(url, item.get("id"))
    if not source_id:
        return None

    street, postcode, city = _split_address(full)
    stats = item.get("stats") or {}
    is_external = bool(item.get("isExternal"))
    return HomeListing(
        source_id=source_id,
        source_name="homedk",
        url=url,
        address=full,
        property_type=property_type,
        price_dkk=price,
        street=street,
        postcode=str(address.get("postalCode") or postcode or "") or None,
        city=address.get("city") or city,
        municipality=municipality,
        sqm=_plausible_sqm(_to_int(stats.get("floorArea"))),
        latitude=address.get("latitude") if isinstance(address.get("latitude"), (int, float)) else None,
        longitude=address.get("longitude") if isinstance(address.get("longitude"), (int, float)) else None,
        is_external=is_external,
        broker=None if is_external else "home",
        headline=(item.get("headline") or None) if not is_external else None,
        thumbnail_url=_thumbnail(item.get("presentationMedia")),
    )


def parse_catalog_page(html: str, property_type: str, municipality: Optional[str]) -> Dict[str, Any]:
    """
    Parse one catalog page.

    Returns {"homes": [...], "results": raw result count, "total": int|None,
             "has_next": bool, "fallback": bool}.
    Raises NuxtDataError if neither the payload nor any CSS card is found.
    """
    try:
        data = parse_nuxt_data(html)
        search = (data.get("data") or {}).get(SEARCH_KEY) if isinstance(data, dict) else None
        if not isinstance(search, dict) or not isinstance(search.get("results"), list):
            raise NuxtDataError(f"no {SEARCH_KEY} results in the payload")
    except NuxtDataError as e:
        homes = parse_catalog_cards(html, property_type, municipality)
        if homes is None:
            raise
        logger.warning(f"home.dk NUXT payload unusable ({e}); used CSS cards")
        return {"homes": homes, "results": len(homes), "total": None,
                "has_next": len(homes) >= PAGE_SIZE, "fallback": True}

    results = search["results"]
    homes = []
    parse_errors = 0
    for item in results:
        try:
            home = parse_result(item, property_type, municipality)
        except Exception as e:  # one odd result must not sink the page
            logger.warning(f"home.dk result skipped, unexpected shape: {type(e).__name__}: {e}")
            parse_errors += 1
            continue
        if home:
            homes.append(home)
    total = search.get("total")
    return {
        "homes": homes,
        "results": len(results),
        "total": total if isinstance(total, int) else None,
        "has_next": bool(search.get("hasNextPage")),
        "fallback": False,
        "parse_errors": parse_errors,
    }


def _parse_danish_int(text: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", text or "")
    return int(digits) if digits else None


def parse_catalog_cards(html: str, property_type: str, municipality: Optional[str]) -> Optional[List[HomeListing]]:
    """CSS fallback. Returns None if the page has no property cards at all."""
    soup = BeautifulSoup(html or "", "html.parser")
    cards = soup.select("div.property-card")
    if not cards:
        return None
    homes = []
    for card in cards:
        link = card.select_one("a.property-card__details[href]")
        address_el = card.select_one("p.property-card-details__address")
        if not link or not address_el:
            continue
        url = _listing_url(link["href"])
        full = address_el.get_text(" ", strip=True)
        lines = [p.get_text(" ", strip=True) for p in card.select(".property-card-details__description p")]
        sqm = price = None
        raw_type = lines[0] if lines else ""
        for line in lines:
            lowered = line.lower()
            sqm_match = SQM_RE.match(lowered)
            if sqm_match:
                sqm = _plausible_sqm(_parse_danish_int(sqm_match.group(1)))  # "147 m2": not the 2
            elif lowered.endswith("kr."):
                price = _parse_danish_int(line)
        if not url or not full or not price or _excluded_type(raw_type):
            continue
        source_id = _source_id(url)
        if not source_id:
            continue
        street, postcode, city = _split_address(full)
        is_external = not url.startswith(BASE_URL)
        homes.append(HomeListing(
            source_id=source_id, source_name="homedk", url=url, address=full,
            property_type=property_type, price_dkk=price, street=street,
            postcode=postcode, city=city, municipality=municipality, sqm=sqm,
            is_external=is_external, broker=None if is_external else "home",
        ))
    return homes


def parse_detail_page(html: str, case_id: str) -> Dict[str, Any]:
    """Fields from a home.dk listing page that the catalog lacks.

    Returns a dict with rooms, year_built, monthly_owner_expenses_dkk,
    energy_label, listed_at (each may be None), plus under_offer (sold
    subject to conditions: "solgt med forbehold", still in the catalog) and
    off_market (sold, or no longer for sale and not under offer).
    Raises NuxtDataError if the case isn't in the payload.
    """
    data = parse_nuxt_data(html)
    case = (data.get("data") or {}).get(f"case-{case_id}") if isinstance(data, dict) else None
    if not isinstance(case, dict):
        raise NuxtDataError(f"no case-{case_id} in the payload")
    stats = case.get("stats") if isinstance(case.get("stats"), dict) else {}
    offer = case.get("offer") if isinstance(case.get("offer"), dict) else {}
    year = stats.get("yearBuilt")
    year_built = None
    if isinstance(year, str) and re.match(r"^\d{4}", year):
        year_built = int(year[:4])
    elif isinstance(year, int):
        year_built = year
    if year_built is not None and not 1500 <= year_built <= 2100:
        year_built = None
    label = stats.get("energyLabel")
    label = label.strip().upper()[:10] if isinstance(label, str) and label.strip() else None
    rooms = _to_int(stats.get("rooms"))
    under_offer = case.get("isUnderSale") is True
    off_market = case.get("isSold") is True or (case.get("isForSale") is False and not under_offer)
    return {
        "rooms": rooms if rooms and 0 < rooms < 100 else None,
        "year_built": year_built,
        "monthly_owner_expenses_dkk": _amount(offer.get("ownerCostsTotalMonthlyAmount")),
        "energy_label": label,
        "listed_at": _naive_utc(case.get("listingDate")),
        "under_offer": under_offer,
        "off_market": off_market,
    }


@register_home_adapter("homedk")
class HomeDkAdapter:
    """
    Homes for sale on home.dk in the configured municipalities.

    Config (homes.<city>.homedk):
        region: home.dk region slug (default "region-hovedstaden")
        municipalities: list of municipality slugs (default: the 14 above)
        categories: list of lejlighed / villa / raekkehus / villalejlighed
        max_pages: catalog pages per run (default 450, capped at 600)
        max_detail_pages: detail pages per run (default 40)
    """

    source_name = "homedk"
    full_catalog = True  # every clean run returns every matching listing

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        hd_config = city_config.get("homedk", {}) or {}
        self.region = str(hd_config.get("region", DEFAULT_REGION))
        self.municipalities = list(hd_config.get("municipalities") or MUNICIPALITY_NAMES)
        self.categories = list(hd_config.get("categories") or CATEGORIES)
        for slug in [self.region, *self.municipalities]:
            if not SLUG_RE.match(str(slug)):
                raise ValueError(f"bad home.dk slug in config: {slug!r}")
        unknown = [c for c in self.categories if c not in CATEGORIES]
        if unknown:
            raise ValueError(f"unknown home.dk categories {unknown}; use {list(CATEGORIES)}")
        self.max_pages = min(int(hd_config.get("max_pages", DEFAULT_MAX_PAGES)), MAX_PAGES_LIMIT)
        self.max_detail_pages = int(hd_config.get("max_detail_pages", DEFAULT_MAX_DETAIL_PAGES))
        self.page_errors: List[str] = []  # set by fetch_listings; pipeline treats as failure
        self.request_count = 0
        self.rate_limited = False  # set on a 429/403; no more requests this run
        self.detail_errors = 0
        self._not_found: Dict[str, int] = {}  # municipality slug -> categories that 404ed
        self._last_request = 0.0

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        """GET with the politeness delay. Retries once on timeouts and 5xx.
        Follows redirects only within home.dk (never to boligsiden).
        Raises RateLimited on 429 or 403, requests.RequestException otherwise."""
        attempt, redirects = 1, 0
        while True:
            wait = REQUEST_DELAY_SECONDS - (time.monotonic() - self._last_request)
            if self._last_request and wait > 0:
                time.sleep(wait)
            self.request_count += 1
            try:
                response = requests.get(url, params=params, headers=HEADERS, timeout=30,
                                        allow_redirects=False)
            except (requests.Timeout, requests.ConnectionError):
                self._last_request = time.monotonic()
                if attempt == 1:
                    attempt += 1
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                raise
            self._last_request = time.monotonic()
            if response.status_code in (429, 403):
                # 429 = slow down; 403 = bot wall. Either way, stop the run.
                self.rate_limited = True
                raise RateLimited(f"HTTP {response.status_code} from {url}")
            if response.status_code in (301, 302, 303, 307, 308):
                target = urljoin(response.url or url, response.headers.get("Location", ""))
                if not target.startswith(BASE_URL) or redirects >= 3:
                    raise requests.RequestException(f"redirect to {target[:100]} not followed")
                url, params, redirects = target, None, redirects + 1
                continue
            if response.status_code >= 500 and attempt == 1:
                attempt += 1
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            response.raise_for_status()
            return response.text

    def fetch_listings(self) -> List[HomeListing]:
        homes: Dict[str, HomeListing] = {}
        pages = 0
        areas = [(category, slug) for category in self.categories for slug in self.municipalities]
        failed_in_a_row = 0
        try:
            for category, slug in areas:
                if pages >= self.max_pages:
                    self.page_errors.append(f"stopped at the {self.max_pages}-page cap; catalog truncated")
                    break
                errors_before = len(self.page_errors)
                pages += self._fetch_area(category, slug, homes, self.max_pages - pages)
                failed_in_a_row = failed_in_a_row + 1 if len(self.page_errors) > errors_before else 0
                if failed_in_a_row >= MAX_FAILED_AREAS_IN_A_ROW:
                    self.page_errors.append(f"stopped after {failed_in_a_row} failing areas in a row")
                    break
        except RateLimited as e:
            logger.error(f"home.dk refused us; stopping the run: {e}")
            self.page_errors.append(f"rate limited or blocked ({e}) after {self.request_count} requests")
        for slug, misses in self._not_found.items():
            if misses == len(self.categories):
                self.page_errors.append(f"{slug}: not found in any category (wrong slug?)")
        logger.info(f"Fetched {len(homes)} home.dk listings ({pages} catalog pages, "
                    f"{self.request_count} requests)")
        return list(homes.values())

    def _fetch_area(self, category: str, slug: str, homes: Dict[str, HomeListing], page_budget: int) -> int:
        """Walk one category x municipality. Returns pages fetched."""
        property_type = CATEGORIES[category]
        municipality = MUNICIPALITY_NAMES.get(slug) or slug.replace("-kommune", "").replace("-", " ").title()
        url = CATALOG_URL.format(category=category, region=self.region, municipality=slug)
        area = f"{category}/{slug}"
        seen = 0
        total = None
        page = 0
        while page < page_budget:
            page += 1
            try:
                html = self._get(url, params={"page": page} if page > 1 else None)
                parsed = parse_catalog_page(html, property_type, municipality)
            except RateLimited:
                raise
            except requests.HTTPError as e:
                if page == 1 and getattr(e.response, "status_code", None) == 404:
                    # home.dk 404s a type with no listings in a municipality
                    # (no villa flats in Herlev). Wrong slugs are caught in
                    # fetch_listings: they 404 in every category.
                    logger.info(f"home.dk {area}: no listings (404)")
                    self._not_found[slug] = self._not_found.get(slug, 0) + 1
                    return page
                logger.error(f"home.dk {area} page {page} failed: {e}")
                self.page_errors.append(f"{area} page {page}: {e}")
                return page
            except (requests.RequestException, NuxtDataError) as e:
                logger.error(f"home.dk {area} page {page} failed: {e}")
                self.page_errors.append(f"{area} page {page}: {e}")
                return page
            if parsed["fallback"] and not any("CSS fallback" in err for err in self.page_errors):
                self.page_errors.append(f"{area}: NUXT payload missing, used CSS fallback (fewer fields)")
            if parsed.get("parse_errors"):
                # A result we couldn't read is still live: an unclean run
                # keeps it (and everything else) from being marked gone
                self.page_errors.append(f"{area} page {page}: {parsed['parse_errors']} results unreadable")
            if total is None:
                total = parsed["total"]
            seen += parsed["results"]
            for home in parsed["homes"]:
                homes.setdefault(home.source_id, home)
            if not parsed["has_next"] or parsed["results"] == 0:
                break
        else:
            self.page_errors.append(f"{area}: page cap reached at page {page}; catalog truncated")
            return page
        logger.info(f"home.dk {area}: {seen} of {total} listings in {page} pages")
        # Paging that ends early must not pass as a full catalog. A small
        # slack covers listings added or removed while we page.
        if isinstance(total, int) and total - seen > max(2, 0.01 * total):
            self.page_errors.append(f"{area}: saw {seen} of {total} listings; paging ended early")
        return page

    def fetch_details(self, rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Detail fields for home.dk's own listings ({source_id: fields}).

        `rows` need source_id and url. External (boligsiden) URLs are skipped,
        never fetched. Stops at the per-run cap or on 429/403.

        A page that can't be parsed or answers 4xx is returned as
        {"failed": True} so it isn't retried every run; timeouts and 5xx are
        left out and retried next run. Failures are counted in detail_errors."""
        details: Dict[str, Dict[str, Any]] = {}
        if self.rate_limited:
            return details
        for row in rows[: self.max_detail_pages]:
            url, source_id = row.get("url") or "", row.get("source_id") or ""
            match = CASE_NUMBER_RE.search(url.split("?")[0])
            if not url.startswith(BASE_URL) or not match:
                continue
            try:
                details[source_id] = parse_detail_page(self._get(url), match.group(1))
            except RateLimited as e:
                logger.warning(f"home.dk detail pages refused; stopping: {e}")
                self.detail_errors += 1
                break
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status in (404, 410):
                    details[source_id] = {"gone": True}
                else:
                    logger.warning(f"home.dk detail {url} failed: {e}")
                    self.detail_errors += 1
                    if status is not None and status < 500:
                        details[source_id] = {"failed": True}
            except NuxtDataError as e:
                logger.warning(f"home.dk detail {url} unparseable: {e}")
                self.detail_errors += 1
                details[source_id] = {"failed": True}
            except requests.RequestException as e:
                logger.warning(f"home.dk detail {url} failed: {e}")
                self.detail_errors += 1
        logger.info(f"Fetched {len(details)} home.dk detail pages ({self.detail_errors} errors)")
        return details
