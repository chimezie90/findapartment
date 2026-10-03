"""DBA.dk car adapter (Denmark's largest classifieds site, cars for sale).

The search page https://www.dba.dk/mobility/search/car is server-rendered:
~50 <article> cards per page, each with make/model, variant, a spec line
("2016 ∙ 136.000 km ∙ Benzin ∙ Manuelt"), price in DKK, location, seller
type, and listing age. We fetch the newest few pages for the configured
region and filter in the UI.

Region filter: `location=0.200001` is DBA's "København og omegn" region
(found in the page's own location filter links).

Liveness: DBA returns 404 ("Siden blev ikke fundet") for item ids that don't
exist, and 200 for live items. The live item page carries no "sold" marker,
so a 200 can't positively confirm a listing is live -- only 404/410 count.
"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from ..adapters.base import check_urls_by_http_status
from . import register_car_adapter
from .models import Car

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.dba.dk/mobility/search/car"
ITEM_URL = "https://www.dba.dk/mobility/item/{id}"
COPENHAGEN_LOCATION = "0.200001"  # "København og omegn"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "da,en;q=0.8",
}
PAGE_DELAY_SECONDS = 1.5
MAX_PAGES_LIMIT = 3

ITEM_ID_RE = re.compile(r"/mobility/item/(\d+)")
YEAR_RE = re.compile(r"^(19|20)\d{2}$")
KM_RE = re.compile(r"^([\d.]+)\s*km$")
AGE_RE = re.compile(r"(\d+)\s*(min|t|time|timer|dag|dage|uge|uger|md|mdr|måned|måneder|år)\b", re.I)

# Makes whose name is more than one word, so "Land Rover Range Rover"
# splits into make "Land Rover" / model "Range Rover".
MULTI_WORD_MAKES = ("Land Rover", "Alfa Romeo", "Aston Martin", "Rolls Royce", "Lynk & Co")

GEARBOX_MAP = {"manuelt": "manual", "automatisk": "automatic"}
SELLER_MAP = {"privat": "private", "forhandler": "dealer"}


def parse_danish_int(text: str) -> Optional[int]:
    """'65.000' -> 65000 ('.' is the Danish thousands separator)."""
    digits = re.sub(r"[^\d]", "", text or "")
    return int(digits) if digits else None


def normalize_fuel(raw: str) -> str:
    lowered = raw.strip().lower()
    if lowered.startswith("plug-in"):
        return "Plug-in hybrid"
    if "hybrid" in lowered:
        return "Hybrid"
    return {"benzin": "Petrol", "diesel": "Diesel", "el": "Electric"}.get(lowered, raw.strip())


def split_make_model(title: str) -> tuple:
    title = " ".join(title.split())
    for make in MULTI_WORD_MAKES:
        if title.lower().startswith(make.lower() + " "):
            return make, title[len(make):].strip() or None
    make, _, model = title.partition(" ")
    return (make or None), (model or None)


def parse_spec_line(text: str) -> Dict[str, Any]:
    """'2014 ∙ 74.000 km ∙ 999 cc ∙ Benzin ∙ Manuelt' -> year/km/fuel/gearbox.
    EVs and plug-ins show a range ('449 km rækkevide') instead of a gearbox."""
    spec: Dict[str, Any] = {"year": None, "mileage_km": None, "fuel": None, "gearbox": None}
    for part in (p.strip() for p in text.split("∙")):
        if not part:
            continue
        lowered = part.lower()
        if YEAR_RE.match(part):
            spec["year"] = int(part)
        elif KM_RE.match(part):
            spec["mileage_km"] = parse_danish_int(part)
        elif lowered in GEARBOX_MAP:
            spec["gearbox"] = GEARBOX_MAP[lowered]
        elif lowered.endswith(" cc") or "rækkevid" in lowered:
            continue
        elif spec["fuel"] is None:
            spec["fuel"] = normalize_fuel(part)
    return spec


def parse_listing_age(text: str, now: datetime) -> Optional[datetime]:
    """'2 min.' / '5 t.' / '4 dage' / '3 uger' -> approximate listing time."""
    match = AGE_RE.search(text or "")
    if not match:
        return None
    n, unit = int(match.group(1)), match.group(2).lower()
    if unit == "min":
        delta = timedelta(minutes=n)
    elif unit in ("t", "time", "timer"):
        delta = timedelta(hours=n)
    elif unit in ("dag", "dage"):
        delta = timedelta(days=n)
    elif unit in ("uge", "uger"):
        delta = timedelta(weeks=n)
    elif unit in ("md", "mdr", "måned", "måneder"):
        delta = timedelta(days=30 * n)
    else:  # år
        delta = timedelta(days=365 * n)
    return now - delta


def _small_thumbnail(src: Optional[str]) -> Optional[str]:
    """DBA serves resized images by path; 240w is ~13 KB vs ~50 KB at 480w."""
    if not src:
        return None
    return src.replace("/dynamic/480w/", "/dynamic/240w/")


def parse_search_page(html: str, city: str, now: Optional[datetime] = None) -> List[Car]:
    """Parse every listing card on a DBA car search page."""
    now = now or datetime.utcnow()
    soup = BeautifulSoup(html, "html.parser")
    cars = []
    for article in soup.find_all("article"):
        car = _parse_card(article, city, now)
        if car:
            cars.append(car)
    return cars


def _parse_card(article, city: str, now: datetime) -> Optional[Car]:
    try:
        link = article.find("a", href=ITEM_ID_RE)
        heading = article.find("h2")
        if not link or not heading:
            return None
        item_id = ITEM_ID_RE.search(link["href"]).group(1)
        make, model = split_make_model(heading.get_text(" ", strip=True))

        variant = None
        sibling = heading.find_next_sibling()
        if sibling is not None and sibling.name == "div" and "text-caption" in (sibling.get("class") or []):
            variant = sibling.get_text(" ", strip=True) or None

        spec_el = article.select_one("span.text-caption.font-bold")
        spec = parse_spec_line(spec_el.get_text(" ", strip=True)) if spec_el else {}

        price_el = article.find("span", class_="t3")
        price = parse_danish_int(price_el.get_text()) if price_el else None
        price_block = price_el.parent.get_text(" ", strip=True).lower() if price_el else ""
        is_monthly = "/md" in price_block or "pr. md" in price_block or "/måned" in price_block
        # Dealer vans etc. list prices ex. VAT; add Danish 25% moms so they
        # filter and sort against private sellers' all-in prices.
        vat_added = price is not None and ("ekskl. moms" in price_block or "ex. moms" in price_block)
        if vat_added:
            price = round(price * 1.25)

        location = seller_type = None
        info = article.find("div", class_="flex-1")
        if info:
            lines = [s.get_text(" ", strip=True) for s in info.find_all("span", recursive=False)]
            if lines:
                # Dealers show "Herlev ∙ Herlev Biler" -- keep the town only
                location = lines[0].split("∙")[0].strip() or None
            if len(lines) > 1:
                seller_raw = lines[1].split("∙")[0].strip()
                seller_type = SELLER_MAP.get(seller_raw.lower(), seller_raw.lower() or None)

        age_el = article.find("span", class_="self-end")
        img = article.find("img")

        return Car(
            source_id=f"dba_{item_id}",
            source_name="dba",
            city=city,
            url=ITEM_URL.format(id=item_id),
            make=make,
            model=model,
            variant=variant,
            year=spec.get("year"),
            mileage_km=spec.get("mileage_km"),
            fuel=spec.get("fuel"),
            gearbox=spec.get("gearbox"),
            listing_type="lease" if is_monthly else "buy",
            price_local=None if is_monthly else price,
            monthly_price_local=price if is_monthly else None,
            currency="DKK",
            location=location,
            seller_type=seller_type,
            is_promoted="Betalt placering" in article.get_text(" ", strip=True),
            vat_added=vat_added,
            thumbnail_url=_small_thumbnail(img.get("src") if img else None),
            listed_at=parse_listing_age(age_el.get_text(" ", strip=True), now) if age_el else None,
        )
    except Exception as e:
        logger.debug(f"Failed to parse DBA car card: {e}")
        return None


@register_car_adapter("dba")
class DbaCarAdapter:
    """
    DBA cars for sale, newest first, for one region.

    Config (cars.<city>.dba):
        location: DBA region code (default "0.200001", København og omegn)
        max_pages: pages of ~50 cards to fetch (default 3, capped at 3)
    """

    source_name = "dba"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        self.config = config
        dba_config = city_config.get("dba", {}) or {}
        self.location = str(dba_config.get("location", COPENHAGEN_LOCATION))
        self.max_pages = min(int(dba_config.get("max_pages", MAX_PAGES_LIMIT)), MAX_PAGES_LIMIT)
        self.city_name = city_config.get("display_name", "Copenhagen")
        self.page_errors: List[str] = []  # set by fetch_listings; pipeline treats as failure

    def fetch_listings(self) -> List[Car]:
        cars: Dict[str, Car] = {}
        for page in range(1, self.max_pages + 1):
            if page > 1:
                time.sleep(PAGE_DELAY_SECONDS)
            params = {"location": self.location, "sort": "PUBLISHED_DESC", "page": page}
            try:
                response = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
            except requests.RequestException as e:
                logger.error(f"DBA page {page} request failed: {e}")
                self.page_errors.append(f"page {page}: {e}")
                break
            # Anything but a full 200 page (403/429, or a 202 with an empty
            # body like Bilbasen's bot wall) means stop -- don't push on.
            if response.status_code != 200 or not response.text.strip():
                logger.error(
                    f"DBA page {page} returned HTTP {response.status_code} "
                    f"({len(response.text)} bytes); stopping"
                )
                self.page_errors.append(f"page {page}: HTTP {response.status_code}")
                break
            page_cars = parse_search_page(response.text, self.city_name)
            if not page_cars:
                logger.warning(f"DBA page {page} had no parseable listings; stopping")
                self.page_errors.append(f"page {page}: no parseable listings")
                break
            for car in page_cars:
                # Paid placements can repeat across pages
                cars.setdefault(car.source_id, car)
        logger.info(f"Fetched {len(cars)} car listings from DBA")
        return list(cars.values())

    def check_status(self, urls: List[str]) -> Dict[str, str]:
        """404/410 -> gone; anything else -> unknown (see module docstring)."""
        return check_urls_by_http_status(urls, self.source_name)
