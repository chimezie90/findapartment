"""StreetEasy adapter for NYC apartment listings using Playwright."""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..models.apartment import Amenities, Apartment
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)


@register_adapter("streeteasy")
class StreetEasyAdapter(BaseAdapter):
    """
    Adapter for StreetEasy apartment listings using Playwright.

    Requires Playwright browser to bypass anti-bot protection.
    """

    BASE_URL = "https://streeteasy.com"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)
        self.areas = city_config.get("streeteasy", {}).get("areas", ["nyc"])

    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from StreetEasy."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return []

        apartments = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled"],
                    slow_mo=100,
                )
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )
                # Hide webdriver
                page = context.new_page()
                page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                """)

                for area in self.areas:
                    try:
                        listings = self._scrape_area(page, area, criteria)
                        apartments.extend(listings)
                    except Exception as e:
                        logger.error(f"Error scraping StreetEasy area {area}: {e}")

                browser.close()

        except Exception as e:
            logger.error(f"Playwright error: {e}")

        logger.info(f"Fetched {len(apartments)} listings from StreetEasy")
        return apartments

    def _scrape_area(self, page, area: str, criteria: SearchCriteria) -> List[Apartment]:
        """Scrape listings from a specific area."""
        # Build URL with filters
        url = f"{self.BASE_URL}/for-rent/{area}/price:{int(criteria.min_price_local)}-{int(criteria.max_price_local)}%7Cbeds:{criteria.min_bedrooms}-{criteria.max_bedrooms}"

        logger.info(f"Fetching StreetEasy listings for {area}")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for page to stabilize and listings to appear
        page.wait_for_timeout(5000)

        # Try to find listings
        try:
            page.wait_for_selector("[data-testid='listing-card'], .listingCard, .searchCardList, .ListingCard", timeout=15000)
        except Exception:
            logger.warning("Could not find listing cards, trying alternate selectors...")

        apartments = []

        # Find all listing cards
        cards = page.query_selector_all("[data-testid='listing-card'], .listingCard, article.listingCard")

        for card in cards[:50]:
            try:
                apartment = self._parse_card(card)
                if apartment:
                    apartments.append(apartment)
            except Exception as e:
                logger.debug(f"Failed to parse StreetEasy card: {e}")

        return apartments

    def _parse_card(self, card) -> Optional[Apartment]:
        """Parse a listing card element."""
        try:
            # Get link and URL
            link = card.query_selector("a[href*='/rental/']")
            if not link:
                link = card.query_selector("a")
            if not link:
                return None

            url = link.get_attribute("href")
            if url and not url.startswith("http"):
                url = self.BASE_URL + url

            # Get title
            title_elem = card.query_selector(".listingCardTop, .listingCardLabel, h2")
            title = title_elem.inner_text() if title_elem else "StreetEasy Listing"

            # Get price
            price = 0.0
            price_elem = card.query_selector("[data-testid='price'], .price, .listingCardPrice")
            if price_elem:
                price_text = price_elem.inner_text()
                price_match = re.search(r"\$?([\d,]+)", price_text)
                if price_match:
                    price = float(price_match.group(1).replace(",", ""))

            # Get bedrooms
            bedrooms = None
            beds_elem = card.query_selector("[data-testid='beds'], .listingCardBeds")
            if beds_elem:
                beds_text = beds_elem.inner_text()
                beds_match = re.search(r"(\d+)", beds_text)
                if beds_match:
                    bedrooms = int(beds_match.group(1))

            # Get sqft
            sqft = None
            sqft_elem = card.query_selector("[data-testid='sqft'], .listingCardSqFt")
            if sqft_elem:
                sqft_text = sqft_elem.inner_text()
                sqft_match = re.search(r"([\d,]+)", sqft_text)
                if sqft_match:
                    sqft = int(sqft_match.group(1).replace(",", ""))

            # Get address/neighborhood
            address = None
            neighborhood = None
            addr_elem = card.query_selector(".listingCardBottom, .listingCardAddress, address")
            if addr_elem:
                address = addr_elem.inner_text().strip()
                neighborhood = address.split(",")[0] if "," in address else address

            # Extract listing ID from URL
            listing_id = url.split("/")[-1] if url else str(hash(title))

            return Apartment(
                source_id=f"streeteasy_{listing_id}",
                source_name="streeteasy",
                title=title.strip(),
                url=url or "",
                price_local=price,
                currency="USD",
                price_usd=price,
                bedrooms=bedrooms,
                bathrooms=None,
                sqft=sqft,
                address=address,
                neighborhood=neighborhood,
                city="New York",
                country="USA",
                latitude=None,
                longitude=None,
                amenities=Amenities(),
                description=None,
                images=[],
                posted_date=None,
                fetched_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None

    def _normalize(self, raw: Dict[str, Any]) -> Optional[Apartment]:
        """Not used in Playwright scraping."""
        return None
