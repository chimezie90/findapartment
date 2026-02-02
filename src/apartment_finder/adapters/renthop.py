"""RentHop adapter for NYC apartment listings using Playwright."""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..models.apartment import Amenities, Apartment
from . import register_adapter
from .base import BaseAdapter, SearchCriteria

logger = logging.getLogger(__name__)


@register_adapter("renthop")
class RentHopAdapter(BaseAdapter):
    """
    Adapter for RentHop apartment listings using Playwright.

    Requires Playwright browser to bypass Cloudflare protection.
    """

    BASE_URL = "https://www.renthop.com"

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        super().__init__(config, city_config)

    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """Fetch apartment listings from RentHop."""
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
                page = context.new_page()
                # Hide webdriver detection
                page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                """)

                try:
                    apartments = self._scrape_listings(page, criteria)
                except Exception as e:
                    logger.error(f"Error scraping RentHop: {e}")

                browser.close()

        except Exception as e:
            logger.error(f"Playwright error: {e}")

        logger.info(f"Fetched {len(apartments)} listings from RentHop")
        return apartments

    def _scrape_listings(self, page, criteria: SearchCriteria) -> List[Apartment]:
        """Scrape listings from RentHop."""
        # Build URL with filters
        bedrooms_param = "&".join([f"bedrooms%5B%5D={i}" for i in range(criteria.min_bedrooms, criteria.max_bedrooms + 1)])
        url = f"{self.BASE_URL}/search/nyc?min_price={int(criteria.min_price_local)}&max_price={int(criteria.max_price_local)}&{bedrooms_param}&sort=hopscore"

        logger.info("Fetching RentHop listings for NYC")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for Cloudflare challenge to complete
        page.wait_for_timeout(8000)

        # Check if we're still on Cloudflare challenge
        content = page.content()
        if "Just a moment" in content or "challenge" in content.lower():
            logger.warning("RentHop Cloudflare challenge detected, waiting...")
            page.wait_for_timeout(10000)

        # Try to find listings
        try:
            page.wait_for_selector(".search-listing, .listing-card, .search-result, [class*='listing']", timeout=15000)
        except Exception:
            logger.warning("Could not find listing cards")

        apartments = []

        # Find all listing cards
        cards = page.query_selector_all(".search-listing, .listing-card, [data-listing-id]")

        for card in cards[:50]:
            try:
                apartment = self._parse_card(card)
                if apartment:
                    apartments.append(apartment)
            except Exception as e:
                logger.debug(f"Failed to parse RentHop card: {e}")

        return apartments

    def _parse_card(self, card) -> Optional[Apartment]:
        """Parse a listing card element."""
        try:
            # Get listing ID
            listing_id = card.get_attribute("data-listing-id")
            if not listing_id:
                href = card.query_selector("a")
                if href:
                    href_val = href.get_attribute("href") or ""
                    id_match = re.search(r"/(\d+)", href_val)
                    listing_id = id_match.group(1) if id_match else str(hash(href_val))

            # Get link and URL
            link = card.query_selector("a[href*='/listings/']")
            if not link:
                link = card.query_selector("a")

            url = ""
            if link:
                url = link.get_attribute("href") or ""
                if url and not url.startswith("http"):
                    url = self.BASE_URL + url

            # Get title/address
            title_elem = card.query_selector(".listing-title, .address, h2, h3")
            title = title_elem.inner_text() if title_elem else "RentHop Listing"

            # Get price
            price = 0.0
            price_elem = card.query_selector(".listing-price, .price, [class*='price']")
            if price_elem:
                price_text = price_elem.inner_text()
                price_match = re.search(r"\$?([\d,]+)", price_text)
                if price_match:
                    price = float(price_match.group(1).replace(",", ""))

            # Get bedrooms
            bedrooms = None
            beds_elem = card.query_selector(".listing-beds, .beds, [class*='bed']")
            if beds_elem:
                beds_text = beds_elem.inner_text()
                beds_match = re.search(r"(\d+)", beds_text)
                if beds_match:
                    bedrooms = int(beds_match.group(1))

            # Get neighborhood
            neighborhood = None
            hood_elem = card.query_selector(".listing-neighborhood, .neighborhood, [class*='hood']")
            if hood_elem:
                neighborhood = hood_elem.inner_text().strip()

            # Get HopScore if available
            score_elem = card.query_selector(".hopscore, [class*='score']")
            hop_score = None
            if score_elem:
                score_text = score_elem.inner_text()
                score_match = re.search(r"([\d.]+)", score_text)
                if score_match:
                    hop_score = float(score_match.group(1))

            return Apartment(
                source_id=f"renthop_{listing_id}",
                source_name="renthop",
                title=title.strip(),
                url=url,
                price_local=price,
                currency="USD",
                price_usd=price,
                bedrooms=bedrooms,
                bathrooms=None,
                sqft=None,
                address=title if "St" in title or "Ave" in title else None,
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
