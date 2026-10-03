"""Abstract base adapter for apartment listing sources."""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from ..models.apartment import Apartment

logger = logging.getLogger(__name__)

LIVENESS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
LIVENESS_DELAY_SECONDS = 1.0
# Per source per run, so liveness can't push the web "Fetch" button (480s
# subprocess timeout) or cron over time. Unchecked URLs go first next run.
LIVENESS_TIME_BUDGET_SECONDS = 90


@dataclass
class SearchCriteria:
    """Normalized search parameters passed to all adapters."""

    min_price_local: float  # In local currency
    max_price_local: float
    min_sqft: int
    min_bedrooms: int
    max_bedrooms: int
    must_have_amenities: List[str]


class BaseAdapter(ABC):
    """
    Abstract base class for all apartment listing source adapters.

    Each adapter must implement:
    - fetch_listings(): Retrieve raw listings from source
    - _normalize(): Convert source-specific format to Apartment model

    Subclasses should use the @register_adapter decorator to register
    themselves with the adapter registry.
    """

    def __init__(self, config: Dict[str, Any], city_config: Dict[str, Any]):
        """
        Initialize adapter with configuration.

        Args:
            config: Source-specific configuration from sources.yaml
            city_config: City-specific configuration including source settings
        """
        self.config = config
        self.city_config = city_config
        self.source_name: str = self.__class__.__name__.replace("Adapter", "").lower()

    @abstractmethod
    def fetch_listings(self, criteria: SearchCriteria) -> List[Apartment]:
        """
        Fetch listings from the source and return normalized Apartment objects.

        Args:
            criteria: Normalized search criteria with local currency prices

        Returns:
            List of Apartment objects
        """
        pass

    @abstractmethod
    def _normalize(self, raw_listing: Dict[str, Any]) -> Optional[Apartment]:
        """
        Convert a raw listing from the source into a normalized Apartment.

        Args:
            raw_listing: Raw listing data from the source API/scraper

        Returns:
            Normalized Apartment object, or None if the listing cannot be normalized
        """
        pass

    def get_source_name(self) -> str:
        """Get the name of this source."""
        return self.source_name

    def is_available(self) -> bool:
        """
        Check if the source is properly configured and accessible.

        Override this method to check for required API keys, etc.

        Returns:
            True if the adapter is ready to use
        """
        return True

    def check_status(self, urls: List[str]) -> Dict[str, str]:
        """
        Check whether previously-seen listings are still live at the source.

        Default: GET each URL; 404/410 means the listing was removed. Anything
        else (200, bot blocks like 403/429, 5xx, timeouts) is inconclusive and
        left out of the result, so the listing keeps its current status.
        Override for sources whose pages always return 200 (client-rendered).

        Returns:
            {url: status} for every URL actually checked: "gone" (confirmed
            removed), "active" (positively confirmed live -- a plain 200 doesn't
            count, many sites serve removed listings with 200), or "unknown".
            URLs left out weren't checked (e.g. time budget ran out).
        """
        results = {}
        deadline = time.monotonic() + LIVENESS_TIME_BUDGET_SECONDS
        for i, url in enumerate(urls):
            if time.monotonic() > deadline:
                logger.info(f"Liveness time budget hit after {i}/{len(urls)} URLs")
                break
            if i:
                time.sleep(LIVENESS_DELAY_SECONDS)
            results[url] = "unknown"
            try:
                with requests.get(
                    url, headers=LIVENESS_HEADERS, timeout=(5, 10), stream=True
                ) as response:
                    status_code = response.status_code
            except requests.RequestException as e:
                logger.debug(f"Liveness check failed for {url}: {e}")
                continue
            if status_code in (404, 410):
                results[url] = "gone"

        # A batch that is (nearly) all 404s is far likelier a URL-scheme change
        # or a block page than mass removal -- don't act on it.
        gone_count = sum(1 for status in results.values() if status == "gone")
        if len(results) >= 5 and gone_count >= 0.8 * len(results):
            logger.warning(
                f"{self.source_name}: {gone_count}/{len(results)} URLs returned 404/410; "
                "treating batch as inconclusive"
            )
            return {url: "unknown" for url in results}
        return results
