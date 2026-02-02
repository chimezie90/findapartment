"""Abstract base adapter for apartment listing sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..models.apartment import Apartment


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
