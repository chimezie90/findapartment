"""Normalized car listing model, shared by all car sources."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Car:
    """One car listing (for sale or for lease), normalized across sources."""

    source_id: str
    source_name: str
    city: str
    url: str
    make: Optional[str] = None
    model: Optional[str] = None
    variant: Optional[str] = None
    year: Optional[int] = None
    mileage_km: Optional[int] = None
    fuel: Optional[str] = None
    gearbox: Optional[str] = None  # "manual" / "automatic"; None for EVs (DBA shows range instead)
    listing_type: str = "buy"  # "buy" or "lease"
    price_local: Optional[float] = None  # Purchase price (buy) -- None for leases
    monthly_price_local: Optional[float] = None  # Lease only
    currency: str = "DKK"
    price_usd: Optional[float] = None
    location: Optional[str] = None
    seller_type: Optional[str] = None  # "private" / "dealer"
    is_promoted: bool = False  # Paid placement at the source
    vat_added: bool = False  # Listed "ekskl. moms"; 25% VAT added so prices compare
    thumbnail_url: Optional[str] = None
    listed_at: Optional[datetime] = None  # Approximate, from the card's "listed X ago"

    @property
    def title(self) -> str:
        return " ".join(part for part in (self.make, self.model) if part) or "Car"
