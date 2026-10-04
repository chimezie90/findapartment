"""Normalized model for homes for sale (owner-occupied flats and houses)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# Our property types. home.dk's catalog URL category -> our type.
PROPERTY_TYPES = ("flat", "terraced", "villa", "villa_flat")


@dataclass
class HomeListing:
    """One home for sale, normalized across sources. Prices are in DKK."""

    source_id: str
    source_name: str
    url: str
    address: str
    property_type: str  # one of PROPERTY_TYPES
    price_dkk: int
    street: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None  # postal district, e.g. "København V"
    municipality: Optional[str] = None  # e.g. "København", "Frederiksberg"
    sqm: Optional[int] = None
    rooms: Optional[int] = None  # detail page only (home.dk's own listings)
    year_built: Optional[int] = None  # detail page only
    monthly_owner_expenses_dkk: Optional[int] = None  # ejerudgift, detail page only
    energy_label: Optional[str] = None  # e.g. "A2015", "C"; detail page only
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_external: bool = False  # listed by another broker; links out via boligsiden
    broker: Optional[str] = None
    headline: Optional[str] = None
    thumbnail_url: Optional[str] = None
    listed_at: Optional[datetime] = None  # detail page only
