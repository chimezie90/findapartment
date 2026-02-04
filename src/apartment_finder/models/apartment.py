"""Normalized apartment data models used across all sources."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class Amenities:
    """Normalized amenities representation across all sources."""

    laundry_in_unit: bool = False
    laundry_in_building: bool = False
    dishwasher: bool = False
    parking: bool = False
    gym: bool = False
    pool: bool = False
    doorman: bool = False
    elevator: bool = False
    pets_allowed: bool = False
    air_conditioning: bool = False

    def has_laundry(self) -> bool:
        """Check if any laundry option is available."""
        return self.laundry_in_unit or self.laundry_in_building

    def to_list(self) -> List[str]:
        """Return list of available amenities as human-readable strings."""
        amenities = []
        if self.laundry_in_unit:
            amenities.append("In-unit laundry")
        elif self.laundry_in_building:
            amenities.append("Building laundry")
        if self.dishwasher:
            amenities.append("Dishwasher")
        if self.parking:
            amenities.append("Parking")
        if self.gym:
            amenities.append("Gym")
        if self.pool:
            amenities.append("Pool")
        if self.doorman:
            amenities.append("Doorman")
        if self.elevator:
            amenities.append("Elevator")
        if self.pets_allowed:
            amenities.append("Pets OK")
        if self.air_conditioning:
            amenities.append("A/C")
        return amenities


@dataclass
class Apartment:
    """
    Normalized apartment listing model.

    All listings from all sources are converted to this format
    for consistent scoring, deduplication, and display.
    """

    # Identification
    source_id: str  # Unique ID: "{source}_{original_id}"
    source_name: str  # e.g., "craigslist", "bayut"

    # Basic info
    title: str
    url: str

    # Pricing (local + normalized USD)
    price_local: float
    currency: str  # ISO code: USD, AED, EUR
    price_usd: Optional[float]  # Converted price for comparison

    # Size
    bedrooms: Optional[int]
    bathrooms: Optional[float]
    sqft: Optional[int]

    # Location
    city: str
    country: str
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Features
    amenities: Amenities = field(default_factory=Amenities)
    description: Optional[str] = None
    images: List[str] = field(default_factory=list)
    thumbnail_url: Optional[str] = None  # Preview image from search results

    # Timestamps
    posted_date: Optional[datetime] = None
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    # Scoring (filled by scoring service)
    score: Optional[float] = None
    score_breakdown: Optional[dict] = None

    def meets_must_haves(self, must_haves: List[str]) -> bool:
        """Check if apartment has all must-have amenities."""
        for requirement in must_haves:
            requirement = requirement.lower()
            if requirement == "laundry" and not self.amenities.has_laundry():
                return False
            if requirement == "dishwasher" and not self.amenities.dishwasher:
                return False
            if requirement == "parking" and not self.amenities.parking:
                return False
            if requirement == "gym" and not self.amenities.gym:
                return False
            if requirement == "doorman" and not self.amenities.doorman:
                return False
            if requirement == "elevator" and not self.amenities.elevator:
                return False
            if requirement == "pets" and not self.amenities.pets_allowed:
                return False
            if requirement == "a/c" and not self.amenities.air_conditioning:
                return False
        return True

    def display_price(self) -> str:
        """Format price for display with currency symbol."""
        symbols = {"USD": "$", "AED": "AED ", "EUR": "\u20ac"}
        symbol = symbols.get(self.currency, f"{self.currency} ")
        return f"{symbol}{self.price_local:,.0f}/mo"

    def display_size(self) -> str:
        """Format size information for display."""
        parts = []
        if self.bedrooms is not None:
            parts.append(f"{self.bedrooms}BR")
        if self.bathrooms is not None:
            ba_str = f"{self.bathrooms:.0f}" if self.bathrooms == int(self.bathrooms) else f"{self.bathrooms}"
            parts.append(f"{ba_str}BA")
        if self.sqft:
            parts.append(f"{self.sqft:,} sqft")
        return " | ".join(parts) if parts else "Size unknown"

    def __repr__(self) -> str:
        return f"Apartment({self.source_id}, {self.display_price()}, {self.display_size()})"
