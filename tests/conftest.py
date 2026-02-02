"""Shared fixtures for apartment-finder tests."""

import pytest
from datetime import datetime, timedelta

from apartment_finder.models.apartment import Amenities, Apartment


@pytest.fixture
def basic_amenities():
    """Basic amenities with laundry and dishwasher."""
    return Amenities(laundry_in_unit=True, dishwasher=True)


@pytest.fixture
def full_amenities():
    """Fully loaded amenities."""
    return Amenities(
        laundry_in_unit=True,
        dishwasher=True,
        parking=True,
        gym=True,
        pool=True,
        doorman=True,
        elevator=True,
        pets_allowed=True,
        air_conditioning=True,
    )


@pytest.fixture
def sample_apartment(basic_amenities):
    """A sample apartment for testing."""
    return Apartment(
        source_id="test_123",
        source_name="test",
        title="Lovely 2BR in Manhattan",
        url="https://example.com/listing/123",
        price_local=3000.0,
        currency="USD",
        price_usd=3000.0,
        bedrooms=2,
        bathrooms=1.0,
        sqft=850,
        city="New York City",
        country="USA",
        address="123 Main St",
        neighborhood="Upper West Side",
        amenities=basic_amenities,
        description="A quiet apartment in a residential area",
        posted_date=datetime.utcnow() - timedelta(days=2),
    )


@pytest.fixture
def cheap_apartment(basic_amenities):
    """A cheap apartment at min budget."""
    return Apartment(
        source_id="cheap_1",
        source_name="test",
        title="Budget 1BR",
        url="https://example.com/cheap",
        price_local=2500.0,
        currency="USD",
        price_usd=2500.0,
        bedrooms=1,
        bathrooms=1.0,
        sqft=700,
        city="New York City",
        country="USA",
        amenities=basic_amenities,
        posted_date=datetime.utcnow(),
    )


@pytest.fixture
def expensive_apartment(basic_amenities):
    """An expensive apartment at max budget."""
    return Apartment(
        source_id="expensive_1",
        source_name="test",
        title="Luxury 2BR",
        url="https://example.com/luxury",
        price_local=4000.0,
        currency="USD",
        price_usd=4000.0,
        bedrooms=2,
        bathrooms=2.0,
        sqft=1200,
        city="New York City",
        country="USA",
        amenities=basic_amenities,
        posted_date=datetime.utcnow() - timedelta(days=10),
    )


@pytest.fixture
def apartment_missing_must_haves():
    """Apartment without required amenities."""
    return Apartment(
        source_id="no_amenities_1",
        source_name="test",
        title="Basic Studio",
        url="https://example.com/basic",
        price_local=2800.0,
        currency="USD",
        price_usd=2800.0,
        bedrooms=0,
        bathrooms=1.0,
        sqft=500,
        city="New York City",
        country="USA",
        amenities=Amenities(),  # No amenities
        posted_date=datetime.utcnow(),
    )
