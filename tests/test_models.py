"""Tests for apartment models."""

import pytest

from apartment_finder.models.apartment import Amenities, Apartment


class TestAmenities:
    """Tests for Amenities dataclass."""

    def test_has_laundry_in_unit(self):
        amenities = Amenities(laundry_in_unit=True)
        assert amenities.has_laundry() is True

    def test_has_laundry_in_building(self):
        amenities = Amenities(laundry_in_building=True)
        assert amenities.has_laundry() is True

    def test_has_no_laundry(self):
        amenities = Amenities()
        assert amenities.has_laundry() is False

    def test_to_list_empty(self):
        amenities = Amenities()
        assert amenities.to_list() == []

    def test_to_list_with_amenities(self):
        amenities = Amenities(
            laundry_in_unit=True,
            dishwasher=True,
            parking=True,
        )
        result = amenities.to_list()
        assert "In-unit laundry" in result
        assert "Dishwasher" in result
        assert "Parking" in result
        assert len(result) == 3

    def test_to_list_prefers_in_unit_laundry(self):
        amenities = Amenities(laundry_in_unit=True, laundry_in_building=True)
        result = amenities.to_list()
        assert "In-unit laundry" in result
        assert "Building laundry" not in result


class TestApartment:
    """Tests for Apartment dataclass."""

    def test_meets_must_haves_all_present(self, sample_apartment):
        assert sample_apartment.meets_must_haves(["laundry", "dishwasher"]) is True

    def test_meets_must_haves_missing_one(self, sample_apartment):
        assert sample_apartment.meets_must_haves(["laundry", "gym"]) is False

    def test_meets_must_haves_empty_list(self, sample_apartment):
        assert sample_apartment.meets_must_haves([]) is True

    def test_meets_must_haves_case_insensitive(self, sample_apartment):
        assert sample_apartment.meets_must_haves(["LAUNDRY", "Dishwasher"]) is True

    def test_display_price_usd(self, sample_apartment):
        assert sample_apartment.display_price() == "$3,000/mo"

    def test_display_price_aed(self):
        apt = Apartment(
            source_id="test",
            source_name="bayut",
            title="Dubai Apt",
            url="https://example.com",
            price_local=15000.0,
            currency="AED",
            price_usd=4000.0,
            bedrooms=2,
            bathrooms=1.0,
            sqft=900,
            city="Dubai",
            country="UAE",
        )
        assert apt.display_price() == "AED 15,000/mo"

    def test_display_price_eur(self):
        apt = Apartment(
            source_id="test",
            source_name="idealista",
            title="Lisbon Apt",
            url="https://example.com",
            price_local=2000.0,
            currency="EUR",
            price_usd=2200.0,
            bedrooms=1,
            bathrooms=1.0,
            sqft=700,
            city="Lisbon",
            country="Portugal",
        )
        assert "\u20ac2,000/mo" == apt.display_price()

    def test_display_size_full(self, sample_apartment):
        result = sample_apartment.display_size()
        assert "2BR" in result
        assert "1BA" in result
        assert "850 sqft" in result

    def test_display_size_no_sqft(self):
        apt = Apartment(
            source_id="test",
            source_name="test",
            title="Test",
            url="https://example.com",
            price_local=3000.0,
            currency="USD",
            price_usd=3000.0,
            bedrooms=2,
            bathrooms=1.5,
            sqft=None,
            city="NYC",
            country="USA",
        )
        result = apt.display_size()
        assert "2BR" in result
        assert "1.5BA" in result
        assert "sqft" not in result

    def test_display_size_unknown(self):
        apt = Apartment(
            source_id="test",
            source_name="test",
            title="Test",
            url="https://example.com",
            price_local=3000.0,
            currency="USD",
            price_usd=3000.0,
            bedrooms=None,
            bathrooms=None,
            sqft=None,
            city="NYC",
            country="USA",
        )
        assert apt.display_size() == "Size unknown"

    def test_repr(self, sample_apartment):
        result = repr(sample_apartment)
        assert "test_123" in result
        assert "$3,000/mo" in result
