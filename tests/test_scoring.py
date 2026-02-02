"""Tests for scoring service."""

import pytest
from datetime import datetime, timedelta

from apartment_finder.models.apartment import Amenities, Apartment
from apartment_finder.services.scoring import ScoringService, ScoringWeights


class TestScoringWeights:
    """Tests for ScoringWeights validation."""

    def test_default_weights_valid(self):
        weights = ScoringWeights()
        weights.validate()  # Should not raise

    def test_custom_weights_valid(self):
        weights = ScoringWeights(
            price=0.40,
            size=0.20,
            amenities=0.20,
            location=0.10,
            freshness=0.10,
        )
        weights.validate()  # Should not raise

    def test_weights_invalid_sum(self):
        weights = ScoringWeights(
            price=0.50,
            size=0.30,
            amenities=0.30,
            location=0.10,
            freshness=0.10,
        )
        with pytest.raises(ValueError, match="must sum to 1.0"):
            weights.validate()


class TestScoringService:
    """Tests for ScoringService."""

    @pytest.fixture
    def scoring_service(self):
        return ScoringService(
            min_price=2500,
            max_price=4000,
            min_sqft=700,
            must_haves=["laundry", "dishwasher"],
            preferences=["quiet_neighborhood"],
        )

    def test_score_apartments_filters_missing_must_haves(
        self, scoring_service, sample_apartment, apartment_missing_must_haves
    ):
        apartments = [sample_apartment, apartment_missing_must_haves]
        scored = scoring_service.score_apartments(apartments)

        assert len(scored) == 1
        assert scored[0].source_id == "test_123"

    def test_score_apartments_filters_no_usd_price(self, scoring_service, basic_amenities):
        apt = Apartment(
            source_id="no_price",
            source_name="test",
            title="No USD",
            url="https://example.com",
            price_local=3000.0,
            currency="AED",
            price_usd=None,  # No USD price
            bedrooms=2,
            bathrooms=1.0,
            sqft=800,
            city="Dubai",
            country="UAE",
            amenities=basic_amenities,
        )
        scored = scoring_service.score_apartments([apt])
        assert len(scored) == 0

    def test_score_apartments_filters_outside_budget(
        self, scoring_service, basic_amenities
    ):
        too_cheap = Apartment(
            source_id="too_cheap",
            source_name="test",
            title="Too Cheap",
            url="https://example.com",
            price_local=2000.0,
            currency="USD",
            price_usd=2000.0,  # Below min
            bedrooms=1,
            bathrooms=1.0,
            sqft=700,
            city="NYC",
            country="USA",
            amenities=basic_amenities,
        )
        too_expensive = Apartment(
            source_id="too_expensive",
            source_name="test",
            title="Too Expensive",
            url="https://example.com",
            price_local=5000.0,
            currency="USD",
            price_usd=5000.0,  # Above max
            bedrooms=2,
            bathrooms=2.0,
            sqft=1000,
            city="NYC",
            country="USA",
            amenities=basic_amenities,
        )

        scored = scoring_service.score_apartments([too_cheap, too_expensive])
        assert len(scored) == 0

    def test_score_apartments_sorted_by_score(
        self, scoring_service, cheap_apartment, expensive_apartment
    ):
        scored = scoring_service.score_apartments([expensive_apartment, cheap_apartment])

        assert len(scored) == 2
        # Cheap apartment should score higher (better price)
        assert scored[0].source_id == "cheap_1"
        assert scored[0].score > scored[1].score

    def test_score_breakdown_includes_all_dimensions(
        self, scoring_service, sample_apartment
    ):
        scored = scoring_service.score_apartments([sample_apartment])

        assert len(scored) == 1
        breakdown = scored[0].score_breakdown

        assert "price" in breakdown
        assert "size" in breakdown
        assert "amenities" in breakdown
        assert "location" in breakdown
        assert "freshness" in breakdown

    def test_score_price_at_minimum(self, scoring_service):
        score = scoring_service._score_price(2500)
        assert score == 100.0

    def test_score_price_at_maximum(self, scoring_service):
        score = scoring_service._score_price(4000)
        assert score == 0.0

    def test_score_price_midpoint(self, scoring_service):
        score = scoring_service._score_price(3250)  # Midpoint
        assert 45 < score < 55  # Should be around 50

    def test_score_size_at_minimum(self, scoring_service):
        score = scoring_service._score_size(700)
        assert score == 50.0

    def test_score_size_above_minimum(self, scoring_service):
        score = scoring_service._score_size(1000)
        assert score > 50.0

    def test_score_size_below_minimum(self, scoring_service):
        score = scoring_service._score_size(500)
        assert score < 50.0

    def test_score_size_unknown(self, scoring_service):
        score = scoring_service._score_size(None)
        assert score == 50.0

    def test_score_freshness_today(self, scoring_service):
        today = datetime.utcnow()
        score = scoring_service._score_freshness(today)
        assert score == 100.0

    def test_score_freshness_old(self, scoring_service):
        old_date = datetime.utcnow() - timedelta(days=30)
        score = scoring_service._score_freshness(old_date)
        assert score == 30.0

    def test_score_freshness_unknown(self, scoring_service):
        score = scoring_service._score_freshness(None)
        assert score == 50.0

    def test_score_location_quiet_signals(self, scoring_service, basic_amenities):
        apt = Apartment(
            source_id="quiet",
            source_name="test",
            title="Quiet Apartment",
            url="https://example.com",
            price_local=3000.0,
            currency="USD",
            price_usd=3000.0,
            bedrooms=2,
            bathrooms=1.0,
            sqft=800,
            city="NYC",
            country="USA",
            neighborhood="Quiet residential area",
            description="Peaceful and serene neighborhood",
            amenities=basic_amenities,
        )
        score = scoring_service._score_location(apt)
        assert score > 50.0  # Positive signals boost score

    def test_score_location_noisy_signals(self, scoring_service, basic_amenities):
        apt = Apartment(
            source_id="noisy",
            source_name="test",
            title="Times Square Apartment",
            url="https://example.com",
            price_local=3000.0,
            currency="USD",
            price_usd=3000.0,
            bedrooms=2,
            bathrooms=1.0,
            sqft=800,
            city="NYC",
            country="USA",
            neighborhood="Times Square",
            description="Vibrant nightlife area near clubs",
            amenities=basic_amenities,
        )
        score = scoring_service._score_location(apt)
        assert score < 50.0  # Negative signals lower score
