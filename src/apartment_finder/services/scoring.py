"""Scoring service for ranking apartments based on user criteria."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from ..models.apartment import Apartment

logger = logging.getLogger(__name__)


@dataclass
class ScoringWeights:
    """Configurable weights for scoring algorithm."""

    price: float = 0.30
    size: float = 0.20
    amenities: float = 0.25
    location: float = 0.15
    freshness: float = 0.10

    def validate(self) -> None:
        """Validate that weights sum to 1.0."""
        total = self.price + self.size + self.amenities + self.location + self.freshness
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Scoring weights must sum to 1.0, got {total}")


class ScoringService:
    """
    Score apartments based on user criteria.

    Each dimension is scored 0-100, then weighted.
    Final score is 0-100 where higher is better.
    """

    # Keywords that suggest a quiet neighborhood
    QUIET_SIGNALS = [
        "quiet",
        "peaceful",
        "residential",
        "tree-lined",
        "family",
        "safe",
        "low traffic",
        "serene",
        "tranquil",
    ]

    # Keywords that suggest a noisy area
    NOISY_SIGNALS = [
        "nightlife",
        "busy",
        "downtown",
        "times square",
        "club",
        "bar district",
        "highway",
        "train",
        "subway",
        "vibrant",
    ]

    def __init__(
        self,
        min_price: float,
        max_price: float,
        min_sqft: int,
        must_haves: List[str],
        preferences: List[str],
        weights: Optional[ScoringWeights] = None,
    ):
        """
        Initialize scoring service.

        Args:
            min_price: Minimum budget in USD
            max_price: Maximum budget in USD
            min_sqft: Minimum square footage
            must_haves: List of required amenities
            preferences: List of preference keywords (e.g., 'quiet_neighborhood')
            weights: Custom scoring weights (uses defaults if None)
        """
        self.min_price = min_price
        self.max_price = max_price
        self.min_sqft = min_sqft
        self.must_haves = [m.lower() for m in must_haves]
        self.preferences = [p.lower() for p in preferences]
        self.weights = weights or ScoringWeights()
        self.weights.validate()

    def score_apartments(self, apartments: List[Apartment]) -> List[Apartment]:
        """
        Score all apartments and sort by score descending.

        Apartments that don't meet must-haves or are missing USD price are filtered out.

        Args:
            apartments: List of apartments to score

        Returns:
            Filtered and scored apartments, sorted by score descending
        """
        scored = []
        for apt in apartments:
            # Skip if missing must-haves
            if not apt.meets_must_haves(self.must_haves):
                logger.debug(f"Skipping {apt.source_id}: missing must-haves")
                continue

            # Skip if no USD price (currency conversion failed)
            if apt.price_usd is None:
                logger.debug(f"Skipping {apt.source_id}: no USD price")
                continue

            # Skip if outside budget
            if apt.price_usd < self.min_price or apt.price_usd > self.max_price:
                logger.debug(f"Skipping {apt.source_id}: outside budget ({apt.price_usd})")
                continue

            apt.score, apt.score_breakdown = self._calculate_score(apt)
            scored.append(apt)

        # Sort by score descending
        scored.sort(key=lambda x: x.score or 0, reverse=True)
        logger.info(f"Scored {len(scored)} apartments (filtered from {len(apartments)})")
        return scored

    def _calculate_score(self, apt: Apartment) -> Tuple[float, dict]:
        """Calculate weighted score for an apartment."""
        breakdown = {}

        # Price score (lower is better, within range)
        breakdown["price"] = self._score_price(apt.price_usd)

        # Size score (bigger is better)
        breakdown["size"] = self._score_size(apt.sqft)

        # Amenities score (more extras beyond must-haves)
        breakdown["amenities"] = self._score_amenities(apt)

        # Location score (quiet preference)
        breakdown["location"] = self._score_location(apt)

        # Freshness score (newer listings preferred)
        breakdown["freshness"] = self._score_freshness(apt.posted_date)

        # Calculate weighted total
        total = (
            breakdown["price"] * self.weights.price
            + breakdown["size"] * self.weights.size
            + breakdown["amenities"] * self.weights.amenities
            + breakdown["location"] * self.weights.location
            + breakdown["freshness"] * self.weights.freshness
        )

        return round(total, 1), breakdown

    def _score_price(self, price_usd: float) -> float:
        """
        Score price: 100 for min_price, decreasing as price increases.
        Prices at max_price get 0.
        """
        if price_usd <= self.min_price:
            return 100.0
        if price_usd >= self.max_price:
            return 0.0

        # Linear decrease from min to max
        range_size = self.max_price - self.min_price
        position = price_usd - self.min_price
        return 100.0 * (1 - position / range_size)

    def _score_size(self, sqft: Optional[int]) -> float:
        """
        Score size: 50 at min_sqft, up to 100 for larger units.
        Penalize if below minimum.
        """
        if sqft is None:
            return 50.0  # Unknown size gets neutral score

        if sqft < self.min_sqft:
            # Penalize for being under minimum
            return max(0, 50 * (sqft / self.min_sqft))

        # Bonus for larger (diminishing returns above 1.5x min)
        ratio = min(sqft / self.min_sqft, 1.5)
        return 50 + (50 * (ratio - 1) / 0.5)

    def _score_amenities(self, apt: Apartment) -> float:
        """Score based on bonus amenities beyond must-haves."""
        score = 60.0  # Base score (has must-haves to get here)

        # Bonus points for extras
        bonuses = [
            (apt.amenities.gym, 10),
            (apt.amenities.doorman, 10),
            (apt.amenities.pool, 5),
            (apt.amenities.elevator, 5),
            (apt.amenities.pets_allowed, 5),
            (apt.amenities.air_conditioning, 5),
            # Extra bonus for in-unit laundry vs building laundry
            (apt.amenities.laundry_in_unit and "laundry" in self.must_haves, 10),
        ]

        for has_amenity, bonus in bonuses:
            if has_amenity:
                score += bonus

        return min(score, 100.0)

    def _score_location(self, apt: Apartment) -> float:
        """Score location based on quiet preference signals."""
        if "quiet_neighborhood" not in self.preferences:
            return 70.0  # Neutral if not a preference

        text = f"{apt.neighborhood or ''} {apt.description or ''}".lower()

        score = 50.0  # Start neutral

        # Positive signals for quiet
        for signal in self.QUIET_SIGNALS:
            if signal in text:
                score += 10

        # Negative signals
        for signal in self.NOISY_SIGNALS:
            if signal in text:
                score -= 15

        return max(0, min(100, score))

    def _score_freshness(self, posted_date: Optional[datetime]) -> float:
        """Score based on listing age. Newer is better."""
        if not posted_date:
            return 50.0  # Unknown date gets neutral

        age = datetime.utcnow() - posted_date

        if age < timedelta(days=1):
            return 100.0
        elif age < timedelta(days=3):
            return 90.0
        elif age < timedelta(days=7):
            return 70.0
        elif age < timedelta(days=14):
            return 50.0
        else:
            return 30.0
