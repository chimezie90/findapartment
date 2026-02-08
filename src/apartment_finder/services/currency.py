"""Currency conversion service using Frankfurter API."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


class CurrencyService:
    """
    Currency conversion using Frankfurter API.

    Features:
    - Free, no API key required
    - Caches rates for 24 hours
    - Converts any currency to USD for comparison
    """

    BASE_URL = "https://api.frankfurter.dev/v1"
    CACHE_DURATION = timedelta(hours=24)

    # Fallback rates in case API is unavailable
    FALLBACK_RATES = {
        "AED_USD": 0.27,  # 1 AED = ~0.27 USD
        "EUR_USD": 1.08,  # 1 EUR = ~1.08 USD
        "GBP_USD": 1.26,  # 1 GBP = ~1.26 USD
        "DKK_USD": 0.14,  # 1 DKK = ~0.14 USD
        "IDR_USD": 0.000063,  # 1 IDR = ~0.000063 USD
    }

    def __init__(self):
        self._rates_cache: Dict[str, float] = {}
        self._cache_time: Optional[datetime] = None

    def convert_to_usd(self, amount: float, from_currency: str) -> Optional[float]:
        """
        Convert an amount to USD.

        Args:
            amount: Amount in source currency
            from_currency: ISO currency code (e.g., 'AED', 'EUR')

        Returns:
            Amount in USD, or None if conversion fails
        """
        if from_currency == "USD":
            return amount

        rate = self._get_rate(from_currency, "USD")
        if rate is None:
            return None

        return round(amount * rate, 2)

    def convert_from_usd(self, amount: float, to_currency: str) -> float:
        """
        Convert USD amount to another currency.

        Args:
            amount: Amount in USD
            to_currency: Target ISO currency code

        Returns:
            Amount in target currency
        """
        if to_currency == "USD":
            return amount

        rate = self._get_rate("USD", to_currency)
        if rate is None:
            # Use inverse of fallback rate
            fallback_key = f"{to_currency}_USD"
            if fallback_key in self.FALLBACK_RATES:
                return amount / self.FALLBACK_RATES[fallback_key]
            return amount  # Return as-is if no conversion available

        return round(amount * rate, 2)

    def _get_rate(self, from_currency: str, to_currency: str) -> Optional[float]:
        """Get exchange rate, using cache if valid."""
        cache_key = f"{from_currency}_{to_currency}"

        # Check cache
        if self._is_cache_valid() and cache_key in self._rates_cache:
            return self._rates_cache[cache_key]

        # Fetch fresh rates
        try:
            self._refresh_rates()
            rate = self._rates_cache.get(cache_key)
            if rate:
                return rate
        except Exception as e:
            logger.error(f"Failed to fetch exchange rates: {e}")

        # Return fallback rate if available (for currencies not in API like AED)
        if cache_key in self.FALLBACK_RATES:
            logger.debug(f"Using fallback rate for {cache_key}")
            return self.FALLBACK_RATES[cache_key]

        # Try inverse of fallback
        inverse_key = f"{to_currency}_{from_currency}"
        if inverse_key in self.FALLBACK_RATES:
            return 1 / self.FALLBACK_RATES[inverse_key]

        return None

    def _is_cache_valid(self) -> bool:
        """Check if cached rates are still valid."""
        if self._cache_time is None:
            return False
        return datetime.utcnow() - self._cache_time < self.CACHE_DURATION

    def _refresh_rates(self) -> None:
        """Fetch current rates from Frankfurter API."""
        # Get USD-based rates for common currencies we use
        currencies = ["AED", "EUR", "GBP", "DKK", "IDR"]

        response = requests.get(
            f"{self.BASE_URL}/latest",
            params={"base": "USD", "symbols": ",".join(currencies)},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        # Store rates both ways
        self._rates_cache = {}
        for currency, rate in data.get("rates", {}).items():
            # USD to X
            self._rates_cache[f"USD_{currency}"] = rate
            # X to USD (inverse)
            self._rates_cache[f"{currency}_USD"] = 1 / rate

        self._cache_time = datetime.utcnow()
        logger.info(f"Refreshed currency rates: {list(self._rates_cache.keys())}")

    def get_cached_rates(self) -> Dict[str, float]:
        """Get all cached exchange rates."""
        return self._rates_cache.copy()
