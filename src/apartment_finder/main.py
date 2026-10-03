"""Main orchestrator for the apartment finder."""

import argparse
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional

from .adapters import ADAPTER_REGISTRY, get_adapter
from .adapters.base import SearchCriteria
from .cars import CAR_ADAPTER_REGISTRY, get_car_adapter
from .cars.service import CarListingService
from .config import load_config
from .models.apartment import Apartment
from .services.currency import CurrencyService
from .services.deduplication import DeduplicationService
from .services.email_sender import EmailService
from .services.scoring import ScoringService, ScoringWeights
from .utils.logging import setup_logging

logger = logging.getLogger(__name__)

# Max listings liveness-checked per source per run (runtime is also capped by
# adapters.base.LIVENESS_TIME_BUDGET_SECONDS).
LIVENESS_BATCH_SIZE = 50


class ApartmentFinder:
    """
    Main orchestrator for the apartment finder.

    Coordinates: fetching -> currency conversion -> scoring ->
                 deduplication -> email delivery
    """

    def __init__(self, config_path: str = "./config/config.yaml"):
        self.config = load_config(config_path)
        self.currency_service = CurrencyService()
        self.dedup_service = DeduplicationService()
        self.email_service = EmailService()

        # Build scoring service from config
        search = self.config["search"]
        weights_config = self.config.get("scoring", {}).get("weights", {})

        self.scoring_service = ScoringService(
            min_price=search["budget"]["min_usd"],
            max_price=search["budget"]["max_usd"],
            min_sqft=search["size"]["min_sqft"],
            must_haves=search.get("must_have", []),
            preferences=search.get("preferences", []),
            weights=ScoringWeights(**weights_config) if weights_config else None,
            preferred_neighborhoods=search.get("preferred_neighborhoods", []),
        )

    def run(self, skip_email: bool = False, only_city: str = None, only_source: str = None) -> Dict[str, List[Apartment]]:
        """
        Execute the full apartment finding pipeline.

        Args:
            skip_email: If True, skip sending email (useful for testing)
            only_city: If set, only process this city key
            only_source: If set, only use this source

        Returns:
            Dict mapping city names to lists of top apartments
        """
        logger.info("Starting apartment finder run")
        run_started = datetime.utcnow()
        results_by_city: Dict[str, List[Apartment]] = {}

        # Process each city
        for city_key, city_config in self.config["cities"].items():
            # Skip if filtering by city
            if only_city and city_key != only_city:
                continue

            display_name = city_config["display_name"]
            logger.info(f"Processing city: {display_name}")

            try:
                apartments = self._process_city(city_key, city_config, only_source)
                results_by_city[display_name] = apartments
            except Exception as e:
                logger.error(f"Failed to process {city_key}: {e}")
                results_by_city[display_name] = []

            try:
                self._check_liveness(city_config, only_source, run_started)
            except Exception as e:
                logger.error(f"Liveness check failed for {city_key}: {e}")

        # Send email if enabled and we have results
        if not skip_email and self.config.get("email", {}).get("enabled", True):
            self._send_email(results_by_city)

        # Cleanup old listings periodically
        self.dedup_service.cleanup_old_listings()

        logger.info("Apartment finder run complete")
        return results_by_city

    def _check_liveness(self, city_config: dict, only_source: Optional[str], seen_before: datetime) -> None:
        """Ask each source whether listings this run didn't re-see are still live."""
        for source_name in city_config.get("sources", []):
            if source_name not in ADAPTER_REGISTRY or (only_source and source_name != only_source):
                continue
            try:
                rows = self.dedup_service.get_listings_to_check(
                    source_name, seen_before, LIVENESS_BATCH_SIZE
                )
                if not rows:
                    continue
                source_config = self.config.get("sources", {}).get(source_name, {})
                adapter = get_adapter(source_name, source_config, city_config)
                statuses = adapter.check_status([r["url"] for r in rows])
                checked, gone, live = [], [], []
                for row in rows:
                    status = statuses.get(row["url"])
                    if status is None:
                        continue  # not checked this run; stays at the front of the queue
                    checked.append(row["source_id"])
                    if status == "gone":
                        gone.append(row["source_id"])
                    elif status == "active":
                        live.append(row["source_id"])
                self.dedup_service.record_status_check(checked, gone, live)
            except Exception as e:
                logger.error(f"Liveness check failed for {source_name}: {e}")

    def _process_city(self, city_key: str, city_config: dict, only_source: str = None) -> List[Apartment]:
        """Process a single city: fetch, convert, score, dedupe."""
        all_apartments = []

        # Build search criteria with local currency
        local_currency = city_config.get("currency", "USD")
        search = self.config["search"]

        # A city can override the global USD budget (e.g. Sri Lanka's market
        # prices out very differently than Copenhagen's) via city_config.budget
        city_budget = city_config.get("budget", {})
        min_usd = city_budget.get("min_usd", search["budget"]["min_usd"])
        max_usd = city_budget.get("max_usd", search["budget"]["max_usd"])

        # Convert USD budget to local currency
        min_local = self.currency_service.convert_from_usd(min_usd, local_currency)
        max_local = self.currency_service.convert_from_usd(max_usd, local_currency)

        criteria = SearchCriteria(
            min_price_local=min_local,
            max_price_local=max_local,
            min_sqft=search["size"]["min_sqft"],
            min_bedrooms=search["size"]["bedrooms"]["min"],
            max_bedrooms=search["size"]["bedrooms"]["max"],
            must_have_amenities=search.get("must_have", []),
        )

        # Fetch from each source configured for this city
        for source_name in city_config.get("sources", []):
            # Skip if filtering by source
            if only_source and source_name != only_source:
                continue

            if source_name not in ADAPTER_REGISTRY:
                logger.warning(f"Unknown source {source_name} for {city_key}")
                continue

            try:
                source_config = self.config.get("sources", {}).get(source_name, {})
                adapter = get_adapter(source_name, source_config, city_config)

                if not adapter.is_available():
                    logger.warning(f"Adapter {source_name} not available (missing config?)")
                    continue

                listings = adapter.fetch_listings(criteria)

                # Convert prices to USD for comparison
                for apt in listings:
                    if apt.price_usd is None:
                        apt.price_usd = self.currency_service.convert_to_usd(
                            apt.price_local, apt.currency
                        )

                all_apartments.extend(listings)

            except Exception as e:
                logger.error(f"Error fetching from {source_name}: {e}")
                continue

        # Filter out previously seen listings
        new_apartments = self.dedup_service.filter_new_listings(all_apartments)

        # Score and rank (using this city's budget, which may override the global one)
        scored = self.scoring_service.score_apartments(new_apartments, min_price=min_usd, max_price=max_usd)

        logger.info(
            f"City {city_key}: {len(all_apartments)} fetched, "
            f"{len(new_apartments)} new, {len(scored)} scored"
        )

        return scored

    def _send_email(self, results: Dict[str, List[Apartment]]) -> None:
        """Send email digest with results."""
        email_config = self.config.get("email", {})
        recipients = email_config.get("recipients", [])
        top_n = email_config.get("top_picks_per_city", 3)

        if not recipients:
            logger.warning("No email recipients configured")
            return

        # Filter out placeholder email
        recipients = [r for r in recipients if r != "your_email@example.com"]
        if not recipients:
            logger.warning("No valid email recipients configured (update config/config.yaml)")
            return

        # Get top N per city
        top_picks = {city: apts[:top_n] for city, apts in results.items()}

        # Check if we have any listings to send
        total = sum(len(apts) for apts in top_picks.values())
        if total == 0:
            logger.info("No new apartments to email")
            return

        # Send email
        success = self.email_service.send_daily_digest(recipients, top_picks, top_n)

        if success:
            # Mark as sent in dedup database
            all_sent = [apt for apts in top_picks.values() for apt in apts]
            self.dedup_service.mark_as_sent(all_sent)


# A source run where more than this share of listings lack core fields is
# treated as a markup change and not stored.
INCOMPLETE_LISTING_RATIO = 0.3


def _is_incomplete(car) -> bool:
    if car.listing_type == "lease":
        # New-car lease offers can lack a model year; price and term can't be missing
        return car.monthly_price_local is None or car.term_months is None
    return car.year is None or car.price_local is None


def run_car_pipeline(
    config: dict, only_city: Optional[str] = None, only_source: Optional[str] = None
) -> Dict[str, int]:
    """
    Fetch car listings for each city under config's `cars:` section, store
    them, liveness-check cars this run didn't re-see, and expire old ones.

    Returns:
        {city display name: number of listings fetched}

    Raises:
        CarPipelineError: after storing and cleanup, if any source raised or
        returned zero listings (so cron runs fail visibly).
    """
    car_cities = config.get("cars") or {}
    if only_city and only_city not in car_cities:
        raise ValueError(f"No cars config for city '{only_city}'. Configured: {list(car_cities)}")

    service = CarListingService()
    currency_service = CurrencyService()
    run_started = datetime.utcnow()
    fetched_by_city: Dict[str, int] = {}
    failures: List[str] = []

    for city_key, city_config in car_cities.items():
        if only_city and city_key != only_city:
            continue
        display_name = city_config.get("display_name", city_key)
        fetched_by_city[display_name] = 0
        attempted = 0

        for source_name in city_config.get("sources", []):
            if only_source and source_name != only_source:
                continue
            if source_name not in CAR_ADAPTER_REGISTRY:
                logger.warning(f"Unknown car source {source_name} for {city_key}")
                failures.append(f"{city_key}/{source_name}: unknown source")
                continue
            attempted += 1
            try:
                source_config = config.get("sources", {}).get(source_name, {})
                adapter = get_car_adapter(source_name, source_config, city_config)
                cars = adapter.fetch_listings()
                for error in getattr(adapter, "page_errors", []):
                    failures.append(f"{city_key}/{source_name}: {error}")
                # Listings that parse but lack core fields mean the source
                # changed under us; storing them would null out good data.
                incomplete = sum(1 for car in cars if _is_incomplete(car))
                if cars and incomplete > INCOMPLETE_LISTING_RATIO * len(cars):
                    failures.append(
                        f"{city_key}/{source_name}: {incomplete}/{len(cars)} listings missing "
                        "core fields (price/year, or monthly price/term for leases); not stored"
                    )
                    continue
                for car in cars:
                    amount = car.price_local if car.price_local is not None else car.monthly_price_local
                    if car.price_usd is None and amount is not None:
                        car.price_usd = currency_service.convert_to_usd(amount, car.currency)
                service.upsert_listings(cars)
                fetched_by_city[display_name] += len(cars)
                if not cars:
                    failures.append(f"{city_key}/{source_name}: 0 listings (blocked or markup changed?)")
                # A source that returns its whole catalog each run: anything it
                # didn't return is gone (or no longer matches our filters), so
                # no per-listing checks are needed. Only after a clean run.
                elif getattr(adapter, "full_catalog", False) and not adapter.page_errors:
                    if service.mark_unseen_gone(source_name, display_name, run_started) is not None:
                        continue
                    failures.append(
                        f"{city_key}/{source_name}: too many listings missing from a 'full' run; "
                        "not marked gone (checking individually instead)"
                    )
            except Exception as e:
                logger.error(f"Error fetching cars from {source_name}: {e}")
                failures.append(f"{city_key}/{source_name}: {e}")
                continue

            try:
                rows = service.get_listings_to_check(source_name, run_started, LIVENESS_BATCH_SIZE)
                if rows:
                    statuses = adapter.check_status([r["url"] for r in rows])
                    checked, gone, live = [], [], []
                    for row in rows:
                        status = statuses.get(row["url"])
                        if status is None:
                            continue  # not checked this run; stays first in line
                        checked.append(row["source_id"])
                        if status == "gone":
                            gone.append(row["source_id"])
                        elif status == "active":
                            live.append(row["source_id"])
                    service.record_status_check(checked, gone, live)
            except Exception as e:
                logger.error(f"Car liveness check failed for {source_name}: {e}")

        if attempted == 0:
            failures.append(f"{city_key}: no car sources ran")

    service.cleanup_old_listings()
    if failures:
        # Fail the cron run visibly: a silently broken scraper ages every
        # listing out after EXPIRY_DAYS (this wiped the apartments DB once).
        raise CarPipelineError(
            f"Fetched {fetched_by_city}; failed sources: " + "; ".join(failures)
        )
    return fetched_by_city


class CarPipelineError(RuntimeError):
    """At least one car source raised or returned no listings."""


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Apartment Finder - Daily apartment search across multiple cities"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="./config/config.yaml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--city",
        help="Only process this city (e.g., nyc, la, dubai)",
    )
    parser.add_argument(
        "--source",
        help="Only use this source (e.g., craigslist, findproperties)",
    )
    parser.add_argument(
        "--cars",
        action="store_true",
        help="Run the car listings pipeline (config 'cars:' section) instead of apartments",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Skip sending email (useful for testing)",
    )
    parser.add_argument(
        "--test-email",
        metavar="EMAIL",
        help="Send a test email to verify configuration",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics and exit",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging("DEBUG" if args.verbose else "INFO")

    # Handle --stats
    if args.stats:
        dedup = DeduplicationService()
        stats = dedup.get_stats()
        print("\n=== Apartment Finder Statistics ===")
        print(f"Total tracked: {stats['total_tracked']}")
        print(f"Total sent: {stats['total_sent']}")
        print("\nBy city:")
        for city, count in stats.get("by_city", {}).items():
            print(f"  {city}: {count}")
        print("\nBy source:")
        for source, count in stats.get("by_source", {}).items():
            print(f"  {source}: {count}")
        return

    # Handle --test-email
    if args.test_email:
        email_service = EmailService()
        if email_service.send_test_email(args.test_email):
            print(f"Test email sent to {args.test_email}")
        else:
            print("Failed to send test email - check your configuration")
            sys.exit(1)
        return

    # Car pipeline: own config section and table; never sends email
    if args.cars:
        try:
            fetched = run_car_pipeline(load_config(args.config), args.city, args.source)
        except Exception as e:
            logger.exception(f"Car pipeline failed: {e}")
            sys.exit(1)
        print("\n=== Car Listings Results ===")
        for city, count in fetched.items():
            print(f"{city}: {count} listings fetched")
        return

    # Run the finder
    try:
        finder = ApartmentFinder(args.config)
        results = finder.run(
            skip_email=args.no_email,
            only_city=args.city,
            only_source=args.source
        )

        # Print summary
        print("\n=== Apartment Finder Results ===")
        for city, apartments in results.items():
            print(f"\n{city}: {len(apartments)} matches")
            for apt in apartments[:3]:
                print(f"  - {apt.title[:50]}")
                print(f"    {apt.display_price()} | {apt.display_size()} | Score: {apt.score}")
                print(f"    {apt.url}")

    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
