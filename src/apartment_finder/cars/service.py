"""Store car listings in PostgreSQL: upsert, liveness bookkeeping, cleanup.

Mirrors services.deduplication.DeduplicationService for the car_listings table.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2

from ..db import get_connection, init_db
from .models import Car

logger = logging.getLogger(__name__)


class CarListingService:
    """Insert new car listings, refresh re-seen ones, track liveness."""

    EXPIRY_DAYS = 30  # Delete listings not seen (or confirmed live) for this long

    def __init__(self):
        init_db()

    def upsert_listings(self, cars: List[Car]) -> List[Car]:
        """
        Insert unseen cars; refresh re-seen ones (last_seen_at, status back to
        active, price, promotion flag; backfill missing fields).

        Returns:
            The cars that were new.
        """
        if not cars:
            return []

        new_cars = []
        skipped = 0
        now = datetime.utcnow()
        with get_connection() as conn:
            cur = conn.cursor()
            for car in cars:
                # One bad row (e.g. an absurd mileage overflowing INTEGER) must
                # not roll back the whole batch run after run.
                cur.execute("SAVEPOINT car_row")
                try:
                    if self._upsert_one(cur, car, now):
                        new_cars.append(car)
                    cur.execute("RELEASE SAVEPOINT car_row")
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT car_row")
                    skipped += 1
                    logger.warning(f"Skipped car {car.source_id}: {e}")

        logger.info(f"Stored {len(cars) - skipped} car listings ({len(new_cars)} new, {skipped} skipped)")
        return new_cars

    @staticmethod
    def _upsert_one(cur, car: Car, now: datetime) -> bool:
        """Insert or refresh one car. Returns True if it was new."""
        cur.execute(
            "SELECT 1 FROM car_listings WHERE source_id = %s", (car.source_id,)
        )
        if cur.fetchone() is None:
            cur.execute(
                """
                INSERT INTO car_listings
                (source_id, source_name, city, listing_type, make, model,
                 variant, year, mileage_km, fuel, gearbox, price_local,
                 currency, price_usd, monthly_price_local, down_payment_local,
                 term_months, km_per_year, lease_kind, location,
                 seller_type, is_promoted, vat_added, url, thumbnail_url, listed_at,
                 first_seen_at, last_seen_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, 'active')
                """,
                (
                    car.source_id, car.source_name, car.city, car.listing_type,
                    car.make, car.model, car.variant, car.year, car.mileage_km,
                    car.fuel, car.gearbox, car.price_local, car.currency,
                    car.price_usd, car.monthly_price_local, car.down_payment_local,
                    car.term_months, car.km_per_year, car.lease_kind, car.location,
                    car.seller_type, car.is_promoted, car.vat_added, car.url, car.thumbnail_url,
                    car.listed_at, now, now,
                ),
            )
            return True

        # Re-seen in a scrape: live again; take the current type, prices
        # and promotion flag as scraped (sellers cut prices, placements
        # expire, a card can switch between cash and monthly price).
        cur.execute(
            """UPDATE car_listings
               SET last_seen_at = %s,
                   status = 'active',
                   listing_type = %s,
                   price_local = %s,
                   price_usd = %s,
                   monthly_price_local = %s,
                   down_payment_local = %s,
                   term_months = %s,
                   km_per_year = %s,
                   lease_kind = %s,
                   is_promoted = %s,
                   vat_added = %s,
                   mileage_km = COALESCE(%s, mileage_km),
                   thumbnail_url = COALESCE(thumbnail_url, %s),
                   variant = COALESCE(variant, %s),
                   fuel = COALESCE(fuel, %s),
                   gearbox = COALESCE(gearbox, %s),
                   location = COALESCE(location, %s),
                   seller_type = COALESCE(seller_type, %s),
                   listed_at = COALESCE(listed_at, %s)
               WHERE source_id = %s""",
            (
                now, car.listing_type, car.price_local, car.price_usd,
                car.monthly_price_local, car.down_payment_local, car.term_months,
                car.km_per_year, car.lease_kind,
                car.is_promoted, car.vat_added, car.mileage_km, car.thumbnail_url, car.variant,
                car.fuel, car.gearbox, car.location, car.seller_type,
                car.listed_at, car.source_id,
            ),
        )
        return False

    def get_listings_to_check(
        self, source_name: str, seen_before: datetime, limit: int
    ) -> List[dict]:
        """Active cars from a source not seen since `seen_before`,
        least-recently-checked first."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT source_id, url FROM car_listings
                   WHERE status = 'active' AND source_name = %s
                     AND last_seen_at < %s AND url IS NOT NULL
                   ORDER BY status_checked_at ASC NULLS FIRST
                   LIMIT %s""",
                (source_name, seen_before, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    def record_status_check(
        self, checked_ids: List[str], gone_ids: List[str], live_ids: Optional[List[str]] = None
    ) -> None:
        """Stamp checked cars; mark gone_ids gone; live_ids count as a sighting."""
        if not checked_ids:
            return
        now = datetime.utcnow()
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE car_listings SET status_checked_at = %s WHERE source_id = ANY(%s)",
                (now, checked_ids),
            )
            if gone_ids:
                cur.execute(
                    "UPDATE car_listings SET status = 'gone' WHERE source_id = ANY(%s)",
                    (gone_ids,),
                )
            if live_ids:
                cur.execute(
                    "UPDATE car_listings SET status = 'active', last_seen_at = %s WHERE source_id = ANY(%s)",
                    (now, live_ids),
                )
        logger.info(
            f"Car liveness: checked {len(checked_ids)}, {len(gone_ids)} gone, "
            f"{len(live_ids or [])} confirmed live"
        )

    def mark_unseen_gone(self, source_name: str, seen_before: datetime) -> int:
        """Mark a source's active cars not seen since `seen_before` as gone."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """UPDATE car_listings SET status = 'gone', status_checked_at = %s
                   WHERE source_name = %s AND status = 'active' AND last_seen_at < %s""",
                (datetime.utcnow(), source_name, seen_before),
            )
            count = cur.rowcount
        logger.info(f"Marked {count} {source_name} cars gone (not in the full catalog)")
        return count

    def cleanup_old_listings(self, days: Optional[int] = None) -> int:
        """Delete cars not seen for `days` (default EXPIRY_DAYS). Returns count."""
        days = days or self.EXPIRY_DAYS
        cutoff = datetime.utcnow() - timedelta(days=days)
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM car_listings WHERE last_seen_at < %s", (cutoff,))
            count = cur.rowcount
        if count:
            logger.info(f"Cleaned up {count} car listings older than {days} days")
        return count
