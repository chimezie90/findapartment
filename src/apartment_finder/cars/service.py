"""Store car listings in PostgreSQL: upsert, liveness bookkeeping, cleanup.

Mirrors services.deduplication.DeduplicationService for the car_listings table.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from ..db import get_connection, init_db
from .models import Car

logger = logging.getLogger(__name__)

UPSERT_CHUNK_SIZE = 500


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

        # Last copy wins if a source returned the same listing twice
        cars = list({car.source_id: car for car in cars}.values())
        new_ids = set()
        skipped = 0
        now = datetime.utcnow()
        with get_connection() as conn:
            cur = conn.cursor()
            for start in range(0, len(cars), UPSERT_CHUNK_SIZE):
                chunk = cars[start:start + UPSERT_CHUNK_SIZE]
                # One statement per chunk: per-row queries cost minutes on a
                # remote database (a DBA run timed out at 8 min on Replit)
                cur.execute("SAVEPOINT car_chunk")
                try:
                    new_ids.update(self._upsert_chunk(cur, chunk, now))
                    cur.execute("RELEASE SAVEPOINT car_chunk")
                    continue
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT car_chunk")
                    logger.warning(f"Chunk upsert failed ({e}); retrying {len(chunk)} cars one by one")
                for car in chunk:
                    # One bad row (e.g. an absurd mileage overflowing INTEGER)
                    # must not roll back the rest, run after run
                    cur.execute("SAVEPOINT car_row")
                    try:
                        if self._upsert_one(cur, car, now):
                            new_ids.add(car.source_id)
                        cur.execute("RELEASE SAVEPOINT car_row")
                    except psycopg2.Error as e:
                        cur.execute("ROLLBACK TO SAVEPOINT car_row")
                        skipped += 1
                        logger.warning(f"Skipped car {car.source_id}: {e}")

        new_cars = [car for car in cars if car.source_id in new_ids]
        logger.info(f"Stored {len(cars) - skipped} car listings ({len(new_cars)} new, {skipped} skipped)")
        return new_cars

    @staticmethod
    def _upsert_chunk(cur, cars: List[Car], now: datetime) -> List[str]:
        """Insert or refresh many cars in one statement; returns the new ids.
        Same rules as _upsert_one: refreshed rows take the scraped type,
        prices and flags, and only backfill the other fields."""
        rows = execute_values(
            cur,
            """
            INSERT INTO car_listings
            (source_id, source_name, city, listing_type, make, model,
             variant, year, mileage_km, fuel, gearbox, price_local,
             currency, price_usd, monthly_price_local, down_payment_local,
             term_months, km_per_year, lease_kind, location,
             seller_type, is_promoted, vat_added, url, thumbnail_url, listed_at,
             first_seen_at, last_seen_at, status)
            VALUES %s
            ON CONFLICT (source_id) DO UPDATE SET
                last_seen_at = EXCLUDED.last_seen_at,
                status = 'active',
                listing_type = EXCLUDED.listing_type,
                price_local = EXCLUDED.price_local,
                price_usd = EXCLUDED.price_usd,
                monthly_price_local = EXCLUDED.monthly_price_local,
                down_payment_local = EXCLUDED.down_payment_local,
                term_months = EXCLUDED.term_months,
                km_per_year = EXCLUDED.km_per_year,
                lease_kind = EXCLUDED.lease_kind,
                is_promoted = EXCLUDED.is_promoted,
                vat_added = EXCLUDED.vat_added,
                mileage_km = COALESCE(EXCLUDED.mileage_km, car_listings.mileage_km),
                thumbnail_url = COALESCE(car_listings.thumbnail_url, EXCLUDED.thumbnail_url),
                variant = COALESCE(car_listings.variant, EXCLUDED.variant),
                fuel = COALESCE(car_listings.fuel, EXCLUDED.fuel),
                gearbox = COALESCE(car_listings.gearbox, EXCLUDED.gearbox),
                location = COALESCE(car_listings.location, EXCLUDED.location),
                seller_type = COALESCE(car_listings.seller_type, EXCLUDED.seller_type),
                listed_at = COALESCE(car_listings.listed_at, EXCLUDED.listed_at)
            RETURNING source_id, (xmax = 0) AS inserted
            """,
            [
                (car.source_id, car.source_name, car.city, car.listing_type,
                 car.make, car.model, car.variant, car.year, car.mileage_km,
                 car.fuel, car.gearbox, car.price_local, car.currency,
                 car.price_usd, car.monthly_price_local, car.down_payment_local,
                 car.term_months, car.km_per_year, car.lease_kind, car.location,
                 car.seller_type, car.is_promoted, car.vat_added, car.url, car.thumbnail_url,
                 car.listed_at, now, now, "active")
                for car in cars
            ],
            page_size=len(cars),
            fetch=True,
        )
        return [row["source_id"] for row in rows if row["inserted"]]

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

    # One run may not hide more than this share of a source's active cars;
    # beyond it, a "full" catalog run is more likely broken than real
    MAX_GONE_SHARE = 0.2

    def mark_unseen_gone(self, source_name: str, city: str, seen_before: datetime) -> Optional[int]:
        """Mark a source's active cars in `city` not seen since `seen_before`
        as gone. Returns the count, or None if it would exceed MAX_GONE_SHARE
        (nothing is changed then)."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT COUNT(*) FILTER (WHERE last_seen_at < %s) AS unseen, COUNT(*) AS active
                   FROM car_listings
                   WHERE source_name = %s AND LOWER(city) = LOWER(%s) AND status = 'active'""",
                (seen_before, source_name, city),
            )
            row = cur.fetchone()
            if row["active"] and row["unseen"] > self.MAX_GONE_SHARE * row["active"]:
                logger.warning(
                    f"{source_name}: {row['unseen']} of {row['active']} active cars unseen; "
                    "too many to mark gone in one run"
                )
                return None
            cur.execute(
                """UPDATE car_listings SET status = 'gone', status_checked_at = %s
                   WHERE source_name = %s AND LOWER(city) = LOWER(%s)
                     AND status = 'active' AND last_seen_at < %s""",
                (datetime.utcnow(), source_name, city, seen_before),
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
