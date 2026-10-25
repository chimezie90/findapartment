"""Deduplication service using PostgreSQL to track seen listings."""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from ..db import get_connection, init_db
from ..models.apartment import Apartment

logger = logging.getLogger(__name__)

SQFT_PER_SQM = 10.764
UPSERT_CHUNK_SIZE = 500
INSERT_COLUMNS = (
    "source_id, source_name, city, title, price_usd, url, thumbnail_url, "
    "description, latitude, longitude, neighborhood, bedrooms, bathrooms, "
    "size_sqm, amenities, posted_date, first_seen_at, last_seen_at"
)


def _listing_metadata(apt: Apartment) -> tuple:
    """(bedrooms, bathrooms, size_sqm, amenities, posted_date) for storage."""
    size_sqm = round(apt.sqft / SQFT_PER_SQM, 1) if apt.sqft else None
    amenities = ", ".join(apt.amenities.to_list()) or None
    return (apt.bedrooms, apt.bathrooms, size_sqm, amenities, apt.posted_date)


class DeduplicationService:
    """
    Track seen listings using PostgreSQL to avoid showing repeats.

    Features:
    - Persist listing IDs across runs
    - Auto-expire old listings (configurable)
    - Track when listing was first/last seen
    - Mark listings as sent in email
    """

    EXPIRY_DAYS = 30  # Remove listings not seen for this many days

    def __init__(self):
        init_db()

    def filter_new_listings(self, apartments: List[Apartment]) -> List[Apartment]:
        """
        Filter out previously seen listings that have already been emailed.

        Updates last_seen_at for existing listings.
        Adds new listings to the database.

        Args:
            apartments: List of apartments to filter

        Returns:
            List of apartments that haven't been emailed yet
        """
        if not apartments:
            return []

        # Last copy wins if a source returned the same listing twice
        apartments = list({apt.source_id: apt for apt in apartments}.values())
        unsent_ids = set()
        skipped = 0
        now = datetime.utcnow()

        with get_connection() as conn:
            cur = conn.cursor()
            for start in range(0, len(apartments), UPSERT_CHUNK_SIZE):
                chunk = apartments[start:start + UPSERT_CHUNK_SIZE]
                # One statement per chunk: per-row queries cost minutes on a
                # remote database (a Berlin run of ~1,300 listings timed out
                # at 15 min on Replit)
                cur.execute("SAVEPOINT listing_chunk")
                try:
                    unsent_ids.update(self._upsert_chunk(cur, chunk, now))
                    cur.execute("RELEASE SAVEPOINT listing_chunk")
                    continue
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT listing_chunk")
                    logger.warning(f"Chunk upsert failed ({e}); retrying {len(chunk)} listings one by one")
                for apt in chunk:
                    cur.execute("SAVEPOINT listing_row")
                    try:
                        if self._upsert_one(cur, apt, now):
                            unsent_ids.add(apt.source_id)
                        cur.execute("RELEASE SAVEPOINT listing_row")
                    except psycopg2.Error as e:
                        cur.execute("ROLLBACK TO SAVEPOINT listing_row")
                        skipped += 1
                        logger.warning(f"Skipped listing {apt.source_id}: {e}")

        new_apartments = [apt for apt in apartments if apt.source_id in unsent_ids]
        logger.info(
            f"Filtered {len(apartments)} listings to {len(new_apartments)} new ones ({skipped} skipped)"
        )
        return new_apartments

    @staticmethod
    def _row(apt: Apartment, now: datetime) -> tuple:
        return (
            apt.source_id, apt.source_name, apt.city, apt.title, apt.price_usd,
            apt.url, apt.thumbnail_url, apt.description, apt.latitude,
            apt.longitude, apt.neighborhood, *_listing_metadata(apt), now, now,
        )

    @classmethod
    def _upsert_chunk(cls, cur, apartments: List[Apartment], now: datetime) -> List[str]:
        """Insert or refresh many listings in one statement; returns the ids
        not yet emailed (new ones included). Same rules as _upsert_one."""
        rows = execute_values(
            cur,
            f"""
            INSERT INTO seen_listings ({INSERT_COLUMNS})
            VALUES %s
            ON CONFLICT (source_id) DO UPDATE SET
                last_seen_at = EXCLUDED.last_seen_at,
                status = 'active',
                price_usd = COALESCE(EXCLUDED.price_usd, seen_listings.price_usd),
                thumbnail_url = COALESCE(seen_listings.thumbnail_url, EXCLUDED.thumbnail_url),
                description = COALESCE(seen_listings.description, EXCLUDED.description),
                latitude = COALESCE(seen_listings.latitude, EXCLUDED.latitude),
                longitude = COALESCE(seen_listings.longitude, EXCLUDED.longitude),
                neighborhood = COALESCE(seen_listings.neighborhood, EXCLUDED.neighborhood),
                bedrooms = COALESCE(seen_listings.bedrooms, EXCLUDED.bedrooms),
                bathrooms = COALESCE(seen_listings.bathrooms, EXCLUDED.bathrooms),
                size_sqm = COALESCE(seen_listings.size_sqm, EXCLUDED.size_sqm),
                amenities = COALESCE(seen_listings.amenities, EXCLUDED.amenities),
                posted_date = COALESCE(seen_listings.posted_date, EXCLUDED.posted_date)
            RETURNING source_id, sent_in_email
            """,
            [cls._row(apt, now) for apt in apartments],
            page_size=len(apartments),
            fetch=True,
        )
        return [row["source_id"] for row in rows if not row["sent_in_email"]]

    @classmethod
    def _upsert_one(cls, cur, apt: Apartment, now: datetime) -> bool:
        """Insert or refresh one listing. Returns True if not yet emailed."""
        cur.execute(
            "SELECT sent_in_email FROM seen_listings WHERE source_id = %s",
            (apt.source_id,),
        )
        row = cur.fetchone()
        if row is None:
            placeholders = ", ".join(["%s"] * len(cls._row(apt, now)))
            cur.execute(
                f"INSERT INTO seen_listings ({INSERT_COLUMNS}) VALUES ({placeholders})",
                cls._row(apt, now),
            )
            return True

        # Seen again: refresh liveness and price, backfill missing data.
        # Re-appearing in a scrape means the listing is live again
        # (e.g. a Balder unit that went Reserveret -> Ledig).
        cur.execute(
            """UPDATE seen_listings
               SET last_seen_at = %s,
                   status = 'active',
                   price_usd = COALESCE(%s, price_usd),
                   thumbnail_url = COALESCE(thumbnail_url, %s),
                   description = COALESCE(description, %s),
                   latitude = COALESCE(latitude, %s),
                   longitude = COALESCE(longitude, %s),
                   neighborhood = COALESCE(neighborhood, %s),
                   bedrooms = COALESCE(bedrooms, %s),
                   bathrooms = COALESCE(bathrooms, %s),
                   size_sqm = COALESCE(size_sqm, %s),
                   amenities = COALESCE(amenities, %s),
                   posted_date = COALESCE(posted_date, %s)
               WHERE source_id = %s""",
            (now, apt.price_usd, apt.thumbnail_url, apt.description,
             apt.latitude, apt.longitude, apt.neighborhood, *_listing_metadata(apt),
             apt.source_id),
        )
        return not row["sent_in_email"]

    def mark_as_sent(self, apartments: List[Apartment]) -> None:
        """Mark listings as sent in email."""
        if not apartments:
            return

        with get_connection() as conn:
            cur = conn.cursor()
            now = datetime.utcnow()
            for apt in apartments:
                cur.execute(
                    "UPDATE seen_listings SET sent_in_email = TRUE, sent_at = %s WHERE source_id = %s",
                    (now, apt.source_id),
                )

        logger.info(f"Marked {len(apartments)} listings as sent")

    def get_listings_to_check(
        self, source_name: str, seen_before: datetime, limit: int
    ) -> List[dict]:
        """Active listings from a source not seen since `seen_before`,
        least-recently-checked first. Not filtered by city: some adapters
        store a city name that differs from config's display_name."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT source_id, url FROM seen_listings
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
        """
        Stamp checked listings and apply results: gone_ids are marked gone;
        live_ids (positively confirmed by the source) count as a sighting.
        """
        if not checked_ids:
            return
        now = datetime.utcnow()
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE seen_listings SET status_checked_at = %s WHERE source_id = ANY(%s)",
                (now, checked_ids),
            )
            if gone_ids:
                cur.execute(
                    "UPDATE seen_listings SET status = 'gone' WHERE source_id = ANY(%s)",
                    (gone_ids,),
                )
            if live_ids:
                cur.execute(
                    "UPDATE seen_listings SET status = 'active', last_seen_at = %s WHERE source_id = ANY(%s)",
                    (now, live_ids),
                )
        logger.info(
            f"Liveness: checked {len(checked_ids)} listings, "
            f"{len(gone_ids)} gone, {len(live_ids or [])} confirmed live"
        )

    def cleanup_old_listings(self, days: Optional[int] = None) -> int:
        """
        Remove listings not seen for a specified number of days.

        Args:
            days: Number of days after which to remove listings.
                  Defaults to EXPIRY_DAYS.

        Returns:
            Number of listings removed
        """
        days = days or self.EXPIRY_DAYS
        cutoff = datetime.utcnow() - timedelta(days=days)

        with get_connection() as conn:
            cur = conn.cursor()
            # Listings with comments/ratings are kept (and shown as inactive):
            # deleting them would violate the FKs and abort the whole cleanup.
            cur.execute(
                """DELETE FROM seen_listings s
                   WHERE s.last_seen_at < %s
                     AND NOT EXISTS (SELECT 1 FROM comments c WHERE c.listing_id = s.source_id)
                     AND NOT EXISTS (SELECT 1 FROM ratings r WHERE r.listing_id = s.source_id)""",
                (cutoff,),
            )
            count = cur.rowcount

        if count > 0:
            logger.info(f"Cleaned up {count} listings older than {days} days")
        return count

    def get_stats(self) -> dict:
        """Get statistics about tracked listings."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS cnt FROM seen_listings")
            total = cur.fetchone()['cnt']

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM seen_listings WHERE sent_in_email = TRUE"
            )
            sent = cur.fetchone()['cnt']

            by_city = {}
            cur.execute(
                "SELECT city, COUNT(*) AS count FROM seen_listings GROUP BY city"
            )
            for row in cur.fetchall():
                by_city[row["city"]] = row["count"]

            by_source = {}
            cur.execute(
                "SELECT source_name, COUNT(*) AS count FROM seen_listings GROUP BY source_name"
            )
            for row in cur.fetchall():
                by_source[row["source_name"]] = row["count"]

            return {
                "total_tracked": total,
                "total_sent": sent,
                "by_city": by_city,
                "by_source": by_source,
            }

    def reset(self) -> None:
        """Clear all tracked listings. Use with caution."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM seen_listings")
        logger.warning("All tracked listings have been reset")
