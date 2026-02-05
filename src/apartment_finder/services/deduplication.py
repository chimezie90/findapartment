"""Deduplication service using PostgreSQL to track seen listings."""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from ..db import get_connection, init_db
from ..models.apartment import Apartment

logger = logging.getLogger(__name__)


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

        new_apartments = []

        with get_connection() as conn:
            cur = conn.cursor()
            for apt in apartments:
                # Check if we've seen this listing
                cur.execute(
                    "SELECT source_id, sent_in_email FROM seen_listings WHERE source_id = %s",
                    (apt.source_id,),
                )
                row = cur.fetchone()

                if row is None:
                    # New listing - add to DB and results
                    cur.execute(
                        """
                        INSERT INTO seen_listings
                        (source_id, source_name, city, title, price_usd, url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            apt.source_id,
                            apt.source_name,
                            apt.city,
                            apt.title,
                            apt.price_usd,
                            apt.url,
                        ),
                    )
                    new_apartments.append(apt)

                elif not row["sent_in_email"]:
                    # Seen before but never emailed - include again
                    cur.execute(
                        "UPDATE seen_listings SET last_seen_at = %s WHERE source_id = %s",
                        (datetime.utcnow(), apt.source_id),
                    )
                    new_apartments.append(apt)

                else:
                    # Already sent - just update last_seen
                    cur.execute(
                        "UPDATE seen_listings SET last_seen_at = %s WHERE source_id = %s",
                        (datetime.utcnow(), apt.source_id),
                    )

        logger.info(f"Filtered {len(apartments)} listings to {len(new_apartments)} new ones")
        return new_apartments

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
            cur.execute(
                "DELETE FROM seen_listings WHERE last_seen_at < %s",
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
