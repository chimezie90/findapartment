"""Deduplication service using SQLite to track seen listings."""

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from ..models.apartment import Apartment

logger = logging.getLogger(__name__)


class DeduplicationService:
    """
    Track seen listings using SQLite to avoid showing repeats.

    Features:
    - Persist listing IDs across runs
    - Auto-expire old listings (configurable)
    - Track when listing was first/last seen
    - Mark listings as sent in email
    """

    DEFAULT_DB_PATH = "./data/listings.db"
    EXPIRY_DAYS = 30  # Remove listings not seen for this many days

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("DATABASE_PATH", self.DEFAULT_DB_PATH)
        self._ensure_db_directory()
        self._init_db()

    def _ensure_db_directory(self) -> None:
        """Create data directory if it doesn't exist."""
        db_dir = Path(self.db_path).parent
        if db_dir and not db_dir.exists():
            db_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS seen_listings (
                    source_id TEXT PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    city TEXT NOT NULL,
                    title TEXT,
                    price_usd REAL,
                    url TEXT,
                    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sent_in_email BOOLEAN DEFAULT FALSE,
                    sent_at TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_source
                ON seen_listings(city, source_name)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_last_seen
                ON seen_listings(last_seen_at)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sent_in_email
                ON seen_listings(sent_in_email)
            """)

        logger.debug(f"Database initialized at {self.db_path}")

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

        with self._get_connection() as conn:
            for apt in apartments:
                # Check if we've seen this listing
                row = conn.execute(
                    "SELECT source_id, sent_in_email FROM seen_listings WHERE source_id = ?",
                    (apt.source_id,),
                ).fetchone()

                if row is None:
                    # New listing - add to DB and results
                    conn.execute(
                        """
                        INSERT INTO seen_listings
                        (source_id, source_name, city, title, price_usd, url)
                        VALUES (?, ?, ?, ?, ?, ?)
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
                    conn.execute(
                        "UPDATE seen_listings SET last_seen_at = ? WHERE source_id = ?",
                        (datetime.utcnow(), apt.source_id),
                    )
                    new_apartments.append(apt)

                else:
                    # Already sent - just update last_seen
                    conn.execute(
                        "UPDATE seen_listings SET last_seen_at = ? WHERE source_id = ?",
                        (datetime.utcnow(), apt.source_id),
                    )

        logger.info(f"Filtered {len(apartments)} listings to {len(new_apartments)} new ones")
        return new_apartments

    def mark_as_sent(self, apartments: List[Apartment]) -> None:
        """Mark listings as sent in email."""
        if not apartments:
            return

        with self._get_connection() as conn:
            now = datetime.utcnow()
            for apt in apartments:
                conn.execute(
                    "UPDATE seen_listings SET sent_in_email = TRUE, sent_at = ? WHERE source_id = ?",
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

        with self._get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM seen_listings WHERE last_seen_at < ?",
                (cutoff,),
            )
            count = cursor.rowcount

        if count > 0:
            logger.info(f"Cleaned up {count} listings older than {days} days")
        return count

    def get_stats(self) -> dict:
        """Get statistics about tracked listings."""
        with self._get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM seen_listings").fetchone()[0]
            sent = conn.execute(
                "SELECT COUNT(*) FROM seen_listings WHERE sent_in_email = TRUE"
            ).fetchone()[0]

            by_city = {}
            for row in conn.execute(
                "SELECT city, COUNT(*) as count FROM seen_listings GROUP BY city"
            ):
                by_city[row["city"]] = row["count"]

            by_source = {}
            for row in conn.execute(
                "SELECT source_name, COUNT(*) as count FROM seen_listings GROUP BY source_name"
            ):
                by_source[row["source_name"]] = row["count"]

            return {
                "total_tracked": total,
                "total_sent": sent,
                "by_city": by_city,
                "by_source": by_source,
            }

    def reset(self) -> None:
        """Clear all tracked listings. Use with caution."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM seen_listings")
        logger.warning("All tracked listings have been reset")
