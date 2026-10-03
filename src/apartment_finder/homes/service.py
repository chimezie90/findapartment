"""Store homes for sale in PostgreSQL: upsert, price history, liveness, cleanup.

Mirrors cars.service.CarListingService for the home_listings table.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psycopg2

from ..db import get_connection, init_db
from .costs import estimate_monthly_cost
from .models import HomeListing

logger = logging.getLogger(__name__)


class HomeListingService:
    """Insert new homes, refresh re-seen ones, track price changes and liveness."""

    EXPIRY_DAYS = 30  # Delete homes not seen for this long
    GONE_AFTER_MISSED_RUNS = 2  # clean full-catalog runs in a row without the home

    def __init__(self):
        init_db()
        self.last_skipped = 0  # rows the last upsert_listings call couldn't store

    def upsert_listings(self, homes: List[HomeListing]) -> List[HomeListing]:
        """
        Insert unseen homes; refresh re-seen ones (last_seen_at, status back
        to active, price with change tracking; backfill missing fields).

        Returns:
            The homes that were new. Rows that failed are counted in
            self.last_skipped (they weren't re-seen, so the caller must not
            treat the run as a full catalog).
        """
        self.last_skipped = 0
        if not homes:
            return []

        new_homes = []
        skipped = 0
        now = datetime.utcnow()
        with get_connection() as conn:
            cur = conn.cursor()
            for home in homes:
                # One bad row must not roll back the whole batch
                cur.execute("SAVEPOINT home_row")
                try:
                    if self._upsert_one(cur, home, now):
                        new_homes.append(home)
                    cur.execute("RELEASE SAVEPOINT home_row")
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT home_row")
                    skipped += 1
                    logger.warning(f"Skipped home {home.source_id}: {e}")

        self.last_skipped = skipped
        logger.info(f"Stored {len(homes) - skipped} home listings ({len(new_homes)} new, {skipped} skipped)")
        return new_homes

    @staticmethod
    def _upsert_one(cur, home: HomeListing, now: datetime) -> bool:
        """Insert or refresh one home. Returns True if it was new."""
        cur.execute(
            "SELECT price_dkk, monthly_owner_expenses_dkk FROM home_listings WHERE source_id = %s",
            (home.source_id,),
        )
        existing = cur.fetchone()
        if existing is None:
            est = estimate_monthly_cost(home.price_dkk, home.monthly_owner_expenses_dkk)["cash_out"]
            cur.execute(
                """
                INSERT INTO home_listings
                (source_id, source_name, url, address, street, postcode, city,
                 municipality, property_type, sqm, rooms, year_built, price_dkk,
                 first_price_dkk, monthly_owner_expenses_dkk, est_monthly_cash_dkk,
                 energy_label, latitude, longitude, is_external, broker, headline,
                 thumbnail_url, listed_at, first_seen_at, last_seen_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
                """,
                (
                    home.source_id, home.source_name, home.url, home.address, home.street,
                    home.postcode, home.city, home.municipality, home.property_type,
                    home.sqm, home.rooms, home.year_built, home.price_dkk, home.price_dkk,
                    home.monthly_owner_expenses_dkk, est, home.energy_label,
                    home.latitude, home.longitude, home.is_external, home.broker,
                    home.headline, home.thumbnail_url, home.listed_at, now, now,
                ),
            )
            return True

        old_price = existing["price_dkk"]
        price_changed = old_price != home.price_dkk
        owner_expenses = home.monthly_owner_expenses_dkk or existing["monthly_owner_expenses_dkk"]
        est = estimate_monthly_cost(home.price_dkk, owner_expenses)["cash_out"]
        # Re-seen: live again. Take the current price (recording the change),
        # URL and listing flags as scraped; backfill fields we lacked.
        cur.execute(
            """UPDATE home_listings
               SET last_seen_at = %s,
                   status = 'active',
                   missed_runs = 0,
                   url = %s,
                   address = %s,
                   property_type = %s,
                   price_dkk = %s,
                   previous_price_dkk = CASE WHEN %s THEN price_dkk ELSE previous_price_dkk END,
                   price_changed_at = CASE WHEN %s THEN %s ELSE price_changed_at END,
                   first_price_dkk = COALESCE(first_price_dkk, price_dkk),
                   est_monthly_cash_dkk = %s,
                   is_external = %s,
                   broker = %s,
                   sqm = COALESCE(%s, sqm),
                   street = COALESCE(%s, street),
                   postcode = COALESCE(%s, postcode),
                   city = COALESCE(%s, city),
                   municipality = COALESCE(%s, municipality),
                   latitude = COALESCE(%s, latitude),
                   longitude = COALESCE(%s, longitude),
                   headline = COALESCE(%s, headline),
                   thumbnail_url = COALESCE(%s, thumbnail_url)
               WHERE source_id = %s""",
            (
                now, home.url, home.address, home.property_type, home.price_dkk,
                price_changed, price_changed, now, est, home.is_external, home.broker,
                home.sqm, home.street, home.postcode, home.city, home.municipality,
                home.latitude, home.longitude, home.headline, home.thumbnail_url,
                home.source_id,
            ),
        )
        return False

    def get_listings_needing_details(self, source_name: str, limit: int) -> List[dict]:
        """Active, non-external homes whose detail page hasn't been read, newest first."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT source_id, url FROM home_listings
                   WHERE source_name = %s AND status = 'active' AND NOT is_external
                     AND details_checked_at IS NULL
                   ORDER BY first_seen_at DESC, source_id
                   LIMIT %s""",
                (source_name, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    def record_details(self, details: Dict[str, Dict[str, Any]]) -> None:
        """Store detail-page fields; a detail page saying gone/not for sale marks the home gone."""
        if not details:
            return
        now = datetime.utcnow()
        gone = failed = 0
        with get_connection() as conn:
            cur = conn.cursor()
            for source_id, fields in details.items():
                cur.execute("SAVEPOINT home_detail")
                try:
                    gone_now = self._record_one_detail(cur, source_id, fields, now)
                    cur.execute("RELEASE SAVEPOINT home_detail")
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT home_detail")
                    failed += 1
                    logger.warning(f"Skipped details for {source_id}: {e}")
                    continue
                gone += gone_now
        logger.info(f"Stored details for {len(details) - failed} homes ({gone} no longer for sale, {failed} skipped)")

    @staticmethod
    def _record_one_detail(cur, source_id: str, fields: Dict[str, Any], now: datetime) -> bool:
        """Store one home's detail fields. Returns True if it was marked gone."""
        if fields.get("gone") or fields.get("off_market"):
            cur.execute(
                """UPDATE home_listings SET status = 'gone', status_checked_at = %s,
                          details_checked_at = %s WHERE source_id = %s""",
                (now, now, source_id),
            )
            return True
        if fields.get("failed"):  # unreadable page: don't retry it every run
            cur.execute(
                "UPDATE home_listings SET details_checked_at = %s WHERE source_id = %s",
                (now, source_id),
            )
            return False
        cur.execute(
            """UPDATE home_listings
               SET rooms = COALESCE(%s, rooms),
                   year_built = COALESCE(%s, year_built),
                   monthly_owner_expenses_dkk = COALESCE(%s, monthly_owner_expenses_dkk),
                   energy_label = COALESCE(%s, energy_label),
                   listed_at = COALESCE(%s, listed_at),
                   under_offer = %s,
                   details_checked_at = %s
               WHERE source_id = %s
               RETURNING price_dkk, monthly_owner_expenses_dkk""",
            (fields.get("rooms"), fields.get("year_built"),
             fields.get("monthly_owner_expenses_dkk"), fields.get("energy_label"),
             fields.get("listed_at"), bool(fields.get("under_offer")), now, source_id),
        )
        row = cur.fetchone()
        if row:  # owner expenses change the monthly estimate
            est = estimate_monthly_cost(row["price_dkk"], row["monthly_owner_expenses_dkk"])["cash_out"]
            cur.execute(
                "UPDATE home_listings SET est_monthly_cash_dkk = %s WHERE source_id = %s",
                (est, source_id),
            )
        return False

    def mark_unseen_gone(self, source_name: str, seen_before: datetime) -> int:
        """After a clean full-catalog run: count a missed run for each active
        home not seen since `seen_before`, and mark it gone once it has missed
        GONE_AFTER_MISSED_RUNS runs in a row (one paging hiccup isn't enough).
        Returns the number marked gone."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """UPDATE home_listings SET missed_runs = missed_runs + 1
                   WHERE source_name = %s AND status = 'active' AND last_seen_at < %s""",
                (source_name, seen_before),
            )
            missed = cur.rowcount
            cur.execute(
                """UPDATE home_listings SET status = 'gone', status_checked_at = %s
                   WHERE source_name = %s AND status = 'active' AND last_seen_at < %s
                     AND missed_runs >= %s""",
                (datetime.utcnow(), source_name, seen_before, self.GONE_AFTER_MISSED_RUNS),
            )
            count = cur.rowcount
        logger.info(f"Marked {count} {source_name} homes gone ({missed} missing from the full catalog)")
        return count

    def cleanup_old_listings(self, days: Optional[int] = None) -> int:
        """Delete homes not seen for `days` (default EXPIRY_DAYS). Returns count."""
        days = days or self.EXPIRY_DAYS
        cutoff = datetime.utcnow() - timedelta(days=days)
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM home_listings WHERE last_seen_at < %s", (cutoff,))
            count = cur.rowcount
        if count:
            logger.info(f"Cleaned up {count} home listings older than {days} days")
        return count
