"""Shared PostgreSQL database module."""

import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor


def _get_database_url():
    """Return the DATABASE_URL from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Set it to a PostgreSQL connection string, e.g. "
            "postgresql://user:pass@host/dbname"
        )
    return url


@contextmanager
def get_connection():
    """Context manager that yields a psycopg2 connection with RealDictCursor.

    Usage:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT ...")
    """
    conn = psycopg2.connect(_get_database_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist."""
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
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
                sent_at TIMESTAMP,
                latitude REAL,
                longitude REAL,
                thumbnail_url TEXT,
                description TEXT,
                neighborhood TEXT
            )
        """)

        # Add neighborhood column if missing (migration for existing DBs)
        cur.execute("""
            ALTER TABLE seen_listings ADD COLUMN IF NOT EXISTS neighborhood TEXT
        """)

        # Listing metadata (filterable in the UI) and liveness tracking.
        # status: 'active' until a liveness check confirms the listing was
        # removed/rented at the source, then 'gone'.
        for column_def in (
            "bedrooms INTEGER",
            "bathrooms REAL",
            "size_sqm REAL",
            "amenities TEXT",
            "posted_date TIMESTAMP",
            "status TEXT NOT NULL DEFAULT 'active'",
            "status_checked_at TIMESTAMP",
        ):
            cur.execute(f"ALTER TABLE seen_listings ADD COLUMN IF NOT EXISTS {column_def}")

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_city_source
            ON seen_listings(city, source_name)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_last_seen
            ON seen_listings(last_seen_at)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sent_in_email
            ON seen_listings(sent_in_email)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id SERIAL PRIMARY KEY,
                listing_id TEXT NOT NULL,
                author TEXT,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (listing_id) REFERENCES seen_listings(source_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_comments_listing
            ON comments(listing_id)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                id SERIAL PRIMARY KEY,
                listing_id TEXT NOT NULL,
                author TEXT NOT NULL,
                rating TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(listing_id, author),
                FOREIGN KEY (listing_id) REFERENCES seen_listings(source_id)
            )
        """)

        # Car listings (the "Cars" section). Kept apart from seen_listings:
        # different fields, no comments/ratings, own liveness bookkeeping.
        # listing_type: 'buy' or 'lease'. monthly_price_local is set for
        # leases only. listed_at is the source's own listing time when the
        # card shows one (approximate: DBA shows "5 t.", "4 dage").
        cur.execute("""
            CREATE TABLE IF NOT EXISTS car_listings (
                source_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                city TEXT NOT NULL,
                listing_type TEXT NOT NULL DEFAULT 'buy',
                make TEXT,
                model TEXT,
                variant TEXT,
                year INTEGER,
                mileage_km INTEGER,
                fuel TEXT,
                gearbox TEXT,
                price_local REAL,
                currency TEXT,
                price_usd REAL,
                monthly_price_local REAL,
                location TEXT,
                seller_type TEXT,
                is_promoted BOOLEAN NOT NULL DEFAULT FALSE,
                url TEXT,
                thumbnail_url TEXT,
                listed_at TIMESTAMP,
                first_seen_at TIMESTAMP NOT NULL,
                last_seen_at TIMESTAMP NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                status_checked_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_car_city_type
            ON car_listings(city, listing_type)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_car_last_seen
            ON car_listings(last_seen_at)
        """)

        # One-time purge: the Boligportal adapter used to insert five
        # hardcoded demo listings (boligportal_cph_001..005) whenever its
        # scrape failed, and they showed up on the site as real listings.
        # Idempotent; delete this block once it has run in prod.
        demo_ids = [f"boligportal_cph_{n:03d}" for n in range(1, 6)]
        cur.execute("DELETE FROM ratings WHERE listing_id = ANY(%s)", (demo_ids,))
        cur.execute("DELETE FROM comments WHERE listing_id = ANY(%s)", (demo_ids,))
        cur.execute("DELETE FROM seen_listings WHERE source_id = ANY(%s)", (demo_ids,))
