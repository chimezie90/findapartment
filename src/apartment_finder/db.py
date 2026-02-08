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
                description TEXT
            )
        """)

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
