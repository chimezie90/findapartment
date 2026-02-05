"""Database abstraction layer supporting PostgreSQL."""

import json
import os
from datetime import datetime
from pathlib import Path

DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    USE_POSTGRES = True
else:
    import sqlite3
    USE_POSTGRES = False


def get_connection():
    """Get a database connection."""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        db_path = _get_sqlite_path()
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn


def _get_sqlite_path():
    """Get SQLite database path."""
    possible_paths = [
        Path(__file__).parent.parent.parent.parent / "data" / "listings.db",
        Path.cwd() / "data" / "listings.db",
    ]
    for path in possible_paths:
        if path.exists():
            return path
    return possible_paths[0]


def execute_query(conn, query, params=None):
    """Execute a query with database-agnostic parameter substitution."""
    if USE_POSTGRES:
        pg_query = query.replace('?', '%s')
        pg_query = pg_query.replace('INSERT OR IGNORE', 'INSERT')
        pg_query = pg_query.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY')
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(pg_query, params)
        return cursor
    else:
        return conn.execute(query, params or ())


def execute_fetchall(conn, query, params=None):
    """Execute query and fetch all results as list of dicts."""
    cursor = execute_query(conn, query, params)
    rows = cursor.fetchall()
    if USE_POSTGRES:
        return [dict(row) for row in rows]
    else:
        return [dict(row) for row in rows]


def execute_fetchone(conn, query, params=None):
    """Execute query and fetch one result as dict."""
    cursor = execute_query(conn, query, params)
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


def init_database():
    """Initialize the database schema."""
    conn = get_connection()
    
    if USE_POSTGRES:
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
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_city_source ON seen_listings(city, source_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON seen_listings(last_seen_at)")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id SERIAL PRIMARY KEY,
                listing_id TEXT NOT NULL,
                author TEXT,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_comments_listing ON comments(listing_id)")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                id SERIAL PRIMARY KEY,
                listing_id TEXT NOT NULL,
                author TEXT NOT NULL,
                rating TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(listing_id, author)
            )
        """)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM seen_listings")
        count = cur.fetchone()[0]
        if count == 0:
            _load_seed_data_postgres(conn)
        
        cur.close()
    else:
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
                sent_at TIMESTAMP,
                latitude REAL,
                longitude REAL,
                thumbnail_url TEXT,
                description TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_city_source ON seen_listings(city, source_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON seen_listings(last_seen_at)")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id TEXT NOT NULL,
                author TEXT,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_listing ON comments(listing_id)")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id TEXT NOT NULL,
                author TEXT NOT NULL,
                rating TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(listing_id, author)
            )
        """)
        
        conn.commit()
        
        cursor = conn.execute("SELECT COUNT(*) FROM seen_listings")
        count = cursor.fetchone()[0]
        if count == 0:
            _load_seed_data_sqlite(conn)
    
    conn.close()
    print(f"Database initialized (PostgreSQL: {USE_POSTGRES})")


def _load_seed_data_postgres(conn):
    """Load seed data into PostgreSQL."""
    seed_paths = [
        Path(__file__).parent.parent.parent.parent / "data" / "seed_listings.json",
        Path.cwd() / "data" / "seed_listings.json",
    ]
    
    for seed_path in seed_paths:
        if seed_path.exists():
            try:
                with open(seed_path, 'r') as f:
                    listings = json.load(f)
                
                cur = conn.cursor()
                for listing in listings:
                    sent_in_email = bool(listing.get('sent_in_email', False))
                    cur.execute("""
                        INSERT INTO seen_listings
                        (source_id, source_name, city, title, price_usd, url,
                         first_seen_at, last_seen_at, sent_in_email)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO NOTHING
                    """, (
                        listing.get('source_id'),
                        listing.get('source_name'),
                        listing.get('city'),
                        listing.get('title'),
                        listing.get('price_usd'),
                        listing.get('url'),
                        listing.get('first_seen_at'),
                        listing.get('last_seen_at'),
                        sent_in_email
                    ))
                
                conn.commit()
                cur.close()
                print(f"Loaded {len(listings)} seed listings from {seed_path}")
                return
            except Exception as e:
                print(f"Error loading seed data: {e}")


def _load_seed_data_sqlite(conn):
    """Load seed data into SQLite."""
    seed_paths = [
        Path(__file__).parent.parent.parent.parent / "data" / "seed_listings.json",
        Path.cwd() / "data" / "seed_listings.json",
    ]
    
    for seed_path in seed_paths:
        if seed_path.exists():
            try:
                with open(seed_path, 'r') as f:
                    listings = json.load(f)
                
                for listing in listings:
                    conn.execute("""
                        INSERT OR IGNORE INTO seen_listings
                        (source_id, source_name, city, title, price_usd, url,
                         first_seen_at, last_seen_at, sent_in_email)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        listing.get('source_id'),
                        listing.get('source_name'),
                        listing.get('city'),
                        listing.get('title'),
                        listing.get('price_usd'),
                        listing.get('url'),
                        listing.get('first_seen_at'),
                        listing.get('last_seen_at'),
                        listing.get('sent_in_email', 0)
                    ))
                
                conn.commit()
                print(f"Loaded {len(listings)} seed listings from {seed_path}")
                return
            except Exception as e:
                print(f"Error loading seed data: {e}")


def get_all_listings():
    """Fetch all listings."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT source_id, source_name, city, title, price_usd, url,
                       first_seen_at, last_seen_at, sent_in_email, latitude,
                       longitude, thumbnail_url
                FROM seen_listings
                ORDER BY first_seen_at DESC
            """)
            rows = cur.fetchall()
            cur.close()
            return [dict(row) for row in rows]
        else:
            cursor = conn.execute("""
                SELECT source_id, source_name, city, title, price_usd, url,
                       first_seen_at, last_seen_at, sent_in_email, latitude,
                       longitude, thumbnail_url
                FROM seen_listings
                ORDER BY first_seen_at DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_listing_by_id(source_id):
    """Fetch a single listing by ID."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT * FROM seen_listings WHERE source_id = %s", (source_id,))
            row = cur.fetchone()
            cur.close()
            return dict(row) if row else None
        else:
            cursor = conn.execute("SELECT * FROM seen_listings WHERE source_id = ?", (source_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_comments(listing_id):
    """Get comments for a listing."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, author, text, created_at
                FROM comments WHERE listing_id = %s
                ORDER BY created_at DESC
            """, (listing_id,))
            rows = cur.fetchall()
            cur.close()
            return [dict(row) for row in rows]
        else:
            cursor = conn.execute("""
                SELECT id, author, text, created_at
                FROM comments WHERE listing_id = ?
                ORDER BY created_at DESC
            """, (listing_id,))
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def add_comment(listing_id, author, text):
    """Add a comment to a listing."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO comments (listing_id, author, text, created_at)
                VALUES (%s, %s, %s, %s)
            """, (listing_id, author, text, datetime.utcnow()))
            conn.commit()
            cur.close()
        else:
            conn.execute("""
                INSERT INTO comments (listing_id, author, text, created_at)
                VALUES (?, ?, ?, ?)
            """, (listing_id, author, text, datetime.utcnow()))
            conn.commit()
    finally:
        conn.close()


def get_ratings(listing_id):
    """Get ratings for a listing."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT author, rating, created_at
                FROM ratings WHERE listing_id = %s
            """, (listing_id,))
            rows = cur.fetchall()
            cur.close()
            return [dict(row) for row in rows]
        else:
            cursor = conn.execute("""
                SELECT author, rating, created_at
                FROM ratings WHERE listing_id = ?
            """, (listing_id,))
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def upsert_rating(listing_id, author, rating):
    """Upsert a rating for a listing."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ratings (listing_id, author, rating, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(listing_id, author) DO UPDATE SET rating = EXCLUDED.rating, created_at = EXCLUDED.created_at
            """, (listing_id, author, rating, datetime.utcnow()))
            conn.commit()
            cur.close()
        else:
            conn.execute("""
                INSERT INTO ratings (listing_id, author, rating, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(listing_id, author) DO UPDATE SET rating = excluded.rating, created_at = excluded.created_at
            """, (listing_id, author, rating, datetime.utcnow()))
            conn.commit()
    finally:
        conn.close()


def get_all_ratings():
    """Get all ratings with listing info."""
    conn = get_connection()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT r.listing_id, r.author, r.rating,
                       l.title, l.price_usd, l.city, l.source_name, l.thumbnail_url
                FROM ratings r
                JOIN seen_listings l ON r.listing_id = l.source_id
                ORDER BY r.created_at DESC
            """)
            rows = cur.fetchall()
            cur.close()
            return [dict(row) for row in rows]
        else:
            cursor = conn.execute("""
                SELECT r.listing_id, r.author, r.rating,
                       l.title, l.price_usd, l.city, l.source_name, l.thumbnail_url
                FROM ratings r
                JOIN seen_listings l ON r.listing_id = l.source_id
                ORDER BY r.created_at DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_stats():
    """Get listing statistics."""
    listings = get_all_listings()
    
    cities = {}
    sources = {}
    
    for listing in listings:
        city = listing.get('city', 'Unknown')
        source = listing.get('source_name', 'Unknown')
        cities[city] = cities.get(city, 0) + 1
        sources[source] = sources.get(source, 0) + 1
    
    is_demo = len(listings) > 0 and str(listings[0].get('source_id', '')).startswith('demo_')
    
    return {
        'total': len(listings),
        'by_city': cities,
        'by_source': sources,
        'is_demo': is_demo
    }
