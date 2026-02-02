"""Simple web viewer for apartment listings."""

import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, request

app = Flask(__name__, static_folder='static')

# Database path - use data directory relative to project root
def get_db_path():
    """Get the database path, trying multiple locations."""
    possible_paths = [
        Path(__file__).parent.parent.parent.parent / "data" / "listings.db",
        Path.cwd() / "data" / "listings.db",
    ]
    for path in possible_paths:
        if path.exists():
            return path
    # Return first path as default (will be created if needed)
    return possible_paths[0]

DB_PATH = get_db_path()

# Sample data for demo/testing when no database exists
SAMPLE_LISTINGS = [
    {"source_id": "demo_1", "source_name": "craigslist", "city": "New York City", "title": "Spacious 1BR in Brooklyn Heights", "price_usd": 2800, "url": "#", "first_seen_at": "2025-01-30", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.6958, "longitude": -73.9936},
    {"source_id": "demo_2", "source_name": "streeteasy", "city": "New York City", "title": "Modern Studio in Williamsburg", "price_usd": 2400, "url": "#", "first_seen_at": "2025-01-29", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.7081, "longitude": -73.9571},
    {"source_id": "demo_3", "source_name": "findproperties", "city": "Dubai", "title": "1BR Apartment in Dubai Marina", "price_usd": 1800, "url": "#", "first_seen_at": "2025-01-28", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 25.0805, "longitude": 55.1403},
    {"source_id": "demo_4", "source_name": "renthop", "city": "New York City", "title": "Cozy 2BR in East Village", "price_usd": 3500, "url": "#", "first_seen_at": "2025-01-27", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.7264, "longitude": -73.9818},
    {"source_id": "demo_5", "source_name": "findproperties", "city": "Dubai", "title": "Luxury Studio in Downtown Dubai", "price_usd": 2200, "url": "#", "first_seen_at": "2025-01-26", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 25.1972, "longitude": 55.2744},
]


def init_db():
    """Initialize the database if it doesn't exist."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_city_source ON seen_listings(city, source_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON seen_listings(last_seen_at)")
    conn.commit()
    conn.close()
    return db_path


def get_listings():
    """Fetch all listings from the database."""
    db_path = get_db_path()
    if not db_path.exists():
        return SAMPLE_LISTINGS

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT
                source_id,
                source_name,
                city,
                title,
                price_usd,
                url,
                first_seen_at,
                last_seen_at,
                sent_in_email
            FROM seen_listings
            ORDER BY first_seen_at DESC
        """)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return SAMPLE_LISTINGS
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database error: {e}")
        return SAMPLE_LISTINGS


@app.route('/')
def index():
    """Serve the main HTML page."""
    return send_from_directory('static', 'index.html')


@app.route('/api/listings')
def api_listings():
    """Return all listings as JSON."""
    listings = get_listings()
    return jsonify(listings)


@app.route('/api/stats')
def api_stats():
    """Return listing statistics."""
    listings = get_listings()

    cities = {}
    sources = {}

    for listing in listings:
        city = listing.get('city', 'Unknown')
        source = listing.get('source_name', 'Unknown')

        cities[city] = cities.get(city, 0) + 1
        sources[source] = sources.get(source, 0) + 1

    # Check if using sample data
    is_demo = len(listings) > 0 and listings[0].get('source_id', '').startswith('demo_')

    return jsonify({
        'total': len(listings),
        'by_city': cities,
        'by_source': sources,
        'is_demo': is_demo
    })


@app.route('/api/fetch', methods=['POST'])
def api_fetch():
    """Trigger a fetch of new listings (works on Replit for Craigslist/FindProperties)."""
    data = request.get_json() or {}
    city = data.get('city', 'nyc')
    source = data.get('source', 'craigslist')

    # Only allow sources that work without a headed browser
    allowed_sources = ['craigslist', 'findproperties']
    if source not in allowed_sources:
        return jsonify({
            'success': False,
            'error': f'Source {source} requires a headed browser and cannot run on Replit. Try: {", ".join(allowed_sources)}'
        }), 400

    try:
        # Initialize database first
        init_db()

        # Run the main scraper
        result = subprocess.run(
            [sys.executable, '-m', 'apartment_finder.main', '--city', city, '--source', source],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(Path(__file__).parent.parent.parent.parent)
        )

        return jsonify({
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        })
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Fetch timed out after 2 minutes'
        }), 504
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Initialize database on startup
with app.app_context():
    init_db()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    db_path = init_db()
    print(f"Database: {db_path}")
    print(f"Starting server at http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True)
