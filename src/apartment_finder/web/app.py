"""Simple web viewer for apartment listings."""

import os
import sqlite3
from pathlib import Path
from flask import Flask, jsonify, send_from_directory

app = Flask(__name__, static_folder='static')

# Try multiple paths to find the database
def find_db_path():
    possible_paths = [
        Path(__file__).parent.parent.parent.parent / "data" / "listings.db",
        Path.cwd() / "data" / "listings.db",
        Path.home() / "Documents" / "projects" / "findapartment" / "data" / "listings.db",
    ]
    for path in possible_paths:
        if path.exists():
            return path
    return possible_paths[0]  # Return first path as default

DB_PATH = find_db_path()

# Sample data for demo/testing when no database exists
SAMPLE_LISTINGS = [
    {"source_id": "demo_1", "source_name": "craigslist", "city": "New York City", "title": "Spacious 1BR in Brooklyn Heights", "price_usd": 2800, "url": "#", "first_seen_at": "2025-01-30", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.6958, "longitude": -73.9936},
    {"source_id": "demo_2", "source_name": "streeteasy", "city": "New York City", "title": "Modern Studio in Williamsburg", "price_usd": 2400, "url": "#", "first_seen_at": "2025-01-29", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.7081, "longitude": -73.9571},
    {"source_id": "demo_3", "source_name": "findproperties", "city": "Dubai", "title": "1BR Apartment in Dubai Marina", "price_usd": 1800, "url": "#", "first_seen_at": "2025-01-28", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 25.0805, "longitude": 55.1403},
    {"source_id": "demo_4", "source_name": "renthop", "city": "New York City", "title": "Cozy 2BR in East Village", "price_usd": 3500, "url": "#", "first_seen_at": "2025-01-27", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.7264, "longitude": -73.9818},
    {"source_id": "demo_5", "source_name": "findproperties", "city": "Dubai", "title": "Luxury Studio in Downtown Dubai", "price_usd": 2200, "url": "#", "first_seen_at": "2025-01-26", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 25.1972, "longitude": 55.2744},
]


def get_listings():
    """Fetch all listings from the database."""
    if not DB_PATH.exists():
        return SAMPLE_LISTINGS

    conn = sqlite3.connect(str(DB_PATH))
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

    return [dict(row) for row in rows]


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

    return jsonify({
        'total': len(listings),
        'by_city': cities,
        'by_source': sources
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"Database: {DB_PATH}")
    print(f"Starting server at http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True)
