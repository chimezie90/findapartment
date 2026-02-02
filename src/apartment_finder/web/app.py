"""Simple web viewer for apartment listings."""

import sqlite3
from pathlib import Path
from flask import Flask, jsonify, send_from_directory

app = Flask(__name__, static_folder='static')

DB_PATH = Path(__file__).parent.parent.parent.parent / "data" / "listings.db"


def get_listings():
    """Fetch all listings from the database."""
    if not DB_PATH.exists():
        return []

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
    print(f"Database: {DB_PATH}")
    print(f"Starting server at http://localhost:5000")
    app.run(debug=True, port=5000)
