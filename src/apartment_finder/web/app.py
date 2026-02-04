"""Simple web viewer for apartment listings."""

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
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


# Sample data for demo/testing when no database exists
SAMPLE_LISTINGS = [
    {"source_id": "demo_1", "source_name": "craigslist", "city": "New York City", "title": "Spacious 1BR in Brooklyn Heights", "price_usd": 2800, "url": "#", "first_seen_at": "2025-01-30", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.6958, "longitude": -73.9936},
    {"source_id": "demo_2", "source_name": "streeteasy", "city": "New York City", "title": "Modern Studio in Williamsburg", "price_usd": 2400, "url": "#", "first_seen_at": "2025-01-29", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.7081, "longitude": -73.9571},
    {"source_id": "demo_3", "source_name": "findproperties", "city": "Dubai", "title": "1BR Apartment in Dubai Marina", "price_usd": 1800, "url": "#", "first_seen_at": "2025-01-28", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 25.0805, "longitude": 55.1403},
    {"source_id": "demo_4", "source_name": "renthop", "city": "New York City", "title": "Cozy 2BR in East Village", "price_usd": 3500, "url": "#", "first_seen_at": "2025-01-27", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 40.7264, "longitude": -73.9818},
    {"source_id": "demo_5", "source_name": "findproperties", "city": "Dubai", "title": "Luxury Studio in Downtown Dubai", "price_usd": 2200, "url": "#", "first_seen_at": "2025-01-26", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 25.1972, "longitude": 55.2744},
    {"source_id": "demo_6", "source_name": "craigslist", "city": "Los Angeles", "title": "Sunny 1BR in Santa Monica", "price_usd": 2600, "url": "#", "first_seen_at": "2025-01-25", "last_seen_at": "2025-01-30", "sent_in_email": 0, "latitude": 34.0195, "longitude": -118.4912},
]


def init_db():
    """Initialize the database if it doesn't exist."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))

    # Listings table
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
            thumbnail_url TEXT
        )
    """)

    # Add thumbnail_url column if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE seen_listings ADD COLUMN thumbnail_url TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_city_source ON seen_listings(city, source_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON seen_listings(last_seen_at)")

    # Comments table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT NOT NULL,
            author TEXT,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (listing_id) REFERENCES seen_listings(source_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_listing ON comments(listing_id)")

    conn.commit()

    # Load seed data if database is empty
    cursor = conn.execute("SELECT COUNT(*) FROM seen_listings")
    count = cursor.fetchone()[0]
    if count == 0:
        load_seed_data(conn)

    conn.close()
    return db_path


def load_seed_data(conn):
    """Load seed listings from JSON file if available."""
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


def get_db():
    """Get a database connection."""
    db_path = get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def get_listings():
    """Fetch all listings from the database."""
    db_path = get_db_path()
    if not db_path.exists():
        return SAMPLE_LISTINGS

    try:
        conn = get_db()
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
                sent_in_email,
                latitude,
                longitude,
                thumbnail_url
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


def get_listing(source_id):
    """Fetch a single listing by ID."""
    # Check sample data first
    for listing in SAMPLE_LISTINGS:
        if listing['source_id'] == source_id:
            return listing

    db_path = get_db_path()
    if not db_path.exists():
        return None

    try:
        conn = get_db()
        cursor = conn.execute("""
            SELECT * FROM seen_listings WHERE source_id = ?
        """, (source_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"Database error: {e}")
        return None


# Routes
@app.route('/')
def index():
    """Serve the main HTML page."""
    return send_from_directory('static', 'index.html')


@app.route('/listing')
def listing_page():
    """Serve the listing detail page."""
    return send_from_directory('static', 'listing.html')


@app.route('/api/listings')
def api_listings():
    """Return all listings as JSON."""
    listings = get_listings()
    return jsonify(listings)


@app.route('/api/listing/<path:source_id>')
def api_listing(source_id):
    """Return a single listing."""
    listing = get_listing(source_id)
    if listing:
        return jsonify(listing)
    return jsonify({'error': 'Listing not found'}), 404


@app.route('/api/listing/<path:source_id>/comments', methods=['GET'])
def api_get_comments(source_id):
    """Get comments for a listing."""
    try:
        conn = get_db()
        cursor = conn.execute("""
            SELECT id, author, text, created_at
            FROM comments
            WHERE listing_id = ?
            ORDER BY created_at DESC
        """, (source_id,))
        rows = cursor.fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])
    except Exception as e:
        print(f"Error getting comments: {e}")
        return jsonify([])


@app.route('/api/listing/<path:source_id>/comments', methods=['POST'])
def api_post_comment(source_id):
    """Add a comment to a listing."""
    data = request.get_json() or {}
    text = data.get('text', '').strip()
    author = data.get('author', '').strip() or 'Anonymous'

    if not text:
        return jsonify({'error': 'Comment text is required'}), 400

    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO comments (listing_id, author, text, created_at)
            VALUES (?, ?, ?, ?)
        """, (source_id, author, text, datetime.utcnow()))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error posting comment: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/listing/<path:source_id>/images')
def api_listing_images(source_id):
    """Scrape images from the original listing URL."""
    import re
    import requests
    from bs4 import BeautifulSoup

    listing = get_listing(source_id)
    if not listing:
        return jsonify({'error': 'Listing not found', 'images': []}), 404

    url = listing.get('url')
    if not url or url == '#':
        return jsonify({'images': [], 'error': 'No valid URL for this listing'})

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        images = []

        # Craigslist: look for gallery images
        for img in soup.select('.gallery img, .swipe img, #thumbs a, .slide img'):
            src = img.get('src') or img.get('data-src') or img.get('href')
            if src and src not in images:
                # Convert thumbnail URL to full-size URL
                if '50x50c' in src:
                    src = src.replace('50x50c', '600x450')
                elif '300x300' in src:
                    src = src.replace('300x300', '600x450')
                images.append(src)

        # Also check for image links in anchors
        for a in soup.select('a[href*="images.craigslist.org"]'):
            href = a.get('href')
            if href and href not in images:
                images.append(href)

        # Look for images in script tags (common pattern)
        for script in soup.select('script'):
            text = script.get_text()
            img_urls = re.findall(r'https://images\.craigslist\.org/[^\s"\'<>]+\.jpg', text)
            for img_url in img_urls:
                if img_url not in images:
                    images.append(img_url)

        # Generic fallback: any large images on the page
        if not images:
            for img in soup.select('img[src*="http"]'):
                src = img.get('src')
                if src and ('jpg' in src or 'jpeg' in src or 'png' in src):
                    if src not in images:
                        images.append(src)

        return jsonify({'images': images[:20]})  # Limit to 20 images

    except requests.RequestException as e:
        print(f"Error fetching images from {url}: {e}")
        return jsonify({'images': [], 'error': str(e)})
    except Exception as e:
        print(f"Error parsing images: {e}")
        return jsonify({'images': [], 'error': str(e)})


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
    """Trigger a fetch of new listings."""
    data = request.get_json() or {}
    city = data.get('city', 'nyc')
    source = data.get('source', 'craigslist')

    # Only allow sources that work without a headed browser
    allowed_sources = ['craigslist', 'findproperties', 'boligportal']
    if source not in allowed_sources:
        return jsonify({
            'success': False,
            'error': f'Source {source} requires a headed browser. Try: {", ".join(allowed_sources)}'
        }), 400

    try:
        # Initialize database first
        init_db()

        # Get project root
        project_root = Path(__file__).parent.parent.parent.parent

        # Run the main scraper
        result = subprocess.run(
            [sys.executable, '-m', 'apartment_finder.main', '--city', city, '--source', source],
            capture_output=True,
            text=True,
            timeout=180,
            cwd=str(project_root),
            env={**os.environ, 'PYTHONPATH': str(project_root / 'src')}
        )

        return jsonify({
            'success': result.returncode == 0,
            'stdout': result.stdout[-2000:] if result.stdout else '',
            'stderr': result.stderr[-2000:] if result.stderr else ''
        })
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Fetch timed out after 3 minutes'
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
