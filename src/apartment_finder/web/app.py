"""Simple web viewer for apartment listings."""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, request
import requests as http_requests

from ..db import get_connection, init_db

app = Flask(__name__, static_folder='static')

# Empty fallback when no database exists
SAMPLE_LISTINGS = []


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

                cur = conn.cursor()
                for listing in listings:
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
                        listing.get('sent_in_email', False)
                    ))

                conn.commit()
                print(f"Loaded {len(listings)} seed listings from {seed_path}")
                return
            except Exception as e:
                print(f"Error loading seed data: {e}")


def _init_app_db():
    """Initialize DB schema and seed data if empty."""
    init_db()

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM seen_listings")
        count = cur.fetchone()['cnt']
        if count == 0:
            load_seed_data(conn)


def get_listings():
    """Fetch all listings from the database."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
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
                    thumbnail_url,
                    description,
                    neighborhood
                FROM seen_listings
                ORDER BY first_seen_at DESC
            """)
            rows = cur.fetchall()

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

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM seen_listings WHERE source_id = %s
            """, (source_id,))
            row = cur.fetchone()
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
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, author, text, created_at
                FROM comments
                WHERE listing_id = %s
                ORDER BY created_at DESC
            """, (source_id,))
            rows = cur.fetchall()
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
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO comments (listing_id, author, text, created_at)
                VALUES (%s, %s, %s, %s)
            """, (source_id, author, text, datetime.utcnow()))
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error posting comment: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/listing/<path:source_id>/ratings', methods=['GET'])
def api_get_ratings(source_id):
    """Get ratings for a listing."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT author, rating, created_at
                FROM ratings
                WHERE listing_id = %s
            """, (source_id,))
            rows = cur.fetchall()
        return jsonify([dict(row) for row in rows])
    except Exception as e:
        print(f"Error getting ratings: {e}")
        return jsonify([])


@app.route('/api/listing/<path:source_id>/ratings', methods=['POST'])
def api_post_rating(source_id):
    """Upsert a rating for a listing."""
    data = request.get_json() or {}
    author = data.get('author', '').strip()
    rating = data.get('rating', '').strip()

    if not author or not rating:
        return jsonify({'error': 'Author and rating are required'}), 400

    if rating not in ('happy', 'neutral', 'sad'):
        return jsonify({'error': 'Rating must be happy, neutral, or sad'}), 400

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ratings (listing_id, author, rating, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(listing_id, author) DO UPDATE SET rating = EXCLUDED.rating, created_at = EXCLUDED.created_at
            """, (source_id, author, rating, datetime.utcnow()))
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error posting rating: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ratings')
def api_all_ratings():
    """Return all rated listings with their ratings."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT r.listing_id, r.author, r.rating,
                       l.title, l.price_usd, l.city, l.source_name, l.thumbnail_url
                FROM ratings r
                JOIN seen_listings l ON r.listing_id = l.source_id
                ORDER BY r.created_at DESC
            """)
            rows = cur.fetchall()

        # Group by listing
        listings = {}
        for row in rows:
            row = dict(row)
            lid = row['listing_id']
            if lid not in listings:
                listings[lid] = {
                    'source_id': lid,
                    'title': row['title'],
                    'price_usd': row['price_usd'],
                    'city': row['city'],
                    'source_name': row['source_name'],
                    'thumbnail_url': row['thumbnail_url'],
                    'ratings': {}
                }
            listings[lid]['ratings'][row['author']] = row['rating']

        return jsonify(list(listings.values()))
    except Exception as e:
        print(f"Error getting all ratings: {e}")
        return jsonify([])


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

        # Backfill thumbnail if listing doesn't have one
        if images and not listing.get('thumbnail_url'):
            try:
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE seen_listings SET thumbnail_url = %s WHERE source_id = %s AND thumbnail_url IS NULL",
                        (images[0], source_id),
                    )
            except Exception as e:
                print(f"Error backfilling thumbnail: {e}")

        return jsonify({'images': images[:20]})  # Limit to 20 images

    except requests.RequestException as e:
        print(f"Error fetching images from {url}: {e}")
        return jsonify({'images': [], 'error': str(e)})
    except Exception as e:
        print(f"Error parsing images: {e}")
        return jsonify({'images': [], 'error': str(e)})


##############################################################################
# Geocoding helpers
##############################################################################

# Track last Nominatim request time (1 req/sec policy)
_last_nominatim_request = 0.0


def _extract_location_hints(title, description):
    """Extract neighborhood/location hints from title and description text."""
    hints = []
    text = f"{title or ''} {description or ''}"

    # Parenthesized text: "(Upper East Side)" or "(Williamsburg)"
    for match in re.findall(r'\(([^)]{3,40})\)', text):
        # Skip things that look like prices, sizes, or common non-location parens
        if re.search(r'\d{3,}|\$|sq\s*ft|bed|bath|br\b|ba\b', match, re.IGNORECASE):
            continue
        hints.append(match.strip())

    # "in {neighborhood}" pattern
    for match in re.findall(r'\bin\s+([A-Z][A-Za-z\s]{2,30})(?:\s*[,\-.|!]|\s*$)', text):
        hints.append(match.strip())

    # "near {landmark}" pattern
    for match in re.findall(r'\bnear\s+([A-Z][A-Za-z\s]{2,30})(?:\s*[,\-.|!]|\s*$)', text):
        hints.append(match.strip())

    return hints


def geocode_listing(listing):
    """Try to geocode a listing using Nominatim from title/description hints.

    Returns (lat, lng) or (None, None).
    """
    global _last_nominatim_request

    city = listing.get('city', '')
    neighborhood = listing.get('neighborhood')

    # Build list of search queries to try, best first
    queries = []
    if neighborhood:
        queries.append(f"{neighborhood}, {city}")

    hints = _extract_location_hints(listing.get('title'), listing.get('description'))
    for hint in hints:
        queries.append(f"{hint}, {city}")

    if not queries:
        return None, None

    headers = {"User-Agent": "FindApartment/1.0 (apartment search tool)"}

    for query in queries[:3]:  # Try at most 3 queries
        # Respect Nominatim rate limit (1 req/sec)
        now = time.time()
        wait = 1.0 - (now - _last_nominatim_request)
        if wait > 0:
            time.sleep(wait)

        try:
            resp = http_requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 1},
                headers=headers,
                timeout=10,
            )
            _last_nominatim_request = time.time()
            resp.raise_for_status()
            results = resp.json()
            if results:
                lat = float(results[0]["lat"])
                lng = float(results[0]["lon"])
                return lat, lng
        except Exception as e:
            print(f"Nominatim error for '{query}': {e}")
            _last_nominatim_request = time.time()

    return None, None


@app.route('/api/listing/<path:source_id>/geocode')
def api_listing_geocode(source_id):
    """Geocode a listing by extracting location hints and querying Nominatim.

    Returns cached coords if available; otherwise geocodes, saves to DB, and returns.
    """
    listing = get_listing(source_id)
    if not listing:
        return jsonify({'error': 'Listing not found'}), 404

    # Already has coords — return immediately
    if listing.get('latitude') and listing.get('longitude'):
        return jsonify({
            'latitude': listing['latitude'],
            'longitude': listing['longitude'],
            'source': 'original',
        })

    # Try geocoding
    lat, lng = geocode_listing(listing)

    if lat is not None and lng is not None:
        # Cache in DB
        try:
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE seen_listings SET latitude = %s, longitude = %s WHERE source_id = %s",
                    (lat, lng, source_id),
                )
        except Exception as e:
            print(f"Error saving geocoded coords: {e}")

        return jsonify({
            'latitude': lat,
            'longitude': lng,
            'source': 'geocoded',
        })

    # Geocoding failed — return nulls (frontend falls back to city center)
    return jsonify({
        'latitude': None,
        'longitude': None,
        'source': 'none',
    })


##############################################################################
# Scoring & Description helpers
##############################################################################

# City suitability data (server-side mirror of listing.html JS data)
CITY_SUITABILITY = {
    'New York City': {
        'immigration': 3, 'work': 3, 'weather': 2, 'family_access': 3,
        'calmness': 1, 'culture': 3, 'healthy_food': 3, 'fitness': 3,
        'cost': 1, 'quality_of_life': 3,
    },
    'Los Angeles': {
        'immigration': 3, 'work': 3, 'weather': 3, 'family_access': 2,
        'calmness': 2, 'culture': 3, 'healthy_food': 3, 'fitness': 2,
        'cost': 1, 'quality_of_life': 2,
    },
    'Dubai': {
        'immigration': 3, 'work': 2, 'weather': 2, 'family_access': 1,
        'calmness': 3, 'culture': 2, 'healthy_food': 3, 'fitness': 3,
        'cost': 2, 'quality_of_life': 3,
    },
    'Lisbon': {
        'immigration': 2, 'work': 2, 'weather': 3, 'family_access': 2,
        'calmness': 3, 'culture': 3, 'healthy_food': 3, 'fitness': 2,
        'cost': 3, 'quality_of_life': 3,
    },
    'Copenhagen': {
        'immigration': 3, 'work': 2, 'weather': 1, 'family_access': 2,
        'calmness': 3, 'culture': 3, 'healthy_food': 3, 'fitness': 3,
        'cost': 1, 'quality_of_life': 3,
    },
    'Bali': {
        'immigration': 3, 'work': 2, 'weather': 3, 'family_access': 1,
        'calmness': 3, 'culture': 3, 'healthy_food': 3, 'fitness': 2,
        'cost': 3, 'quality_of_life': 3,
    },
}

# Approximate median rents per city (USD/month)
CITY_MEDIAN_RENT = {
    'New York City': 3200,
    'Los Angeles': 2800,
    'Dubai': 2000,
    'Lisbon': 1200,
    'Copenhagen': 1800,
    'Bali': 800,
}

POSITIVE_VIBES = [
    'spacious', 'bright', 'modern', 'renovated', 'quiet', 'luxury',
    'charming', 'stunning', 'gorgeous', 'beautiful', 'sunny', 'cozy',
    'elegant', 'pristine', 'designer', 'premium', 'penthouse',
]

RED_FLAGS = ['basement', 'no windows', 'windowless', 'sublet only', 'temporary']


def extract_features(title, description):
    """Parse title + description text and return a dict of detected features."""
    text = f"{title or ''} {description or ''}".lower()

    # Bedrooms
    bedrooms = None
    if 'studio' in text:
        bedrooms = 0
    else:
        m = re.search(r'(\d+)\s*(?:bed|bedroom|br|bd)\b', text)
        if m:
            bedrooms = int(m.group(1))

    # Bathrooms
    bathrooms = None
    m = re.search(r'(\d+)\s*(?:bath|ba)\b', text)
    if m:
        bathrooms = int(m.group(1))

    # Square footage
    sqft = None
    m = re.search(r'(\d[\d,]*)\s*(?:sq\.?\s*ft|sf|sqft)\b', text)
    if m:
        sqft = int(m.group(1).replace(',', ''))

    # Boolean features via keyword search
    def has_keyword(*keywords):
        return any(kw in text for kw in keywords)

    features = {
        'bedrooms': bedrooms,
        'bathrooms': bathrooms,
        'sqft': sqft,
        'has_laundry': has_keyword('laundry', 'washer', 'dryer', 'w/d'),
        'has_dishwasher': has_keyword('dishwasher', 'dish washer'),
        'has_outdoor': has_keyword('balcony', 'outdoor', 'terrace', 'patio', 'roof', 'garden', 'yard', 'deck'),
        'has_doorman': has_keyword('doorman', 'concierge'),
        'has_elevator': has_keyword('elevator', 'lift'),
        'has_gym': has_keyword('gym', 'fitness center', 'fitness centre'),
        'has_parking': has_keyword('parking', 'garage'),
        'is_furnished': has_keyword('furnished'),
        'no_broker_fee': has_keyword('no fee', 'no broker', 'owner direct', 'no commission'),
        'pets_allowed': has_keyword('pet', 'cat friendly', 'dog friendly', 'pets ok', 'pets allowed'),
        'positive_vibes': [w for w in POSITIVE_VIBES if w in text],
        'red_flags': [w for w in RED_FLAGS if w in text],
    }
    return features


def _match_city(city_name):
    """Find best-matching city key from our data dicts."""
    if not city_name:
        return None
    for key in CITY_SUITABILITY:
        if key.lower() in city_name.lower() or city_name.lower() in key.lower():
            return key
    return None


def _get_preferences():
    """Analyze existing ratings to build a preference profile."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT r.listing_id, r.author, r.rating,
                       l.title, l.price_usd, l.city, l.description
                FROM ratings r
                JOIN seen_listings l ON r.listing_id = l.source_id
            """)
            rows = [dict(row) for row in cur.fetchall()]
    except Exception:
        rows = []

    if len(rows) < 3:
        return {'has_data': False}

    happy_prices = []
    sad_prices = []
    city_scores = {}
    feature_happy = {}
    feature_sad = {}

    for row in rows:
        rating = row['rating']
        price = row.get('price_usd')
        city = row.get('city', '')
        features = extract_features(row.get('title', ''), row.get('description', ''))

        if rating == 'happy':
            if price:
                happy_prices.append(price)
            city_scores[city] = city_scores.get(city, 0) + 1
            for k, v in features.items():
                if isinstance(v, bool) and v:
                    feature_happy[k] = feature_happy.get(k, 0) + 1
        elif rating == 'sad':
            if price:
                sad_prices.append(price)
            city_scores[city] = city_scores.get(city, 0) - 1
            for k, v in features.items():
                if isinstance(v, bool) and v:
                    feature_sad[k] = feature_sad.get(k, 0) + 1

    ideal_price = sum(happy_prices) / len(happy_prices) if happy_prices else None

    # Features that appear often in happy but not sad listings
    boosted_features = []
    for feat in feature_happy:
        happy_ct = feature_happy.get(feat, 0)
        sad_ct = feature_sad.get(feat, 0)
        if happy_ct > sad_ct:
            boosted_features.append(feat)

    return {
        'has_data': True,
        'ideal_price': ideal_price,
        'city_scores': city_scores,
        'boosted_features': boosted_features,
    }


def compute_score(listing, features, preferences):
    """Compute a 0-100 match score for a listing."""
    city = listing.get('city', '')
    price = listing.get('price_usd')
    city_key = _match_city(city)

    # --- City fit (25 points) ---
    if city_key and city_key in CITY_SUITABILITY:
        vals = list(CITY_SUITABILITY[city_key].values())
        city_fit = (sum(vals) / len(vals)) / 3.0 * 25
    else:
        city_fit = 12  # neutral fallback

    # --- Price value (25 points) ---
    median = CITY_MEDIAN_RENT.get(city_key, 2500) if city_key else 2500
    if price:
        ratio = price / median
        if ratio <= 0.8:
            price_score = 25
        elif ratio <= 1.0:
            price_score = 20
        elif ratio <= 1.2:
            price_score = 15
        elif ratio <= 1.5:
            price_score = 10
        else:
            price_score = 5
        # Learned adjustment
        if preferences.get('has_data') and preferences.get('ideal_price'):
            ideal = preferences['ideal_price']
            diff = abs(price - ideal) / ideal
            if diff < 0.1:
                price_score = min(25, price_score + 5)
            elif diff > 0.5:
                price_score = max(0, price_score - 3)
    else:
        price_score = 12

    # --- Features (25 points) ---
    desirable = [
        'has_laundry', 'has_dishwasher', 'has_outdoor', 'has_doorman',
        'has_elevator', 'has_gym', 'has_parking', 'is_furnished',
        'no_broker_fee', 'pets_allowed',
    ]
    feat_count = sum(1 for f in desirable if features.get(f))
    feature_score = min(25, feat_count * 3)

    # --- Vibe / quality (15 points) ---
    vibe_count = len(features.get('positive_vibes', []))
    red_count = len(features.get('red_flags', []))
    vibe_score = min(15, vibe_count * 3) - (red_count * 5)
    vibe_score = max(0, vibe_score)

    # --- Preference match (10 points) ---
    pref_score = 0
    if preferences.get('has_data'):
        boosted = preferences.get('boosted_features', [])
        pref_hits = sum(1 for f in boosted if features.get(f))
        pref_score = min(10, pref_hits * 3)
        # City preference bonus
        city_pref = preferences.get('city_scores', {}).get(city, 0)
        if city_pref > 0:
            pref_score = min(10, pref_score + 2)

    total = round(city_fit + price_score + feature_score + vibe_score + pref_score)
    total = max(0, min(100, total))

    # Label
    if total >= 85:
        label = 'Great Match'
    elif total >= 70:
        label = 'Good Match'
    elif total >= 50:
        label = 'Decent'
    elif total >= 35:
        label = 'Below Average'
    else:
        label = 'Poor Match'

    # Pros / Cons
    pros = []
    cons = []
    if features.get('has_laundry'):
        pros.append('In-unit laundry')
    if features.get('has_outdoor'):
        pros.append('Outdoor space')
    if features.get('no_broker_fee'):
        pros.append('No broker fee')
    if features.get('has_dishwasher'):
        pros.append('Dishwasher')
    if features.get('has_doorman'):
        pros.append('Doorman building')
    if features.get('has_elevator'):
        pros.append('Elevator')
    if features.get('has_gym'):
        pros.append('Gym access')
    if features.get('has_parking'):
        pros.append('Parking')
    if features.get('is_furnished'):
        pros.append('Furnished')
    if features.get('pets_allowed'):
        pros.append('Pets allowed')
    vibes = features.get('positive_vibes', [])
    if vibes:
        pros.append(', '.join(v.capitalize() for v in vibes[:3]))

    if price and city_key and price > CITY_MEDIAN_RENT.get(city_key, 2500):
        cons.append('Above average price for area')
    if not features.get('has_laundry'):
        cons.append('No laundry mentioned')
    if not features.get('has_parking'):
        cons.append('No parking mentioned')
    for flag in features.get('red_flags', []):
        cons.append(flag.capitalize())

    # Summary
    beds_str = f"{features['bedrooms']}BR" if features.get('bedrooms') is not None else 'apartment'
    if features.get('bedrooms') == 0:
        beds_str = 'studio'
    vibe_adj = vibes[0].capitalize() if vibes else 'Nice'
    price_adj = 'fair' if price_score >= 18 else ('great' if price_score >= 22 else 'high')
    city_short = city_key or city or 'Unknown'
    top_feats = ', '.join(pros[:2]) if pros else 'basic amenities'
    summary = f"{vibe_adj} {beds_str} in {city_short} at a {price_adj} price. Has {top_feats}."

    return {
        'score': total,
        'label': label,
        'summary': summary,
        'pros': pros[:6],
        'cons': cons[:4],
        'breakdown': {
            'city_fit': round(city_fit),
            'price': round(price_score),
            'features': round(feature_score),
            'vibe': round(vibe_score),
            'preference': round(pref_score),
        },
    }


def scrape_description(listing):
    """Scrape description and thumbnail from the original listing URL.

    Returns (description, thumbnail_url) tuple.
    """
    import requests
    from bs4 import BeautifulSoup

    url = listing.get('url')
    if not url or url == '#':
        return None, None

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception:
        return None, None

    soup = BeautifulSoup(response.text, 'html.parser')
    source = (listing.get('source_name') or '').lower()
    description = None
    thumbnail_url = None

    # Craigslist
    if 'craigslist' in source or 'craigslist.org' in url:
        body = soup.select_one('#postingbody')
        if body:
            # Remove QR code div and boilerplate
            for tag in body.select('.print-qrcode-container, .print-information'):
                tag.decompose()
            description = body.get_text(separator='\n').strip()

        # Extract thumbnail from detail page images
        img_urls = re.findall(
            r'https://images\.craigslist\.org/[^\s"\'<>]+\.jpg',
            response.text,
        )
        for img_url in img_urls:
            if '50x50c' not in img_url:
                thumbnail_url = img_url
                break

    # Lejebolig
    elif 'lejebolig' in source or 'lejebolig' in url:
        parts = []
        for sel in ['.description', '.lease-description']:
            el = soup.select_one(sel)
            if el:
                parts.append(el.get_text(separator='\n').strip())
        if parts:
            description = '\n\n'.join(parts)

    # PropertyFinder / FindProperties — __NEXT_DATA__ JSON
    elif any(s in source for s in ['propertyfinder', 'findproperties']) or 'propertyfinder' in url:
        script = soup.select_one('script#__NEXT_DATA__')
        if script:
            try:
                data = json.loads(script.string)
                # Navigate common Next.js structures
                props = data.get('props', {}).get('pageProps', {})
                desc = (props.get('description') or props.get('description_en')
                        or props.get('listing', {}).get('description', ''))
                if desc:
                    description = desc
            except (json.JSONDecodeError, AttributeError):
                pass

    # Generic fallback for description
    if not description:
        # Try meta description
        meta = soup.select_one('meta[name="description"]')
        if meta and meta.get('content'):
            description = meta['content']
        else:
            # Find first large paragraph
            for p in soup.select('p'):
                text = p.get_text().strip()
                if len(text) > 100:
                    description = text
                    break

    # Generic fallback for thumbnail
    if not thumbnail_url:
        og_img = soup.select_one('meta[property="og:image"]')
        if og_img and og_img.get('content'):
            thumbnail_url = og_img['content']

    return description, thumbnail_url


##############################################################################
# New API endpoints: descriptions, scoring, preferences
##############################################################################

@app.route('/api/listing/<path:source_id>/description')
def api_listing_description(source_id):
    """Scrape & cache description, extract features."""
    listing = get_listing(source_id)
    if not listing:
        return jsonify({'error': 'Listing not found'}), 404

    description = listing.get('description')

    # If not cached, scrape and store (also grabs thumbnail)
    if not description:
        description, thumbnail_url = scrape_description(listing)
        if description or thumbnail_url:
            try:
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """UPDATE seen_listings
                           SET description = COALESCE(description, %s),
                               thumbnail_url = COALESCE(thumbnail_url, %s)
                           WHERE source_id = %s""",
                        (description, thumbnail_url, source_id),
                    )
            except Exception as e:
                print(f"Error saving description/thumbnail: {e}")

    features = extract_features(listing.get('title', ''), description)
    return jsonify({'description': description or '', 'features': features})


@app.route('/api/listing/<path:source_id>/score')
def api_listing_score(source_id):
    """Compute match score for a listing."""
    listing = get_listing(source_id)
    if not listing:
        return jsonify({'error': 'Listing not found'}), 404

    features = extract_features(listing.get('title', ''), listing.get('description', ''))
    preferences = _get_preferences()
    result = compute_score(listing, features, preferences)
    return jsonify(result)


@app.route('/api/scores')
def api_bulk_scores():
    """Bulk scores for all listings (main page)."""
    listings = get_listings()
    preferences = _get_preferences()
    scores = {}
    for listing in listings:
        features = extract_features(listing.get('title', ''), listing.get('description', ''))
        result = compute_score(listing, features, preferences)
        scores[listing['source_id']] = {
            'score': result['score'],
            'label': result['label'],
        }
    return jsonify(scores)


@app.route('/api/preferences')
def api_preferences():
    """Learned preference profile from ratings."""
    prefs = _get_preferences()
    return jsonify(prefs)


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

    # Only allow sources that have working scrapers
    working_scrapers = {
        'nyc': 'craigslist',
        'la': 'craigslist',
        'dubai': 'propertyfinder',
        'copenhagen': 'lejebolig',
        'lisbon': 'casasapo',
        'bali': 'rumah123',
    }

    if city not in working_scrapers:
        return jsonify({
            'success': False,
            'error': f'No working scraper for {city} yet. Only NYC, LA, Dubai, Copenhagen, Lisbon, and Bali have real listings.'
        }), 400

    source = working_scrapers[city]

    try:
        _init_app_db()

        # Get project root
        project_root = Path(__file__).parent.parent.parent.parent

        # Run the main scraper
        result = subprocess.run(
            [sys.executable, '-m', 'apartment_finder.main', '--city', city, '--source', source],
            capture_output=True,
            text=True,
            timeout=180,
            cwd=str(project_root),
            env={**os.environ, 'PYTHONPATH': str(project_root / 'src') + ':' + os.environ.get('PYTHONPATH', '')}
        )

        if result.returncode == 0:
            return jsonify({
                'success': True,
                'stdout': result.stdout[-2000:] if result.stdout else ''
            })
        else:
            return jsonify({
                'success': False,
                'error': result.stderr[-1000:] if result.stderr else 'Scraper failed'
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
    _init_app_db()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    _init_app_db()
    print(f"Starting server at http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True)
