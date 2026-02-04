"""Daily fetch script to update seed_listings.json with new listings."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from apartment_finder.adapters import get_adapter, ADAPTER_REGISTRY
from apartment_finder.adapters.base import SearchCriteria
from apartment_finder.config import load_config
from apartment_finder.services.currency import CurrencyService


def fetch_all_listings():
    """Fetch listings from all available sources."""
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    
    if not config_path.exists():
        print(f"Config not found at {config_path}")
        return []
    
    config = load_config(str(config_path))
    currency_service = CurrencyService()
    all_listings = []
    
    for city_key, city_config in config.get("cities", {}).items():
        display_name = city_config.get("display_name", city_key)
        local_currency = city_config.get("currency", "USD")
        search = config.get("search", {})
        
        min_local = currency_service.convert_from_usd(
            search.get("budget", {}).get("min_usd", 1000), local_currency
        )
        max_local = currency_service.convert_from_usd(
            search.get("budget", {}).get("max_usd", 5000), local_currency
        )
        
        criteria = SearchCriteria(
            min_price_local=min_local,
            max_price_local=max_local,
            min_sqft=search.get("size", {}).get("min_sqft", 400),
            min_bedrooms=search.get("size", {}).get("bedrooms", {}).get("min", 0),
            max_bedrooms=search.get("size", {}).get("bedrooms", {}).get("max", 3),
            must_have_amenities=search.get("must_have", []),
        )
        
        for source_name in city_config.get("sources", []):
            if source_name not in ["craigslist", "findproperties"]:
                continue
                
            if source_name not in ADAPTER_REGISTRY:
                continue
            
            try:
                source_config = config.get("sources", {}).get(source_name, {})
                adapter = get_adapter(source_name, source_config, city_config)
                
                if not adapter.is_available():
                    continue
                
                print(f"Fetching from {source_name} for {display_name}...")
                listings = adapter.fetch_listings(criteria)
                
                for apt in listings:
                    if apt.price_usd is None and apt.price_local:
                        apt.price_usd = currency_service.convert_to_usd(
                            apt.price_local, apt.currency or "USD"
                        )
                    
                    all_listings.append({
                        "source_id": apt.source_id,
                        "source_name": apt.source_name,
                        "city": display_name,
                        "title": apt.title,
                        "price_usd": apt.price_usd,
                        "url": apt.url,
                        "first_seen_at": datetime.now().strftime("%Y-%m-%d"),
                        "last_seen_at": datetime.now().strftime("%Y-%m-%d"),
                        "sent_in_email": 0,
                        "latitude": apt.latitude,
                        "longitude": apt.longitude,
                    })
                
                print(f"  Found {len(listings)} listings")
                
            except Exception as e:
                print(f"  Error: {e}")
                continue
    
    return all_listings


def update_seed_file(new_listings):
    """Update seed_listings.json with new listings."""
    seed_path = Path(__file__).parent.parent.parent / "data" / "seed_listings.json"
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    
    existing = []
    if seed_path.exists():
        try:
            with open(seed_path, 'r') as f:
                existing = json.load(f)
        except Exception as e:
            print(f"Error reading existing seed file: {e}")
            existing = []
    
    existing_ids = {l.get("source_id") for l in existing}
    
    added = 0
    for listing in new_listings:
        if listing["source_id"] not in existing_ids:
            existing.append(listing)
            existing_ids.add(listing["source_id"])
            added += 1
    
    with open(seed_path, 'w') as f:
        json.dump(existing, f, indent=2)
    
    print(f"\nAdded {added} new listings to seed file")
    print(f"Total listings in seed file: {len(existing)}")
    return added


def main():
    """Run daily fetch and update seed file."""
    print(f"=== Daily Apartment Fetch - {datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n")
    
    listings = fetch_all_listings()
    
    if listings:
        update_seed_file(listings)
    else:
        print("No listings fetched")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
