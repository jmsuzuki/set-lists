# Sample Setlist Data Ingestion
# This script demonstrates how to structure and ingest setlist data

import requests
from datetime import date, datetime
from typing import List, Dict, Any


def create_sample_goose_show():
    """Create sample Goose show data"""
    return {
        "primary_key": "goose-2025-01-12-moon-palace",
        "band_name": "Goose",
        "show_date": "2025-01-12",
        "venue_name": "Moon Palace",
        "venue_city": "Riviera Maya",
        "venue_state": None,
        "venue_country": "Mexico",
        "tour_name": "Dead Ahead Festival",
        "show_notes": "Special guest appearance with Dead Ahead",
        "verified": True,
        "source_url": "https://elgoose.net/setlists/",
        "created_at": "2025-01-15T20:30:00Z"  # Use ISO format with Z timezone
    }


def create_sample_setlist_entries():
    """Create sample setlist entries for the show"""
    show_id = "goose-2025-01-12-moon-palace"
    show_date = "2025-01-12"
    
    # Sample setlist based on the El Goose data
    setlist = [
        # Set 1
        {"song": "Shakedown Street", "set": "Set 1", "pos": 1, "notes": "Without Rick"},
        {"song": "Friend of the Devil", "set": "Set 1", "pos": 2},
        {"song": "Jack Straw", "set": "Set 1", "pos": 3, "notes": "Without Sturgill"},
        {"song": "Samson & Delilah", "set": "Set 1", "pos": 4, "notes": "Without Sturgill"},
        {"song": "Bird Song", "set": "Set 1", "pos": 5, "notes": "Without Sturgill"},
        {"song": "Loser", "set": "Set 1", "pos": 6, "notes": "Without Sturgill"},
        {"song": "Box of Rain", "set": "Set 1", "pos": 7, "notes": "Without Sturgill"},
        {"song": "Mama Tried", "set": "Set 1", "pos": 8, "notes": "Without Rick"},
        {"song": "Ripple", "set": "Set 1", "pos": 9, "notes": "Without Rick"},
        {"song": "Me & My Uncle", "set": "Set 1", "pos": 10},
        
        # Set 2  
        {"song": "Deal", "set": "Set 2", "pos": 1},
        {"song": "Scarlet Begonias", "set": "Set 2", "pos": 2, "transitions": "Estimated Prophet"},
        {"song": "Estimated Prophet", "set": "Set 2", "pos": 3, "transitions": "Eyes of the World"},
        {"song": "Eyes of the World", "set": "Set 2", "pos": 4, "duration": 15.5},
        {"song": "Morning Dew", "set": "Set 2", "pos": 5, "duration": 12.3},
        {"song": "Cold Rain & Snow", "set": "Set 2", "pos": 6},
        
        # Encore
        {"song": "Turn On Your Love Light", "set": "Encore", "pos": 1, "duration": 8.7}
    ]
    
    entries = []
    for item in setlist:
        entry = {
            "primary_key": f"{show_id}-{item['set'].lower().replace(' ', '')}-{item['pos']}",
            "show_id": show_id,
            "band_name": "Dead Ahead",  # Special project band
            "show_date": show_date,
            "set_type": item["set"],
            "set_position": item["pos"],
            "song_name": item["song"],
            "song_duration_minutes": item.get("duration"),
            "transitions_into": item.get("transitions"),
            "transitions_from": None,
            "is_jam": item.get("duration", 0) > 15,  # Consider 15+ min songs as jams
            "is_tease": False,
            "is_partial": False,
            "performance_notes": item.get("notes"),
            "guest_musicians": None,
            "created_at": "2025-01-15T20:30:00Z"  # Use ISO format with Z timezone
        }
        entries.append(entry)
    
    return entries


def ingest_sample_data(base_url: str = "http://localhost:4000"):
    """Ingest sample data via the Moose APIs"""
    
    # Ingest the show
    show_data = create_sample_goose_show()
    print(f"Ingesting show: {show_data['band_name']} - {show_data['show_date']}")
    
    try:
        response = requests.post(f"{base_url}/ingest/Show", json=show_data)
        if response.status_code == 200:
            print("✅ Show ingested successfully")
        else:
            print(f"❌ Failed to ingest show: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error ingesting show: {e}")
    
    # Ingest the setlist entries
    setlist_entries = create_sample_setlist_entries()
    print(f"Ingesting {len(setlist_entries)} setlist entries...")
    
    success_count = 0
    for entry in setlist_entries:
        try:
            response = requests.post(f"{base_url}/ingest/SetlistEntry", json=entry)
            if response.status_code == 200:
                success_count += 1
                print(f"✅ {entry['song_name']} ({entry['set_type']})")
            else:
                print(f"❌ Failed to ingest {entry['song_name']}: {response.status_code}")
        except Exception as e:
            print(f"❌ Error ingesting {entry['song_name']}: {e}")
    
    print(f"\n🎵 Successfully ingested {success_count}/{len(setlist_entries)} setlist entries")


def query_sample_data(base_url: str = "http://localhost:4000"):
    """Query the ingested data to verify it worked"""
    
    print("\n🔍 Querying song statistics...")
    try:
        response = requests.get(f"{base_url}/consumption/song-stats?band_name=Dead Ahead&limit=10")
        if response.status_code == 200:
            data = response.json()
            print("📊 Song Statistics:")
            for row in data.get('data', []):
                print(f"  • {row['song_name']}: {row['total_plays']} plays")
        else:
            print(f"❌ Failed to query song stats: {response.status_code}")
    except Exception as e:
        print(f"❌ Error querying data: {e}")


if __name__ == "__main__":
    print("🎸 Setlist Data Ingestion Example")
    print("=" * 40)
    
    # Ingest sample data
    ingest_sample_data()
    
    # Query the data
    query_sample_data()
    
    print("\n✨ Sample data ingestion complete!")
    print("You can now query your data using the consumption APIs:")
    print("  • GET /consumption/song-stats - Song play statistics")
    print("  • GET /consumption/shows - Show information")  
    print("  • GET /consumption/setlist-entries - Individual song performances") 