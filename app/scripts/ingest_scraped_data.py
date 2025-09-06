#!/usr/bin/env python3
"""
Ingest Scraped Setlist Data
Processes JSON files from El Goose scraper and ingests into Moose
Uses the Show -> SetlistEntry transform to process embedded entries
"""

import requests
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any


def load_scraped_data(filepath: str) -> Dict[str, Any]:
    """Load scraped setlist data from JSON file"""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Scraped data file not found: {filepath}")
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    print(f"📁 Loaded data from {filepath}")
    print(f"📊 Contains {data.get('total_shows', 0)} shows")
    print(f"🕐 Scraped at: {data.get('scraped_at', 'unknown')}")
    
    return data


def prepare_show_for_ingestion(show_data: Dict[str, Any], setlist_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Prepare show data for ingestion with embedded setlist entries as JSON string
    """
    # Remove fields that don't exist in our new schema
    show = show_data.copy()
    show.pop('primary_key', None)  # We don't use primary keys anymore
    show.pop('is_prediction', None)  # This field doesn't exist in Show model
    
    # Prepare setlist entries for embedding (remove primary_key and show_id)
    cleaned_entries = []
    for entry in setlist_entries:
        cleaned_entry = {
            'set_type': entry.get('set_type', 'Set 1'),
            'set_position': entry.get('set_position', 1),
            'song_name': entry.get('song_name', ''),
            'song_duration_minutes': entry.get('song_duration_minutes'),
            'transitions_into': entry.get('transitions_into'),
            'transitions_from': entry.get('transitions_from'),
            'is_jam': entry.get('is_jam', False),
            'is_tease': entry.get('is_tease', False),
            'is_partial': entry.get('is_partial', False),
            'is_cover': entry.get('is_cover', False),
            'original_artist': entry.get('original_artist'),
            'performance_notes': entry.get('performance_notes'),
            'guest_musicians': entry.get('guest_musicians', [])
        }
        cleaned_entries.append(cleaned_entry)
    
    # Add setlist entries as JSON string (our transform expects this)
    show['setlist_entries'] = json.dumps(cleaned_entries)
    
    # Ensure created_at is present
    if 'created_at' not in show:
        show['created_at'] = datetime.now().isoformat()
    
    return show


def ingest_show_with_entries(show_data: Dict[str, Any], base_url: str = "http://localhost:4000") -> bool:
    """
    Ingest a show with embedded setlist entries
    The Show -> SetlistEntry transform will automatically create individual entries
    """
    try:
        response = requests.post(
            f"{base_url}/ingest/Show", 
            json=show_data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200 or response.text.strip() == 'SUCCESS':
            venue_info = f"{show_data.get('venue_city', '')}, {show_data.get('venue_state', '')}"
            entry_count = len(json.loads(show_data.get('setlist_entries', '[]')))
            print(f"✅ Show: {show_data['band_name']} - {show_data['show_date']} at {show_data['venue_name']} ({venue_info}) - {entry_count} songs")
            return True
        else:
            print(f"❌ Failed to ingest show {show_data['show_date']}: {response.status_code} - {response.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Error ingesting show {show_data.get('show_date', 'unknown')}: {e}")
        return False


def ingest_all_shows(data: Dict[str, Any], base_url: str = "http://localhost:4000", limit: int = None) -> None:
    """Ingest all shows from scraped data"""
    
    setlists = data.get('setlists', [])
    
    if limit:
        setlists = setlists[:limit]
        print(f"🔢 Limiting to first {limit} shows")
    
    print(f"\n📤 Starting ingestion of {len(setlists)} shows...")
    print("=" * 50)
    
    success_count = 0
    failed_count = 0
    total_songs = 0
    
    for i, setlist_data in enumerate(setlists, 1):
        show = setlist_data.get('show', {})
        entries = setlist_data.get('setlist_entries', [])
        
        # Prepare show with embedded entries
        prepared_show = prepare_show_for_ingestion(show, entries)
        
        # Ingest the show (transform will handle creating SetlistEntry records)
        if ingest_show_with_entries(prepared_show, base_url):
            success_count += 1
            total_songs += len(entries)
        else:
            failed_count += 1
        
        # Progress indicator
        if i % 10 == 0:
            print(f"📊 Progress: {i}/{len(setlists)} shows processed...")
    
    print("\n" + "=" * 50)
    print("📊 INGESTION SUMMARY")
    print(f"✅ Successfully ingested: {success_count} shows with {total_songs} total songs")
    print(f"❌ Failed: {failed_count} shows")
    print(f"📈 Success rate: {(success_count / len(setlists) * 100):.1f}%")


def verify_ingestion(base_url: str = "http://localhost:4000") -> None:
    """Verify the data was ingested correctly"""
    
    print("\n🔍 Verifying ingestion...")
    
    try:
        # Check shows
        response = requests.get(f"{base_url}/consumption/shows?band_name=Goose&limit=5")
        if response.status_code == 200:
            shows = response.json().get('shows', [])
            print(f"✅ Found {len(shows)} shows in database")
            
            if shows:
                print("\n📅 Recent shows:")
                for show in shows[:5]:
                    print(f"   - {show['show_date']}: {show['venue_name']}")
        
        # Check setlist entries via API
        response = requests.get(f"{base_url}/consumption/setlists?band_name=Goose&limit=10")
        if response.status_code == 200:
            entries = response.json().get('items', [])
            print(f"\n✅ Found setlist entries in database")
            
            if entries:
                print("\n🎵 Sample songs:")
                for entry in entries[:5]:
                    print(f"   - {entry['song_name']} ({entry['set_type']})")
    
    except Exception as e:
        print(f"❌ Error verifying ingestion: {e}")


def main():
    """Main ingestion function"""
    
    print("🎸 GOOSE SETLIST DATA INGESTION")
    print("=" * 50)
    
    # Check if Moose is running
    try:
        response = requests.get("http://localhost:4000/health", timeout=2)
        if response.status_code != 200:
            print("❌ Moose dev server not running on localhost:4000")
            print("💡 Start it with: moose dev")
            sys.exit(1)
    except:
        print("❌ Moose dev server not running on localhost:4000")
        print("💡 Start it with: moose dev")
        sys.exit(1)
    
    print("✅ Moose dev server is running")
    
    # Default data file
    data_file = "data/goose_setlists.json"
    
    # Check for command line argument
    if len(sys.argv) > 1:
        data_file = sys.argv[1]
    
    # Check for limit argument
    limit = None
    if len(sys.argv) > 2:
        try:
            limit = int(sys.argv[2])
        except:
            print(f"⚠️  Invalid limit value: {sys.argv[2]}, ignoring")
    
    # Load and ingest data
    try:
        data = load_scraped_data(data_file)
        ingest_all_shows(data, limit=limit)
        verify_ingestion()
        
        print("\n✨ Ingestion complete!")
        print("💡 You can now query the data using the consumption APIs")
        
    except FileNotFoundError as e:
        print(f"❌ {e}")
        print("💡 Make sure you've run the scraper first: python app/scripts/scrape_elgoose.py")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()