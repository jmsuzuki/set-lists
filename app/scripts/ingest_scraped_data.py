# Ingest Scraped Setlist Data
# Processes JSON files from El Goose scraper and ingests into Moose

import requests
import json
import os
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


def ingest_show_data(show_data: Dict[str, Any], base_url: str = "http://localhost:4000") -> bool:
    """Ingest a single show into Moose"""
    try:
        response = requests.post(f"{base_url}/ingest/Show", json=show_data)
        if response.status_code == 200:
            print(f"✅ Show: {show_data['band_name']} - {show_data['show_date']} at {show_data['venue_name']}")
            return True
        else:
            print(f"❌ Failed to ingest show {show_data['show_date']}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error ingesting show {show_data.get('show_date', 'unknown')}: {e}")
        return False


def ingest_setlist_entries(entries: List[Dict[str, Any]], base_url: str = "http://localhost:4000") -> int:
    """Ingest setlist entries into Moose"""
    success_count = 0
    
    print(f"🎵 Ingesting {len(entries)} setlist entries...")
    
    for entry in entries:
        try:
            response = requests.post(f"{base_url}/ingest/SetlistEntry", json=entry)
            if response.status_code == 200:
                success_count += 1
                set_indicator = "🔥" if entry.get('is_jam') else "🎵"
                print(f"  {set_indicator} {entry['song_name']} ({entry['set_type']} #{entry['set_position']})")
            else:
                print(f"  ❌ Failed: {entry['song_name']} - {response.status_code}")
        except Exception as e:
            print(f"  ❌ Error: {entry['song_name']} - {e}")
    
    return success_count


def ingest_all_scraped_data(filepath: str, base_url: str = "http://localhost:4000"):
    """Process and ingest all scraped setlist data"""
    
    # Load the scraped data
    try:
        data = load_scraped_data(filepath)
    except FileNotFoundError:
        print(f"❌ Could not find scraped data file: {filepath}")
        print("💡 Run the scraper first: python app/scripts/scrape_elgoose.py")
        return
    
    setlists = data.get('setlists', [])
    if not setlists:
        print("❌ No setlist data found in file")
        return
    
    print(f"\n🚀 Starting ingestion of {len(setlists)} shows...")
    print("=" * 50)
    
    total_shows_success = 0
    total_songs_success = 0
    total_songs = 0
    
    for i, setlist in enumerate(setlists, 1):
        print(f"\n📅 Processing show {i}/{len(setlists)}")
        
        # Ingest the show data
        show_data = setlist.get('show')
        if show_data:
            if ingest_show_data(show_data, base_url):
                total_shows_success += 1
        
        # Ingest the setlist entries
        entries = setlist.get('setlist_entries', [])
        if entries:
            total_songs += len(entries)
            songs_success = ingest_setlist_entries(entries, base_url)
            total_songs_success += songs_success
        
        print(f"   ✅ Songs ingested: {len(entries) if entries else 0}")
    
    # Summary
    print("\n" + "=" * 50)
    print("🎉 INGESTION COMPLETE!")
    print(f"📊 Shows: {total_shows_success}/{len(setlists)} successful")
    print(f"🎵 Songs: {total_songs_success}/{total_songs} successful") 
    print(f"📈 Success rate: {(total_songs_success/total_songs*100):.1f}%" if total_songs > 0 else "No songs to process")


def query_ingested_data(base_url: str = "http://localhost:4000"):
    """Query the newly ingested data to verify it worked"""
    
    print("\n🔍 VERIFYING INGESTED DATA")
    print("=" * 30)
    
    try:
        # Get song statistics for Goose (the real scraped data)
        response = requests.get(f"{base_url}/consumption/song-stats?band_name=Goose&limit=10")
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"✅ Found {len(data)} Goose songs in database!")
                print("\n🎵 Top songs:")
                for i, song in enumerate(data[:5], 1):
                    duration = f"({song['longest_version']} min)" if song['longest_version'] else ""
                    jam_indicator = " 🔥" if song['jam_count'] > 0 else ""
                    print(f"  {i:2}. {song['song_name']:25} | {song['total_plays']} plays {duration}{jam_indicator}")
            else:
                print("⚠️  No Goose songs found - data may not have been ingested properly")
        else:
            print(f"❌ Error querying data: {response.status_code}")
    
    except Exception as e:
        print(f"❌ Error verifying data: {e}")


def main():
    """Main function to run scraped data ingestion"""
    print("🎸 GOOSE SETLIST DATA INGESTION")
    print("=" * 40)
    
    # Default path where scraper saves data
    default_filepath = "data/goose_setlists.json"
    
    # Check if scraped data exists
    if not os.path.exists(default_filepath):
        print(f"❌ Scraped data file not found: {default_filepath}")
        print("\n💡 First run the scraper to get data:")
        print("   python app/scripts/scrape_elgoose.py")
        print("\n   Then run this script to ingest the data:")
        print("   python app/scripts/ingest_scraped_data.py")
        return
    
    # Ingest the scraped data
    ingest_all_scraped_data(default_filepath)
    
    # Verify the ingestion worked
    query_ingested_data()
    
    print("\n🚀 READY FOR ANALYTICS!")
    print("You can now query your real Goose setlist data:")
    print("  • curl 'http://localhost:4000/consumption/song-stats?band_name=Goose'")
    print("  • curl 'http://localhost:4000/consumption/shows?band_name=Goose'")
    print("  • Use the APIs to build dashboards and analytics!")


if __name__ == "__main__":
    main() 