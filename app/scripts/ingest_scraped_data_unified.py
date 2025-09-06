#!/usr/bin/env python3
"""
Unified Ingestion Script for Scraped Setlist Data
Processes JSON files from El Goose scraper and ingests into Moose using the unified Show model.
Only posts to the Show endpoint - SetlistEntry records are created via transform.
"""

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


def create_unified_show(show_data: Dict[str, Any], entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create a unified Show record with embedded setlist entries.
    
    Args:
        show_data: The show metadata
        entries: List of setlist entries
        
    Returns:
        Unified Show record with embedded entries
    """
    # Create the unified show record
    unified_show = {
        **show_data,  # Include all existing show fields
        'setlist_entries': entries  # Add embedded entries
    }
    
    return unified_show


def ingest_unified_show(unified_show: Dict[str, Any], base_url: str = "http://localhost:4000") -> bool:
    """
    Ingest a unified show with embedded setlist entries.
    The SetlistEntry records will be created automatically via transform.
    """
    try:
        response = requests.post(f"{base_url}/ingest/Show", json=unified_show)
        if response.status_code == 200:
            show_info = f"{unified_show['band_name']} - {unified_show['show_date']} at {unified_show['venue_name']}"
            num_songs = len(unified_show.get('setlist_entries', []))
            print(f"✅ Show: {show_info} ({num_songs} songs)")
            return True
        else:
            print(f"❌ Failed to ingest show {unified_show['show_date']}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error ingesting show {unified_show.get('show_date', 'unknown')}: {e}")
        return False


def ingest_all_scraped_data(filepath: str, base_url: str = "http://localhost:4000"):
    """Process and ingest all scraped setlist data using unified model"""
    
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
    
    print(f"\n🚀 Starting unified ingestion of {len(setlists)} shows...")
    print("=" * 50)
    
    total_shows_success = 0
    total_songs = 0
    
    for i, setlist in enumerate(setlists, 1):
        print(f"\n📅 Processing show {i}/{len(setlists)}")
        
        # Get show data and entries
        show_data = setlist.get('show')
        entries = setlist.get('setlist_entries', [])
        
        if not show_data:
            print("   ⚠️ No show data found, skipping...")
            continue
        
        # Create unified show with embedded entries
        unified_show = create_unified_show(show_data, entries)
        
        # Ingest the unified show
        if ingest_unified_show(unified_show, base_url):
            total_shows_success += 1
            total_songs += len(entries)
        
        # Display song details
        if entries:
            print(f"   🎵 Songs included:")
            for entry in entries[:5]:  # Show first 5 songs
                set_indicator = "🔥" if entry.get('is_jam') else "🎵"
                print(f"      {set_indicator} {entry['song_name']} ({entry['set_type']} #{entry['set_position']})")
            if len(entries) > 5:
                print(f"      ... and {len(entries) - 5} more songs")
    
    # Summary
    print("\n" + "=" * 50)
    print("🎉 UNIFIED INGESTION COMPLETE!")
    print(f"📊 Shows: {total_shows_success}/{len(setlists)} successful")
    print(f"🎵 Total songs: {total_songs}")
    print(f"📈 Success rate: {(total_shows_success/len(setlists)*100):.1f}%" if setlists else "No shows to process")
    print("\n💡 SetlistEntry records were created automatically via transform")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest scraped setlist data using unified model')
    parser.add_argument(
        '--file',
        type=str,
        default='data/goose_setlists.json',
        help='Path to scraped data JSON file (default: data/goose_setlists.json)'
    )
    parser.add_argument(
        '--url',
        type=str,
        default='http://localhost:4000',
        help='Moose API base URL (default: http://localhost:4000)'
    )
    
    args = parser.parse_args()
    
    # Run the ingestion
    ingest_all_scraped_data(args.file, args.url)


if __name__ == "__main__":
    main()
