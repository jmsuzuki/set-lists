# Check Scraped Data Quality
# Quick utility to inspect scraped setlist data

import json
import os

def check_scraped_data(filepath: str = "data/goose_setlists.json"):
    """Check the quality of scraped setlist data"""
    
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    print("🔍 SCRAPED DATA QUALITY CHECK")
    print("=" * 40)
    print(f"📊 Total shows: {len(data['setlists'])}")
    print(f"🕐 Scraped at: {data.get('scraped_at', 'unknown')}")
    
    for i, show in enumerate(data['setlists']):
        show_info = show['show']
        entries = show['setlist_entries']
        
        print(f"\n🎵 Show {i+1}: {show_info['show_date']}")
        print(f"   🏟️  Venue: {show_info['venue_name']}")
        
        venue_city = show_info.get('venue_city')
        venue_state = show_info.get('venue_state')
        if venue_city or venue_state:
            location = f"{venue_city or 'Unknown City'}, {venue_state or 'Unknown State'}"
            print(f"   📍 Location: {location}")
        
        print(f"   🎶 Songs: {len(entries)}")
        
        if entries:
            print("   📋 Sample songs:")
            for j, song in enumerate(entries[:8]):  # Show first 8
                jam_indicator = " 🔥" if song.get('is_jam') else ""
                print(f"     {j+1:2}. {song['song_name'][:35]:35} ({song['set_type']}){jam_indicator}")
            
            if len(entries) > 8:
                print(f"     ... and {len(entries) - 8} more songs")
            
            # Check for quality indicators
            unique_songs = set(song['song_name'] for song in entries)
            if len(unique_songs) < len(entries) * 0.8:  # Less than 80% unique
                print("   ⚠️  Warning: Many duplicate song names detected")
            
            # Check for obvious non-songs
            non_songs = [song for song in entries if any(word in song['song_name'].lower() 
                        for word in ['home', 'login', 'copyright', 'powered by', 'built in'])]
            if non_songs:
                print(f"   ⚠️  Warning: {len(non_songs)} potential non-song entries detected")
            
            # Check for reasonable song distribution
            sets = {}
            for song in entries:
                set_type = song['set_type']
                sets[set_type] = sets.get(set_type, 0) + 1
            
            print(f"   📊 Set distribution: {dict(sets)}")
            
        else:
            print("   ❌ No songs found - likely parsing issue")

def main():
    check_scraped_data()
    
    print("\n💡 RECOMMENDATIONS:")
    print("• If songs look good: Run ingestion script")
    print("• If many non-songs: Adjust scraper filters")
    print("• If no songs: Check El Goose site structure changes")

if __name__ == "__main__":
    main() 