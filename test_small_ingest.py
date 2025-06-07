#!/usr/bin/env python3

import json
import requests

# Load the big dataset but only use the first 3 shows
with open('data/goose_setlists.json', 'r') as f:
    data = json.load(f)

# Create a smaller test dataset
test_data = {
    'scraped_at': data['scraped_at'],
    'total_shows': 3,
    'setlists': data['setlists'][:3]
}

print(f'✅ Created test dataset with {len(test_data["setlists"])} shows')
print(f'📊 Expected max song count should be: {len(test_data["setlists"])} plays per song')

# Now ingest just these 3 shows
base_url = "http://localhost:4000"

for i, setlist_data in enumerate(test_data["setlists"], 1):
    print(f"\n📅 Processing test show {i}/{len(test_data['setlists'])}")
    
    # Ingest show
    show_response = requests.post(f"{base_url}/ingest/Show", json=setlist_data["show"])
    if show_response.status_code == 200:
        print(f"✅ Show: {setlist_data['show']['band_name']} - {setlist_data['show']['show_date']}")
    else:
        print(f"❌ Error ingesting show: {show_response.text}")
        continue
    
    # Ingest setlist entries
    print(f"🎵 Ingesting {len(setlist_data['setlist_entries'])} setlist entries...")
    success_count = 0
    
    for entry in setlist_data["setlist_entries"]:
        response = requests.post(f"{base_url}/ingest/SetlistEntry", json=entry)
        if response.status_code == 200:
            success_count += 1
        else:
            print(f"  ❌ Error: {entry['song_name']} - {response.text}")
    
    print(f"   ✅ Songs ingested: {success_count}")

print("\n🔍 VERIFYING TEST DATA")
print("=" * 30)

# Check song stats
try:
    response = requests.get(f"{base_url}/consumption/song-stats?band_name=Goose&limit=10")
    if response.status_code == 200:
        songs = response.json()
        print(f"✅ Found {len(songs)} Goose songs in database!")
        
        print("\n🎵 Top songs:")
        for i, song in enumerate(songs[:5], 1):
            jam_indicator = " 🔥" if song["jam_count"] > 0 else ""
            print(f"   {i}. {song['song_name'][:50]:<50} | {song['total_plays']} plays{jam_indicator}")
        
        # Verify the counts make sense
        max_plays = max(song["total_plays"] for song in songs) if songs else 0
        expected_max = 3  # We only ingested 3 shows
        
        if max_plays <= expected_max:
            print(f"\n✅ COUNTS LOOK CORRECT! Max plays: {max_plays} (expected ≤ {expected_max})")
        else:
            print(f"\n❌ COUNTS STILL WRONG! Max plays: {max_plays} (expected ≤ {expected_max})")
            
    else:
        print(f"❌ Error querying song stats: {response.text}")
        
except Exception as e:
    print(f"❌ Error verifying data: {e}") 