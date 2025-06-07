#!/usr/bin/env python3

import json

with open('data/goose_setlists.json', 'r') as f:
    data = json.load(f)

print("🔍 VERIFYING SCRAPED DATA")
print("=" * 40)

# Check first few shows
shows_to_check = [0, 10, 25, 49]

print("📅 SHOW METADATA:")
for i in shows_to_check:
    show = data["setlists"][i]["show"]
    print(f"Show {i+1:2}: {show['show_date']} at {show['venue_name']}")

print("\n🎵 SETLIST CONTENT:")
for i in shows_to_check:
    first_3_songs = [e['song_name'] for e in data['setlists'][i]['setlist_entries'][:3]]
    print(f"Show {i+1:2} first 3 songs: {first_3_songs}")

print("\n🎯 SETLIST IDENTICAL CHECK:")
first_setlist = [e['song_name'] for e in data['setlists'][0]['setlist_entries']]
all_identical = True

for i in range(1, min(5, len(data['setlists']))):
    current_setlist = [e['song_name'] for e in data['setlists'][i]['setlist_entries']]
    if first_setlist != current_setlist:
        all_identical = False
        print(f"❌ Show {i+1} has DIFFERENT setlist!")
        break
    else:
        print(f"✅ Show {i+1} has IDENTICAL setlist to Show 1")

if all_identical:
    print(f"\n🚨 PROBLEM CONFIRMED: All shows have identical setlists!")
    print(f"   • {len(first_setlist)} songs per show")
    print(f"   • Same song order every show")
    print(f"   • Only dates/venues differ")
else:
    print(f"\n✅ GOOD: Shows have varied setlists!") 