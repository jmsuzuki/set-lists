#!/usr/bin/env python3

import json
from collections import Counter

with open('data/goose_setlists.json', 'r') as f:
    data = json.load(f)

print('🔍 Analyzing scraped data structure...')
print(f'Total setlists in file: {len(data["setlists"])}')

# Check the first setlist for structure
first_setlist = data['setlists'][0] 
print(f'📅 First show date: {first_setlist["show"]["show_date"]}')
print(f'🎵 Songs in first show: {len(first_setlist["setlist_entries"])}')

# Check for duplicate songs within a single show
song_names = [entry['song_name'] for entry in first_setlist['setlist_entries']]
unique_songs = set(song_names)
print(f'🎶 Unique songs in first show: {len(unique_songs)}')
print(f'📊 Total song entries in first show: {len(song_names)}')

if len(unique_songs) != len(song_names):
    print('❌ DUPLICATES FOUND within single show!')
    counts = Counter(song_names)
    duplicates = {song: count for song, count in counts.items() if count > 1}
    for song, count in duplicates.items():
        print(f'  - {song}: {count} times')
else:
    print('✅ No duplicates within first show')

# Check if all shows have identical setlists (which would be wrong)
print('\n🔍 Checking if all shows are identical...')
first_show_songs = set(song_names)
all_identical = True

for i, setlist in enumerate(data['setlists'][:5]):  # Check first 5
    show_songs = set(entry['song_name'] for entry in setlist['setlist_entries'])
    if show_songs != first_show_songs:
        all_identical = False
        break
    print(f'Show {i+1}: {setlist["show"]["show_date"]} - {len(show_songs)} songs')

if all_identical:
    print('❌ ALL SHOWS HAVE IDENTICAL SETLISTS! This is the problem!')
    print('The scraper is probably extracting the same setlist for every URL.')
else:
    print('✅ Shows have different setlists')

# Show sample of first few song names
print(f'\n🎵 First 10 songs from first show:')
for i, entry in enumerate(first_setlist['setlist_entries'][:10]):
    print(f'  {i+1}. {entry["song_name"]} ({entry["set_type"]})') 