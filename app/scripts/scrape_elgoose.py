# El Goose Setlist Scraper
# Scrapes real setlist data from year-specific pages like https://elgoose.net/setlists/2025
# Also scrapes individual song pages for detailed performance statistics

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, date, UTC
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, quote
import time
import os


class ElGooseScraper:
    def __init__(self, base_url: str = "https://elgoose.net"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.song_stats_cache = {}  # Cache song statistics to avoid duplicate requests
        self.covers_database = {}   # Cache covers information from the covers page
        self.originals_database = {}  # Cache original songs information from originals page
        self.master_song_database = {}  # Combined song database with all metadata
    
    def get_setlists_by_year(self, year: int = 2025) -> List[Dict[str, Any]]:
        """Get all setlists for a specific year from the year-specific page"""
        year_url = f"{self.base_url}/setlists/{year}"
        print(f"🔍 Fetching setlists for year {year} from {year_url}")
        
        try:
            response = self.session.get(year_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            return self.parse_setlists_from_html(soup, year_url)
            
        except requests.RequestException as e:
            print(f"❌ Error fetching year {year}: {e}")
            return []
    
    def scrape_multiple_years(self, years: List[int] = [2019, 2020, 2021, 2022, 2023, 2024, 2025], start_from_date: str = None) -> List[Dict[str, Any]]:
        """Scrape setlists from multiple years, optionally starting from a specific date"""
        all_setlists = []
        
        for year in years:
            print(f"📅 Processing year {year}...")
            year_setlists = self.get_setlists_by_year(year)
            
            # Filter by start date if specified
            if start_from_date:
                year_setlists = [setlist for setlist in year_setlists 
                               if setlist['show']['show_date'] >= start_from_date]
            
            all_setlists.extend(year_setlists)
            
            # Be respectful with requests
            time.sleep(0.5)
        
        return all_setlists
    
    def parse_setlists_from_html(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Parse setlists from the HTML page - data is embedded, not loaded via AJAX"""
        setlists = []
        
        # Find all setlist sections - they have class 'setlist' and id like '2025-06-06'
        setlist_sections = soup.find_all('section', class_='setlist', id=True)
        print(f"📋 Found {len(setlist_sections)} setlist sections in HTML")
        
        for section in setlist_sections:
            section_id = section.get('id')
            print(f"🎯 Processing setlist: {section_id}")
            
            # Parse the date from the ID
            try:
                show_date = datetime.strptime(section_id, '%Y-%m-%d').date()
            except ValueError:
                print(f"⚠️  Invalid date format in section ID: {section_id}")
                continue
            
            # Extract setlist data from this section
            setlist_data = self.parse_setlist_section(section, show_date, source_url)
            if setlist_data:
                setlists.append(setlist_data)
        
        return setlists

    def parse_setlist_section(self, section: BeautifulSoup, show_date: date, url: str) -> Optional[Dict[str, Any]]:
        """Parse a single setlist section from the HTML page"""
        
        # Extract venue information from setlist header
        venue_name = "Unknown Venue"
        venue_city = None
        venue_state = None
        
        header = section.find('div', class_='setlist-header')
        if header:
            # Look for venue links
            venue_link = header.find('a', class_='venue')
            if venue_link:
                venue_name = venue_link.get_text().strip()
            
            # Look for city/state links
            city_links = header.find_all('a', href=re.compile(r'/venues/city/'))
            if city_links:
                venue_city = city_links[0].get_text().strip()
            
            state_links = header.find_all('a', href=re.compile(r'/venues/state/'))
            if state_links:
                venue_state = state_links[0].get_text().strip()
        
        # Extract setlist content from setlist-body
        setlist_body = section.find('div', class_='setlist-body')
        if not setlist_body:
            print(f"⚠️  No setlist body found for {show_date}")
            return None
        
        # Parse songs from the setlist body
        songs = self.parse_setlist_body(setlist_body)
        
        if not songs:
            print(f"⚠️  No songs found for {show_date}")
            return None
        
        # Create show data
        show_id = f"goose-{show_date}-{venue_name.lower().replace(' ', '-').replace("'", '').replace(',', '').replace('&', 'and')}"
        
        show_data = {
            "primary_key": show_id,
            "band_name": "Goose",
            "show_date": show_date.isoformat(),
            "venue_name": venue_name,
            "venue_city": venue_city,
            "venue_state": venue_state,
            "venue_country": "USA",
            "tour_name": f"{show_date.year} Tour",
            "show_notes": f"Retrieved from el-goose.net",
            "verified": True,
            "source_url": url,
            "created_at": datetime.now(UTC).isoformat()
        }
        
        # Create setlist entries
        setlist_entries = []
        for song in songs:
            entry = {
                "primary_key": f"{show_id}-{song['set'].lower().replace(' ', '')}-{song['position']}",
                "show_id": show_id,
                "band_name": "Goose",
                "show_date": show_date.isoformat(),
                "set_type": song["set"],
                "set_position": song["position"],
                "song_name": song["name"],
                "song_duration_minutes": song.get("duration"),
                "transitions_into": song.get("transitions_into"),
                "transitions_from": song.get("transitions_from"),
                "is_jam": song.get("is_jam", False),
                "is_tease": song.get("is_tease", False),
                "is_partial": song.get("is_partial", False),
                "performance_notes": song.get("notes"),
                "performance_description": song.get("performance_description"),
                "is_jam_chart": song.get("is_jam_chart", False),
                "guest_musicians": [],
                "created_at": datetime.now(UTC).isoformat()
            }
            setlist_entries.append(entry)
        
        return {
            "show": show_data,
            "setlist_entries": setlist_entries,
            "url": url,
            "scraped_at": datetime.now(UTC).isoformat()
        }
    
    def parse_setlist_body(self, setlist_body: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse songs from the setlist body HTML"""
        songs = []
        
        # Find all set labels and their content
        paragraphs = setlist_body.find_all('p')
        
        for paragraph in paragraphs:
            # Look for set labels like <b class='setlabel set-1'>Set 1:</b>
            set_label = paragraph.find('b', class_=re.compile(r'setlabel'))
            if not set_label:
                continue
            
            set_name = set_label.get_text().strip().rstrip(':')
            
            # Find all song boxes in this paragraph
            song_boxes = paragraph.find_all('span', class_='setlist-songbox')
            
            for i, song_box in enumerate(song_boxes, 1):
                song_link = song_box.find('a')
                if not song_link:
                    continue
                
                # Get song name from text content (this should always be the song name)
                song_name_text = song_link.get_text().strip()
                title_attr = song_link.get('title', '').strip()
                
                # Debug: Check if we have a mismatch between text and title length
                if title_attr and len(title_attr) > 50 and len(song_name_text) < 30:
                    # This looks like a jam chart song with performance description in title
                    song_name = song_name_text
                    performance_description = title_attr
                    is_jam_chart = True
                else:
                    # Regular song or title is just song name
                    song_name = song_name_text or title_attr
                    performance_description = None
                    is_jam_chart = 'jamchart' in song_link.get('class', [])
                    
                    if is_jam_chart and title_attr and title_attr != song_name:
                        performance_description = title_attr
                
                # Ensure we have a valid song name
                if not song_name:
                    continue
                
                # Skip if we somehow got a performance description as song name
                if len(song_name) > 100:
                    print(f"⚠️  Skipping likely performance description parsed as song name: '{song_name[:80]}...'")
                    continue
                
                # Check for transitions
                transition_span = song_box.find('span', class_='setlist-transition')
                transitions_into = None
                is_jam = False
                
                if transition_span:
                    transition_text = transition_span.get_text().strip()
                    if '->' in transition_text or '>' in transition_text:
                        is_jam = True
                        # Could parse the actual transition target here if needed
                
                # Check for footnotes (performance notes)
                footnotes = song_box.find_all('sup')
                notes = []
                is_tease = False
                is_partial = False
                
                for footnote in footnotes:
                    note_text = footnote.get('title', '')
                    if note_text:
                        notes.append(note_text)
                        if 'tease' in note_text.lower():
                            is_tease = True
                        if 'unfinished' in note_text.lower() or 'partial' in note_text.lower():
                            is_partial = True
                
                # Combine footnotes with performance description
                all_notes = []
                if performance_description:
                    all_notes.append(f"Performance: {performance_description}")
                if notes:
                    all_notes.extend(notes)
                
                song_data = {
                    "name": song_name.strip(),
                    "set": set_name,
                    "position": i,
                    "duration": None,
                    "notes": '; '.join(all_notes) if all_notes else None,
                    "performance_description": performance_description,
                    "is_jam_chart": is_jam_chart,
                    "transitions_into": transitions_into,
                    "transitions_from": None,
                    "is_jam": is_jam,
                    "is_tease": is_tease,
                    "is_partial": is_partial
                }
                
                songs.append(song_data)
        
        return songs
    
    def scrape_all_setlists(self, limit: int = 200, enrich_with_song_stats: bool = True) -> List[Dict[str, Any]]:
        """Main method to scrape setlists with optimized song database approach"""
        print("🚀 Starting comprehensive Goose setlist scraper!")
        
        # PHASE 1: Build master song database from authoritative sources
        print("\n📊 PHASE 1: Building master song database...")
        self.build_master_song_database()
        
        # PHASE 2: Scrape basic setlist data from year pages
        print("\n📋 PHASE 2: Scraping basic setlist data from year pages...")
        years_to_scrape = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
        all_setlists = self.scrape_multiple_years(years_to_scrape)
        
        # Limit results if requested
        if limit and len(all_setlists) > limit:
            all_setlists = all_setlists[:limit]
            print(f"📊 Limited results to {limit} setlists")
        else:
            print(f"📊 Collected all {len(all_setlists)} available setlists")
        
        # PHASE 3: Validate and enrich setlist data with master song database
        if enrich_with_song_stats and all_setlists:
            print("\n✨ PHASE 3: Validating and enriching setlist data...")
            all_setlists = self.enrich_setlists_with_master_database(all_setlists)
            print("🎉 Enrichment completed!")
        
        return all_setlists
    
    def build_master_song_database(self) -> None:
        """Build comprehensive song database from originals and covers pages"""
        print("🎸 Scraping original songs database...")
        originals_data = self.scrape_originals_database()
        
        print("🎭 Scraping covers database...")  
        covers_data = self.scrape_covers_database()
        
        # Combine into master database
        self.master_song_database = {}
        
        # Add originals
        for song_name, song_info in originals_data.items():
            self.master_song_database[song_name] = {
                **song_info,
                "is_cover": False,
                "original_artist": song_info.get("original_artist", "Goose")
            }
        
        # Add covers
        for song_name, song_info in covers_data.items():
            self.master_song_database[song_name] = {
                **song_info,
                "is_cover": True
            }
        
        print(f"🎵 Master song database built: {len(originals_data)} originals + {len(covers_data)} covers = {len(self.master_song_database)} total songs")
        
        # Show some sample data
        sample_songs = list(self.master_song_database.items())[:3]
        for song_name, info in sample_songs:
            cover_tag = "[COVER]" if info.get("is_cover") else "[ORIGINAL]"
            artist = info.get("original_artist", "Unknown")
            print(f"  • {song_name} {cover_tag} by {artist}")
    
    def scrape_originals_database(self) -> Dict[str, Dict[str, Any]]:
        """Scrape the originals page to get all original Goose songs with metadata"""
        originals_url = f"{self.base_url}/song/by/goose?&filter=originals"
        print(f"🎸 Fetching originals database from {originals_url}")
        
        try:
            response = self.session.get(originals_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            originals_data = self.parse_song_database_page(soup, is_covers=False)
            
            # Cache the results
            self.originals_database = originals_data
            print(f"✅ Built originals database with {len(originals_data)} songs")
            
            return originals_data
            
        except requests.RequestException as e:
            print(f"❌ Error fetching originals database: {e}")
            return {}
    
    def parse_song_database_page(self, soup: BeautifulSoup, is_covers: bool = True) -> Dict[str, Dict[str, Any]]:
        """Parse either the covers or originals page table to extract song information"""
        songs_data = {}
        
        # Find the main data table
        tables = soup.find_all('table')
        if not tables:
            print("⚠️  No tables found on page")
            return {}
        
        # Use the largest table (should be the song data table)
        table = max(tables, key=lambda t: len(t.find_all('tr')))
        rows = table.find_all('tr')
        
        print(f"📊 Processing table with {len(rows)} rows")
        
        # Find the header row to determine column structure
        header_row = None
        data_start_row = 0
        
        for i, row in enumerate(rows):
            cells = row.find_all(['th', 'td'])
            if cells and len(cells) >= 5:
                cell_texts = [c.get_text().strip().lower() for c in cells]
                if 'song name' in cell_texts or 'song' in cell_texts[0]:
                    header_row = i
                    data_start_row = i + 1
                    break
        
        if header_row is None:
            print("⚠️  Could not find header row")
            # Try to start from row 1 assuming row 0 is header
            data_start_row = 1
        
        # Parse data rows
        for i in range(data_start_row, len(rows)):
            row = rows[i]
            cells = row.find_all('td')
            
            if len(cells) < 5:  # Need at least 5 columns for meaningful data
                continue
            
            try:
                # Extract song name from first cell (should have a link)
                song_name_cell = cells[0]
                song_link = song_name_cell.find('a')
                if song_link:
                    song_name = song_link.get_text().strip()
                else:
                    song_name = song_name_cell.get_text().strip()
                
                # Skip empty or invalid song names
                if not song_name or len(song_name) < 2:
                    continue
                
                # For covers page: [Song, Original Artist, Debut, Last Played, Times Played, Avg Gap]
                # For originals page: [Song, Original Artist, Debut, Last Played, Times Played, Avg Gap]
                original_artist = cells[1].get_text().strip() if len(cells) > 1 else "Goose"
                debut_date = cells[2].get_text().strip() if len(cells) > 2 else None
                last_played = cells[3].get_text().strip() if len(cells) > 3 else None
                times_played = cells[4].get_text().strip() if len(cells) > 4 else None
                avg_show_gap = cells[5].get_text().strip() if len(cells) > 5 else None
                
                # Parse numeric values
                times_played_int = self.parse_times_played(times_played)
                avg_gap_float = self.parse_avg_show_gap(avg_show_gap)
                debut_date_parsed = self.parse_debut_date(debut_date)
                
                song_info = {
                    "song_name": song_name,
                    "original_artist": original_artist,
                    "debut_date": debut_date_parsed,
                    "last_played": last_played,
                    "times_played_total": times_played_int,
                    "avg_show_gap": avg_gap_float,
                    "is_cover": is_covers
                }
                
                songs_data[song_name] = song_info
                
                # Debug: show first few entries
                if len(songs_data) <= 3:
                    cover_tag = "[COVER]" if is_covers else "[ORIGINAL]"
                    print(f"    ✅ Added {cover_tag}: '{song_name}' by {original_artist}")
                
            except Exception as e:
                print(f"⚠️  Error parsing row {i}: {e}")
                continue
        
        return songs_data
    
    def enrich_setlists_with_master_database(self, setlists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich setlist data using the master song database"""
        print("🔍 Validating and enriching setlist entries against master song database...")
        
        enriched_setlists = []
        total_songs = 0
        matched_songs = 0
        unknown_songs = set()
        
        for setlist_idx, setlist in enumerate(setlists, 1):
            if setlist_idx % 50 == 0:
                print(f"📈 Processing setlist {setlist_idx}/{len(setlists)}...")
            
            enriched_entries = []
            
            for entry in setlist["setlist_entries"]:
                song_name = entry["song_name"]
                total_songs += 1
                
                # Skip obviously invalid song names (likely parsing errors)
                if self.is_invalid_song_name(song_name):
                    print(f"⚠️  Skipping invalid song name: '{song_name[:100]}...'")
                    continue
                
                # Try to find song in master database
                song_info = self.find_song_in_database(song_name)
                
                if song_info:
                    # Found in database - enrich with all known metadata
                    matched_songs += 1
                    entry.update({
                        "is_cover": song_info["is_cover"],
                        "original_artist": song_info["original_artist"],
                        "song_debut_date": song_info["debut_date"],
                        "song_last_played": song_info["last_played"],
                        "song_total_times_played": song_info["times_played_total"],
                        "song_avg_show_gap": song_info["avg_show_gap"]
                    })
                else:
                    # Unknown song - mark for investigation
                    unknown_songs.add(song_name)
                    entry.update({
                        "is_cover": None,  # Unknown
                        "original_artist": "Unknown",
                        "validation_status": "unknown_song"
                    })
                
                enriched_entries.append(entry)
            
            enriched_setlist = setlist.copy()
            enriched_setlist["setlist_entries"] = enriched_entries
            enriched_setlists.append(enriched_setlist)
        
        # Report validation statistics
        print(f"\n📊 Validation Results:")
        print(f"  • Total songs processed: {total_songs}")
        print(f"  • Successfully matched: {matched_songs} ({matched_songs/total_songs*100:.1f}%)")
        print(f"  • Unknown songs: {len(unknown_songs)}")
        
        if unknown_songs and len(unknown_songs) <= 10:
            print(f"  • Unknown song samples:")
            for song in list(unknown_songs)[:5]:
                print(f"    - '{song[:60]}...'")
        
        return enriched_setlists
    
    def is_invalid_song_name(self, song_name: str) -> bool:
        """Check if a song name is obviously invalid (likely a parsing error)"""
        if not song_name or len(song_name.strip()) == 0:
            return True
        
        # Only filter out extremely long descriptions that are clearly parsing errors
        if len(song_name) > 200:
            return True
        
        # Don't filter based on content - we now handle jam chart descriptions properly
        return False
    
    def find_song_in_database(self, song_name: str) -> Optional[Dict[str, Any]]:
        """Find a song in the master database, with fuzzy matching"""
        # Direct match first
        if song_name in self.master_song_database:
            return self.master_song_database[song_name]
        
        # Try case-insensitive match
        for db_song_name, song_info in self.master_song_database.items():
            if song_name.lower() == db_song_name.lower():
                return song_info
        
        # Try fuzzy matching for common variations
        song_clean = song_name.strip().lower()
        for db_song_name, song_info in self.master_song_database.items():
            db_song_clean = db_song_name.strip().lower()
            
            # Handle common variations
            if (song_clean.replace("&", "and") == db_song_clean.replace("&", "and") or
                song_clean.replace("'", "") == db_song_clean.replace("'", "")):
                return song_info
        
        return None
    
    def save_to_json(self, setlists: List[Dict[str, Any]], filename: str = "goose_setlists.json"):
        """Save scraped data to JSON file"""
        os.makedirs("data", exist_ok=True)
        filepath = f"data/{filename}"
        
        with open(filepath, 'w') as f:
            json.dump({
                "scraped_at": datetime.now(UTC).isoformat(),
                "total_shows": len(setlists),
                "source": "el-goose.net year-specific pages",
                "setlists": setlists
            }, f, indent=2, default=str)
        
        print(f"💾 Saved {len(setlists)} setlists to {filepath}")
        return filepath

    def get_song_slug_from_name(self, song_name: str) -> str:
        """Convert song name to URL slug format"""
        # Basic slug conversion - lowercase, replace spaces/special chars with hyphens
        slug = song_name.lower()
        slug = re.sub(r'[^a-z0-9\s\-&]', '', slug)  # Remove special chars except &
        slug = re.sub(r'\s+', '-', slug)  # Replace spaces with hyphens
        slug = re.sub(r'-+', '-', slug)  # Replace multiple hyphens with single
        slug = slug.strip('-')  # Remove leading/trailing hyphens
        
        # Handle common cases
        slug = slug.replace('&', 'and')
        
        return slug
    
    def scrape_song_statistics(self, song_name: str) -> Dict[str, Any]:
        """Scrape detailed statistics for a specific song"""
        if song_name in self.song_stats_cache:
            return self.song_stats_cache[song_name]
        
        song_slug = self.get_song_slug_from_name(song_name)
        song_url = f"{self.base_url}/song/{song_slug}"
        
        print(f"🎵 Fetching statistics for '{song_name}' from {song_url}")
        
        try:
            response = self.session.get(song_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            stats = self.parse_song_statistics_page(soup, song_name)
            
            # Cache the results
            self.song_stats_cache[song_name] = stats
            
            # Be respectful with requests
            time.sleep(0.2)
            
            return stats
            
        except requests.RequestException as e:
            print(f"⚠️  Error fetching song statistics for '{song_name}': {e}")
            return {"song_stats": {}, "performances": []}
    
    def parse_song_statistics_page(self, soup: BeautifulSoup, song_name: str) -> Dict[str, Any]:
        """Parse the song statistics page to extract performance data"""
        performances = []
        song_stats = {}
        
        # Extract overall song statistics from the description text
        self.extract_song_overview_stats(soup, song_stats, song_name)
        
        # Find the main statistics table
        table = soup.find('table')
        if not table:
            print(f"⚠️  No statistics table found for '{song_name}'")
            return {"song_stats": song_stats, "performances": []}
        
        # Parse table rows (skip header)
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 7:  # Need at least 7 columns
                continue
            
            try:
                # Extract data from table columns
                date_played = cells[0].get_text().strip()
                venue = cells[1].get_text().strip()
                show_gap = cells[2].get_text().strip()
                set_info = cells[3].get_text().strip()
                track_time = cells[4].get_text().strip()
                song_before = cells[5].get_text().strip()
                song_after = cells[6].get_text().strip()
                footnote = cells[7].get_text().strip() if len(cells) > 7 else ""
                
                # Debug: show track time extraction for first few rows
                if hasattr(self, '_debug_track_time') and self._debug_track_time and len(performances) < 5:
                    print(f"  📊 Row data: date={date_played}, track_time='{track_time}'")
                
                # Parse track time to minutes
                duration_minutes = self.parse_track_time_to_minutes(track_time)
                
                # Clean up song names (remove transition indicators)
                song_before_clean = self.clean_song_name(song_before)
                song_after_clean = self.clean_song_name(song_after)
                
                # Parse show gap to integer
                show_gap_int = self.parse_show_gap(show_gap)
                
                performance = {
                    "date_played": date_played,
                    "venue": venue,
                    "show_gap": show_gap_int,
                    "set": set_info,
                    "duration_minutes": duration_minutes,
                    "song_before": song_before_clean,
                    "song_after": song_after_clean,
                    "footnote": footnote
                }
                
                performances.append(performance)
                
            except Exception as e:
                print(f"⚠️  Error parsing row for '{song_name}': {e}")
                continue
        
        print(f"📊 Found {len(performances)} performances for '{song_name}'")
        return {"song_stats": song_stats, "performances": performances}
    
    def extract_song_overview_stats(self, soup: BeautifulSoup, song_stats: Dict[str, Any], song_name: str):
        """Extract overall song statistics from the page description"""
        try:
            # Find the text that contains statistics like "has been played by Goose 129 times"
            description_text = ""
            
            # Look for paragraphs containing the statistics
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text()
                if 'has been played by Goose' in text and 'times' in text:
                    description_text = text
                    break
            
            if description_text:
                # Extract total times played
                times_match = re.search(r'has been played by Goose (\d+) times', description_text)
                if times_match:
                    song_stats['total_times_played'] = int(times_match.group(1))
                
                # Extract show percentage
                percentage_match = re.search(r'It was played at ([\d.]+)% of Goose shows', description_text)
                if percentage_match:
                    song_stats['show_percentage'] = float(percentage_match.group(1))
                
                # Extract last played info
                last_played_match = re.search(r'It was last played ([\d-]+)', description_text)
                if last_played_match:
                    song_stats['last_played'] = last_played_match.group(1)
                
                # Extract shows since last played
                shows_ago_match = re.search(r'which was (\d+) show\(s\) ago', description_text)
                if shows_ago_match:
                    song_stats['shows_since_last_played'] = int(shows_ago_match.group(1))
                
                # Extract average frequency
                frequency_match = re.search(r'once every (\d+) show\(s\)', description_text)
                if frequency_match:
                    song_stats['average_frequency_shows'] = int(frequency_match.group(1))
                
                # Extract total shows since debut
                total_shows_match = re.search(r'There have been (\d+) show\(s\) since the live debut', description_text)
                if total_shows_match:
                    song_stats['total_shows_since_debut'] = int(total_shows_match.group(1))
                
                print(f"📈 Extracted song stats for '{song_name}': {song_stats}")
                
        except Exception as e:
            print(f"⚠️  Error extracting overview stats for '{song_name}': {e}")
    
    def parse_show_gap(self, show_gap_text: str) -> Optional[int]:
        """Parse show gap text to integer"""
        if not show_gap_text or show_gap_text in ['***', '']:
            return None
        
        try:
            return int(show_gap_text)
        except ValueError:
            return None
    
    def parse_track_time_to_minutes(self, track_time: str) -> Optional[float]:
        """Convert track time string (e.g., '10:33') to minutes"""
        if not track_time or track_time in ['***', '']:
            return None
        
        # Debug: show what we're trying to parse
        if hasattr(self, '_debug_track_time') and self._debug_track_time:
            print(f"    🕐 Parsing track time: '{track_time}'")
        
        try:
            # Handle formats like "10:33" or "1:23:45"
            parts = track_time.split(':')
            if len(parts) == 2:  # MM:SS
                minutes, seconds = map(int, parts)
                result = round(minutes + seconds / 60.0, 2)
                if hasattr(self, '_debug_track_time') and self._debug_track_time:
                    print(f"    ✅ Parsed '{track_time}' as {result} minutes")
                return result
            elif len(parts) == 3:  # HH:MM:SS
                hours, minutes, seconds = map(int, parts)
                result = round(hours * 60 + minutes + seconds / 60.0, 2)
                if hasattr(self, '_debug_track_time') and self._debug_track_time:
                    print(f"    ✅ Parsed '{track_time}' as {result} minutes")
                return result
            else:
                if hasattr(self, '_debug_track_time') and self._debug_track_time:
                    print(f"    ❌ Invalid format: '{track_time}'")
                return None
        except (ValueError, IndexError) as e:
            if hasattr(self, '_debug_track_time') and self._debug_track_time:
                print(f"    ❌ Parse error for '{track_time}': {e}")
            return None
    
    def clean_song_name(self, song_text: str) -> Optional[str]:
        """Clean song name by removing transition indicators"""
        if not song_text or song_text in ['***', '']:
            return None
        
        # Remove transition indicators like >, ->, <, etc.
        cleaned = re.sub(r'^[>\-<\s]+|[>\-<\s]+$', '', song_text)
        cleaned = cleaned.strip()
        
        return cleaned if cleaned else None
    
    def scrape_covers_database(self) -> Dict[str, Dict[str, Any]]:
        """Scrape the covers page to build a comprehensive covers database"""
        covers_url = f"{self.base_url}/song/by/goose?&filter=covers"
        print(f"🎭 Fetching covers database from {covers_url}")
        
        try:
            response = self.session.get(covers_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            covers_data = self.parse_song_database_page(soup, is_covers=True)
            
            # Cache the covers database
            self.covers_database = covers_data
            print(f"✅ Built covers database with {len(covers_data)} cover songs")
            
            return covers_data
            
        except requests.RequestException as e:
            print(f"❌ Error fetching covers database: {e}")
            return {}
    
    def find_matching_performance(self, performances: List[Dict[str, Any]], target_date: str) -> Optional[Dict[str, Any]]:
        """Find performance data that matches the target show date"""
        for performance in performances:
            if performance["date_played"] == target_date:
                return performance
        return None

    def parse_debut_date(self, debut_text: str) -> Optional[str]:
        """Parse debut date from table cell"""
        if not debut_text:
            return None
        
        # Look for date pattern YYYY-MM-DD
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', debut_text)
        if date_match:
            return date_match.group(1)
        
        return None
    
    def parse_times_played(self, times_text: str) -> Optional[int]:
        """Parse times played count"""
        if not times_text:
            return None
        
        try:
            return int(times_text.strip())
        except ValueError:
            return None
    
    def parse_avg_show_gap(self, gap_text: str) -> Optional[float]:
        """Parse average show gap"""
        if not gap_text:
            return None
        
        try:
            return float(gap_text.strip())
        except ValueError:
            return None


def test_song_enrichment():
    """Test song enrichment functionality on a small subset"""
    print("🧪 Testing song enrichment functionality...")
    
    scraper = ElGooseScraper()
    
    # Test URL slug generation
    test_songs = [
        "Jive I",
        "Hot Love & The Lazy Poet", 
        "Me & My Uncle",
        "Mississippi Half-Step Uptown Toodeloo"
    ]
    
    print("🔗 Testing URL slug generation:")
    for song in test_songs:
        slug = scraper.get_song_slug_from_name(song)
        print(f"  '{song}' -> '{slug}'")
    
    # Test covers database
    print(f"\n🎭 Testing covers database scraping:")
    covers_data = scraper.scrape_covers_database()
    
    if covers_data:
        print(f"✅ Found {len(covers_data)} cover songs")
        
        # Show some sample covers
        sample_covers = list(covers_data.items())[:3]
        print("📊 Sample cover data:")
        for song_name, cover_info in sample_covers:
            print(f"  '{song_name}' by {cover_info['original_artist']} (debut: {cover_info['debut_date']})")
    else:
        print("❌ No covers data found")
    
    # Test scraping one song's statistics
    print(f"\n🎵 Testing song statistics scraping for 'Jive I':")
    scraper._debug_track_time = True  # Enable debug output for track time parsing
    stats = scraper.scrape_song_statistics("Jive I")
    
    if stats["performances"]:
        print(f"✅ Found {len(stats['performances'])} performances")
        
        # Show overall song statistics
        if stats["song_stats"]:
            print("📈 Overall song statistics:")
            song_stats = stats["song_stats"]
            if song_stats.get("total_times_played"):
                print(f"  Total times played: {song_stats['total_times_played']}")
            if song_stats.get("show_percentage"):
                print(f"  Show percentage: {song_stats['show_percentage']}%")
            if song_stats.get("average_frequency_shows"):
                print(f"  Average frequency: once every {song_stats['average_frequency_shows']} shows")
        
        print("�� Sample performance data:")
        sample = stats["performances"][0]
        for key, value in sample.items():
            print(f"  {key}: {value}")
        
        # Check for performances with duration data
        performances_with_duration = [p for p in stats["performances"] if p.get("duration_minutes")]
        print(f"\n⏱️  Duration analysis:")
        print(f"  Performances with duration: {len(performances_with_duration)}/{len(stats['performances'])}")
        
        if performances_with_duration:
            recent_with_duration = performances_with_duration[-1]  # Most recent with duration
            print(f"  Recent performance with duration:")
            for key, value in recent_with_duration.items():
                print(f"    {key}: {value}")
        else:
            print(f"  ⚠️  No performances found with duration data!")
            # Show a few recent performances to debug
            recent_performances = stats["performances"][-3:]
            for i, perf in enumerate(recent_performances):
                print(f"  Recent performance {i+1}: date={perf['date_played']}, duration={perf.get('duration_minutes')}")
    else:
        print("❌ No performance data found")
    
    # Test if Jive I is properly identified as original vs cover
    if "Jive I" in covers_data:
        print(f"🎭 'Jive I' identified as cover by {covers_data['Jive I']['original_artist']}")
    else:
        print("🎸 'Jive I' identified as original Goose song")
    
    print("\n🧪 Song enrichment test complete!")


def main():
    """Run the El Goose setlist scraper"""
    print("🎸 El Goose Setlist Scraper")
    print("=" * 40)
    
    # Ask user if they want to run test mode first
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_song_enrichment()
        return
    
    print("✨ Scraping setlists from year-specific pages!")
    print("📅 Years: 2019-2025 (7 years of data)")
    print("🎵 Including detailed song statistics!")
    print("\n💡 Run with --test flag to test song enrichment first")
    
    scraper = ElGooseScraper()
    
    # Running full dataset with improved jam chart parsing
    limit_for_testing = None
    print(f"🚀 Running FULL dataset with improved jam chart performance description capture!")
    
    # Scrape comprehensive setlist dataset from el-goose.net (2019-2025)
    # Including detailed song-level statistics
    setlists = scraper.scrape_all_setlists(limit=limit_for_testing, enrich_with_song_stats=True)
    
    if setlists:
        # Save with timestamp to avoid overwriting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"goose_setlists_enriched_{timestamp}.json"
        filepath = scraper.save_to_json(setlists, filename)
        print(f"\n✅ Successfully scraped {len(setlists)} real setlists with detailed song data!")
        print(f"📁 Data saved to: {filepath}")
        
        # Show sample statistics
        all_songs = []
        songs_with_duration = 0
        songs_with_transitions = 0
        songs_with_show_gap = 0
        songs_with_total_plays = 0
        covers_count = 0
        originals_count = 0
        songs_with_debut_date = 0
        
        for setlist in setlists:
            for entry in setlist["setlist_entries"]:
                all_songs.append(entry["song_name"])
                if entry.get("song_duration_minutes"):
                    songs_with_duration += 1
                if entry.get("transitions_into") or entry.get("transitions_from"):
                    songs_with_transitions += 1
                if entry.get("show_gap") is not None:
                    songs_with_show_gap += 1
                if entry.get("song_total_times_played"):
                    songs_with_total_plays += 1
                if entry.get("is_cover"):
                    covers_count += 1
                else:
                    originals_count += 1
                if entry.get("song_debut_date"):
                    songs_with_debut_date += 1
        
        unique_songs = set(all_songs)
        print(f"\n📊 Enriched Dataset Statistics:")
        print(f"  • Total songs played: {len(all_songs)}")
        print(f"  • Unique songs: {len(unique_songs)}")
        print(f"  • Cover songs: {covers_count}")
        print(f"  • Original Goose songs: {originals_count}")
        print(f"  • Songs with duration data: {songs_with_duration}")
        print(f"  • Songs with transition data: {songs_with_transitions}")
        print(f"  • Songs with show gap data: {songs_with_show_gap}")
        print(f"  • Songs with total play counts: {songs_with_total_plays}")
        print(f"  • Songs with debut dates: {songs_with_debut_date}")
        print(f"  • Average songs per show: {len(all_songs) / len(setlists):.1f}")
        print(f"  • Data enrichment rate: {(songs_with_duration / len(all_songs) * 100):.1f}%")
        print(f"  • Cover percentage: {(covers_count / len(all_songs) * 100):.1f}%")
        
        # Show sample setlists to verify they're different
        if len(setlists) >= 2:
            print(f"\n🎵 Sample comparison (with enriched data):")
            show1_songs = []
            show2_songs = []
            
            for entry in setlists[0]["setlist_entries"][:2]:
                duration = f" ({entry.get('song_duration_minutes', 'N/A')}min)" if entry.get('song_duration_minutes') else ""
                cover_indicator = " [COVER]" if entry.get('is_cover') else ""
                artist = f" by {entry.get('original_artist', 'N/A')}" if entry.get('is_cover') else ""
                show1_songs.append(f"{entry['song_name']}{cover_indicator}{artist}{duration}")
            
            for entry in setlists[1]["setlist_entries"][:2]:
                duration = f" ({entry.get('song_duration_minutes', 'N/A')}min)" if entry.get('song_duration_minutes') else ""
                cover_indicator = " [COVER]" if entry.get('is_cover') else ""
                artist = f" by {entry.get('original_artist', 'N/A')}" if entry.get('is_cover') else ""
                show2_songs.append(f"{entry['song_name']}{cover_indicator}{artist}{duration}")
            
            print(f"  Show 1 ({setlists[0]['show']['show_date']}): {show1_songs}")
            print(f"  Show 2 ({setlists[1]['show']['show_date']}): {show2_songs}")
        
        print("\n🚀 Next steps:")
        print("  1. Verify the enriched data includes covers, originals, and debut dates")
        print("  2. Run the ingestion script to load into Moose")
        print("  3. Query your COMPREHENSIVE Goose analytics!")
    else:
        print("❌ No setlists were successfully scraped")
        print("💡 Check the el-goose.net website and response format")


if __name__ == "__main__":
    main() 