# El Goose Setlist Scraper
# Scrapes real setlist data from https://elgoose.net/setlists/ embedded in main page

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, date, UTC
from typing import List, Dict, Any, Optional
import time
import os


class ElGooseScraper:
    def __init__(self, base_url: str = "https://elgoose.net"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def scrape_all_setlists_from_main_page(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Scrape setlists from the main embedded page"""
        main_url = f"{self.base_url}/setlists/"
        print(f"🔍 Fetching all setlists from {main_url}")
        
        try:
            response = self.session.get(main_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all setlist sections - they have class 'setlist' and id like '2025-06-06'
            setlist_sections = soup.find_all('section', class_='setlist', id=True)
            print(f"📋 Found {len(setlist_sections)} setlist sections")
            
            setlists = []
            
            for i, section in enumerate(setlist_sections[:limit], 1):
                section_id = section.get('id')
                print(f"🎯 Processing {i}/{min(limit, len(setlist_sections))}: {section_id}")
                
                # Parse the date from the ID
                try:
                    show_date = datetime.strptime(section_id, '%Y-%m-%d').date()
                except ValueError:
                    print(f"⚠️  Invalid date format in section ID: {section_id}")
                    continue
                
                # Extract setlist data from this section
                setlist_data = self.parse_setlist_section(section, show_date, main_url)
                if setlist_data:
                    setlists.append(setlist_data)
                
                # Small delay to be respectful
                time.sleep(0.1)
            
            return setlists
            
        except requests.RequestException as e:
            print(f"❌ Error fetching main page: {e}")
            return []
    
    def parse_setlist_section(self, section: BeautifulSoup, show_date: date, url: str) -> Optional[Dict[str, Any]]:
        """Parse a single setlist section from the main page"""
        
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
            "tour_name": "2025 Tour",
            "show_notes": f"Scraped from el-goose.net",
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
                
                song_name = song_link.get('title') or song_link.get_text()
                if not song_name:
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
                
                song_data = {
                    "name": song_name.strip(),
                    "set": set_name,
                    "position": i,
                    "duration": None,
                    "notes": '; '.join(notes) if notes else None,
                    "transitions_into": transitions_into,
                    "transitions_from": None,
                    "is_jam": is_jam,
                    "is_tease": is_tease,
                    "is_partial": is_partial
                }
                
                songs.append(song_data)
        
        return songs
    
    def scrape_all_setlists(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Main method to scrape setlists"""
        return self.scrape_all_setlists_from_main_page(limit)
    
    def save_to_json(self, setlists: List[Dict[str, Any]], filename: str = "goose_setlists.json"):
        """Save scraped data to JSON file"""
        os.makedirs("data", exist_ok=True)
        filepath = f"data/{filename}"
        
        with open(filepath, 'w') as f:
            json.dump({
                "scraped_at": datetime.now(UTC).isoformat(),
                "total_shows": len(setlists),
                "setlists": setlists
            }, f, indent=2, default=str)
        
        print(f"💾 Saved {len(setlists)} setlists to {filepath}")
        return filepath


def main():
    """Run the El Goose scraper"""
    print("🎸 El Goose Real Setlist Scraper")
    print("=" * 40)
    
    scraper = ElGooseScraper()
    
    # Scrape real varied setlists from el-goose.net
    setlists = scraper.scrape_all_setlists(limit=10)  # Start with 10 to test
    
    if setlists:
        filename = scraper.save_to_json(setlists)
        print(f"\n✅ Successfully scraped {len(setlists)} real setlists!")
        print(f"📁 Data saved to: {filename}")
        
        # Show sample statistics
        all_songs = []
        for setlist in setlists:
            all_songs.extend([entry["song_name"] for entry in setlist["setlist_entries"]])
        
        unique_songs = set(all_songs)
        print(f"\n📊 Real Dataset Statistics:")
        print(f"  • Total songs played: {len(all_songs)}")
        print(f"  • Unique songs: {len(unique_songs)}")
        print(f"  • Average songs per show: {len(all_songs) / len(setlists):.1f}")
        print(f"  • Expected max plays per song: ~{len(setlists)}")
        
        # Show sample setlists to verify they're different
        if len(setlists) >= 2:
            print(f"\n🎵 Sample comparison:")
            show1_songs = [e["song_name"] for e in setlists[0]["setlist_entries"][:3]]
            show2_songs = [e["song_name"] for e in setlists[1]["setlist_entries"][:3]]
            print(f"  Show 1 ({setlists[0]['show']['show_date']}): {show1_songs}")
            print(f"  Show 2 ({setlists[1]['show']['show_date']}): {show2_songs}")
            print(f"  Different? {'✅ YES' if show1_songs != show2_songs else '❌ NO'}")
        
        print("\n🚀 Next steps:")
        print("  1. Verify the scraped data has varied setlists")
        print("  2. Run the ingestion script to load into Moose")
        print("  3. Query your REAL Goose analytics!")
    else:
        print("❌ No setlists were successfully scraped")
        print("💡 Check the el-goose.net site structure and parsing logic")


if __name__ == "__main__":
    main() 