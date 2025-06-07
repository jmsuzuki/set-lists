# El Goose Setlist Scraper
# Scrapes setlist data from https://elgoose.net/setlists/ and converts to JSON

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import time
import os


class ElGooseScraper:
    def __init__(self, base_url: str = "https://elgoose.net"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def get_setlist_urls(self, limit: int = 50) -> List[str]:
        """Get URLs for individual setlist pages"""
        setlist_page = f"{self.base_url}/setlists/"
        print(f"🔍 Fetching setlist URLs from {setlist_page}")
        
        try:
            response = self.session.get(setlist_page)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for setlist fragment links (El Goose uses #setlist-YYYY-MM-DD)
            setlist_links = []
            
            # Look for fragment URLs with date patterns
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and re.search(r'#setlist-\d{4}-\d{2}-\d{2}', href):
                    full_url = urljoin(self.base_url, href)
                    if full_url not in setlist_links:
                        setlist_links.append(full_url)
            
            # If no fragment links found, try traditional date patterns
            if not setlist_links:
                for link in soup.find_all('a', href=True):
                    href = link.get('href')
                    if href and re.search(r'\d{4}-\d{2}-\d{2}', href):
                        full_url = urljoin(self.base_url, href)
                        if full_url not in setlist_links:
                            setlist_links.append(full_url)
            
            print(f"📋 Found {len(setlist_links)} setlist URLs")
            return setlist_links[:limit]
            
        except requests.RequestException as e:
            print(f"❌ Error fetching setlist URLs: {e}")
            return []
    
    def parse_date_from_url_or_text(self, url: str, page_text: str) -> Optional[date]:
        """Extract date from URL or page content"""
        # Try to find date in URL first
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', url)
        if date_match:
            try:
                return datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
            except ValueError:
                pass
        
        # Try common date patterns in text
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{4}-\d{2}-\d{2})',
            r'(\w+ \d{1,2}, \d{4})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, page_text)
            if match:
                date_str = match.group(1)
                for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%B %d, %Y']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except ValueError:
                        continue
        
        return None
    
    def parse_setlist_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Parse a single setlist page"""
        print(f"🎵 Scraping setlist: {url}")
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            page_text = soup.get_text()
            
            # Extract basic show information
            show_date = self.parse_date_from_url_or_text(url, page_text)
            if not show_date:
                print(f"⚠️  Could not parse date from {url}")
                return None
            
            # Try to find venue information from setlist-header
            venue_name = "Unknown Venue"
            venue_city = None
            venue_state = None
            
            # Look for setlist-header first
            header = soup.find('div', class_='setlist-header')
            if header:
                header_text = header.get_text()
                # Parse venue info from header structure
                lines = [line.strip() for line in header_text.split('\n') if line.strip()]
                for line in lines:
                    # Look for venue-like patterns (not dates)
                    if not re.search(r'\d{4}', line) and not re.search(r'Goose,?', line) and len(line) > 3:
                        if ',' in line:
                            # Format: "Venue Name, City, State, Country"
                            parts = [p.strip() for p in line.split(',')]
                            if len(parts) >= 2:
                                venue_name = parts[0]
                                venue_city = parts[1] if len(parts) > 1 else None
                                venue_state = parts[2] if len(parts) > 2 else None
                            break
                        elif venue_name == "Unknown Venue":
                            venue_name = line
            
            # Fallback patterns if header parsing didn't work
            if venue_name == "Unknown Venue":
                venue_patterns = [
                    r'@\s*([^,\n]+)',  # @ Venue Name
                    r'at\s+([^,\n]+)',  # at Venue Name
                    r'venue[:\s]+([^,\n]+)',  # venue: Name
                ]
                
                for pattern in venue_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        venue_name = match.group(1).strip()
                        break
            
            # Extract songs - look for common setlist patterns
            songs = self.extract_songs_from_page(soup, page_text)
            
            # Create show data
            show_id = f"goose-{show_date}-{venue_name.lower().replace(' ', '-')}"
            
            show_data = {
                "primary_key": show_id,
                "band_name": "Goose",
                "show_date": show_date.isoformat(),
                "venue_name": venue_name,
                "venue_city": venue_city,
                "venue_state": venue_state,
                "venue_country": "USA",  # Default for Goose shows
                "tour_name": None,
                "show_notes": f"Scraped from {url}",
                "verified": False,  # Mark as unverified scraped data
                "source_url": url,
                "created_at": datetime.utcnow().isoformat() + "Z"
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
                    "transitions_from": None,
                    "is_jam": song.get("is_jam", False),
                    "is_tease": song.get("is_tease", False),
                    "is_partial": song.get("is_partial", False),
                    "performance_notes": song.get("notes"),
                    "guest_musicians": song.get("guests"),
                    "created_at": datetime.utcnow().isoformat() + "Z"
                }
                setlist_entries.append(entry)
            
            return {
                "show": show_data,
                "setlist_entries": setlist_entries,
                "url": url,
                "scraped_at": datetime.utcnow().isoformat()
            }
            
        except requests.RequestException as e:
            print(f"❌ Error scraping {url}: {e}")
            return None
    
    def extract_songs_from_page(self, soup: BeautifulSoup, page_text: str) -> List[Dict[str, Any]]:
        """Extract song list from page content - targeting setlist containers"""
        songs = []
        
        # First, try to find setlist-specific containers
        setlist_containers = soup.find_all('div', class_='setlist-body')
        
        if setlist_containers:
            print("✅ Found setlist-body containers, extracting structured data...")
            for container in setlist_containers:
                container_text = container.get_text()
                songs.extend(self._extract_songs_from_text(container_text))
        else:
            # Fallback: look for other potential containers
            print("⚠️  No setlist-body found, trying alternative containers...")
            
            # Try other common setlist container patterns
            potential_containers = [
                soup.find_all('div', class_=re.compile(r'setlist', re.I)),
                soup.find_all('div', class_=re.compile(r'songs', re.I)),
                soup.find_all('div', class_=re.compile(r'tracklist', re.I)),
            ]
            
            for container_group in potential_containers:
                if container_group:
                    for container in container_group:
                        container_text = container.get_text()
                        # Skip if it's navigation or very short
                        if len(container_text) > 100 and not any(nav in container_text.lower() for nav in ['home', 'login', 'account', 'navigation']):
                            songs.extend(self._extract_songs_from_text(container_text))
                    break
            
            # Final fallback: use page text but filter more aggressively
            if not songs:
                print("⚠️  No containers found, using filtered page text...")
                # Remove obvious navigation sections
                filtered_text = self._filter_navigation_text(page_text)
                songs = self._extract_songs_from_text(filtered_text)
        
        return songs
    
    def _filter_navigation_text(self, text: str) -> str:
        """Filter out navigation, footer, and other non-setlist content"""
        lines = text.split('\n')
        filtered_lines = []
        
        # Skip lines that are clearly navigation or metadata
        skip_patterns = [
            r'home|login|account|create account',
            r'copyright|powered by|built in \d+',
            r'instagram|facebook|bluesky|api',
            r'shows|setlists|tours|venues|music',
            r'jam charts|debut chart|tease chart',
            r'^[A-Z]{2,3}$',  # State abbreviations
            r'^\d{1,2}$',     # Just numbers
            r'close|jump|support',
        ]
        
        for line in lines:
            line = line.strip()
            if line and not any(re.search(pattern, line, re.IGNORECASE) for pattern in skip_patterns):
                filtered_lines.append(line)
        
        return '\n'.join(filtered_lines)
    
    def _extract_songs_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Extract songs from text content"""
        songs = []
        current_set = "Set 1"
        position = 1
        
        # Split by sets first
        set_sections = re.split(r'(Set\s+(?:\d+|One|Two|Three|I{1,3})|Encore)', text, flags=re.IGNORECASE)
        
        for i, section in enumerate(set_sections):
            section = section.strip()
            
            # Check if this is a set header
            if re.match(r'Set\s+(\d+|One|Two|Three|I{1,3})', section, re.IGNORECASE):
                match = re.search(r'(\d+)', section)
                current_set = f"Set {match.group(1)}" if match else "Set 1"
                position = 1
                continue
            elif re.match(r'Encore', section, re.IGNORECASE):
                current_set = "Encore"
                position = 1
                continue
            
            # Extract songs from this section
            if section and len(section) > 10:  # Skip empty or very short sections
                section_songs = self._parse_song_section(section, current_set, position)
                songs.extend(section_songs)
                position += len(section_songs)
        
        # If no structured sets found, try simpler extraction
        if not songs:
            songs = self._parse_song_section(text, "Set 1", 1)
        
        return songs[:50]  # Limit to reasonable number to avoid navigation spam
    
    def _parse_song_section(self, text: str, set_name: str, start_position: int) -> List[Dict[str, Any]]:
        """Parse individual songs from a text section"""
        songs = []
        position = start_position
        
        # Clean up the text and split by common separators
        # Look for songs separated by commas, arrow symbols, or line breaks
        song_separators = [',', ' -> ', ' > ', '\n']
        
        # First try comma-separated (most common in setlists)
        if ',' in text:
            raw_songs = [s.strip() for s in text.split(',')]
        elif ' -> ' in text or ' > ' in text:
            # Handle transitions
            raw_songs = re.split(r'\s+(?:->|>)\s+', text)
        else:
            # Line-by-line
            raw_songs = [line.strip() for line in text.split('\n') if line.strip()]
        
        for raw_song in raw_songs:
            song_name = self._clean_song_name(raw_song)
            
            # Filter out non-songs
            if self._is_valid_song_name(song_name):
                song_data = {
                    "name": song_name,
                    "set": set_name,
                    "position": position,
                    "duration": None,
                    "notes": None
                }
                
                # Check for transitions and jams
                if '->' in raw_song or '>' in raw_song:
                    song_data["transitions_into"] = True
                
                if any(indicator in raw_song.lower() for indicator in ['jam', 'reprise']):
                    song_data["is_jam"] = True
                
                songs.append(song_data)
                position += 1
        
        return songs
    
    def _clean_song_name(self, raw_name: str) -> str:
        """Clean up a raw song name"""
        # Remove common prefixes/suffixes
        name = re.sub(r'^[\d\.\-\*\>\s]*', '', raw_name)  # Remove numbers, arrows
        name = re.sub(r'^:\s*', '', name)                 # Remove leading colon and space
        name = re.sub(r'\s*\[[^\]]*\]\s*', '', name)      # Remove [notes]
        name = re.sub(r'\s*\([^)]*\)\s*$', '', name)      # Remove (notes) at end
        name = re.sub(r'\s*->\s*$', '', name)             # Remove trailing arrows
        name = re.sub(r'\s*>\s*$', '', name)              # Remove trailing arrows
        name = re.sub(r',\s*$', '', name)                 # Remove trailing commas
        
        return name.strip()
    
    def _is_valid_song_name(self, name: str) -> bool:
        """Check if this looks like a valid song name"""
        if not name or len(name) < 2:
            return False
        
        # Filter out obvious non-songs
        invalid_patterns = [
            r'^[A-Z]{2,3}$',  # State abbreviations
            r'^\d+$',         # Just numbers
            r'^(usa|canada)$',
            r'(copyright|powered by|built in)',
            r'^(home|shows|login|account)$',
            r'el goose\.net',
            r'^(venue|city|state|country)$',
            r'no known setlist',
            r'upcoming shows',
            r'^\d{1,2}/\d{1,2}/\d{4}$',  # Dates
            r'^\w+, \w+ \d{1,2}, \d{4}$',  # "Month Day, Year"  
            r'^\w+, [A-Z]{2}, USA$',      # "City, ST, USA"
            r'amphitheatre|pavilion|theater|theatre|arena|garden|park|bowl|hall',  # Venue keywords
            r'^\w+, \w+, \w+$',  # Generic "City, State, Country" pattern
            r'festival|music|live|concert',
        ]
        
        for pattern in invalid_patterns:
            if re.search(pattern, name, re.IGNORECASE):
                return False
        
        # Must contain letters
        if not re.search(r'[A-Za-z]', name):
            return False
        
        # Reasonable length for song names
        if len(name) > 80:  # Reduced from 100
            return False
        
        # Exclude if it looks like a venue (contains common venue indicators)
        if any(word in name.lower() for word in ['amphitheatre', 'pavilion', 'center', 'ballroom', 'grounds']):
            return False
        
        return True
    
    def scrape_all_setlists(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Scrape multiple setlists"""
        urls = self.get_setlist_urls(limit)
        setlists = []
        
        for i, url in enumerate(urls, 1):
            print(f"🎯 Processing {i}/{len(urls)}: {url}")
            
            setlist_data = self.parse_setlist_page(url)
            if setlist_data:
                setlists.append(setlist_data)
            
            # Be respectful - add delay between requests
            time.sleep(1)
        
        return setlists
    
    def save_to_json(self, setlists: List[Dict[str, Any]], filename: str = "goose_setlists.json"):
        """Save scraped data to JSON file"""
        os.makedirs("data", exist_ok=True)
        filepath = f"data/{filename}"
        
        with open(filepath, 'w') as f:
            json.dump({
                "scraped_at": datetime.utcnow().isoformat(),
                "total_shows": len(setlists),
                "setlists": setlists
            }, f, indent=2, default=str)
        
        print(f"💾 Saved {len(setlists)} setlists to {filepath}")
        return filepath


def main():
    """Run the El Goose scraper"""
    print("🎸 El Goose Setlist Scraper")
    print("=" * 40)
    
    scraper = ElGooseScraper()
    
    # Scrape a small number of setlists first (focusing on quality)
    setlists = scraper.scrape_all_setlists(limit=3)
    
    if setlists:
        filename = scraper.save_to_json(setlists)
        print(f"\n✅ Successfully scraped {len(setlists)} setlists!")
        print(f"📁 Data saved to: {filename}")
        print("\n🚀 Next steps:")
        print("  1. Review the scraped data")
        print("  2. Run the ingestion script to load into Moose")
        print("  3. Query your real Goose analytics!")
    else:
        print("❌ No setlists were successfully scraped")
        print("💡 You may need to adjust the scraping logic for the actual El Goose site structure")


if __name__ == "__main__":
    main() 