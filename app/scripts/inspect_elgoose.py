# Inspect El Goose HTML Structure
# Helps understand how to properly extract setlist data

import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

def inspect_main_page():
    """Inspect the main setlists page to find real setlist links"""
    print("🔍 Inspecting https://elgoose.net/setlists/")
    
    response = requests.get('https://elgoose.net/setlists/')
    soup = BeautifulSoup(response.content, 'html.parser')
    
    print("\n📋 Looking for real setlist links...")
    setlist_links = []
    
    # Look for links with date patterns
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        text = link.get_text().strip()
        
        # Look for date patterns in URL or text
        if href and (re.search(r'\d{4}-\d{2}-\d{2}', href) or re.search(r'\d{4}-\d{2}-\d{2}', text)):
            full_url = urljoin('https://elgoose.net', href)
            print(f"  📅 {full_url}")
            print(f"     Text: {text[:60]}")
            setlist_links.append(full_url)
            
            if len(setlist_links) >= 5:
                break
    
    return setlist_links

def inspect_setlist_page(url):
    """Inspect a specific setlist page to understand its structure"""
    print(f"\n🎵 Inspecting setlist page: {url}")
    
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for common setlist container patterns
        print("\n🔍 Searching for setlist containers...")
        
        # Check for various container types
        containers = [
            soup.find_all('div', class_=re.compile(r'setlist|songs|tracklist', re.I)),
            soup.find_all('table', class_=re.compile(r'setlist|songs|tracklist', re.I)),
            soup.find_all('ul', class_=re.compile(r'setlist|songs|tracklist', re.I)),
            soup.find_all('ol', class_=re.compile(r'setlist|songs|tracklist', re.I)),
        ]
        
        for container_type in containers:
            if container_type:
                print(f"  ✅ Found {len(container_type)} potential containers")
                for i, container in enumerate(container_type[:2]):  # Show first 2
                    print(f"     Container {i+1}: {container.name} class='{container.get('class')}'")
                    content = container.get_text()[:200]
                    print(f"     Content preview: {content}")
        
        # Look for specific song patterns
        print("\n🎶 Looking for song patterns...")
        text = soup.get_text()
        
        # Common setlist patterns
        patterns = [
            r'Set\s+(\d+|One|Two|I{1,3})[:\s]*(.+?)(?=Set\s+\d+|Encore|$)',
            r'(Encore)[:\s]*(.+?)(?=Set\s+|$)',
            r'(\d+\.\s*[A-Za-z\s]+)',  # Numbered songs
            r'([A-Z][a-z\s]+(?:\s+>))',  # Song transitions
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.MULTILINE | re.IGNORECASE)
            if matches:
                print(f"  🎯 Pattern '{pattern[:30]}...' found {len(matches)} matches")
                for match in matches[:3]:  # Show first 3
                    print(f"     {match}")
        
        # Look for specific HTML elements that might contain songs
        print("\n🏷️  Checking for specific elements...")
        
        # Check for lists
        lists = soup.find_all(['ul', 'ol'])
        for i, lst in enumerate(lists[:3]):
            items = lst.find_all('li')
            if len(items) > 5:  # Likely a setlist if it has many items
                print(f"  📝 List {i+1}: {len(items)} items")
                for j, item in enumerate(items[:3]):
                    print(f"     Item {j+1}: {item.get_text().strip()[:50]}")
        
        return soup
        
    except Exception as e:
        print(f"❌ Error inspecting {url}: {e}")
        return None

def main():
    print("🎸 EL GOOSE HTML STRUCTURE INSPECTOR")
    print("=" * 50)
    
    # First, find real setlist links
    setlist_links = inspect_main_page()
    
    if not setlist_links:
        print("❌ No setlist links found")
        return
    
    # Inspect the first real setlist page
    print(f"\n📊 Found {len(setlist_links)} setlist links")
    print("🔬 Analyzing first setlist page in detail...")
    
    soup = inspect_setlist_page(setlist_links[0])
    
    if soup:
        print("\n💡 RECOMMENDATIONS FOR SCRAPER:")
        print("1. Target specific HTML containers (div, table, ul, ol) with setlist-related classes")
        print("2. Look for numbered lists or structured song layouts")
        print("3. Filter out navigation elements by excluding header/footer/nav tags")
        print("4. Use text patterns to identify set boundaries (Set 1, Set 2, Encore)")
        print("5. Focus on content within main content areas, avoid sidebars")

if __name__ == "__main__":
    main() 