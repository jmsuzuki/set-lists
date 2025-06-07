# Clean Database Script
# Clears all data from Show and SetlistEntry tables for fresh starts

import requests
import sys

def clean_database(base_url: str = "http://localhost:4000"):
    """Clean all data from the database tables"""
    
    print("🧹 CLEANING DATABASE")
    print("=" * 30)
    
    # Note: Moose doesn't have built-in delete APIs, so we'll use ClickHouse directly
    # For now, we'll just inform the user how to clean manually
    
    print("📋 To clean your database, run these ClickHouse commands:")
    print()
    print("1. Connect to ClickHouse:")
    print("   docker exec -it $(docker ps | grep clickhouse | awk '{print $1}') clickhouse-client")
    print()
    print("2. Clear the tables:")
    print("   TRUNCATE TABLE Show;")
    print("   TRUNCATE TABLE SetlistEntry;")
    print()
    print("3. Or restart fresh:")
    print("   Ctrl+C to stop moose dev, then restart with 'moose dev'")
    print()
    print("💡 Alternatively, you can restart the entire Moose dev environment:")
    print("   1. Stop: Ctrl+C")
    print("   2. Clean: docker-compose down -v")
    print("   3. Restart: moose dev")

def verify_clean_database(base_url: str = "http://localhost:4000"):
    """Verify the database is clean"""
    
    try:
        # Check shows
        response = requests.get(f"{base_url}/consumption/shows?limit=1")
        if response.status_code == 200:
            shows = response.json()
            show_count = len(shows) if shows else 0
        else:
            show_count = "unknown"
        
        # Check songs  
        response = requests.get(f"{base_url}/consumption/song-stats?limit=1")
        if response.status_code == 200:
            songs = response.json()
            song_count = len(songs) if songs else 0
        else:
            song_count = "unknown"
        
        print(f"📊 Current database state:")
        print(f"   Shows: {show_count}")
        print(f"   Songs: {song_count}")
        
        if show_count == 0 and song_count == 0:
            print("✅ Database is clean!")
            return True
        else:
            print("⚠️  Database still contains data")
            return False
            
    except Exception as e:
        print(f"❌ Could not verify database state: {e}")
        return False

def main():
    """Main cleanup function"""
    print("🗑️  DATABASE CLEANUP UTILITY")
    print("=" * 40)
    
    # Check if Moose is running
    try:
        response = requests.get("http://localhost:4000/health", timeout=2)
        if response.status_code != 200:
            print("❌ Moose dev server not running on localhost:4000")
            print("💡 Start it with: moose dev")
            sys.exit(1)
    except:
        print("❌ Moose dev server not running on localhost:4000")
        print("💡 Start it with: moose dev")
        sys.exit(1)
    
    print("✅ Moose dev server is running")
    
    # Verify current state
    verify_clean_database()
    
    # Show cleanup instructions
    clean_database()

if __name__ == "__main__":
    main() 