"""
Transform function: Show -> SetlistEntry
Extracts embedded setlist entries from Show records and creates individual SetlistEntry records.
"""

from typing import List, Dict, Any
from datetime import datetime, UTC
import json
import hashlib
from app.ingest.models import Show, SetlistEntry

def show__setlistentry(show: Show) -> List[SetlistEntry]:
    """
    Transform a Show record into SetlistEntry records.
    Extracts embedded setlist_entries and creates individual records.
    
    Args:
        show: The Show record containing embedded setlist entries
        
    Returns:
        List of SetlistEntry records
    """
    print(f"🎭 Transform triggered for Show: {show.band_name} on {show.show_date}")
    
    # Create a unique hash for this show to detect if it's being processed multiple times
    show_hash = hashlib.md5(f"{show.band_name}_{show.show_date}_{show.venue_name}_{id(show)}".encode()).hexdigest()[:8]
    print(f"  📍 Transform execution ID: {show_hash} at {datetime.now(UTC).isoformat()}")
    
    # If no embedded entries, return empty list
    if not show.setlist_entries:
        print(f"  ⚠️ No setlist_entries found in Show")
        return []
    
    # Parse JSON string if needed
    try:
        if isinstance(show.setlist_entries, str):
            setlist_data = json.loads(show.setlist_entries)
        else:
            setlist_data = show.setlist_entries
    except (json.JSONDecodeError, TypeError) as e:
        print(f"  ❌ Error parsing setlist_entries: {e}")
        return []
    
    entries = []
    
    # Process each embedded entry
    for entry_data in setlist_data:
        # Create the SetlistEntry record with a unique transform execution ID
        # This helps identify if the same entry is being created multiple times
        created_timestamp = datetime.now(UTC).isoformat()
        
        entry = SetlistEntry(
            band_name=show.band_name,
            show_date=show.show_date,
            venue_name=show.venue_name,
            tour_name=show.tour_name,
            set_type=entry_data.get('set_type', 'Set 1'),
            set_position=entry_data.get('set_position', 1),
            song_name=entry_data.get('song_name', ''),
            song_duration_minutes=entry_data.get('song_duration_minutes'),
            transitions_into=entry_data.get('transitions_into'),
            transitions_from=entry_data.get('transitions_from'),
            is_jam=entry_data.get('is_jam', False),
            is_tease=entry_data.get('is_tease', False),
            is_partial=entry_data.get('is_partial', False),
            is_cover=entry_data.get('is_cover', False),
            original_artist=entry_data.get('original_artist'),
            performance_notes=entry_data.get('performance_notes'),
            guest_musicians=entry_data.get('guest_musicians'),
            is_enriched=False,  # New entries haven't been enriched yet
            created_at=created_timestamp
        )
        
        entries.append(entry)
    
    # Log the transformation
    print(f"  ✅ Extracted {len(entries)} setlist entries from {show.band_name} show on {show.show_date}")
    
    # Debug: Show first few entries
    if entries:
        print(f"  📝 First 3 entries created:")
        for i, entry in enumerate(entries[:3], 1):
            print(f"     {i}. {entry.set_type} - {entry.song_name[:50]}")
    
    return entries


# Register the transform from Show to SetlistEntry
from app.ingest.models import show_pipeline, setlist_entry_pipeline
from moose_lib import TransformConfig, DeadLetterQueue

# Dead letter queue for failed transformations
show_transform_dlq = DeadLetterQueue[Show](name="ShowTransformDead")

# Register the transform
show_pipeline.get_stream().add_transform(
    destination=setlist_entry_pipeline.get_stream(),
    transformation=show__setlistentry,
    config=TransformConfig(
        dead_letter_queue=show_transform_dlq
    )
)
