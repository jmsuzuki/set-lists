"""
Transform from Show to SetlistEntry - Pure DMv2 version
"""

from typing import List
from app.ingest.models.Show import Show
from app.ingest.models.SetlistEntry import SetlistEntry, SetType
from moose_lib import transform, TransformConfig, DeadLetterQueue
import json
from datetime import datetime, UTC


@transform(
    source=Show,
    destination=SetlistEntry,
    config=TransformConfig(
        dead_letter_queue=DeadLetterQueue[Show](name="ShowTransformDead")
    )
)
def show_to_setlistentry(show: Show) -> List[SetlistEntry]:
    """
    Pure DMv2 transform using @transform decorator.
    Transforms a Show into multiple SetlistEntry records.
    """
    entries = []
    
    # If show has embedded setlist entries, parse them
    if show.setlist_entries:
        try:
            setlist_data = json.loads(show.setlist_entries)
            
            for entry_data in setlist_data:
                entry = SetlistEntry(
                    band_name=show.band_name,
                    show_date=show.show_date,
                    venue_name=show.venue_name,
                    tour_name=show.tour_name,
                    set_type=SetType(entry_data.get("set_type", "Other")),
                    set_position=entry_data.get("set_position", 1),
                    song_name=entry_data["song_name"],
                    song_duration_minutes=entry_data.get("song_duration_minutes"),
                    transitions_into=entry_data.get("transitions_into"),
                    transitions_from=entry_data.get("transitions_from"),
                    is_jam=entry_data.get("is_jam", False),
                    is_tease=entry_data.get("is_tease", False),
                    is_partial=entry_data.get("is_partial", False),
                    is_cover=entry_data.get("is_cover", False),
                    original_artist=entry_data.get("original_artist"),
                    performance_notes=entry_data.get("performance_notes"),
                    guest_musicians=entry_data.get("guest_musicians"),
                    is_enriched=False,
                    created_at=datetime.now(UTC).isoformat()
                )
                entries.append(entry)
                
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error parsing setlist entries for show {show.show_date}: {e}")
    
    return entries
