from app.ingest.models import show_pipeline, setlist_entry_pipeline, Show, SetlistEntry
from moose_lib import DeadLetterQueue, DeadLetterModel, TransformConfig
from datetime import datetime
from typing import Dict, Any


# Dead letter queue for failed setlist entries
setlist_dlq = DeadLetterQueue[SetlistEntry](name="SetlistEntryDead")


def enrich_setlist_entry(entry: SetlistEntry) -> SetlistEntry:
    """
    Enrich setlist entries with additional analytics-friendly data
    """
    # Add any enrichment logic here
    # For example, standardize song names, detect jams, etc.
    
    # Basic song name standardization
    standardized_name = entry.song_name.strip()
    
    # Detect jams from song names (common patterns)
    is_jam = (
        "jam" in standardized_name.lower() or
        ">" in standardized_name or
        entry.song_duration_minutes and entry.song_duration_minutes > 20
    )
    
    # Detect teases
    is_tease = "tease" in standardized_name.lower()
    
    # Detect partial performances
    is_partial = "partial" in standardized_name.lower() or ">" in standardized_name
    
    # Create enriched entry
    enriched_entry = entry.model_copy()
    enriched_entry.song_name = standardized_name
    enriched_entry.is_jam = is_jam
    enriched_entry.is_tease = is_tease
    enriched_entry.is_partial = is_partial
    
    return enriched_entry


# Transform raw setlist entries to enriched versions
setlist_entry_pipeline.get_stream().add_transform(
    destination=setlist_entry_pipeline.get_stream(),
    transformation=enrich_setlist_entry,
    config=TransformConfig(
        dead_letter_queue=setlist_dlq
    )
)


# Add streaming consumers for monitoring and debugging
def log_show_ingestion(show: Show):
    """Log when new shows are ingested"""
    print(f"🎵 New show ingested:")
    print(f"  Band: {show.band_name}")
    print(f"  Date: {show.show_date}")
    print(f"  Venue: {show.venue_name}")
    if show.venue_city:
        print(f"  Location: {show.venue_city}")
    print(f"  Verified: {'✓' if show.verified else '✗'}")
    print("---")


def log_setlist_entry(entry: SetlistEntry):
    """Log individual song performances"""
    print(f"🎶 Song performance:")
    print(f"  Song: {entry.song_name}")
    print(f"  Band: {entry.band_name}")
    print(f"  Date: {entry.show_date}")
    print(f"  Set: {entry.set_type} (Position {entry.set_position})")
    if entry.song_duration_minutes:
        print(f"  Duration: {entry.song_duration_minutes} minutes")
    if entry.is_jam:
        print(f"  🎸 JAM VERSION")
    if entry.transitions_into:
        print(f"  → Transitions into: {entry.transitions_into}")
    print("---")


# Add consumers to pipelines
show_pipeline.get_stream().add_consumer(log_show_ingestion)
setlist_entry_pipeline.get_stream().add_consumer(log_setlist_entry)


# Handle dead letter queue messages
def handle_failed_setlist_entries(dead_letter: DeadLetterModel[SetlistEntry]):
    """Handle failed setlist entry processing"""
    print(f"❌ Failed to process setlist entry:")
    print(f"  Error: {dead_letter.error}")
    print(f"  Timestamp: {dead_letter.timestamp}")
    
    # Try to extract the original entry for debugging
    try:
        original_entry = dead_letter.as_typed()
        print(f"  Original song: {original_entry.song_name}")
        print(f"  Original show: {original_entry.show_id}")
    except Exception as e:
        print(f"  Could not parse original entry: {e}")
    print("---")


setlist_dlq.add_consumer(handle_failed_setlist_entries)
