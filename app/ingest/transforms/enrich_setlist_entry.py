from app.ingest.models.SetlistEntry import setlist_entry_pipeline, SetlistEntry
from moose_lib import DeadLetterQueue, TransformConfig


# Dead letter queue for failed setlist entries
setlist_dlq = DeadLetterQueue[SetlistEntry](name="SetlistEntryDead")


def enrich_setlist_entry(entry: SetlistEntry) -> SetlistEntry:
    """
    Enrich setlist entries with additional analytics-friendly data
    Only processes entries that haven't been enriched yet to prevent infinite loops
    """
    
    # Skip if already enriched to prevent infinite loop
    if entry.is_enriched:
        return None
    
    # Basic song name standardization
    standardized_name = entry.song_name.strip()
    
    # Detect jams from song names (common patterns) - preserve original or detect new
    is_jam = bool(
        entry.is_jam or  # Preserve original value
        "jam" in standardized_name.lower() or
        ">" in standardized_name or
        (entry.song_duration_minutes is not None and entry.song_duration_minutes > 20)
    )
    
    # Detect teases - preserve original or detect new
    is_tease = bool(entry.is_tease or "tease" in standardized_name.lower())
    
    # Detect partial performances - preserve original or detect new  
    is_partial = bool(entry.is_partial or "partial" in standardized_name.lower() or ">" in standardized_name)
    
    # Create enriched entry
    enriched_entry = entry.model_copy()
    enriched_entry.song_name = standardized_name
    enriched_entry.is_jam = is_jam
    enriched_entry.is_tease = is_tease
    enriched_entry.is_partial = is_partial
    enriched_entry.is_enriched = True  # Mark as enriched
    
    return enriched_entry


# DISABLED - This transform causes duplicates when writing back to the same pipeline
# The issue is that both the original and enriched entries end up in the database
# TODO: Need to redesign this to either:
#   1. Modify entries in-place instead of creating copies
#   2. Write to a different pipeline/table for enriched entries
#   3. Use a different enrichment strategy
# setlist_entry_pipeline.get_stream().add_transform(
#     destination=setlist_entry_pipeline.get_stream(),
#     transformation=enrich_setlist_entry,
#     config=TransformConfig(
#         dead_letter_queue=setlist_dlq
#     )
# )
