# Setlist Analytics API
# Provides endpoints for querying setlist data and analytics

from moose_lib import ConsumptionApi
from app.ingest.models import show_pipeline, setlist_entry_pipeline
from pydantic import BaseModel
from typing import Optional, List


class SongStatsParams(BaseModel):
    """Parameters for song statistics queries"""
    song_name: Optional[str] = None
    band_name: Optional[str] = None
    limit: int = 50


class ShowsParams(BaseModel):
    """Parameters for show queries"""
    band_name: Optional[str] = None
    venue_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: int = 50


class SetlistParams(BaseModel):
    """Parameters for setlist queries"""
    show_id: Optional[str] = None
    band_name: Optional[str] = None
    show_date: Optional[str] = None
    limit: int = 100


# Response models for better type safety
class SongStatsResponse(BaseModel):
    song_name: str
    band_name: str
    total_plays: int
    avg_duration: Optional[float]
    longest_version: Optional[float]
    first_played: str
    last_played: str
    jam_count: int


class ShowResponse(BaseModel):
    """Response model for show queries"""
    primary_key: str
    band_name: str
    show_date: str
    venue_name: str
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_country: Optional[str] = None
    tour_name: Optional[str] = None
    show_notes: Optional[str] = None
    verified: bool
    source_url: Optional[str] = None


class SetlistEntryResponse(BaseModel):
    """Response model for setlist entry queries"""
    primary_key: str
    show_id: str
    band_name: str
    show_date: str
    set_type: str
    set_position: int
    song_name: str
    song_duration_minutes: Optional[float] = None
    transitions_into: Optional[str] = None
    transitions_from: Optional[str] = None
    is_jam: bool
    is_tease: bool
    is_partial: bool
    performance_notes: Optional[str] = None
    guest_musicians: Optional[List[str]] = None


# API endpoint to get song play statistics
def get_song_stats(client, params: SongStatsParams):
    """Get statistics about song performances"""
    table_name = setlist_entry_pipeline.get_table().name
    
    query = f"""
    SELECT 
        song_name,
        band_name,
        COUNT(*) as total_plays,
        AVG(song_duration_minutes) as avg_duration,
        MAX(song_duration_minutes) as longest_version,
        toString(MIN(show_date)) as first_played,
        toString(MAX(show_date)) as last_played,
        COUNT(CASE WHEN is_jam = true THEN 1 END) as jam_count
    FROM {table_name}
    WHERE 1=1
    """
    
    # Build dynamic WHERE conditions
    if params.song_name:
        query += f" AND song_name ILIKE '%{params.song_name}%'"
    
    if params.band_name:
        query += f" AND band_name = '{params.band_name}'"
    
    query += f"""
    GROUP BY song_name, band_name
    ORDER BY total_plays DESC
    LIMIT {params.limit}
    """
    
    return client.query.execute(query, {})


# API endpoint to get show information
def get_shows(client, params: ShowsParams):
    """Get show information with optional filters"""
    table_name = show_pipeline.get_table().name
    
    query = f"""
    SELECT 
        primary_key,
        band_name,
        toString(show_date) as show_date,
        venue_name,
        venue_city,
        venue_state,
        venue_country,
        tour_name,
        show_notes,
        verified,
        source_url
    FROM {table_name}
    WHERE 1=1
    """
    
    # Build dynamic WHERE conditions
    if params.band_name:
        query += f" AND band_name = '{params.band_name}'"
    
    if params.venue_name:
        query += f" AND venue_name ILIKE '%{params.venue_name}%'"
    
    if params.start_date:
        query += f" AND show_date >= '{params.start_date}'"
    
    if params.end_date:
        query += f" AND show_date <= '{params.end_date}'"
    
    query += f" ORDER BY show_date DESC LIMIT {params.limit}"
    
    return client.query.execute(query, {})


# API endpoint to get setlist entries
def get_setlist_entries(client, params: SetlistParams):
    """Get individual setlist entries with optional filters"""
    table_name = setlist_entry_pipeline.get_table().name
    
    query = f"""
    SELECT 
        primary_key,
        show_id,
        band_name,
        toString(show_date) as show_date,
        set_type,
        set_position,
        song_name,
        song_duration_minutes,
        transitions_into,
        transitions_from,
        is_jam,
        is_tease,
        is_partial,
        performance_notes,
        guest_musicians
    FROM {table_name}
    WHERE 1=1
    """
    
    # Build dynamic WHERE conditions  
    if params.show_id:
        query += f" AND show_id = '{params.show_id}'"
    
    if params.band_name:
        query += f" AND band_name = '{params.band_name}'"
    
    if params.show_date:
        query += f" AND show_date = '{params.show_date}'"
    
    query += f" ORDER BY show_date DESC, set_position ASC LIMIT {params.limit}"
    
    return client.query.execute(query, {})


# Create consumption APIs using the correct Moose Python pattern
song_stats_api = ConsumptionApi[SongStatsParams, SongStatsResponse](
    "song-stats", 
    query_function=get_song_stats
)

shows_api = ConsumptionApi[ShowsParams, ShowResponse](
    "shows", 
    query_function=get_shows
)

setlist_entries_api = ConsumptionApi[SetlistParams, SetlistEntryResponse](
    "setlist-entries", 
    query_function=get_setlist_entries
)
