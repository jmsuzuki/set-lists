# Setlist Analytics API
# Provides endpoints for querying setlist data and analytics

from moose_lib import ConsumptionApi, ConsumptionApiConfig
from app.ingest.models import show_pipeline, setlist_entry_pipeline
from pydantic import BaseModel
from typing import Optional, List
from datetime import date


class SongStatsParams(BaseModel):
    """Parameters for song statistics queries"""
    song_name: Optional[str] = None
    band_name: Optional[str] = None
    limit: int = 50


class ShowsParams(BaseModel):
    """Parameters for show queries"""
    band_name: Optional[str] = None
    venue_name: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit: int = 50


class SetlistParams(BaseModel):
    """Parameters for setlist queries"""
    show_id: Optional[str] = None
    band_name: Optional[str] = None
    show_date: Optional[date] = None
    limit: int = 100


# API endpoint to get song play statistics
def get_song_stats(client, params: SongStatsParams):
    """Get statistics about song performances"""
    query = """
    SELECT 
        song_name,
        band_name,
        COUNT(*) as total_plays,
        AVG(song_duration_minutes) as avg_duration,
        MAX(song_duration_minutes) as longest_version,
        MIN(show_date) as first_played,
        MAX(show_date) as last_played,
        COUNT(CASE WHEN is_jam = true THEN 1 END) as jam_count
    FROM {table: Identifier}
    WHERE 1=1
    """
    
    params_dict = {"table": setlist_entry_pipeline.get_table().name}
    
    if params.song_name:
        query += " AND song_name ILIKE {song_name: String}"
        params_dict["song_name"] = f"%{params.song_name}%"
    
    if params.band_name:
        query += " AND band_name = {band_name: String}"
        params_dict["band_name"] = params.band_name
    
    query += """
    GROUP BY song_name, band_name
    ORDER BY total_plays DESC
    LIMIT {limit: Int32}
    """
    params_dict["limit"] = params.limit
    
    return client.query.execute(query, params_dict)


# API endpoint to get show information
def get_shows(client, params: ShowsParams):
    """Get show information with optional filters"""
    query = """
    SELECT *
    FROM {table: Identifier}
    WHERE 1=1
    """
    
    params_dict = {"table": show_pipeline.get_table().name}
    
    if params.band_name:
        query += " AND band_name = {band_name: String}"
        params_dict["band_name"] = params.band_name
    
    if params.venue_name:
        query += " AND venue_name ILIKE {venue_name: String}"
        params_dict["venue_name"] = f"%{params.venue_name}%"
    
    if params.start_date:
        query += " AND show_date >= {start_date: Date}"
        params_dict["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND show_date <= {end_date: Date}"
        params_dict["end_date"] = params.end_date
    
    query += " ORDER BY show_date DESC LIMIT {limit: Int32}"
    params_dict["limit"] = params.limit
    
    return client.query.execute(query, params_dict)


# API endpoint to get setlist entries
def get_setlist_entries(client, params: SetlistParams):
    """Get individual setlist entries with optional filters"""
    query = """
    SELECT *
    FROM {table: Identifier}
    WHERE 1=1
    """
    
    params_dict = {"table": setlist_entry_pipeline.get_table().name}
    
    if params.show_id:
        query += " AND show_id = {show_id: String}"
        params_dict["show_id"] = params.show_id
    
    if params.band_name:
        query += " AND band_name = {band_name: String}"
        params_dict["band_name"] = params.band_name
    
    if params.show_date:
        query += " AND show_date = {show_date: Date}"
        params_dict["show_date"] = params.show_date
    
    query += " ORDER BY show_date DESC, set_position ASC LIMIT {limit: Int32}"
    params_dict["limit"] = params.limit
    
    return client.query.execute(query, params_dict)


# Create consumption APIs
song_stats_api = ConsumptionApi[SongStatsParams]("song-stats", ConsumptionApiConfig(
    query_function=get_song_stats
))

shows_api = ConsumptionApi[ShowsParams]("shows", ConsumptionApiConfig(
    query_function=get_shows
))

setlist_entries_api = ConsumptionApi[SetlistParams]("setlist-entries", ConsumptionApiConfig(
    query_function=get_setlist_entries
))
