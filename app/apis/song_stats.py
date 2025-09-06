"""
Song Statistics API
Provides analytics on song performance history and patterns.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class SongStatsParams(BaseModel):
    """Parameters for song statistics queries"""
    song_name: Optional[str] = None
    band_name: Optional[str] = None
    limit: int = 50


class SongStatsResponse(BaseModel):
    """Response model for song statistics"""
    song_name: str
    band_name: str
    total_plays: int
    avg_duration: Optional[float]
    longest_version: Optional[float]
    first_played: str
    last_played: str
    jam_count: int
    tease_count: int
    avg_set_position: float
    most_common_set: str


class SongStatsListResponse(BaseModel):
    """Wrapper for list of songstats responses"""
    items: List[SongStatsResponse]

def get_song_stats(client, params: SongStatsParams) -> SongStatsListResponse:
    """
    Get statistics for songs including play counts, durations, and patterns.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of song statistics
    """
    
    query = """
    SELECT 
        song_name,
        band_name,
        COUNT(*) as total_plays,
        AVG(song_duration_minutes) as avg_duration,
        MAX(song_duration_minutes) as longest_version,
        MIN(show_date) as first_played,
        MAX(show_date) as last_played,
        SUM(CASE WHEN is_jam THEN 1 ELSE 0 END) as jam_count,
        SUM(CASE WHEN is_tease THEN 1 ELSE 0 END) as tease_count,
        AVG(set_position) as avg_set_position,
        topK(1)(set_type)[1] as most_common_set
    FROM SetlistEntry
    WHERE is_prediction = false
    """
    
    query_params = {}
    
    if params.song_name:
        query += " AND song_name ILIKE {song_pattern}"
        query_params["song_pattern"] = f"%{params.song_name}%"
    
    if params.band_name:
        query += " AND band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    query += """
    GROUP BY song_name, band_name
    ORDER BY total_plays DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return SongStatsListResponse(items=[])
    
    return SongStatsListResponse(items=[SongStatsResponse(**row) for row in result])


# Create the consumption API
song_stats_api = ConsumptionApi[SongStatsParams, SongStatsListResponse](
    "songStats",
    query_function=get_song_stats,
    source="SetlistEntry",
    config=EgressConfig()
)
