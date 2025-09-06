"""
Song Frequency Analytics API
Analyzes song play frequency and trends over time.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import List, Optional


class SongFrequencyParams(BaseModel):
    """Parameters for song frequency analysis"""
    band_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    min_plays: Optional[int] = None
    limit: int = 100


class SongFrequencyResponse(BaseModel):
    """Response model for song frequency analysis"""
    song_name: str
    play_count: int
    percentage_of_shows: float
    avg_days_between_plays: float
    last_played: str
    first_played: str
    trend: str  # increasing, decreasing, stable
    days_since_last_played: int


class SongFrequencyListResponse(BaseModel):
    """Wrapper for list of songfrequency responses"""
    items: List[SongFrequencyResponse]

def get_song_frequency(client, params: SongFrequencyParams) -> SongFrequencyListResponse:
    """
    Analyze song play frequency and trends over time.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of song frequency statistics
    """
    
    query = """
    WITH show_counts AS (
        SELECT COUNT(DISTINCT show_date) as total_shows
        FROM Show
        WHERE is_prediction = false
    ),
    song_stats AS (
        SELECT 
            song_name,
            COUNT(*) as play_count,
            COUNT(DISTINCT show_date) as shows_played,
            MAX(show_date) as last_played,
            MIN(show_date) as first_played,
            dateDiff('day', MIN(show_date), MAX(show_date)) / GREATEST(COUNT(*) - 1, 1) as avg_days_between,
            dateDiff('day', MAX(show_date), today()) as days_since_last
        FROM SetlistEntry
        WHERE is_prediction = false
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.start_date:
        query += " AND show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    query += """
        GROUP BY song_name
    """
    
    if params.min_plays:
        query += " HAVING play_count >= {min_plays}"
        query_params["min_plays"] = params.min_plays
    
    query += """
    )
    SELECT 
        ss.song_name,
        ss.play_count,
        ss.shows_played * 100.0 / sc.total_shows as percentage_of_shows,
        ss.avg_days_between as avg_days_between_plays,
        ss.last_played,
        ss.first_played,
        CASE 
            WHEN ss.avg_days_between < 30 THEN 'increasing'
            WHEN ss.avg_days_between > 60 THEN 'decreasing'
            ELSE 'stable'
        END as trend,
        ss.days_since_last as days_since_last_played
    FROM song_stats ss
    CROSS JOIN show_counts sc
    ORDER BY play_count DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return SongFrequencyListResponse(items=[])
    
    return SongFrequencyListResponse(items=[SongFrequencyResponse(**row) for row in result])


# Create the consumption API
song_frequency_api = ConsumptionApi[SongFrequencyParams, SongFrequencyListResponse](
    "analyticsSongFrequency",
    query_function=get_song_frequency,
    source="SetlistEntry",
    config=EgressConfig()
)
