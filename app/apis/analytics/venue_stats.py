"""
Venue Statistics Analytics API
Provides statistics and analytics for venues.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import List, Optional


class VenueStatsParams(BaseModel):
    """Parameters for venue statistics queries"""
    band_name: Optional[str] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_country: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    min_shows: int = 1
    limit: int = 100


class VenueStatsResponse(BaseModel):
    """Response model for venue statistics"""
    venue_name: str
    venue_city: Optional[str]
    venue_state: Optional[str]
    venue_country: Optional[str]
    show_count: int
    unique_songs_played: int
    avg_songs_per_show: float
    first_show_date: str
    last_show_date: str
    most_played_song: str
    most_played_count: int
    days_since_last_show: int


class VenueStatsListResponse(BaseModel):
    """Wrapper for list of venuestats responses"""
    items: List[VenueStatsResponse]

def get_venue_stats(client, params: VenueStatsParams) -> VenueStatsListResponse:
    """
    Get statistics for venues including show counts and unique songs.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of venue statistics
    """
    
    query = """
    WITH venue_songs AS (
        SELECT 
            s.venue_name,
            s.venue_city,
            s.venue_state,
            s.venue_country,
            s.show_date,
            se.song_name,
            COUNT(*) OVER (PARTITION BY s.venue_name, se.song_name) as song_count
        FROM Show s
        JOIN SetlistEntry se ON se.show_id = s.primary_key
        WHERE s.is_prediction = false AND se.is_prediction = false
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND s.band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.venue_city:
        query += " AND s.venue_city = {venue_city}"
        query_params["venue_city"] = params.venue_city
    
    if params.venue_state:
        query += " AND s.venue_state = {venue_state}"
        query_params["venue_state"] = params.venue_state
    
    if params.venue_country:
        query += " AND s.venue_country = {venue_country}"
        query_params["venue_country"] = params.venue_country
    
    if params.start_date:
        query += " AND s.show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND s.show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    query += """
    )
    SELECT 
        venue_name,
        any(venue_city) as venue_city,
        any(venue_state) as venue_state,
        any(venue_country) as venue_country,
        COUNT(DISTINCT show_date) as show_count,
        COUNT(DISTINCT song_name) as unique_songs_played,
        COUNT(*) / COUNT(DISTINCT show_date) as avg_songs_per_show,
        MIN(show_date) as first_show_date,
        MAX(show_date) as last_show_date,
        topK(1)(song_name)[1] as most_played_song,
        MAX(song_count) as most_played_count,
        dateDiff('day', MAX(show_date), today()) as days_since_last_show
    FROM venue_songs
    GROUP BY venue_name
    HAVING show_count >= {min_shows}
    ORDER BY show_count DESC
    LIMIT {limit}
    """
    query_params["min_shows"] = params.min_shows
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return VenueStatsListResponse(items=[])
    
    return VenueStatsListResponse(items=[VenueStatsResponse(**row) for row in result])


# Create the consumption API
venue_stats_api = ConsumptionApi[VenueStatsParams, VenueStatsListResponse](
    "analyticsVenueStats",
    query_function=get_venue_stats,
    source="Show",
    config=EgressConfig()
)
