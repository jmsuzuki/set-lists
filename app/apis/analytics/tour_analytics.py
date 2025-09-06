"""
Tour Analytics API
Analyzes patterns and statistics across tours.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class TourAnalyticsParams(BaseModel):
    """Parameters for tour analytics queries"""
    band_name: Optional[str] = None
    tour_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: int = 100


class TourStatsResponse(BaseModel):
    """Response model for tour statistics"""
    tour_name: Optional[str]
    show_count: int
    unique_venues: int
    unique_songs: int
    avg_songs_per_show: float
    total_songs_played: int
    tour_start: str
    tour_end: str
    most_played_song: str
    most_played_count: int
    unique_covers: int
    jam_percentage: float


class TourStatsListResponse(BaseModel):
    """Wrapper for list of tourstats responses"""
    items: List[TourStatsResponse]

class TourComparisonResponse(BaseModel):
    """Response model for comparing tours"""
    tour_name: Optional[str]
    metric_name: str
    metric_value: float
    rank: int


class TourComparisonListResponse(BaseModel):
    """Wrapper for list of tourcomparison responses"""
    items: List[TourComparisonResponse]

def get_tour_stats(client, params: TourAnalyticsParams) -> TourStatsListResponse:
    """
    Get statistics for tours including show counts, unique songs, and patterns.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of tour statistics
    """
    
    query = """
    WITH tour_data AS (
        SELECT 
            COALESCE(s.tour_name, 'No Tour') as tour_name,
            s.show_date,
            s.venue_name,
            se.song_name,
            se.is_jam,
            se.is_cover
        FROM Show s
        JOIN SetlistEntry se ON se.show_id = s.primary_key
        WHERE s.is_prediction = false AND se.is_prediction = false
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND s.band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.tour_name:
        query += " AND s.tour_name = {tour_name}"
        query_params["tour_name"] = params.tour_name
    
    if params.start_date:
        query += " AND s.show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND s.show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    query += """
    )
    SELECT 
        tour_name,
        COUNT(DISTINCT show_date) as show_count,
        COUNT(DISTINCT venue_name) as unique_venues,
        COUNT(DISTINCT song_name) as unique_songs,
        COUNT(*) / COUNT(DISTINCT show_date) as avg_songs_per_show,
        COUNT(*) as total_songs_played,
        MIN(show_date) as tour_start,
        MAX(show_date) as tour_end,
        topK(1)(song_name)[1] as most_played_song,
        MAX(COUNT(*)) OVER (PARTITION BY tour_name, song_name) as most_played_count,
        COUNT(DISTINCT CASE WHEN is_cover THEN song_name END) as unique_covers,
        SUM(CASE WHEN is_jam THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as jam_percentage
    FROM tour_data
    GROUP BY tour_name
    ORDER BY show_count DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return TourStatsListResponse(items=[])
    
    return TourStatsListResponse(items=[TourStatsResponse(**row) for row in result])


def compare_tours(client, params: TourAnalyticsParams) -> TourComparisonListResponse:
    """
    Compare metrics across different tours.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of tour comparison metrics
    """
    
    query = """
    WITH tour_metrics AS (
        SELECT 
            COALESCE(s.tour_name, 'No Tour') as tour_name,
            COUNT(DISTINCT s.show_date) as shows,
            COUNT(DISTINCT se.song_name) as unique_songs,
            COUNT(DISTINCT s.venue_name) as unique_venues,
            AVG(se.song_duration_minutes) as avg_song_duration,
            SUM(CASE WHEN se.is_jam THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as jam_percentage,
            SUM(CASE WHEN se.is_cover THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as cover_percentage
        FROM Show s
        JOIN SetlistEntry se ON se.show_id = s.primary_key
        WHERE s.is_prediction = false AND se.is_prediction = false
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND s.band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.start_date:
        query += " AND s.show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND s.show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    query += """
        GROUP BY tour_name
    )
    SELECT 
        tour_name,
        metric_name,
        metric_value,
        RANK() OVER (PARTITION BY metric_name ORDER BY metric_value DESC) as rank
    FROM (
        SELECT tour_name, 'shows' as metric_name, shows as metric_value FROM tour_metrics
        UNION ALL
        SELECT tour_name, 'unique_songs' as metric_name, unique_songs as metric_value FROM tour_metrics
        UNION ALL
        SELECT tour_name, 'unique_venues' as metric_name, unique_venues as metric_value FROM tour_metrics
        UNION ALL
        SELECT tour_name, 'avg_song_duration' as metric_name, avg_song_duration as metric_value FROM tour_metrics
        UNION ALL
        SELECT tour_name, 'jam_percentage' as metric_name, jam_percentage as metric_value FROM tour_metrics
        UNION ALL
        SELECT tour_name, 'cover_percentage' as metric_name, cover_percentage as metric_value FROM tour_metrics
    )
    ORDER BY metric_name, rank
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return TourComparisonListResponse(items=[])
    
    return TourComparisonListResponse(items=[TourComparisonResponse(**row) for row in result])


# Create the consumption APIs
tour_stats_api = ConsumptionApi[TourAnalyticsParams, TourStatsListResponse](
    "analytics/tourStats",
    query_function=get_tour_stats,
    source="Show",
    config=EgressConfig()
)

tour_comparison_api = ConsumptionApi[TourAnalyticsParams, TourComparisonListResponse](
    "analyticsTourComparison",
    query_function=compare_tours,
    source="Show",
    config=EgressConfig()
)
