"""
Shows API
Provides endpoints for querying show/concert information.
"""

from moose_lib import Api, EgressConfig, ApiConfig
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class ShowsParams(BaseModel):
    """Parameters for show queries"""
    band_name: Optional[str] = None
    venue_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    verified_only: bool = False
    limit: int = 50


class ShowResponse(BaseModel):
    """Response model for show data"""
    band_name: str
    show_date: str
    venue_name: str
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_country: Optional[str] = None
    tour_name: Optional[str] = None
    show_notes: Optional[str] = None
    verified: bool = False
    source_url: Optional[str] = None
    total_songs: Optional[int] = None
    created_at: Optional[str] = None


class ShowsResponse(BaseModel):
    """Response wrapper for multiple shows"""
    shows: List[ShowResponse]


def get_shows(client, params: ShowsParams) -> ShowsResponse:
    """
    Query shows with optional filtering by band, venue, date range, etc.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        ShowsResponse containing list of shows matching the criteria
    """
    
    query = """
    SELECT 
        s.band_name,
        s.show_date,
        s.venue_name,
        s.venue_city,
        s.venue_state,
        s.venue_country,
        s.tour_name,
        s.show_notes,
        s.verified,
        s.source_url,
        s.created_at,
        COUNT(se.song_name) as total_songs
    FROM Show s
    LEFT JOIN SetlistEntry se ON 
        se.band_name = s.band_name AND
        se.show_date = s.show_date AND
        se.venue_name = s.venue_name AND
        se.tour_name = s.tour_name
    WHERE 1=1
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND s.band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.venue_name:
        query += " AND s.venue_name ILIKE {venue_pattern}"
        query_params["venue_pattern"] = f"%{params.venue_name}%"
    
    if params.start_date:
        query += " AND s.show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND s.show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    if params.verified_only:
        query += " AND s.verified = true"
    
    query += """
    GROUP BY 
        s.band_name,
        s.show_date,
        s.venue_name,
        s.venue_city,
        s.venue_state,
        s.venue_country,
        s.tour_name,
        s.show_notes,
        s.verified,
        s.source_url,
        s.created_at
    ORDER BY s.show_date DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return ShowsResponse(shows=[])
    
    shows = [ShowResponse(**row) for row in result]
    return ShowsResponse(shows=shows)


# Create the consumption API
shows_api = Api[ShowsParams, ShowsResponse](
    "pipelines/shows",
    query_function=get_shows,
    source="Show",
    config=ApiConfig()
)