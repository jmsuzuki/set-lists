"""
Setlists API
Provides endpoints for querying setlist entries and song performances.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import Optional, List


class SetlistParams(BaseModel):
    """Parameters for setlist queries"""
    band_name: Optional[str] = None
    show_date: Optional[str] = None
    venue_name: Optional[str] = None
    set_type: Optional[str] = None
    limit: int = 100


class SetlistEntryResponse(BaseModel):
    """Response model for setlist entries"""
    band_name: str
    show_date: str
    venue_name: str
    tour_name: Optional[str] = None
    set_type: str
    set_position: int
    song_name: str
    song_duration_minutes: Optional[float] = None
    transitions_into: Optional[str] = None
    transitions_from: Optional[str] = None
    is_jam: bool = False
    is_tease: bool = False
    is_partial: bool = False
    is_cover: bool = False
    original_artist: Optional[str] = None
    performance_notes: Optional[str] = None
    guest_musicians: Optional[List[str]] = None
    created_at: str


class SetlistEntryListResponse(BaseModel):
    """Wrapper for list of setlistentry responses"""
    items: List[SetlistEntryResponse]

def get_setlist(client, params: SetlistParams) -> SetlistEntryListResponse:
    """
    Get setlist entries for a specific show or date range.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of setlist entries
    """
    
    query = """
    SELECT 
        band_name,
        show_date,
        venue_name,
        tour_name,
        set_type,
        set_position,
        song_name,
        song_duration_minutes,
        transitions_into,
        transitions_from,
        is_jam,
        is_tease,
        is_partial,
        is_cover,
        original_artist,
        performance_notes,
        guest_musicians,
        created_at
    FROM SetlistEntry
    WHERE 1=1
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.show_date:
        query += " AND show_date = {show_date}"
        query_params["show_date"] = params.show_date
    
    if params.venue_name:
        query += " AND venue_name = {venue_name}"
        query_params["venue_name"] = params.venue_name
    
    if params.set_type:
        query += " AND set_type = {set_type}"
        query_params["set_type"] = params.set_type
    
    query += """
    ORDER BY show_date DESC, set_type, set_position
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return SetlistEntryListResponse(items=[])
    
    return SetlistEntryListResponse(items=[SetlistEntryResponse(**row) for row in result])


# Create the consumption API
setlists_api = ConsumptionApi[SetlistParams, SetlistEntryListResponse](
    "setlists",
    query_function=get_setlist,
    source="SetlistEntry",
    config=EgressConfig()
)
