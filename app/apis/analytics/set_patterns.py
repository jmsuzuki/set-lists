"""
Set Patterns Analytics API
Analyzes common patterns in setlists (openers, closers, encores).
"""

from moose_lib import ConsumptionApi, ApiConfig
from pydantic import BaseModel
from typing import List, Optional


class SetPatternsParams(BaseModel):
    """Parameters for set pattern analysis"""
    band_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    pattern_type: Optional[str] = None  # opener, set1_closer, set2_closer, encore
    min_occurrences: int = 2
    limit: int = 100


class SetPatternResponse(BaseModel):
    """Response model for set patterns"""
    pattern_type: str  # opener, set1_closer, set2_closer, encore
    song_name: str
    occurrence_count: int
    percentage: float
    last_occurrence: str
    avg_days_between: Optional[float]


class SetPatternListResponse(BaseModel):
    """Wrapper for list of setpattern responses"""
    items: List[SetPatternResponse]

def get_set_patterns(client, params: SetPatternsParams) -> SetPatternListResponse:
    """
    Analyze common set patterns (openers, closers, encores).
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of set pattern statistics
    """
    
    query = """
    WITH patterns AS (
        SELECT 
            CASE 
                WHEN set_type = 'Set 1' AND set_position = 1 THEN 'opener'
                WHEN set_type = 'Set 1' AND set_position = (
                    SELECT MAX(set_position) 
                    FROM SetlistEntry se2 
                    WHERE se2.show_id = se.show_id AND se2.set_type = 'Set 1'
                ) THEN 'set1_closer'
                WHEN set_type = 'Set 2' AND set_position = (
                    SELECT MAX(set_position) 
                    FROM SetlistEntry se2 
                    WHERE se2.show_id = se.show_id AND se2.set_type = 'Set 2'
                ) THEN 'set2_closer'
                WHEN set_type = 'Encore' THEN 'encore'
                ELSE 'other'
            END as pattern_type,
            song_name,
            show_date
        FROM SetlistEntry se
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
    ),
    pattern_stats AS (
        SELECT 
            pattern_type,
            song_name,
            COUNT(*) as occurrence_count,
            MAX(show_date) as last_occurrence,
            dateDiff('day', MIN(show_date), MAX(show_date)) / GREATEST(COUNT(*) - 1, 1) as avg_days_between
        FROM patterns
        WHERE pattern_type != 'other'
    """
    
    if params.pattern_type:
        query += " AND pattern_type = {pattern_type}"
        query_params["pattern_type"] = params.pattern_type
    
    query += """
        GROUP BY pattern_type, song_name
        HAVING occurrence_count >= {min_occurrences}
    )
    SELECT 
        ps.pattern_type,
        ps.song_name,
        ps.occurrence_count,
        ps.occurrence_count * 100.0 / SUM(ps.occurrence_count) OVER (PARTITION BY ps.pattern_type) as percentage,
        ps.last_occurrence,
        ps.avg_days_between
    FROM pattern_stats ps
    ORDER BY ps.pattern_type, ps.occurrence_count DESC
    LIMIT {limit}
    """
    query_params["min_occurrences"] = params.min_occurrences
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return SetPatternListResponse(items=[])
    
    return SetPatternListResponse(items=[SetPatternResponse(**row) for row in result])


# Create the consumption API
set_patterns_api = ConsumptionApi[SetPatternsParams, SetPatternListResponse](
    "analytics/setPatterns",
    query_function=get_set_patterns,
    source="SetlistEntry",
    config=ApiConfig(
        mode="egress",
        path="analytics/setPatterns"
    )
)
