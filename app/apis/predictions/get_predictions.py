"""
Get Predictions API
Retrieves predicted setlists with filtering options.
"""

from moose_lib import ConsumptionApi, EgressConfig, ApiConfig
from pydantic import BaseModel
from typing import List, Optional


class GetPredictionsParams(BaseModel):
    """Parameters for retrieving predictions"""
    band_name: Optional[str] = None
    prediction_date: Optional[str] = None
    prediction_type: Optional[str] = None  # opener, encore, rotation_candidate, etc.
    min_confidence: Optional[float] = None
    show_id: Optional[str] = None  # For predictions tied to a specific show
    include_reasoning: bool = True
    limit: int = 50


class PredictionResponse(BaseModel):
    """Response model for individual predictions"""
    song_name: str
    band_name: str
    show_date: str
    set_type: str
    set_position: int
    prediction_confidence: float
    prediction_reasoning: Optional[List[str]]
    is_cover: bool
    original_artist: Optional[str]
    show_id: str
    prediction_type: str  # Derived from set position


class PredictionListResponse(BaseModel):
    """Wrapper for list of prediction responses"""
    items: List[PredictionResponse]

def get_predictions(client, params: GetPredictionsParams) -> PredictionListResponse:
    """
    Get predicted setlist entries with optional filtering.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of predicted songs
    """
    
    query = """
    SELECT 
        song_name,
        band_name,
        show_date,
        set_type,
        set_position,
        prediction_confidence,
    """
    
    if params.include_reasoning:
        query += "prediction_reasoning,"
    else:
        query += "NULL as prediction_reasoning,"
    
    query += """
        is_cover,
        original_artist,
        show_id,
        CASE 
            WHEN set_type = 'Set 1' AND set_position = 1 THEN 'opener'
            WHEN set_type = 'Set 1' AND set_position = 99 THEN 'set1_closer'
            WHEN set_type = 'Set 2' AND set_position = 99 THEN 'set2_closer'
            WHEN set_type = 'Encore' THEN 'encore'
            ELSE 'rotation_candidate'
        END as prediction_type
    FROM SetlistEntry
    WHERE is_prediction = true
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.prediction_date:
        query += " AND show_date = {prediction_date}"
        query_params["prediction_date"] = params.prediction_date
    
    if params.min_confidence is not None:
        query += " AND prediction_confidence >= {min_confidence}"
        query_params["min_confidence"] = params.min_confidence
    
    if params.show_id:
        query += " AND show_id = {show_id}"
        query_params["show_id"] = params.show_id
    
    # Filter by prediction type based on set position patterns
    if params.prediction_type:
        if params.prediction_type == "opener":
            query += " AND set_type = 'Set 1' AND set_position = 1"
        elif params.prediction_type == "encore":
            query += " AND set_type = 'Encore'"
        elif params.prediction_type == "set1_closer":
            query += " AND set_type = 'Set 1' AND set_position = 99"
        elif params.prediction_type == "set2_closer":
            query += " AND set_type = 'Set 2' AND set_position = 99"
        elif params.prediction_type == "rotation_candidate":
            query += " AND NOT (set_position IN (1, 99) OR set_type = 'Encore')"
    
    query += """
    ORDER BY show_date DESC, prediction_confidence DESC, set_type, set_position
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return PredictionListResponse(items=[])
    
    return PredictionListResponse(items=[PredictionResponse(**row) for row in result])


# Create the consumption API
get_predictions_api = ConsumptionApi[GetPredictionsParams, PredictionListResponse](
    "predictions/getPredictions",
    query_function=get_predictions,
    source="SetlistEntry",
    config=ApiConfig()
)
