"""
Prediction Metadata API
Retrieves metadata about prediction runs and algorithms.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import List, Optional


class PredictionMetadataParams(BaseModel):
    """Parameters for prediction metadata queries"""
    band_name: Optional[str] = None
    prediction_date: Optional[str] = None
    algorithm_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: int = 50


class PredictionMetadataResponse(BaseModel):
    """Response model for prediction metadata"""
    prediction_date: str
    band_name: str
    algorithm_name: str
    algorithm_version: str
    total_predictions: int
    total_shows_analyzed: int
    data_through_date: str
    generated_at: str
    recent_shows_days: int
    rotation_threshold: float
    confidence_threshold: float
    cover_percentage: Optional[float]
    avg_songs_per_show: Optional[float]
    prediction_notes: Optional[str]


class PredictionMetadataListResponse(BaseModel):
    """Wrapper for list of predictionmetadata responses"""
    items: List[PredictionMetadataResponse]

def get_prediction_metadata(client, params: PredictionMetadataParams) -> PredictionMetadataListResponse:
    """
    Get metadata about prediction runs.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of prediction metadata records
    """
    
    query = """
    SELECT 
        prediction_date,
        band_name,
        algorithm_name,
        algorithm_version,
        total_predictions,
        total_shows_analyzed,
        data_through_date,
        generated_at,
        recent_shows_days,
        rotation_threshold,
        confidence_threshold,
        cover_percentage,
        avg_songs_per_show,
        prediction_notes
    FROM PredictionMetadata
    WHERE 1=1
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.prediction_date:
        query += " AND prediction_date = {prediction_date}"
        query_params["prediction_date"] = params.prediction_date
    
    if params.algorithm_name:
        query += " AND algorithm_name = {algorithm_name}"
        query_params["algorithm_name"] = params.algorithm_name
    
    if params.start_date:
        query += " AND prediction_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND prediction_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    query += """
    ORDER BY generated_at DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return PredictionMetadataListResponse(items=[])
    
    return PredictionMetadataListResponse(items=[PredictionMetadataResponse(**row) for row in result])


# Create the consumption API
prediction_metadata_api = ConsumptionApi[PredictionMetadataParams, PredictionMetadataListResponse](
    "predictionMetadata",
    query_function=get_prediction_metadata,
    source="PredictionMetadata",
    config=EgressConfig()
)
