"""
List all predictions API - clean implementation
"""

from moose_lib import MooseClient, ConsumptionApi
from pydantic import BaseModel, Field
from typing import Optional


class ListAllPredictionsParams(BaseModel):
    """Parameters for listing predictions"""
    band_name: Optional[str] = Field(None)
    show_date: Optional[str] = Field(None)
    limit: int = Field(50, gt=0, le=100)
    offset: int = Field(0, ge=0)


class PredictionItem(BaseModel):
    """Response model for a prediction"""
    band_name: str
    show_date: str
    venue_name: Optional[str] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    algorithm_name: str
    algorithm_version: Optional[str] = None
    generated_at: str
    confidence_score: Optional[float] = None
    total_shows_analyzed: Optional[int] = None
    created_at: str


def run(client: MooseClient, params: ListAllPredictionsParams):
    """Get all predictions with optional filtering"""
    
    # Build query with optional filters
    query = f"""
    SELECT 
        band_name,
        show_date,
        venue_name,
        venue_city,
        venue_state,
        algorithm_name,
        algorithm_version,
        generated_at,
        confidence_score,
        total_shows_analyzed,
        created_at
    FROM Prediction
    WHERE 1=1
    """
    
    if params.band_name:
        band_escaped = params.band_name.replace("'", "''")
        query += f" AND band_name = '{band_escaped}'"
    
    if params.show_date:
        date_escaped = params.show_date.replace("'", "''")
        query += f" AND show_date = '{date_escaped}'"
    
    query += f"""
    ORDER BY show_date DESC, created_at DESC
    LIMIT {params.limit}
    OFFSET {params.offset}
    """
    
    return client.query.execute(query, {})


# Register the API
listAllPredictions_api = ConsumptionApi[ListAllPredictionsParams, PredictionItem](
    name="listAllPredictions",
    query_function=run
)
