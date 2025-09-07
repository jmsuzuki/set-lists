"""
Prediction Accuracy API
Evaluates and tracks the accuracy of predictions against actual shows.
"""

from moose_lib import ConsumptionApi, EgressConfig
from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class PredictionAccuracyParams(BaseModel):
    """Parameters for prediction accuracy queries"""
    band_name: Optional[str] = None
    show_date: Optional[str] = None
    algorithm_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    min_confidence: Optional[float] = None
    limit: int = 100


class PredictionAccuracyResponse(BaseModel):
    """Response model for prediction accuracy analysis"""
    show_date: str
    band_name: str
    algorithm_name: Optional[str]
    total_predictions: int
    correct_predictions: int
    accuracy_percentage: float
    opener_accuracy: float
    encore_accuracy: float
    high_confidence_accuracy: float  # Accuracy for predictions > 0.7 confidence
    avg_confidence: float


class PredictionAccuracyListResponse(BaseModel):
    """Wrapper for list of predictionaccuracy responses"""
    items: List[PredictionAccuracyResponse]

class DetailedAccuracyResponse(BaseModel):
    """Detailed accuracy for individual predictions"""
    show_date: str
    song_name: str
    prediction_type: str
    confidence: float
    was_correct: bool
    actual_position: Optional[str]
    notes: Optional[str]


class DetailedAccuracyListResponse(BaseModel):
    """Wrapper for list of detailedaccuracy responses"""
    items: List[DetailedAccuracyResponse]

def get_prediction_accuracy(client, params: PredictionAccuracyParams) -> PredictionAccuracyListResponse:
    """
    Calculate accuracy metrics for predictions compared to actual shows.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of prediction accuracy metrics
    """
    
    query = """
    WITH predictions AS (
        SELECT 
            se.show_date,
            se.band_name,
            s.prediction_algorithm as algorithm_name,
            se.song_name,
            se.set_type,
            se.set_position,
            se.prediction_confidence,
            CASE 
                WHEN se.set_type = 'Set 1' AND se.set_position = 1 THEN 'opener'
                WHEN se.set_type = 'Encore' THEN 'encore'
                ELSE 'other'
            END as prediction_type
        FROM SetlistEntry se
        JOIN Show s ON s.primary_key = se.show_id
        WHERE se.is_prediction = true
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND se.band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.show_date:
        query += " AND se.show_date = {show_date}"
        query_params["show_date"] = params.show_date
    
    if params.algorithm_name:
        query += " AND s.prediction_algorithm = {algorithm_name}"
        query_params["algorithm_name"] = params.algorithm_name
    
    if params.start_date:
        query += " AND se.show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND se.show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    if params.min_confidence:
        query += " AND se.prediction_confidence >= {min_confidence}"
        query_params["min_confidence"] = params.min_confidence
    
    query += """
    ),
    actuals AS (
        SELECT 
            show_date,
            band_name,
            song_name,
            set_type,
            set_position
        FROM SetlistEntry
        WHERE is_prediction = false
    ),
    accuracy_calc AS (
        SELECT 
            p.show_date,
            p.band_name,
            p.algorithm_name,
            p.song_name,
            p.prediction_type,
            p.prediction_confidence,
            CASE 
                WHEN a.song_name IS NOT NULL THEN 1 
                ELSE 0 
            END as was_correct
        FROM predictions p
        LEFT JOIN actuals a ON 
            p.show_date = a.show_date 
            AND p.band_name = a.band_name 
            AND p.song_name = a.song_name
    )
    SELECT 
        show_date,
        band_name,
        algorithm_name,
        COUNT(*) as total_predictions,
        SUM(was_correct) as correct_predictions,
        SUM(was_correct) * 100.0 / COUNT(*) as accuracy_percentage,
        SUM(CASE WHEN prediction_type = 'opener' THEN was_correct ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN prediction_type = 'opener' THEN 1 ELSE 0 END), 0) as opener_accuracy,
        SUM(CASE WHEN prediction_type = 'encore' THEN was_correct ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN prediction_type = 'encore' THEN 1 ELSE 0 END), 0) as encore_accuracy,
        SUM(CASE WHEN prediction_confidence > 0.7 THEN was_correct ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN prediction_confidence > 0.7 THEN 1 ELSE 0 END), 0) as high_confidence_accuracy,
        AVG(prediction_confidence) as avg_confidence
    FROM accuracy_calc
    GROUP BY show_date, band_name, algorithm_name
    ORDER BY show_date DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return PredictionAccuracyListResponse(items=[])
    
    return PredictionAccuracyListResponse(items=[PredictionAccuracyResponse(**row) for row in result])


def get_detailed_accuracy(client, params: PredictionAccuracyParams) -> DetailedAccuracyListResponse:
    """
    Get detailed accuracy information for individual predictions.
    
    Args:
        client: Database client for executing queries
        params: Query parameters for filtering
        
    Returns:
        List of detailed prediction accuracy records
    """
    
    query = """
    WITH predictions AS (
        SELECT 
            se.show_date,
            se.band_name,
            se.song_name,
            se.set_type as predicted_set,
            se.set_position as predicted_position,
            se.prediction_confidence as confidence,
            CASE 
                WHEN se.set_type = 'Set 1' AND se.set_position = 1 THEN 'opener'
                WHEN se.set_type = 'Encore' THEN 'encore'
                WHEN se.set_position = 99 THEN 'closer'
                ELSE 'rotation'
            END as prediction_type
        FROM SetlistEntry se
        WHERE se.is_prediction = true
    """
    
    query_params = {}
    
    if params.band_name:
        query += " AND se.band_name = {band_name}"
        query_params["band_name"] = params.band_name
    
    if params.show_date:
        query += " AND se.show_date = {show_date}"
        query_params["show_date"] = params.show_date
    
    if params.start_date:
        query += " AND se.show_date >= {start_date}"
        query_params["start_date"] = params.start_date
    
    if params.end_date:
        query += " AND se.show_date <= {end_date}"
        query_params["end_date"] = params.end_date
    
    if params.min_confidence:
        query += " AND se.prediction_confidence >= {min_confidence}"
        query_params["min_confidence"] = params.min_confidence
    
    query += """
    ),
    actuals AS (
        SELECT 
            show_date,
            band_name,
            song_name,
            set_type,
            set_position
        FROM SetlistEntry
        WHERE is_prediction = false
    )
    SELECT 
        p.show_date,
        p.song_name,
        p.prediction_type,
        p.confidence,
        CASE WHEN a.song_name IS NOT NULL THEN true ELSE false END as was_correct,
        CASE 
            WHEN a.song_name IS NOT NULL THEN 
                concat(a.set_type, ' #', toString(a.set_position))
            ELSE NULL 
        END as actual_position,
        CASE 
            WHEN a.song_name IS NOT NULL AND p.predicted_set = a.set_type THEN 'Correct set'
            WHEN a.song_name IS NOT NULL THEN 'Wrong set'
            ELSE 'Not played'
        END as notes
    FROM predictions p
    LEFT JOIN actuals a ON 
        p.show_date = a.show_date 
        AND p.band_name = a.band_name 
        AND p.song_name = a.song_name
    ORDER BY p.show_date DESC, p.confidence DESC
    LIMIT {limit}
    """
    query_params["limit"] = params.limit
    
    result = client.query.execute(query, query_params)
    
    if not result:
        return DetailedAccuracyListResponse(items=[])
    
    return DetailedAccuracyListResponse(items=[DetailedAccuracyResponse(**row) for row in result])


# Create the consumption APIs
prediction_accuracy_api = ConsumptionApi[PredictionAccuracyParams, PredictionAccuracyListResponse](
    "predictionsAccuracy",
    query_function=get_prediction_accuracy,
    source="SetlistEntry",
    config=EgressConfig()
)

detailed_accuracy_api = ConsumptionApi[PredictionAccuracyParams, DetailedAccuracyListResponse](
    "predictionsDetailedAccuracy",
    query_function=get_detailed_accuracy,
    source="SetlistEntry",
    config=EgressConfig()
)
