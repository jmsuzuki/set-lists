from moose_lib import Key, IngestPipeline, IngestPipelineConfig, OlapConfig
from datetime import datetime, UTC
from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum


class PredictionType(str, Enum):
    OPENER = "opener"
    SET1_CLOSER = "set1_closer"
    SET2_CLOSER = "set2_closer"
    ENCORE = "encore"
    ROTATION_CANDIDATE = "rotation_candidate"


class PredictionMetadata(BaseModel):
    """Metadata about a prediction run for tracking and analysis"""
    # Key fields (used for ordering in ClickHouse)
    band_name: Key[str] = Field(..., description="Name of the band being predicted for")
    algorithm_name: Key[str] = Field(..., description="Algorithm used (e.g., 'goldilocks_v8')")
    prediction_date: Key[str] = Field(..., description="Date the prediction was made for (YYYY-MM-DD)")
    generated_at: Key[str] = Field(..., description="When the prediction was generated (ISO format)")
    
    # Algorithm metadata
    algorithm_version: str = Field(..., description="Version of the algorithm")
    data_through_date: str = Field(..., description="Latest show date in the analysis dataset")
    total_shows_analyzed: int = Field(..., description="Total number of shows analyzed for prediction")
    recent_shows_days: int = Field(90, description="Number of days of recent shows analyzed")
    rotation_threshold: float = Field(0.7, description="Threshold for rotation candidates")
    confidence_threshold: float = Field(0.5, description="Minimum confidence threshold")
    cover_percentage: Optional[float] = Field(None, description="Recent cover percentage used in analysis")
    avg_songs_per_show: Optional[float] = Field(None, description="Average songs per show from analysis")
    total_predictions: int = Field(..., description="Total number of predictions generated")
    prediction_notes: Optional[str] = Field(None, description="Any notes about this prediction run")
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


# Create ingest pipeline with ordering
prediction_metadata_pipeline = IngestPipeline[PredictionMetadata]("PredictionMetadata", IngestPipelineConfig(
    ingest=False,   # No direct ingestion - created via transform from Prediction
    stream=True,    # Stream processing capabilities
    table=OlapConfig(
        order_by_fields=["band_name", "algorithm_name", "prediction_date", "generated_at"]
    )  # Store in ClickHouse with proper ordering
))
