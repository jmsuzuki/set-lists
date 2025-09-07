from moose_lib import Key, IngestPipeline, IngestPipelineConfig, OlapConfig
from datetime import datetime, UTC
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class Prediction(BaseModel):
    """A predicted show/setlist"""
    # Key fields (used for ordering in ClickHouse)
    band_name: Key[str] = Field(..., description="Name of the band being predicted for")
    algorithm_name: Key[str] = Field(..., description="Algorithm used for prediction (e.g., 'goldilocks_v8')")
    show_date: Key[str] = Field(..., description="Date of the predicted show (YYYY-MM-DD format)")
    generated_at: Key[str] = Field(default_factory=lambda: datetime.now(UTC).isoformat(), description="When the prediction was generated")
    
    # Venue information
    venue_name: Optional[str] = Field("TBD", description="Venue name if known, otherwise TBD")
    venue_city: Optional[str] = Field(None, description="City if known")
    venue_state: Optional[str] = Field(None, description="State if known")
    venue_country: Optional[str] = Field(None, description="Country if known")
    tour_name: Optional[str] = Field(None, description="Tour name if applicable")
    
    # Prediction-specific fields
    algorithm_version: Optional[str] = Field(None, description="Version of the algorithm")
    confidence_score: Optional[float] = Field(None, description="Overall confidence in the prediction (0.0 to 1.0)")
    
    # Analysis metadata
    data_through_date: Optional[str] = Field(None, description="Latest show date in the analysis dataset")
    total_shows_analyzed: Optional[int] = Field(None, description="Number of shows analyzed to make prediction")
    
    # Embedded predicted setlist entries
    setlist_entries: Optional[List[Dict[str, Any]]] = Field(None, description="Embedded predicted setlist entries")
    
    # Notes and metadata
    prediction_notes: Optional[str] = Field(None, description="Any notes about this prediction")
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


# Create ingest pipeline with ordering
prediction_pipeline = IngestPipeline[Prediction]("Prediction", IngestPipelineConfig(
    ingest=True,   # API endpoint for ingesting predictions
    stream=True,   # Stream processing capabilities
    table=OlapConfig(
        order_by_fields=["band_name", "algorithm_name", "show_date", "generated_at"]
    )  # Store in ClickHouse with proper ordering
))
