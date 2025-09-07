from moose_lib import Key, IngestPipeline, IngestPipelineConfig, OlapConfig
from datetime import datetime, UTC
from typing import Optional, List
from pydantic import BaseModel, Field
from app.ingest.models.SetlistEntry import SetType


class PredictedSetlistEntry(BaseModel):
    """Individual predicted song performance within a predicted show"""
    # Key fields (used for ordering in ClickHouse)
    band_name: Key[str] = Field(..., description="Name of the band")
    algorithm_name: Key[str] = Field(..., description="Algorithm that made this prediction")
    song_name: Key[str] = Field(..., description="Name of the predicted song")
    show_date: Key[str] = Field(..., description="Date of the predicted show (YYYY-MM-DD format)")
    
    # Position in setlist
    set_type: SetType = Field(..., description="Which set this song is predicted for")
    set_position: int = Field(..., description="Position within the set (1, 2, 3, etc.)")
    
    # Song information
    is_cover: bool = Field(False, description="Whether this is predicted to be a cover")
    original_artist: Optional[str] = Field(None, description="Original artist if predicted as cover")
    
    # Prediction confidence and reasoning
    confidence: float = Field(..., description="Confidence score for this specific song prediction (0.0 to 1.0)")
    reasoning: Optional[List[str]] = Field(None, description="Reasoning for this prediction")
    prediction_type: Optional[str] = Field(None, description="Type of prediction (opener, encore, rotation_candidate, etc.)")
    
    # Historical context used for prediction
    last_played: Optional[str] = Field(None, description="When the song was last played")
    days_since_played: Optional[int] = Field(None, description="Days since song was last played")
    total_plays: Optional[int] = Field(None, description="Total historical plays of this song")
    avg_position: Optional[float] = Field(None, description="Average historical position in set")
    
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


# Create ingest pipeline with ordering
predicted_setlist_entry_pipeline = IngestPipeline[PredictedSetlistEntry]("PredictedSetlistEntry", IngestPipelineConfig(
    ingest=False,   # No direct ingestion - created via transform from Prediction
    stream=True,    # Stream processing capabilities
    table=OlapConfig(
        order_by_fields=["band_name", "algorithm_name", "song_name", "show_date"]
    )  # Store in ClickHouse with proper ordering
))
