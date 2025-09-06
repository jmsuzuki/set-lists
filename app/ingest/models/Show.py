from moose_lib import IngestPipeline, IngestPipelineConfig, OlapConfig
from datetime import datetime, UTC
from typing import Optional, List, Dict, Any
from pydantic import Json
from pydantic import BaseModel, Field


class Show(BaseModel):
    """A concert/show with metadata"""
    band_name: str = Field(..., description="Name of the performing band")
    show_date: str = Field(..., description="Date of the show (YYYY-MM-DD format)")
    venue_name: str = Field(..., description="Name of the venue")
    venue_city: Optional[str] = Field(None, description="City where venue is located")
    venue_state: Optional[str] = Field(None, description="State/province where venue is located") 
    venue_country: Optional[str] = Field(None, description="Country where venue is located")
    tour_name: Optional[str] = Field(None, description="Name of the tour if applicable")
    show_notes: Optional[str] = Field(None, description="Any notes about the show")
    verified: bool = Field(False, description="Whether the setlist has been verified")
    source_url: Optional[str] = Field(None, description="URL where setlist data was sourced from")
    setlist_entries: Optional[str] = Field(None, description="JSON string of embedded setlist entries to be extracted via transform")
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


# Create ingest pipeline with ordering
show_pipeline = IngestPipeline[Show]("Show", IngestPipelineConfig(
    ingest=True,   # API endpoint for ingesting show data
    stream=True,   # Stream processing capabilities
    table=OlapConfig(
        order_by_fields=["band_name", "venue_name", "show_date",]
    )  # Store in ClickHouse with proper ordering (low to high cardinality)
))
