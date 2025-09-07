from moose_lib import Key, IngestPipeline, IngestPipelineConfig, OlapConfig
from datetime import datetime, UTC
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


class SetType(str, Enum):
    SET_1 = "Set 1"
    SET_2 = "Set 2" 
    SET_3 = "Set 3"
    ONE_SET = "One Set"
    ENCORE = "Encore"
    SOUNDCHECK = "Soundcheck"
    OTHER = "Other"


class SetlistEntry(BaseModel):
    """Individual song performance within a show"""
    # Key fields (used for ordering in ClickHouse)
    band_name: Key[str] = Field(..., description="Name of the performing band")
    song_name: Key[str] = Field(..., description="Name of the song")
    show_date: Key[str] = Field(..., description="Date of the show (YYYY-MM-DD format)")
    
    # Regular fields
    venue_name: str = Field(..., description="Name of the venue")
    tour_name: Optional[str] = Field(None, description="Name of the tour if applicable")
    set_type: SetType = Field(..., description="Which set this song was played in")
    set_position: int = Field(..., description="Position within the set (1, 2, 3, etc.)")
    song_duration_minutes: Optional[float] = Field(None, description="Duration of the song in minutes")
    transitions_into: Optional[str] = Field(None, description="Song this transitions into (for > symbols)")
    transitions_from: Optional[str] = Field(None, description="Song this transitions from") 
    is_jam: bool = Field(False, description="Whether this was noted as a jam version")
    is_tease: bool = Field(False, description="Whether this was a tease of the song")
    is_partial: bool = Field(False, description="Whether this was a partial/incomplete version")
    is_cover: bool = Field(False, description="Whether this is a cover song")
    original_artist: Optional[str] = Field(None, description="Original artist if this is a cover")
    performance_notes: Optional[str] = Field(None, description="Notes about this specific performance")
    guest_musicians: Optional[List[str]] = Field(None, description="Any guest musicians for this song")
    is_enriched: bool = Field(False, description="Whether this entry has been processed by the enrichment transform")
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


# Create ingest pipeline with ordering
setlist_entry_pipeline = IngestPipeline[SetlistEntry]("SetlistEntry", IngestPipelineConfig(
    ingest=False,   # No direct ingestion - created via transform from Show
    stream=True,    # Stream processing capabilities  
    table=OlapConfig(
        order_by_fields=["band_name", "song_name", "show_date",]
    )  # Store in ClickHouse with proper ordering (low to high cardinality)
))
