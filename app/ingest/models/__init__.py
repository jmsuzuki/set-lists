"""
Data models for the setlist prediction system.
"""

from app.ingest.models.Show import Show, show_pipeline
from app.ingest.models.SetlistEntry import SetlistEntry, SetType, setlist_entry_pipeline
from app.ingest.models.Prediction import Prediction, prediction_pipeline
from app.ingest.models.PredictedSetlistEntry import PredictedSetlistEntry, predicted_setlist_entry_pipeline
from app.ingest.models.PredictionMetadata import PredictionMetadata, PredictionType, prediction_metadata_pipeline

__all__ = [
    'Show',
    'show_pipeline',
    'SetlistEntry',
    'SetType',
    'setlist_entry_pipeline',
    'Prediction',
    'prediction_pipeline',
    'PredictedSetlistEntry',
    'predicted_setlist_entry_pipeline',
    'PredictionMetadata',
    'PredictionType',
    'prediction_metadata_pipeline',
]
