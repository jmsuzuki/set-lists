"""
Predictions API Endpoints

Endpoints for managing and analyzing setlist predictions.
"""

from app.apis.predictions.get_predictions import get_predictions_api
from app.apis.predictions.prediction_metadata import prediction_metadata_api
from app.apis.predictions.prediction_accuracy import prediction_accuracy_api, detailed_accuracy_api

__all__ = [
    'get_predictions_api',
    'prediction_metadata_api',
    'prediction_accuracy_api',
    'detailed_accuracy_api',
]
