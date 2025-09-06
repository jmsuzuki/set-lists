"""
API Endpoints for Setlist Analytics and Predictions

This module provides various API endpoints organized by category:

Core APIs:
- shows: Query show/concert information
- setlists: Get setlist entries for specific shows
- song_stats: Analyze song performance statistics

Analytics APIs (app.apis.analytics):
- song_frequency: Analyze play frequency and trends
- set_patterns: Common patterns (openers, closers, encores)
- venue_stats: Venue statistics and history
- tour_analytics: Tour statistics and comparisons

Predictions APIs (app.apis.predictions):
- get_predictions: Retrieve predicted setlists
- prediction_metadata: Metadata about prediction runs
- prediction_accuracy: Evaluate prediction accuracy

Each API is self-contained with its own models and query logic.
"""

# Import core APIs
from app.apis.shows import shows_api
from app.apis.setlists import setlists_api
from app.apis.song_stats import song_stats_api
from app.apis.predictions.list_all_predictions import listAllPredictions_api

# Import analytics APIs
from app.apis.analytics import (
    song_frequency_api,
    set_patterns_api,
    venue_stats_api,
    tour_stats_api,
    tour_comparison_api
)

# Import predictions APIs
from app.apis.predictions import (
    get_predictions_api,
    prediction_metadata_api,
    prediction_accuracy_api,
    detailed_accuracy_api
)

__all__ = [
    # Core APIs
    'shows_api',
    'setlists_api',
    'song_stats_api',
    'listAllPredictions_api',
    
    # Analytics APIs
    'song_frequency_api',
    'set_patterns_api',
    'venue_stats_api',
    'tour_stats_api',
    'tour_comparison_api',
    
    # Predictions APIs
    'get_predictions_api',
    'prediction_metadata_api',
    'prediction_accuracy_api',
    'detailed_accuracy_api',
]
