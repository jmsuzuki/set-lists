"""
Analytics API Endpoints

Advanced analytics endpoints for setlist patterns, trends, and statistics.
"""

from app.apis.analytics.song_frequency import song_frequency_api
from app.apis.analytics.set_patterns import set_patterns_api
from app.apis.analytics.venue_stats import venue_stats_api
from app.apis.analytics.tour_analytics import tour_stats_api, tour_comparison_api

__all__ = [
    'song_frequency_api',
    'set_patterns_api',
    'venue_stats_api',
    'tour_stats_api',
    'tour_comparison_api',
]
