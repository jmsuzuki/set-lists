"""
Logging consumers for monitoring and debugging data flow.
"""

from app.ingest.models.Show import show_pipeline, Show
from app.ingest.models.SetlistEntry import setlist_entry_pipeline, SetlistEntry
from app.ingest.models.Prediction import prediction_pipeline, Prediction
from app.ingest.models.PredictedSetlistEntry import predicted_setlist_entry_pipeline, PredictedSetlistEntry
from app.ingest.models.PredictionMetadata import prediction_metadata_pipeline, PredictionMetadata
from moose_lib import DeadLetterModel
from app.ingest.transforms.enrich_setlist_entry import setlist_dlq


def log_show_ingestion(show: Show):
    """Log when new shows are ingested"""
    print(f"🎵 New show ingested:")
    print(f"  Band: {show.band_name}")
    print(f"  Date: {show.show_date}")
    print(f"  Venue: {show.venue_name}")
    if show.venue_city:
        print(f"  Location: {show.venue_city}")
    print(f"  Verified: {'✓' if show.verified else '✗'}")
    print("---")


def log_setlist_entry(entry: SetlistEntry):
    """Log individual song performances"""
    print(f"🎶 Song performance:")
    print(f"  Song: {entry.song_name}")
    print(f"  Band: {entry.band_name}")
    print(f"  Date: {entry.show_date}")
    print(f"  Set: {entry.set_type} (Position {entry.set_position})")
    if entry.song_duration_minutes:
        print(f"  Duration: {entry.song_duration_minutes} minutes")
    if entry.is_jam:
        print(f"  🎸 JAM VERSION")
    if entry.transitions_into:
        print(f"  → Transitions into: {entry.transitions_into}")
    print("---")


def log_prediction(prediction: Prediction):
    """Log when new predictions are ingested"""
    print(f"🔮 New prediction ingested:")
    print(f"  Band: {prediction.band_name}")
    print(f"  Date: {prediction.show_date}")
    print(f"  Algorithm: {prediction.algorithm_name}")
    if prediction.confidence_score:
        print(f"  Confidence: {prediction.confidence_score:.1%}")
    if prediction.setlist_entries:
        print(f"  Songs predicted: {len(prediction.setlist_entries)}")
    print("---")


def log_predicted_entry(entry: PredictedSetlistEntry):
    """Log predicted song entries"""
    print(f"🔮 Predicted song:")
    print(f"  Song: {entry.song_name}")
    print(f"  Set: {entry.set_type} (Position {entry.set_position})")
    print(f"  Confidence: {entry.confidence:.1%}")
    if entry.is_cover and entry.original_artist:
        print(f"  Cover of: {entry.original_artist}")
    if entry.days_since_played:
        print(f"  Days since played: {entry.days_since_played}")
    print("---")


def log_prediction_metadata(metadata: PredictionMetadata):
    """Log prediction metadata"""
    print(f"📊 Prediction metadata:")
    print(f"  Band: {metadata.band_name}")
    print(f"  Date: {metadata.prediction_date}")
    print(f"  Algorithm: {metadata.algorithm_name} v{metadata.algorithm_version}")
    print(f"  Shows analyzed: {metadata.total_shows_analyzed}")
    print(f"  Total predictions: {metadata.total_predictions}")
    print("---")


def handle_failed_setlist_entries(dead_letter: DeadLetterModel[SetlistEntry]):
    """Handle failed setlist entry processing"""
    print(f"❌ Failed to process setlist entry:")
    print(f"  Error: {dead_letter.error}")
    print(f"  Timestamp: {dead_letter.timestamp}")
    
    # Try to extract the original entry for debugging
    try:
        original_entry = dead_letter.as_typed()
        print(f"  Original song: {original_entry.song_name}")
        print(f"  Original show: {original_entry.show_id}")
    except Exception as e:
        print(f"  Could not parse original entry: {e}")
    print("---")


# Register all consumers
show_pipeline.get_stream().add_consumer(log_show_ingestion)
setlist_entry_pipeline.get_stream().add_consumer(log_setlist_entry)
prediction_pipeline.get_stream().add_consumer(log_prediction)
predicted_setlist_entry_pipeline.get_stream().add_consumer(log_predicted_entry)
prediction_metadata_pipeline.get_stream().add_consumer(log_prediction_metadata)
setlist_dlq.add_consumer(handle_failed_setlist_entries)
