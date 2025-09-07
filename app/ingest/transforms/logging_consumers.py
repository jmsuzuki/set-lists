"""
Logging consumers for monitoring and debugging data flow.
Using hybrid DMv2 approach with pipelines.
"""

from app.ingest.models import (
    Show, show_pipeline,
    SetlistEntry, setlist_entry_pipeline,
    Prediction, prediction_pipeline,
    PredictedSetlistEntry, predicted_setlist_entry_pipeline,
    PredictionMetadata, prediction_metadata_pipeline
)


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
    print(f"  Show: {entry.show_date} at {entry.venue_name}")
    print(f"  Set: {entry.set_type} - Position {entry.set_position}")
    if entry.is_cover:
        print(f"  Cover of: {entry.original_artist}")
    if entry.is_jam:
        print(f"  🎸 JAM VERSION")
    print("---")


def log_prediction(prediction: Prediction):
    """Log when predictions are generated"""
    print(f"🔮 New prediction generated:")
    print(f"  For: {prediction.band_name} on {prediction.show_date}")
    print(f"  Algorithm: {prediction.algorithm_name} v{prediction.algorithm_version}")
    print(f"  Confidence: {prediction.confidence_score:.2f}" if prediction.confidence_score else "  Confidence: N/A")
    print(f"  Based on: {prediction.total_shows_analyzed} shows")
    if prediction.setlist_entries:
        try:
            import json
            entries = prediction.setlist_entries if isinstance(prediction.setlist_entries, list) else json.loads(prediction.setlist_entries)
            print(f"  Predicted {len(entries)} songs")
        except:
            print(f"  Predicted songs: [unable to parse]")
    print("---")


def log_predicted_entry(entry: PredictedSetlistEntry):
    """Log individual predicted songs"""
    print(f"🎯 Predicted song:")
    print(f"  Song: {entry.song_name}")
    print(f"  For: {entry.band_name} on {entry.show_date}")
    print(f"  Position: {entry.set_type} - {entry.set_position}")
    print(f"  Confidence: {entry.confidence:.2%}")
    if entry.prediction_type:
        print(f"  Type: {entry.prediction_type}")
    if entry.reasoning:
        print(f"  Reasoning: {', '.join(entry.reasoning[:2])}")
    print("---")


def log_prediction_metadata(metadata: PredictionMetadata):
    """Log prediction evaluation metadata"""
    print(f"📊 Prediction metadata:")
    print(f"  Date: {metadata.prediction_date}")
    print(f"  Algorithm: {metadata.algorithm_name} v{metadata.algorithm_version}")
    print(f"  Analyzed: {metadata.total_shows_analyzed} shows")
    print(f"  Generated: {metadata.total_predictions} predictions")
    print("---")


# Register consumers using pipeline approach (hybrid DMv2)
show_pipeline.get_stream().add_consumer(log_show_ingestion)
setlist_entry_pipeline.get_stream().add_consumer(log_setlist_entry)
prediction_pipeline.get_stream().add_consumer(log_prediction)
predicted_setlist_entry_pipeline.get_stream().add_consumer(log_predicted_entry)
prediction_metadata_pipeline.get_stream().add_consumer(log_prediction_metadata)