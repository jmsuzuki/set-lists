"""
Transform function: Show -> Prediction
When a show is ingested, generate predictions for the next show using Goldilocks algorithm.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, UTC
import json
from app.ingest.models import Show, Prediction
from app.functions.goldilocks_v8_algorithm import goldilocks_v8_predictions


def show_to_prediction(show: Show) -> Optional[Prediction]:
    """
    Generate predictions for the next show when a show is ingested.
    
    This transform:
    1. Takes an ingested show
    2. Determines when/where the next show might be
    3. Generates predictions using Goldilocks v8
    4. Returns a Prediction record
    
    Args:
        show: The Show record that was just ingested
        
    Returns:
        Prediction record for the next show, or None if no prediction needed
    """
    print(f"🔮 Prediction transform triggered for show: {show.band_name} on {show.show_date}")
    
    # Parse the show date
    try:
        show_date = datetime.fromisoformat(show.show_date.replace('Z', '+00:00'))
    except:
        try:
            show_date = datetime.strptime(show.show_date, "%Y-%m-%d")
        except:
            print(f"  ❌ Could not parse show date: {show.show_date}")
            return None
    
    # Generate prediction for the next day (typical tour pattern)
    # In a real scenario, you'd look up the actual next show date from the tour schedule
    next_show_date = show_date + timedelta(days=1)
    next_show_date_str = next_show_date.strftime("%Y-%m-%d")
    
    # For now, use placeholder venue info (in production, look up from tour schedule)
    # TODO: Query the database for the actual next show's venue
    next_venue_name = "Unknown Venue"
    next_venue_city = "Unknown City"
    next_venue_state = "XX"
    
    print(f"  📅 Generating predictions for next show: {next_show_date_str}")
    
    try:
        # Check how many shows we have in the database for historical context
        import requests
        response = requests.get(
            f"http://localhost:4000/consumption/shows",
            params={"band_name": show.band_name, "limit": 1000}
        )
        
        historical_shows_count = 0
        if response.status_code == 200:
            historical_shows_count = len(response.json().get('shows', []))
        
        print(f"  📊 Historical shows available: {historical_shows_count}")
        
        # Minimum threshold for predictions (can be tuned)
        MIN_SHOWS_FOR_PREDICTIONS = 5
        
        if historical_shows_count < MIN_SHOWS_FOR_PREDICTIONS:
            print(f"  ℹ️ Need at least {MIN_SHOWS_FOR_PREDICTIONS} shows for predictions (have {historical_shows_count})")
            print(f"     Predictions will start after more shows are ingested")
            return None
        
        # Get historical songs for the algorithm
        historical_songs = []
        try:
            # Query for recent setlist entries
            import requests as req
            resp = req.get(
                f"http://localhost:4000/consumption/setlists",
                params={"band_name": show.band_name, "limit": 500}
            )
            if resp.status_code == 200:
                entries = resp.json().get('items', [])
                historical_songs = list(set(e['song_name'] for e in entries if e.get('song_name')))
                print(f"  📚 Found {len(historical_songs)} unique historical songs")
        except Exception as e:
            print(f"  ⚠️ Could not get historical songs: {e}")
        
        # Call Goldilocks v8 algorithm
        predictions = goldilocks_v8_predictions(
            show_date=next_show_date_str,
            venue_name=next_venue_name,
            venue_city=next_venue_city,
            venue_state=next_venue_state,
            band_name=show.band_name,
            historical_songs=historical_songs
        )
        
        if not predictions:
            print(f"  ⚠️ No predictions generated (algorithm may need more data)")
            return None
        
        # Add metadata about prediction quality based on data availability
        confidence_boost = min(1.0, historical_shows_count / 50)  # Max confidence at 50+ shows
        print(f"  📈 Confidence factor: {confidence_boost:.1%} (based on {historical_shows_count} shows)")
        
        # Create Prediction record with embedded setlist entries
        # Adjust confidence based on available data
        base_confidence = sum(p.get('confidence', 0.5) for p in predictions) / len(predictions)
        adjusted_confidence = base_confidence * confidence_boost
        
        prediction = Prediction(
            band_name=show.band_name,
            show_date=next_show_date_str,
            venue_name=next_venue_name,
            venue_city=next_venue_city,
            venue_state=next_venue_state,
            algorithm_name="goldilocks_v8",
            algorithm_version="8.0",
            generated_at=datetime.now(UTC).isoformat(),
            confidence_score=adjusted_confidence,
            data_through_date=show.show_date,
            total_shows_analyzed=historical_shows_count,
            setlist_entries=predictions  # Embed predictions as list of dicts
        )
        
        print(f"  ✅ Generated {len(predictions)} predictions for {next_show_date_str}")
        print(f"  📊 Average confidence: {prediction.confidence_score:.1%}")
        
        # Show top 3 predictions
        for i, pred in enumerate(predictions[:3], 1):
            print(f"     {i}. {pred['song_name']} ({pred.get('confidence', 0.5):.1%} confidence)")
        
        return prediction
        
    except Exception as e:
        print(f"  ❌ Error generating predictions: {e}")
        import traceback
        traceback.print_exc()
        return None


# Register the transform from Show to Prediction
from app.ingest.models import show_pipeline
from moose_lib import TransformConfig, DeadLetterQueue

# Dead letter queue for failed predictions
prediction_dlq = DeadLetterQueue[Show](name="ShowToPredictionDead")

# Register the transform
from app.ingest.models import show_pipeline, prediction_pipeline

show_pipeline.get_stream().add_transform(
    destination=prediction_pipeline.get_stream(),
    transformation=show_to_prediction,
    config=TransformConfig(
        dead_letter_queue=prediction_dlq
    )
)
