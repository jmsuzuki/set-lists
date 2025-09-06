"""
Transform function: Show -> PredictionMetadata
When a show is ingested, evaluate any predictions we had for this show.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, UTC
import requests
from app.ingest.models import Show, PredictionMetadata, Prediction, PredictedSetlistEntry


def calculate_prediction_accuracy(predicted_entries: List[PredictedSetlistEntry], actual_songs: List[str]) -> Dict[str, float]:
    """
    Calculate accuracy metrics for predictions vs actual setlist.
    
    Returns:
        Dictionary with accuracy metrics
    """
    if not predicted_entries or not actual_songs:
        return {
            "exact_matches": 0,
            "accuracy_rate": 0.0,
            "precision": 0.0,
            "recall": 0.0
        }
    
    # Extract predicted song names from PredictedSetlistEntry objects
    predicted_songs = [entry.song_name for entry in predicted_entries if entry.song_name]
    
    # Calculate metrics
    exact_matches = sum(1 for song in predicted_songs if song in actual_songs)
    
    # Precision: What % of our predictions were correct?
    precision = exact_matches / len(predicted_songs) if predicted_songs else 0.0
    
    # Recall: What % of the actual songs did we predict?
    recall = exact_matches / len(actual_songs) if actual_songs else 0.0
    
    # F1 Score
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {
        "exact_matches": exact_matches,
        "total_predictions": len(predicted_songs),
        "total_actual": len(actual_songs),
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
        "accuracy_rate": precision  # Using precision as main accuracy metric
    }


def show_to_prediction_evaluation(show: Show) -> Optional[PredictionMetadata]:
    """
    Evaluate predictions for this show when it's ingested.
    
    This transform:
    1. Looks for predictions we made for this show date
    2. Compares them to the actual setlist
    3. Generates evaluation metadata
    
    Args:
        show: The Show record that was just ingested
        
    Returns:
        PredictionMetadata with evaluation results, or None if no predictions found
    """
    print(f"📊 Evaluation transform triggered for show: {show.band_name} on {show.show_date}")
    
    try:
        # Query for predictions we made for this show
        # Using the consumption API to get predictions
        response = requests.get(
            f"http://localhost:4000/consumption/predictions",
            params={
                "band_name": show.band_name,
                "show_date": show.show_date,
                "limit": 1
            }
        )
        
        if response.status_code != 200 or not response.json().get('items'):
            print(f"  ℹ️ No predictions found for this show")
            return None
        
        prediction_data = response.json()['items'][0]
        
        # Get the actual setlist from the show
        actual_songs = []
        if show.setlist_entries:
            import json
            try:
                entries = json.loads(show.setlist_entries) if isinstance(show.setlist_entries, str) else show.setlist_entries
                actual_songs = [entry.get('song_name', '') for entry in entries]
            except:
                print(f"  ❌ Could not parse setlist entries")
                return None
        
        # Get predicted songs - convert from API response to objects
        predicted_entries = []
        response = requests.get(
            f"http://localhost:4000/consumption/predicted_entries",
            params={
                "band_name": show.band_name,
                "show_date": show.show_date,
                "limit": 50
            }
        )
        
        if response.status_code == 200:
            entries_data = response.json().get('items', [])
            # Convert dict responses to PredictedSetlistEntry objects
            for entry_dict in entries_data:
                try:
                    predicted_entry = PredictedSetlistEntry(**entry_dict)
                    predicted_entries.append(predicted_entry)
                except Exception as e:
                    print(f"  ⚠️ Could not parse predicted entry: {e}")
                    continue
        
        # Calculate accuracy metrics
        accuracy_metrics = calculate_prediction_accuracy(predicted_entries, actual_songs)
        
        # Generate insights
        insights = []
        
        if accuracy_metrics['precision'] > 0.3:
            insights.append(f"Good precision: {accuracy_metrics['precision']:.1%} of predictions were correct")
        elif accuracy_metrics['precision'] < 0.1:
            insights.append(f"Low precision: Only {accuracy_metrics['precision']:.1%} of predictions were correct")
        
        if accuracy_metrics['recall'] > 0.2:
            insights.append(f"Good coverage: Predicted {accuracy_metrics['recall']:.1%} of actual songs")
        elif accuracy_metrics['recall'] < 0.05:
            insights.append(f"Poor coverage: Only predicted {accuracy_metrics['recall']:.1%} of actual songs")
        
        # Find surprises (songs we didn't predict)
        predicted_song_names = {entry.song_name for entry in predicted_entries if entry.song_name}
        surprises = [song for song in actual_songs if song not in predicted_song_names]
        if surprises:
            insights.append(f"Surprise songs: {', '.join(surprises[:3])}")
        
        # Create evaluation metadata
        metadata = PredictionMetadata(
            band_name=show.band_name,
            prediction_date=show.show_date,
            algorithm_name=prediction_data.get('algorithm_name', 'unknown'),
            algorithm_version=prediction_data.get('algorithm_version', '0.0'),
            generated_at=prediction_data.get('generated_at', datetime.now(UTC).isoformat()),
            data_through_date=prediction_data.get('data_through_date', ''),
            total_shows_analyzed=prediction_data.get('total_shows_analyzed', 0),
            recent_shows_days=90,
            rotation_threshold=0.7,
            confidence_threshold=0.3,
            accuracy_metrics=accuracy_metrics,
            evaluation_notes="; ".join(insights) if insights else "No specific insights",
            created_at=datetime.now(UTC).isoformat()
        )
        
        print(f"  ✅ Evaluation complete:")
        print(f"     - Precision: {accuracy_metrics['precision']:.1%}")
        print(f"     - Recall: {accuracy_metrics['recall']:.1%}")
        print(f"     - F1 Score: {accuracy_metrics['f1_score']:.2f}")
        print(f"     - Exact matches: {accuracy_metrics['exact_matches']}/{accuracy_metrics['total_predictions']}")
        
        if insights:
            print(f"  💡 Insights:")
            for insight in insights[:3]:
                print(f"     - {insight}")
        
        return metadata
        
    except Exception as e:
        print(f"  ❌ Error evaluating predictions: {e}")
        import traceback
        traceback.print_exc()
        return None


# Register the transform from Show to PredictionMetadata
from app.ingest.models import show_pipeline, prediction_metadata_pipeline
from moose_lib import TransformConfig, DeadLetterQueue

# Dead letter queue for failed evaluations
evaluation_dlq = DeadLetterQueue[Show](name="ShowToEvaluationDead")

# Register the transform
show_pipeline.get_stream().add_transform(
    destination=prediction_metadata_pipeline.get_stream(),
    transformation=show_to_prediction_evaluation,
    config=TransformConfig(
        dead_letter_queue=evaluation_dlq
    )
)
