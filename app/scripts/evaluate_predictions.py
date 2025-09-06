#!/usr/bin/env python3
"""
Evaluate predictions against actual shows.
Compares predicted setlists with actual setlists to measure accuracy.
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from clickhouse_driver import Client


def get_predictions_for_date(client: Client, band_name: str, show_date: str) -> Dict[str, Any]:
    """
    Get all predictions for a specific band and date.
    """
    query = """
    SELECT 
        s.primary_key as show_id,
        s.prediction_algorithm,
        s.prediction_generated_at,
        se.song_name,
        se.set_type,
        se.set_position,
        se.prediction_confidence,
        se.prediction_reasoning,
        se.is_cover,
        se.original_artist
    FROM Show s
    JOIN SetlistEntry se ON se.show_id = s.primary_key
    WHERE s.band_name = %(band_name)s
      AND s.show_date = %(show_date)s
      AND s.is_prediction = true
    ORDER BY se.set_type, se.set_position
    """
    
    results = client.execute(
        query,
        {'band_name': band_name, 'show_date': show_date}
    )
    
    if not results:
        return {}
    
    # Group by algorithm
    predictions = {}
    for row in results:
        algorithm = row[1] or 'unknown'
        if algorithm not in predictions:
            predictions[algorithm] = {
                'show_id': row[0],
                'generated_at': row[2],
                'songs': []
            }
        
        predictions[algorithm]['songs'].append({
            'song_name': row[3],
            'set_type': row[4],
            'set_position': row[5],
            'confidence': row[6],
            'reasoning': row[7],
            'is_cover': row[8],
            'original_artist': row[9]
        })
    
    return predictions


def get_actual_setlist(client: Client, band_name: str, show_date: str) -> List[Dict[str, Any]]:
    """
    Get the actual setlist for a specific show.
    """
    query = """
    SELECT 
        se.song_name,
        se.set_type,
        se.set_position,
        se.is_jam,
        se.is_tease,
        se.is_partial,
        se.is_cover,
        se.original_artist,
        se.song_duration_minutes
    FROM Show s
    JOIN SetlistEntry se ON se.show_id = s.primary_key
    WHERE s.band_name = %(band_name)s
      AND s.show_date = %(show_date)s
      AND s.is_prediction = false
    ORDER BY se.set_type, se.set_position
    """
    
    results = client.execute(
        query,
        {'band_name': band_name, 'show_date': show_date}
    )
    
    songs = []
    for row in results:
        songs.append({
            'song_name': row[0],
            'set_type': row[1],
            'set_position': row[2],
            'is_jam': row[3],
            'is_tease': row[4],
            'is_partial': row[5],
            'is_cover': row[6],
            'original_artist': row[7],
            'duration_minutes': row[8]
        })
    
    return songs


def calculate_accuracy(predictions: List[Dict], actual: List[Dict]) -> Dict[str, Any]:
    """
    Calculate accuracy metrics for predictions.
    """
    if not predictions or not actual:
        return {
            'accuracy': 0.0,
            'correct_songs': 0,
            'total_predicted': len(predictions),
            'total_actual': len(actual),
            'details': []
        }
    
    # Extract song names
    predicted_songs = {p['song_name'].lower() for p in predictions}
    actual_songs = {a['song_name'].lower() for a in actual}
    
    # Calculate basic accuracy
    correct_songs = predicted_songs.intersection(actual_songs)
    accuracy = len(correct_songs) / len(predicted_songs) if predicted_songs else 0
    
    # Check position accuracy for correct songs
    position_accuracy = []
    for pred in predictions:
        if pred['song_name'].lower() in correct_songs:
            # Find actual position
            actual_match = next(
                (a for a in actual if a['song_name'].lower() == pred['song_name'].lower()),
                None
            )
            if actual_match:
                position_accuracy.append({
                    'song': pred['song_name'],
                    'predicted_set': pred['set_type'],
                    'actual_set': actual_match['set_type'],
                    'predicted_position': pred['set_position'],
                    'actual_position': actual_match['set_position'],
                    'set_match': pred['set_type'] == actual_match['set_type'],
                    'confidence': pred.get('confidence', 0)
                })
    
    # Calculate opener accuracy
    predicted_opener = next((p for p in predictions if p['set_position'] == 1 and p['set_type'] == 'Set 1'), None)
    actual_opener = next((a for a in actual if a['set_position'] == 1 and a['set_type'] == 'Set 1'), None)
    opener_correct = (
        predicted_opener and actual_opener and 
        predicted_opener['song_name'].lower() == actual_opener['song_name'].lower()
    )
    
    # Calculate encore accuracy
    predicted_encores = [p for p in predictions if p['set_type'] == 'Encore']
    actual_encores = [a for a in actual if a['set_type'] == 'Encore']
    encore_accuracy = 0
    if predicted_encores and actual_encores:
        predicted_encore_songs = {p['song_name'].lower() for p in predicted_encores}
        actual_encore_songs = {a['song_name'].lower() for a in actual_encores}
        encore_correct = predicted_encore_songs.intersection(actual_encore_songs)
        encore_accuracy = len(encore_correct) / len(predicted_encore_songs) if predicted_encore_songs else 0
    
    return {
        'overall_accuracy': accuracy,
        'correct_songs': len(correct_songs),
        'total_predicted': len(predictions),
        'total_actual': len(actual),
        'opener_correct': opener_correct,
        'encore_accuracy': encore_accuracy,
        'position_details': position_accuracy,
        'correct_song_names': list(correct_songs),
        'missed_songs': list(actual_songs - predicted_songs),
        'incorrect_predictions': list(predicted_songs - actual_songs)
    }


def evaluate_show(client: Client, band_name: str, show_date: str) -> Dict[str, Any]:
    """
    Evaluate all predictions for a specific show.
    """
    # Get predictions
    predictions = get_predictions_for_date(client, band_name, show_date)
    
    # Get actual setlist
    actual = get_actual_setlist(client, band_name, show_date)
    
    if not predictions:
        return {
            'show_date': show_date,
            'band_name': band_name,
            'status': 'no_predictions',
            'message': 'No predictions found for this show'
        }
    
    if not actual:
        return {
            'show_date': show_date,
            'band_name': band_name,
            'status': 'no_actual_show',
            'message': 'No actual show data found',
            'predictions': predictions
        }
    
    # Evaluate each algorithm's predictions
    evaluations = {}
    for algorithm, pred_data in predictions.items():
        accuracy = calculate_accuracy(pred_data['songs'], actual)
        evaluations[algorithm] = {
            'generated_at': pred_data['generated_at'],
            'accuracy_metrics': accuracy
        }
    
    return {
        'show_date': show_date,
        'band_name': band_name,
        'status': 'evaluated',
        'actual_songs': len(actual),
        'evaluations': evaluations
    }


def main():
    """
    Evaluate predictions for recent shows.
    """
    # Connect to ClickHouse
    client = Client(host='localhost', port=9000)
    
    # Example: Evaluate a specific show
    band_name = "Goose"
    show_date = datetime.now().strftime("%Y-%m-%d")  # Today's date
    
    print(f"🔍 Evaluating predictions for {band_name} on {show_date}")
    
    result = evaluate_show(client, band_name, show_date)
    
    if result['status'] == 'evaluated':
        print(f"\n✅ Evaluation complete!")
        print(f"Actual songs played: {result['actual_songs']}")
        
        for algorithm, eval_data in result['evaluations'].items():
            metrics = eval_data['accuracy_metrics']
            print(f"\n📊 Algorithm: {algorithm}")
            print(f"  Overall accuracy: {metrics['overall_accuracy']:.1%}")
            print(f"  Correct songs: {metrics['correct_songs']}/{metrics['total_predicted']}")
            print(f"  Opener correct: {'✅' if metrics['opener_correct'] else '❌'}")
            print(f"  Encore accuracy: {metrics['encore_accuracy']:.1%}")
            
            if metrics['correct_song_names']:
                print(f"  ✅ Correctly predicted: {', '.join(metrics['correct_song_names'])}")
            if metrics['incorrect_predictions']:
                print(f"  ❌ Incorrect predictions: {', '.join(metrics['incorrect_predictions'][:5])}")
            if metrics['missed_songs']:
                print(f"  ⚠️ Missed songs: {', '.join(metrics['missed_songs'][:5])}")
    else:
        print(f"\n⚠️ {result['message']}")
    
    # Get recent predictions summary
    print("\n📈 Recent Predictions Summary:")
    query = """
    SELECT 
        show_date,
        prediction_algorithm,
        COUNT(DISTINCT primary_key) as num_predictions
    FROM Show
    WHERE is_prediction = true
      AND band_name = %(band_name)s
      AND show_date >= today() - 7
    GROUP BY show_date, prediction_algorithm
    ORDER BY show_date DESC, prediction_algorithm
    """
    
    results = client.execute(query, {'band_name': band_name})
    for row in results:
        print(f"  {row[0]}: {row[1]} ({row[2]} predictions)")


if __name__ == "__main__":
    main()
