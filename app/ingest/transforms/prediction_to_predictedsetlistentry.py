"""
Transform function: Prediction -> PredictedSetlistEntry
Extracts embedded predicted setlist entries from Prediction records.
"""

from typing import List, Dict, Any
from datetime import datetime


def prediction_to_predictedsetlistentry(prediction: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform a Prediction record into PredictedSetlistEntry records.
    Extracts embedded setlist_entries and creates individual records.
    
    Args:
        prediction: The Prediction record containing embedded setlist entries
        
    Returns:
        List of PredictedSetlistEntry records
    """
    # If no embedded entries, return empty list
    if not prediction.get('setlist_entries'):
        return []
    
    entries = []
    prediction_id = prediction.get('primary_key', '')
    band_name = prediction.get('band_name', '')
    show_date = prediction.get('show_date', '')
    
    # Process each embedded entry
    for entry_data in prediction.get('setlist_entries', []):
        # Create the PredictedSetlistEntry record
        entry = {
            'primary_key': entry_data.get('primary_key', f"{prediction_id}_{entry_data.get('set_type', 'unknown')}_{entry_data.get('set_position', 0):03d}"),
            'prediction_id': prediction_id,
            'band_name': band_name,
            'show_date': show_date,
            'set_type': entry_data.get('set_type', 'Set 1'),
            'set_position': entry_data.get('set_position', 1),
            'song_name': entry_data.get('song_name', ''),
            'is_cover': entry_data.get('is_cover', False),
            'original_artist': entry_data.get('original_artist'),
            'confidence': entry_data.get('confidence', 0.5),
            'reasoning': entry_data.get('reasoning'),
            'prediction_type': entry_data.get('prediction_type'),
            'last_played': entry_data.get('last_played'),
            'days_since_played': entry_data.get('days_since_played'),
            'total_plays': entry_data.get('total_plays'),
            'avg_position': entry_data.get('avg_position'),
            'created_at': datetime.now().isoformat()
        }
        
        entries.append(entry)
    
    # Log the transformation
    if entries:
        print(f"🔮 Extracted {len(entries)} predicted songs from prediction {prediction_id}")
    
    return entries
