#!/usr/bin/env python3
"""
Goldilocks v7.0 Algorithm - Data-Driven Setlist Prediction Engine
==================================================================

Major improvements based on comprehensive historical analysis:
- Tier-based song predictability (High/Medium/Low frequency)
- Position-specific prediction models (opener/closer/encore)
- Venue preference incorporation
- Seasonal pattern recognition
- Song combination/sequence awareness
- Failed pattern elimination
- Increased prediction volume for better coverage

Performance Target: >40% hit rate, >30% coverage based on historical patterns
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime
from collections import Counter, defaultdict
import random

# Core data structures from historical analysis
CORE_ROTATION_SONGS = {
    # High Predictability Tier (>20% frequency)
    "Hot Tea": {"frequency": 0.267, "opener_rate": 0.015, "encore_rate": 0.079, "jam_vehicle": False},
    "Arcadia": {"frequency": 0.258, "opener_rate": 0.012, "encore_rate": 0.050, "jam_vehicle": False},
    "Tumble": {"frequency": 0.246, "opener_rate": 0.019, "encore_rate": 0.023, "jam_vehicle": False},
    "Madhuvan": {"frequency": 0.242, "opener_rate": 0.017, "encore_rate": 0.016, "jam_vehicle": True},
    "All I Need": {"frequency": 0.236, "opener_rate": 0.021, "encore_rate": 0.013, "jam_vehicle": True},
    "Yeti": {"frequency": 0.231, "opener_rate": 0.029, "encore_rate": 0.020, "jam_vehicle": False},
    "Echo of a Rose": {"frequency": 0.227, "opener_rate": 0.019, "encore_rate": 0.023, "jam_vehicle": True},
    "Drive": {"frequency": 0.219, "opener_rate": 0.039, "encore_rate": 0.016, "jam_vehicle": True}
}

MEDIUM_ROTATION_SONGS = {
    # Medium Predictability Tier (10-20% frequency) 
    "Flodown": {"frequency": 0.203, "opener_rate": 0.039, "encore_rate": 0.013, "jam_vehicle": False},
    "Creatures": {"frequency": 0.200, "opener_rate": 0.017, "encore_rate": 0.020, "jam_vehicle": True},
    "Time to Flee": {"frequency": 0.188, "opener_rate": 0.031, "encore_rate": 0.016, "jam_vehicle": False},
    "Butter Rum": {"frequency": 0.182, "opener_rate": 0.025, "encore_rate": 0.050, "jam_vehicle": False},
    "Slow Ready": {"frequency": 0.180, "opener_rate": 0.008, "encore_rate": 0.059, "jam_vehicle": False},
    "Arrow": {"frequency": 0.174, "opener_rate": 0.019, "encore_rate": 0.020, "jam_vehicle": True},
    "So Ready": {"frequency": 0.169, "opener_rate": 0.012, "encore_rate": 0.026, "jam_vehicle": False},
    "The Empress Of Organos": {"frequency": 0.169, "opener_rate": 0.010, "encore_rate": 0.040, "jam_vehicle": False},
    "Turned Clouds": {"frequency": 0.165, "opener_rate": 0.012, "encore_rate": 0.013, "jam_vehicle": False},
    "Jive I": {"frequency": 0.163, "opener_rate": 0.025, "encore_rate": 0.013, "jam_vehicle": True},
    "Wysteria Lane": {"frequency": 0.155, "opener_rate": 0.012, "encore_rate": 0.023, "jam_vehicle": True},
    "Rockdale": {"frequency": 0.149, "opener_rate": 0.015, "encore_rate": 0.020, "jam_vehicle": False},
    "Atlas Dogs": {"frequency": 0.147, "opener_rate": 0.029, "encore_rate": 0.016, "jam_vehicle": False},
    "Hungersite": {"frequency": 0.145, "opener_rate": 0.012, "encore_rate": 0.026, "jam_vehicle": True},
    "White Lights": {"frequency": 0.131, "opener_rate": 0.006, "encore_rate": 0.056, "jam_vehicle": False},
    "Doobie Song": {"frequency": 0.118, "opener_rate": 0.010, "encore_rate": 0.030, "jam_vehicle": True},
    "Into the Myst": {"frequency": 0.116, "opener_rate": 0.012, "encore_rate": 0.016, "jam_vehicle": True},
    "Elmeg The Wise": {"frequency": 0.110, "opener_rate": 0.010, "encore_rate": 0.013, "jam_vehicle": False}
}

# Strong song combination patterns
SONG_SEQUENCES = {
    "Seekers on the Ridge pt I": "Seekers on the Ridge pt II",
    "Jive I": "Jive Lee", 
    "Jive II": "Jive Lee",
    "Yeti": "Pumped Up Kicks",
    "I'm Alright": "Make The Move",
    "Creatures": "Shama Lama Ding Dong",
    "Borne": "Hungersite",
    "726": "Dripfield"
}

# Position-specific high-success songs
OPENER_SPECIALISTS = ["Flodown", "Drive", "Time to Flee", "Yeti", "Atlas Dogs", "Butter Rum", "Jive I"]
ENCORE_SPECIALISTS = ["Hot Tea", "Slow Ready", "White Lights", "Turn On Your Love Light", "Butter Rum", "Arcadia"]

# Seasonal preferences (stronger in certain seasons)
SEASONAL_BOOSTS = {
    "Spring": ["Arcadia", "Hot Tea", "Tumble"],
    "Summer": ["Madhuvan", "All I Need", "Arcadia"],
    "Fall": ["Hot Tea", "All I Need", "Madhuvan"],
    "Winter": ["Echo of a Rose", "Hot Tea", "Arcadia"]
}

# Venue type preferences
VENUE_TYPE_PREFERENCES = {
    "amphitheater": ["Creatures", "Wysteria Lane", "Echo of a Rose", "All I Need"],
    "theater": ["Hot Tea", "Slow Ready", "Arcadia", "Drive"],
    "festival": ["Madhuvan", "Jive I", "Time to Flee", "Flodown"],
    "club": ["Butter Rum", "Arrow", "Doobie Song", "Atlas Dogs"]
}

# ELIMINATED: Songs with proven 0% success rate
FAILED_PATTERNS = [
    "No Rain", "Shama Lama Ding Dong", "Pancakes"  # These consistently fail in predictions
]

def get_season(date_str: str) -> str:
    """Extract season from date string"""
    try:
        month = int(date_str[5:7])
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        else:
            return "Fall"
    except:
        return "Summer"  # Default

def classify_venue_type(venue_name: str) -> str:
    """Classify venue type based on name patterns"""
    venue_lower = venue_name.lower()
    
    if any(word in venue_lower for word in ["amphitheater", "amphitheatre", "pavilion", "shed"]):
        return "amphitheater"
    elif any(word in venue_lower for word in ["theatre", "theater", "hall", "center", "opera"]):
        return "theater" 
    elif any(word in venue_lower for word in ["festival", "grounds", "field", "park"]):
        return "festival"
    else:
        return "club"

def calculate_base_confidence(song: str, song_data: Dict) -> float:
    """Calculate base confidence from historical frequency"""
    frequency = song_data.get("frequency", 0.1)
    
    # Convert frequency to confidence with boost for high-frequency songs
    if frequency > 0.20:
        return min(95.0, 75.0 + (frequency * 50))  # High tier: 85-95%
    elif frequency > 0.10:
        return min(85.0, 65.0 + (frequency * 40))  # Medium tier: 69-85%
    else:
        return min(75.0, 45.0 + (frequency * 60))  # Low tier: 45-75%

def apply_contextual_boosts(confidence: float, song: str, context: Dict) -> float:
    """Apply contextual boosts based on venue, season, position"""
    boosted_confidence = confidence
    season = context.get("season", "Summer")
    venue_type = context.get("venue_type", "club")
    position_type = context.get("position_type", "rotation")
    
    # Seasonal boost
    if song in SEASONAL_BOOSTS.get(season, []):
        boosted_confidence += 8.0
        
    # Venue type boost  
    if song in VENUE_TYPE_PREFERENCES.get(venue_type, []):
        boosted_confidence += 6.0
        
    # Position-specific boosts
    if position_type == "opener" and song in OPENER_SPECIALISTS:
        boosted_confidence += 12.0
    elif position_type == "encore" and song in ENCORE_SPECIALISTS:
        boosted_confidence += 15.0
        
    # Weekend energy boost for high-energy songs
    if context.get("is_weekend", False):
        if song in ["Madhuvan", "Creatures", "Jive I", "Drive"]:
            boosted_confidence += 5.0
            
    return min(98.0, boosted_confidence)

def goldilocks_v7_predictions(
    show_date: str,
    venue_name: str,
    venue_city: str,
    venue_state: str,
    band_name: str = "Goose",
    historical_songs: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Goldilocks v7.0 - Comprehensive data-driven setlist prediction
    
    Based on analysis of 516 shows, 5,973 songs, 387 unique tracks
    Target: >40% hit rate, >30% coverage
    """
    
    predictions = []
    
    # Context analysis
    season = get_season(show_date)
    venue_type = classify_venue_type(venue_name)
    
    try:
        show_datetime = datetime.strptime(show_date, "%Y-%m-%d")
        is_weekend = show_datetime.weekday() >= 5
    except:
        is_weekend = False
    
    # Combine all songs with their data
    all_songs = {**CORE_ROTATION_SONGS, **MEDIUM_ROTATION_SONGS}
    
    # Generate opener predictions (2-3 songs)
    opener_context = {
        "season": season,
        "venue_type": venue_type, 
        "position_type": "opener",
        "is_weekend": is_weekend
    }
    
    opener_candidates = []
    for song in OPENER_SPECIALISTS:
        if song in all_songs and song not in FAILED_PATTERNS:
            base_conf = calculate_base_confidence(song, all_songs[song])
            final_conf = apply_contextual_boosts(base_conf, song, opener_context)
            opener_candidates.append((song, final_conf))
    
    # Select top 3 openers
    opener_candidates.sort(key=lambda x: x[1], reverse=True)
    for i, (song, confidence) in enumerate(opener_candidates[:3]):
        predictions.append({
            "song_name": song,
            "confidence": round(confidence, 1),
            "prediction_type": "opener",
            "reasoning": f"Top opener candidate #{i+1}. Historical opener rate: {all_songs[song].get('opener_rate', 0)*100:.1f}%. {season} season boost applied.",
            "algorithm_version": "goldilocks_v7.0",
            "show_date": show_date,
            "venue_name": venue_name
        })
    
    # Generate rotation predictions (8-10 songs from high-frequency pool)
    rotation_context = {
        "season": season,
        "venue_type": venue_type,
        "position_type": "rotation", 
        "is_weekend": is_weekend
    }
    
    rotation_candidates = []
    for song, data in all_songs.items():
        if song not in FAILED_PATTERNS and song not in [p["song_name"] for p in predictions]:
            base_conf = calculate_base_confidence(song, data)
            final_conf = apply_contextual_boosts(base_conf, song, rotation_context)
            rotation_candidates.append((song, final_conf))
    
    # Select top 10 rotation songs
    rotation_candidates.sort(key=lambda x: x[1], reverse=True)
    for i, (song, confidence) in enumerate(rotation_candidates[:10]):
        predictions.append({
            "song_name": song,
            "confidence": round(confidence, 1),
            "prediction_type": "rotation_candidate",
            "reasoning": f"High-frequency song (appears {all_songs[song]['frequency']*100:.1f}% of shows). Contextual boosts for {venue_type} venue in {season}.",
            "algorithm_version": "goldilocks_v7.0",
            "show_date": show_date,
            "venue_name": venue_name
        })
    
    # Generate encore predictions (2-3 songs)
    encore_context = {
        "season": season,
        "venue_type": venue_type,
        "position_type": "encore",
        "is_weekend": is_weekend
    }
    
    encore_candidates = []
    for song in ENCORE_SPECIALISTS:
        if song in all_songs and song not in FAILED_PATTERNS and song not in [p["song_name"] for p in predictions]:
            base_conf = calculate_base_confidence(song, all_songs[song])
            final_conf = apply_contextual_boosts(base_conf, song, encore_context)
            encore_candidates.append((song, final_conf))
    
    # Select top 3 encores
    encore_candidates.sort(key=lambda x: x[1], reverse=True)
    for i, (song, confidence) in enumerate(encore_candidates[:3]):
        predictions.append({
            "song_name": song,
            "confidence": round(confidence, 1),
            "prediction_type": "encore",
            "reasoning": f"Top encore candidate #{i+1}. Historical encore rate: {all_songs[song].get('encore_rate', 0)*100:.1f}%. Strong closer with {confidence:.1f}% confidence.",
            "algorithm_version": "goldilocks_v7.0",
            "show_date": show_date,
            "venue_name": venue_name
        })
    
    # Add sequence predictions if applicable
    for primary_song, follow_song in SONG_SEQUENCES.items():
        if primary_song in [p["song_name"] for p in predictions]:
            if follow_song not in [p["song_name"] for p in predictions] and follow_song in all_songs:
                predictions.append({
                    "song_name": follow_song,
                    "confidence": 92.0,
                    "prediction_type": "sequence_follow",
                    "reasoning": f"Strong sequence pattern: {primary_song} -> {follow_song} occurs frequently in historical data.",
                    "algorithm_version": "goldilocks_v7.0",
                    "show_date": show_date,
                    "venue_name": venue_name
                })
    
    # Sort by confidence and limit to top 16 predictions for optimal coverage
    predictions.sort(key=lambda x: x["confidence"], reverse=True)
    final_predictions = predictions[:16]
    
    # Add summary metadata
    total_confidence = sum(p["confidence"] for p in final_predictions)
    avg_confidence = total_confidence / len(final_predictions) if final_predictions else 0
    
    print(f"🎯 Goldilocks v7.0 Generated {len(final_predictions)} predictions")
    print(f"📊 Average confidence: {avg_confidence:.1f}%")
    print(f"🎪 Context: {season} season, {venue_type} venue")
    print(f"🎵 Prediction mix: {len([p for p in final_predictions if p['prediction_type'] == 'opener'])} openers, "
          f"{len([p for p in final_predictions if p['prediction_type'] == 'rotation_candidate'])} rotation, "
          f"{len([p for p in final_predictions if p['prediction_type'] == 'encore'])} encores")
    
    return final_predictions

# Standalone test function
if __name__ == "__main__":
    test_predictions = goldilocks_v7_predictions(
        show_date="2025-06-19",
        venue_name="Jacobs Pavilion at Nautica", 
        venue_city="Cleveland",
        venue_state="OH"
    )
    
    print(f"\n🎸 TEST PREDICTIONS FOR 6/19/2025:")
    for i, pred in enumerate(test_predictions, 1):
        print(f"  {i:2}. {pred['song_name']:25} | {pred['confidence']:5.1f}% | {pred['prediction_type']:15} | {pred['reasoning'][:80]}...") 