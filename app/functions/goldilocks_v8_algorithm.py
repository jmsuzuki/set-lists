#!/usr/bin/env python3
"""
Goldilocks v8.0 Algorithm - Hybrid Predictability Engine
=========================================================

Key improvements over v7:
- Hybrid approach: Mix of high-frequency AND deep cut predictions
- Wild card component for rare songs (like v6's successful predictions)
- Curveball show detection and adaptation
- Better balance between safety and surprise

Target: Restore v6 performance levels while improving on deep cut prediction
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime
from collections import Counter, defaultdict
import random

# High Predictability Core (proven performers)
CORE_ROTATION_SONGS = {
    "Hot Tea": {"frequency": 0.267, "opener_rate": 0.015, "encore_rate": 0.079},
    "Arcadia": {"frequency": 0.258, "opener_rate": 0.012, "encore_rate": 0.050},
    "Tumble": {"frequency": 0.246, "opener_rate": 0.019, "encore_rate": 0.023},
    "Madhuvan": {"frequency": 0.242, "opener_rate": 0.017, "encore_rate": 0.016},
    "All I Need": {"frequency": 0.236, "opener_rate": 0.021, "encore_rate": 0.013},
    "Yeti": {"frequency": 0.231, "opener_rate": 0.029, "encore_rate": 0.020},
    "Echo of a Rose": {"frequency": 0.227, "opener_rate": 0.019, "encore_rate": 0.023},
    "Drive": {"frequency": 0.219, "opener_rate": 0.039, "encore_rate": 0.016}
}

# Medium Predictability (rotation staples)
MEDIUM_ROTATION_SONGS = {
    "Flodown": {"frequency": 0.203, "opener_rate": 0.039, "encore_rate": 0.013},
    "Creatures": {"frequency": 0.200, "opener_rate": 0.017, "encore_rate": 0.020},
    "Time to Flee": {"frequency": 0.188, "opener_rate": 0.031, "encore_rate": 0.016},
    "Butter Rum": {"frequency": 0.182, "opener_rate": 0.025, "encore_rate": 0.050},
    "Slow Ready": {"frequency": 0.180, "opener_rate": 0.008, "encore_rate": 0.059},
    "Arrow": {"frequency": 0.174, "opener_rate": 0.019, "encore_rate": 0.020},
    "Atlas Dogs": {"frequency": 0.147, "opener_rate": 0.029, "encore_rate": 0.016},
    "Hungersite": {"frequency": 0.145, "opener_rate": 0.012, "encore_rate": 0.026},
    "Into the Myst": {"frequency": 0.140, "opener_rate": 0.012, "encore_rate": 0.016},
    "White Lights": {"frequency": 0.131, "opener_rate": 0.006, "encore_rate": 0.056}
}

# Wild Card Pool (v6 success stories + deep cuts that occasionally appear)
WILD_CARD_SONGS = {
    # v6 got these right when v7 failed
    "Doobie Song": {"frequency": 0.047, "context": "surprising_encore"},
    "Hungersite": {"frequency": 0.145, "context": "jam_vehicle"},  # Actually medium freq but acts like wild card
    
    # Deep cuts that appear in rotation
    "Honeybee": {"frequency": 0.078, "context": "opener_surprise"},
    "Electric Avenue": {"frequency": 0.039, "context": "fun_cover"},
    "Elmeg The Wise": {"frequency": 0.110, "context": "storytelling"},
    "Hot Love & The Lazy Poet": {"frequency": 0.056, "context": "rare_gem"},
    "Mr. Action": {"frequency": 0.070, "context": "energy_boost"},
    "The Labyrinth": {"frequency": 0.021, "context": "experimental"},
    "Mas Que Nada": {"frequency": 0.043, "context": "latin_flavor"},
    "Torero": {"frequency": 0.008, "context": "very_rare"},
    
    # Other proven wild cards
    "Shama Lama Ding Dong": {"frequency": 0.035, "context": "fun_cover"},
    "Pumped Up Kicks": {"frequency": 0.058, "context": "crowd_pleaser"},
    "Turn On Your Love Light": {"frequency": 0.062, "context": "encore_jam"},
    "Elizabeth": {"frequency": 0.062, "context": "emotional_moment"},
    "Rosewood Heart": {"frequency": 0.054, "context": "beautiful_ballad"}
}

# Position specialists
OPENER_SPECIALISTS = ["Flodown", "Drive", "Time to Flee", "Yeti", "Atlas Dogs", "Honeybee"]
ENCORE_SPECIALISTS = ["Hot Tea", "Slow Ready", "White Lights", "Turn On Your Love Light", "Butter Rum", "Doobie Song"]

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
        return "Summer"

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

def detect_curveball_potential(show_date: str, venue_name: str) -> float:
    """Detect likelihood of a curveball/deep cuts show"""
    curveball_score = 0.0
    
    # Summer amphitheater shows tend to be more adventurous
    season = get_season(show_date)
    venue_type = classify_venue_type(venue_name)
    
    if season == "Summer" and venue_type == "amphitheater":
        curveball_score += 0.3
    
    # Smaller venues allow for more experimentation
    if venue_type in ["club", "theater"]:
        curveball_score += 0.2
        
    # Weekend shows may be more adventurous
    try:
        show_datetime = datetime.strptime(show_date, "%Y-%m-%d")
        if show_datetime.weekday() >= 5:  # Weekend
            curveball_score += 0.1
    except:
        pass
    
    # Special venue names that suggest experimentation
    venue_lower = venue_name.lower()
    if any(word in venue_lower for word in ["community", "rec", "small", "intimate"]):
        curveball_score += 0.2
    
    return min(1.0, curveball_score)

def goldilocks_v8_predictions(
    show_date: str,
    venue_name: str,
    venue_city: str,
    venue_state: str,
    band_name: str = "Goose",
    historical_songs: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Goldilocks v8.0 - Hybrid Predictability Engine
    
    Strategy:
    - 40% high-frequency reliable songs (core rotation)
    - 35% medium-frequency rotation songs  
    - 25% wild card / deep cut predictions
    """
    
    predictions = []
    
    # Context analysis
    season = get_season(show_date)
    venue_type = classify_venue_type(venue_name)
    curveball_potential = detect_curveball_potential(show_date, venue_name)
    
    try:
        show_datetime = datetime.strptime(show_date, "%Y-%m-%d")
        is_weekend = show_datetime.weekday() >= 5
    except:
        is_weekend = False
    
    print(f"🎲 Curveball potential: {curveball_potential:.1%}")
    
    # CORE PREDICTIONS (6-7 songs) - High reliability
    core_candidates = []
    for song, data in CORE_ROTATION_SONGS.items():
        confidence = 85.0 + (data["frequency"] * 30)  # 85-95% range
        
        # Seasonal boost
        if season == "Summer" and song in ["Madhuvan", "All I Need"]:
            confidence += 5.0
        elif season == "Fall" and song in ["Hot Tea", "Arcadia"]:
            confidence += 5.0
            
        core_candidates.append((song, confidence))
    
    core_candidates.sort(key=lambda x: x[1], reverse=True)
    for i, (song, confidence) in enumerate(core_candidates[:7]):
        predictions.append({
            "song_name": song,
            "confidence": round(confidence, 1),
            "prediction_type": "core_rotation",
            "reasoning": f"High-frequency core song ({CORE_ROTATION_SONGS[song]['frequency']*100:.1f}% of shows). Reliable prediction.",
            "algorithm_version": "goldilocks_v8.0",
            "show_date": show_date,
            "venue_name": venue_name
        })
    
    # MEDIUM ROTATION PREDICTIONS (4-5 songs)
    medium_candidates = []
    for song, data in MEDIUM_ROTATION_SONGS.items():
        if song not in [p["song_name"] for p in predictions]:
            confidence = 70.0 + (data["frequency"] * 40)  # 70-85% range
            medium_candidates.append((song, confidence))
    
    medium_candidates.sort(key=lambda x: x[1], reverse=True)
    for i, (song, confidence) in enumerate(medium_candidates[:5]):
        predictions.append({
            "song_name": song,
            "confidence": round(confidence, 1),
            "prediction_type": "medium_rotation",
            "reasoning": f"Medium-frequency rotation song ({MEDIUM_ROTATION_SONGS[song]['frequency']*100:.1f}% of shows).",
            "algorithm_version": "goldilocks_v8.0",
            "show_date": show_date,
            "venue_name": venue_name
        })
    
    # WILD CARD PREDICTIONS (3-4 songs) - The v6 secret sauce
    wild_card_count = 3 + int(curveball_potential * 2)  # 3-5 wild cards based on curveball potential
    
    wild_candidates = []
    for song, data in WILD_CARD_SONGS.items():
        if song not in [p["song_name"] for p in predictions]:
            # Base confidence for wild cards
            base_confidence = 55.0 + (data["frequency"] * 100)  # 55-75% range
            
            # Boost for curveball shows
            curveball_boost = curveball_potential * 15  # Up to +15% for high curveball potential
            confidence = base_confidence + curveball_boost
            
            # Context-specific boosts
            context = data.get("context", "")
            if context == "surprising_encore" and venue_type in ["club", "theater"]:
                confidence += 10
            elif context == "opener_surprise" and curveball_potential > 0.5:
                confidence += 8
            elif context == "jam_vehicle" and venue_type == "amphitheater":
                confidence += 6
                
            wild_candidates.append((song, confidence))
    
    # Include some truly random deep cuts for maximum curveball coverage
    if curveball_potential > 0.6:
        print(f"🎪 High curveball potential detected - adding extra wild cards")
    
    wild_candidates.sort(key=lambda x: x[1], reverse=True)
    for i, (song, confidence) in enumerate(wild_candidates[:wild_card_count]):
        predictions.append({
            "song_name": song,
            "confidence": round(confidence, 1),
            "prediction_type": "wild_card",
            "reasoning": f"Wild card prediction ({WILD_CARD_SONGS[song]['frequency']*100:.1f}% frequency). Curveball potential: {curveball_potential:.1%}.",
            "algorithm_version": "goldilocks_v8.0",
            "show_date": show_date,
            "venue_name": venue_name
        })
    
    # POSITION-SPECIFIC PREDICTIONS
    # Add opener specialist if not already included
    for opener in OPENER_SPECIALISTS:
        if opener not in [p["song_name"] for p in predictions]:
            if opener in CORE_ROTATION_SONGS:
                confidence = 88.0
            elif opener in MEDIUM_ROTATION_SONGS:
                confidence = 78.0
            else:
                confidence = 65.0
                
            predictions.append({
                "song_name": opener,
                "confidence": confidence,
                "prediction_type": "opener_specialist",
                "reasoning": f"Opener specialist song. Strong opener potential.",
                "algorithm_version": "goldilocks_v8.0", 
                "show_date": show_date,
                "venue_name": venue_name
            })
            break  # Only add one opener specialist
    
    # Add encore specialist if not already included
    for encore in ENCORE_SPECIALISTS:
        if encore not in [p["song_name"] for p in predictions]:
            if encore in CORE_ROTATION_SONGS:
                confidence = 85.0
            elif encore in MEDIUM_ROTATION_SONGS:
                confidence = 82.0
            else:
                confidence = 70.0
                
            predictions.append({
                "song_name": encore,
                "confidence": confidence,
                "prediction_type": "encore_specialist",
                "reasoning": f"Encore specialist song. Strong closer potential.",
                "algorithm_version": "goldilocks_v8.0",
                "show_date": show_date,
                "venue_name": venue_name
            })
            break  # Only add one encore specialist
    
    # Sort by confidence and limit to top 16
    predictions.sort(key=lambda x: x["confidence"], reverse=True)
    final_predictions = predictions[:16]
    
    # Summary
    core_count = len([p for p in final_predictions if p["prediction_type"] == "core_rotation"])
    medium_count = len([p for p in final_predictions if p["prediction_type"] == "medium_rotation"])
    wild_count = len([p for p in final_predictions if p["prediction_type"] == "wild_card"])
    
    avg_confidence = sum(p["confidence"] for p in final_predictions) / len(final_predictions)
    
    print(f"🎯 Goldilocks v8.0 Generated {len(final_predictions)} predictions")
    print(f"📊 Strategy: {core_count} core, {medium_count} medium, {wild_count} wild cards")
    print(f"🎲 Curveball adaptation: {curveball_potential:.1%}")
    print(f"📈 Average confidence: {avg_confidence:.1f}%")
    
    return final_predictions

# Test function
if __name__ == "__main__":
    test_predictions = goldilocks_v8_predictions(
        show_date="2025-06-19",
        venue_name="Jacobs Pavilion at Nautica",
        venue_city="Cleveland",
        venue_state="OH"
    )
    
    print(f"\n🎸 v8 TEST PREDICTIONS FOR 6/19/2025:")
    for i, pred in enumerate(test_predictions, 1):
        print(f"  {i:2}. {pred['song_name']:25} | {pred['confidence']:5.1f}% | {pred['prediction_type']:15} | {pred['reasoning'][:60]}...") 