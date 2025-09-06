"""
Transform functions and consumers for data processing.
"""

# Import transforms to ensure they're registered
import app.ingest.transforms.enrich_setlist_entry
import app.ingest.transforms.logging_consumers
# Just import the module to register the transform, don't import the function
import app.ingest.transforms.show_to_setlistentry
import app.ingest.transforms.prediction_to_predictedsetlistentry
import app.ingest.transforms.show_to_prediction
import app.ingest.transforms.show_to_prediction_evaluation

__all__ = [
    'enrich_setlist_entry',
    'logging_consumers',
    'show_to_setlistentry',
    'prediction_to_predictedsetlistentry',
    'show_to_prediction',
    'show_to_prediction_evaluation',
]
