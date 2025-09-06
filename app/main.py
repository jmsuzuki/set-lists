# Setlist Analytics - Moose Project
# Ingest and analyze concert setlist data from various bands

# Import models and transforms (required for Moose to discover them)
import app.ingest.models
import app.ingest.transforms

# Import APIs - this will register all API endpoints with Moose
import app.apis

# Import views for aggregated data
import app.views.bar_aggregated

# Note: The following imports reference files that no longer exist
# and have been reorganized into the new structure above:
# - app.datamodels.* -> moved to app.ingest.models
# - app.functions.show_to_* -> moved to app.ingest.transforms
# - Individual API files are now imported via app.apis.__init__