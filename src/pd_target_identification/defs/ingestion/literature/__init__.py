"""Literature data ingestion module."""

# Import assets from the dedicated assets module
from .assets import literature_data, literature_processed

# Expose assets for load_assets_from_modules()
__all__ = ["literature_data", "literature_processed"]
