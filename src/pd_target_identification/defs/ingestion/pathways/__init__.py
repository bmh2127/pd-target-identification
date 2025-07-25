"""Pathways data ingestion module."""

# Import assets from the dedicated assets module
from .assets import pathways_data, pathways_filtered

# Expose assets for load_assets_from_modules()
__all__ = ["pathways_data", "pathways_filtered"]
