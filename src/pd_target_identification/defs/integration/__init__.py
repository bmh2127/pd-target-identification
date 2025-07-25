"""Data integration module."""

# Import assets from the dedicated assets module
from .assets import integrated_target_scores

# Create assets list for load_assets_from_modules()
assets = [integrated_target_scores]

__all__ = ["assets"]
