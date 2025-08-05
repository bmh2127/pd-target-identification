"""Data integration module."""

# Import assets from the dedicated assets module
from .assets import integrated_target_scores
from .enhanced_scoring import enhanced_scoring_with_census

# Create assets list for load_assets_from_modules()
assets = [integrated_target_scores, enhanced_scoring_with_census]

__all__ = ["assets", "enhanced_scoring_with_census"]
