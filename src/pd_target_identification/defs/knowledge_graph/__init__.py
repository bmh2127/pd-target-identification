"""Knowledge graph module."""

# Import assets from the dedicated assets module
from .assets import gwas_graphiti_episodes

# Create assets list for load_assets_from_modules()
assets = [gwas_graphiti_episodes]

__all__ = ["assets"]
