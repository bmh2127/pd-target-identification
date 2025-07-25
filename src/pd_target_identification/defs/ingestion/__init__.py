"""Data ingestion modules for PD target identification."""

# Import submodules to make them available for load_assets_from_modules()
from . import gwas
from . import expression  
from . import pathways
from . import literature

# Make modules available for import
__all__ = ["gwas", "expression", "pathways", "literature"]
