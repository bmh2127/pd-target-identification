"""GWAS data ingestion module."""

# Import assets from the dedicated assets module
# from .assets import raw_european_gwas_data, raw_east_asian_gwas_data, credible_sets

# # Expose assets for load_assets_from_modules()
# __all__ = ["raw_european_gwas_data", "raw_east_asian_gwas_data", "credible_sets"]

from .assets import raw_gwas_data

__all__ = ["raw_gwas_data"]