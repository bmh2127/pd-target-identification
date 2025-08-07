"""Expression data ingestion module."""

# Import assets from the dedicated assets module
from .assets import gtex_brain_eqtls, gtex_gene_version_mapping, gwas_eqtl_integrated

# Expose assets for load_assets_from_modules()
__all__ = ["gtex_brain_eqtls", "gtex_gene_version_mapping", "gwas_eqtl_integrated"]
