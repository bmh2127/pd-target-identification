"""Literature data ingestion module."""

# Import assets from the dedicated assets module
from .assets import pubmed_literature_search, literature_evidence_extraction, literature_gene_summary

# Expose assets for load_assets_from_modules()
__all__ = ["pubmed_literature_search", "literature_evidence_extraction", "literature_gene_summary"]
