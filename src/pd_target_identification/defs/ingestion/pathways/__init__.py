"""Pathways data ingestion module."""

# Import assets from the dedicated assets module
from .assets import string_protein_interactions, string_functional_enrichment, pathway_network_summary, multi_evidence_integrated

# Expose assets for load_assets_from_modules()
__all__ = ["string_protein_interactions", "string_functional_enrichment", "pathway_network_summary", "multi_evidence_integrated"]
