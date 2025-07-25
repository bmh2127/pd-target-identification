# pd_target_identification/definitions.py
from dagster import Definitions

# Import individual assets that are working
from .defs.ingestion.gwas.assets import raw_gwas_data, gwas_data_with_mappings
from .defs.ingestion.expression.assets import (
    gtex_gene_version_mapping, gtex_brain_eqtls, gtex_eqtl_summary, gwas_eqtl_integrated
)
# from .defs.integration.assets import integrated_target_scores
from .defs.knowledge_graph.assets import gwas_graphiti_episodes
from .defs.ingestion.gene_mapping.assets import gene_mapping_table, gene_mapping_lookup, dynamic_gene_mapping
# Import resources
from .defs.shared.resources import GWASCatalogResource, GTExResource
from .defs.shared.io_managers import pd_duckdb_io_manager, default_io_manager

# Combine all working assets
all_assets = [
    # Gene mapping foundation
    gene_mapping_table,
    gene_mapping_lookup,
    dynamic_gene_mapping,
    
    # Existing assets
    raw_gwas_data,  # Enhanced GWAS data with gene mapping
    gwas_data_with_mappings,  # Filtered GWAS data with complete mappings
    
    # GTEx eQTL assets
    gtex_gene_version_mapping,  # Find correct gene versions for GTEx
    gtex_brain_eqtls,  # Brain eQTL data
    gtex_eqtl_summary,  # eQTL summary by gene
    gwas_eqtl_integrated,  # Integrated GWAS + eQTL
    # integrated_target_scores,  # Evidence integration (commented out for now)
    gwas_graphiti_episodes  # Knowledge graph episodes
]

defs = Definitions(
    assets=all_assets,
    resources={
        "gwas_catalog": GWASCatalogResource(),
        "gtex": GTExResource(),
        "io_manager": pd_duckdb_io_manager,
        "default_io_manager": default_io_manager      # For dictionaries
    }
)