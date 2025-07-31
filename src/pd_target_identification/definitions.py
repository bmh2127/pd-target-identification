# pd-target-identification/src/pd_target_identification/definitions.py

from dagster import Definitions

# Import individual assets that are working
from .defs.ingestion.gwas.assets import raw_gwas_data, gwas_data_with_mappings
from .defs.ingestion.expression.assets import (
    gtex_gene_version_mapping, gtex_brain_eqtls, gtex_eqtl_summary, gwas_eqtl_integrated
)
from .defs.ingestion.literature.assets import (
    pubmed_literature_search, literature_evidence_extraction, literature_gene_summary
)
from .defs.knowledge_graph.assets import (
    gene_profile_episodes,
    gwas_evidence_episodes,
    eqtl_evidence_episodes,
    literature_evidence_episodes,
    pathway_evidence_episodes,
    integration_episodes,
    complete_knowledge_graph_episodes,
    graphiti_ready_episodes,
    graphiti_export
)
from .defs.ingestion.gene_mapping.assets import gene_mapping_table, gene_mapping_lookup, dynamic_gene_mapping
from .defs.ingestion.pathways.assets import (
    string_protein_interactions, string_functional_enrichment, pathway_network_summary, multi_evidence_integrated
)

# Import resources
from .defs.shared.resources import GWASCatalogResource, GTExResource, STRINGResource, PubMedResource
from .defs.shared.io_managers import pd_duckdb_io_manager, default_io_manager

# Combine all working assets
all_assets = [
    # Gene mapping foundation
    gene_mapping_table,
    gene_mapping_lookup,
    dynamic_gene_mapping,
    
    # GWAS ingestion
    raw_gwas_data,
    gwas_data_with_mappings,
    
    # Expression/eQTL ingestion
    gtex_gene_version_mapping,
    gtex_brain_eqtls,
    gtex_eqtl_summary,
    gwas_eqtl_integrated,
    
    # Literature ingestion
    pubmed_literature_search,
    literature_evidence_extraction,
    literature_gene_summary,
    
    # Pathways ingestion
    string_protein_interactions,
    string_functional_enrichment,
    pathway_network_summary,
    
    # Final integration (now includes literature)
    multi_evidence_integrated,
    
    # Knowledge graph episode generation assets
    gene_profile_episodes,
    gwas_evidence_episodes,
    eqtl_evidence_episodes,
    literature_evidence_episodes,
    pathway_evidence_episodes,
    integration_episodes,
    complete_knowledge_graph_episodes,
    graphiti_ready_episodes,
    graphiti_export
    
]

defs = Definitions(
    assets=all_assets,
    resources={
        "gwas_catalog": GWASCatalogResource(),
        "gtex": GTExResource(),
        "string_db": STRINGResource(),
        "pubmed": PubMedResource(),  
        "io_manager": pd_duckdb_io_manager,
        "default_io_manager": default_io_manager
    }
)