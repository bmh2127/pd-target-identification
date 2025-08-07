# pd-target-identification/src/pd_target_identification/definitions.py

from dagster import Definitions

# Import individual assets that are working
from .defs.ingestion.gwas.assets import raw_gwas_data
from .defs.ingestion.expression.assets import (
    gtex_gene_version_mapping, gtex_brain_eqtls, gwas_eqtl_integrated
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
    graphiti_export,
    graphiti_knowledge_graph_ingestion
)
from .defs.knowledge_graph.census_episodes import census_validation_episodes
# Import MCP assets
from .defs.knowledge_graph.mcp_assets import (
    graphiti_mcp_direct_ingestion,
    mcp_ingestion_comparison
)
from .defs.ingestion.gene_mapping.assets import dynamic_gene_mapping
from .defs.ingestion.pathways.assets import (
    string_protein_interactions, string_functional_enrichment, pathway_network_summary, multi_evidence_integrated
)
from .defs.ingestion.single_cell.assets import census_expression_validation
from .defs.integration.enhanced_scoring import enhanced_scoring_with_census

# Import resources
from .defs.shared.resources import GWASCatalogResource, GTExResource, STRINGResource, PubMedResource, GraphitiServiceResource, CellxGeneCensusResource
from .defs.shared.io_managers import pd_duckdb_io_manager, default_io_manager

# Combine all working assets
all_assets = [
    # Gene mapping foundation
    dynamic_gene_mapping,
    
    # GWAS ingestion
    raw_gwas_data,
    
    # Expression/eQTL ingestion
    gtex_gene_version_mapping,
    gtex_brain_eqtls,
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
    
    # Census validation (new)
    census_expression_validation,
    enhanced_scoring_with_census,
    
    # Knowledge graph episode generation assets
    gene_profile_episodes,
    gwas_evidence_episodes,
    eqtl_evidence_episodes,
    literature_evidence_episodes,
    pathway_evidence_episodes,
    integration_episodes,
    census_validation_episodes,  # Census validation episodes
    complete_knowledge_graph_episodes,
    graphiti_ready_episodes,
    graphiti_export,
    
    # Graphiti service integration
    graphiti_knowledge_graph_ingestion,
    
    # MCP direct integration
    graphiti_mcp_direct_ingestion,
    mcp_ingestion_comparison
    
]

defs = Definitions(
    assets=all_assets,
    resources={
        "gwas_catalog": GWASCatalogResource(),
        "gtex": GTExResource(),
        "string_db": STRINGResource(),
        "pubmed": PubMedResource(),
        "census": CellxGeneCensusResource(
            use_sample_range=True,  # Testing mode for now
            enable_batch_processing=True,  # Enable batch processing for production
            genes_per_batch=5,  # Process 5 genes per batch
            batch_timeout=180  # 3 minutes per batch
        ),
        "graphiti_service": GraphitiServiceResource(
            service_url="http://localhost:8002",
            request_timeout=300,  # 5 minutes for requests
            max_retries=3,
            retry_delay=15,
            polling_interval=30,  # Poll every 30 seconds
            max_polling_duration=7200  # 2 hours max for ingestion
        ),
        "io_manager": pd_duckdb_io_manager,
        "default_io_manager": default_io_manager
    }
)