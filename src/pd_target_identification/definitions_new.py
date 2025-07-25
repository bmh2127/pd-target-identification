from dagster import Definitions

# Import individual assets that are working
from .defs.ingestion.gwas.assets import raw_gwas_data
from .defs.ingestion.expression.assets import brain_expression_data, expression_normalized
from .defs.integration.assets import integrated_target_scores
from .defs.knowledge_graph.assets import gwas_graphiti_episodes

# Combine all working assets
all_assets = [
    raw_gwas_data,  # GWAS mock data
    brain_expression_data,  # Multi-asset for brain expression
    expression_normalized,  # Expression normalization
    integrated_target_scores,  # Evidence integration
    gwas_graphiti_episodes  # Knowledge graph episodes
]

defs = Definitions(
    assets=all_assets
)