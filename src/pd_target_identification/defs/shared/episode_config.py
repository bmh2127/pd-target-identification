"""
Episode Type Configuration for PD Target Identification Pipeline

This module defines all episode types, their processing order, and metadata.
This is the single source of truth for episode types throughout the pipeline.

To add a new evidence source:
1. Add the episode type to EPISODE_TYPES with appropriate order and metadata
2. Create the corresponding asset in the appropriate module
3. Update definitions.py to include the new asset
4. The export and validation will automatically include the new type

"""

from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class EpisodeTypeConfig:
    """Configuration for an episode type"""
    order: int
    description: str
    dependencies: List[str]
    group_name: str
    compute_kind: str
    tags: Dict[str, str]

# ============================================================================
# EPISODE TYPE DEFINITIONS
# ============================================================================
# Define all episode types with their processing order and metadata
# Lower order numbers are processed first

EPISODE_TYPES: Dict[str, EpisodeTypeConfig] = {
    "gene_profile": EpisodeTypeConfig(
        order=1,
        description="Central gene entity profiles with comprehensive data",
        dependencies=[],
        group_name="knowledge_graph",
        compute_kind="python",
        tags={"data_type": "episodes", "source": "integration"}
    ),
    
    "gwas_evidence": EpisodeTypeConfig(
        order=2,
        description="Genetic association evidence from GWAS studies",
        dependencies=["gene_profile"],
        group_name="knowledge_graph", 
        compute_kind="python",
        tags={"data_type": "episodes", "source": "gwas"}
    ),
    
    "eqtl_evidence": EpisodeTypeConfig(
        order=3,
        description="Gene expression regulation evidence from eQTL studies",
        dependencies=["gene_profile"],
        group_name="knowledge_graph",
        compute_kind="python", 
        tags={"data_type": "episodes", "source": "eqtl"}
    ),
    
    "literature_evidence": EpisodeTypeConfig(
        order=4,
        description="Scientific literature and research evidence",
        dependencies=["gene_profile"],
        group_name="knowledge_graph",
        compute_kind="api",
        tags={"data_type": "episodes", "source": "pubmed"}
    ),
    
    "pathway_evidence": EpisodeTypeConfig(
        order=5,
        description="Biological pathway and protein interaction evidence",
        dependencies=["gene_profile"],
        group_name="knowledge_graph",
        compute_kind="api",
        tags={"data_type": "episodes", "source": "string"}
    ),
    
    "census_validation": EpisodeTypeConfig(
        order=6,
        description="Single-cell RNA expression validation from CellxGene Census",
        dependencies=["gene_profile"],
        group_name="knowledge_graph",
        compute_kind="python",
        tags={"data_type": "episodes", "source": "census"}
    ),
    
    "integration": EpisodeTypeConfig(
        order=7,
        description="Multi-evidence therapeutic target assessments and synthesis",
        dependencies=["gene_profile", "gwas_evidence", "eqtl_evidence", 
                     "literature_evidence", "pathway_evidence", "census_validation"],
        group_name="knowledge_graph",
        compute_kind="python",
        tags={"data_type": "episodes", "source": "integration"}
    )
}

# ============================================================================
# DERIVED CONFIGURATIONS  
# ============================================================================
# These are automatically generated from EPISODE_TYPES

# Processing order for ingestion and export
INGESTION_ORDER: List[str] = [
    episode_type for episode_type, config in 
    sorted(EPISODE_TYPES.items(), key=lambda x: x[1].order)
]

# All episode type names for validation
ALL_EPISODE_TYPES: List[str] = list(EPISODE_TYPES.keys())

# Episode types by dependency level (for parallel processing)
DEPENDENCY_LEVELS: Dict[int, List[str]] = {}
for episode_type, config in EPISODE_TYPES.items():
    level = len(config.dependencies)
    if level not in DEPENDENCY_LEVELS:
        DEPENDENCY_LEVELS[level] = []
    DEPENDENCY_LEVELS[level].append(episode_type)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_episode_config(episode_type: str) -> Optional[EpisodeTypeConfig]:
    """Get configuration for a specific episode type"""
    return EPISODE_TYPES.get(episode_type)

def validate_episode_type(episode_type: str) -> bool:
    """Check if episode type is valid"""
    return episode_type in EPISODE_TYPES

def get_dependencies(episode_type: str) -> List[str]:
    """Get dependencies for an episode type"""
    config = get_episode_config(episode_type)
    return config.dependencies if config else []

def get_processing_order() -> List[str]:
    """Get the recommended processing order for all episode types"""
    return INGESTION_ORDER.copy()

def get_episode_types_for_export() -> List[str]:
    """Get episode types in export order (same as ingestion order)"""
    return INGESTION_ORDER.copy()

def add_custom_episode_type(
    name: str, 
    order: int, 
    description: str,
    dependencies: List[str] = None,
    group_name: str = "knowledge_graph",
    compute_kind: str = "python",
    tags: Dict[str, str] = None
) -> None:
    """
    Dynamically add a custom episode type (for extensions/plugins)
    
    Note: This modifies the global configuration at runtime.
    Use with caution in production environments.
    """
    global EPISODE_TYPES, INGESTION_ORDER, ALL_EPISODE_TYPES, DEPENDENCY_LEVELS
    
    if dependencies is None:
        dependencies = []
    if tags is None:
        tags = {"data_type": "episodes", "source": "custom"}
    
    # Add to main config
    EPISODE_TYPES[name] = EpisodeTypeConfig(
        order=order,
        description=description,
        dependencies=dependencies,
        group_name=group_name,
        compute_kind=compute_kind,
        tags=tags
    )
    
    # Regenerate derived configurations
    INGESTION_ORDER = [
        episode_type for episode_type, config in 
        sorted(EPISODE_TYPES.items(), key=lambda x: x[1].order)
    ]
    ALL_EPISODE_TYPES = list(EPISODE_TYPES.keys())
    
    # Update dependency levels
    DEPENDENCY_LEVELS = {}
    for episode_type, config in EPISODE_TYPES.items():
        level = len(config.dependencies)
        if level not in DEPENDENCY_LEVELS:
            DEPENDENCY_LEVELS[level] = []
        DEPENDENCY_LEVELS[level].append(episode_type)

# ============================================================================
# CONFIGURATION SUMMARY
# ============================================================================

def print_episode_config_summary():
    """Print a summary of the current episode configuration"""
    print("📋 Episode Type Configuration Summary")
    print("=" * 50)
    print(f"Total Episode Types: {len(ALL_EPISODE_TYPES)}")
    print(f"Processing Order: {' → '.join(INGESTION_ORDER)}")
    print("\nDetailed Configuration:")
    
    for episode_type in INGESTION_ORDER:
        config = EPISODE_TYPES[episode_type]
        deps = ', '.join(config.dependencies) if config.dependencies else 'None'
        print(f"  {config.order}. {episode_type}")
        print(f"     Description: {config.description}")
        print(f"     Dependencies: {deps}")
        print(f"     Source: {config.tags.get('source', 'unknown')}")
        print()

if __name__ == "__main__":
    print_episode_config_summary()