# pd_target_identification/defs/knowledge_graph/__init__.py
"""
Knowledge Graph Schema Package for PD Target Discovery Pipeline.

This package provides comprehensive schema definitions, templates, validation, and utilities
for creating Graphiti episodes from multi-omics Parkinson's disease target discovery data.
The package transforms integrated GWAS, eQTL, literature, and pathway evidence into a
structured knowledge graph enabling dynamic querying and relationship discovery.
"""

# Core schema exports - fundamental types and entity definitions
from .graph_schema import (
    # Entity models
    GeneEntity,
    VariantEntity,
    PublicationEntity,
    PathwayEntity,
    
    # Enums for type safety
    EvidenceType,
    ConfidenceLevel,
    TherapeuticPriority,
    TissueRelevance,
    
    # Relationship schemas
    EvidenceRelationship,
    GeneticAssociation,
    RegulatoryRelationship
)

# Episode template exports - structured templates for all evidence types
from .episode_templates import (
    get_gene_profile_template,
    get_gwas_evidence_template,
    get_eqtl_evidence_template,
    get_literature_evidence_template,
    get_pathway_evidence_template,
    get_integration_template,
    get_relationship_template
)

# Validation exports - data quality and structure validation functions
from .schema_validation import (
    validate_gene_data,
    validate_evidence_data,
    validate_episode_structure,
    safe_episode_creation
)

# Utility exports - classification, scoring, and analysis functions
from .schema_utilities import (
    classify_therapeutic_priority,
    determine_confidence_level,
    calculate_evidence_breakdown,
    derive_gene_rank,
    generate_target_summary,
    extract_dopamine_pathway_involvement,
    assess_brain_tissue_specificity,
    # calculate_evidence_strength,
    normalize_gene_symbol,
    # extract_relationships,
    # generate_episode_name
)

# Constants exports - configuration, thresholds, and reference data
from .schema_constants import (
    # Evidence type definitions
    PIPELINE_EVIDENCE_TYPES,
    EVIDENCE_TYPE_WEIGHTS,
    
    # Confidence and scoring thresholds
    CONFIDENCE_THRESHOLDS,
    LITERATURE_SCORING_WEIGHTS,
    PATHWAY_SCORING_WEIGHTS,
    INTEGRATION_SCORING_WEIGHTS,
    
    # Gene classifications
    PIPELINE_GENES,
    ESTABLISHED_PD_GENES,
    EMERGING_PD_TARGETS,
    GENE_CLASSIFICATIONS,
    
    # Biological pathway keywords
    DOPAMINE_PATHWAY_KEYWORDS,
    NEURODEGENERATION_KEYWORDS,
    BRAIN_TISSUE_KEYWORDS,
    THERAPEUTIC_TARGET_KEYWORDS,
    BIOMARKER_KEYWORDS,
    
    # Configuration defaults
    DEFAULT_GROUP_ID,
    DEFAULT_SOURCE,
    DEFAULT_SOURCE_DESCRIPTION,
    EPISODE_NAMING,
    TISSUE_CLASSIFICATIONS,
    SIGNIFICANCE_LEVELS,
    
    # Quality and validation parameters
    DATA_QUALITY_THRESHOLDS,
    VALIDATION_PARAMETERS,
    
    # Version information
    SCHEMA_VERSION,
    PIPELINE_VERSION
)

# Package metadata
__version__ = "1.0.0"
__author__ = "PD Target Discovery Pipeline"
__description__ = "Knowledge graph schema for multi-omics Parkinson's disease target discovery"

# Comprehensive exports list for clean import control
__all__ = [
    # Core schema entities and types
    "GeneEntity",
    "VariantEntity", 
    "PublicationEntity",
    "PathwayEntity",
    "EvidenceType",
    "ConfidenceLevel",
    "TherapeuticPriority",
    "TissueRelevance",
    "EvidenceRelationship",
    "GeneticAssociation",
    "RegulatoryRelationship",
    
    # Episode templates
    "get_gene_profile_template",
    "get_gwas_evidence_template",
    "get_eqtl_evidence_template",
    "get_literature_evidence_template", 
    "get_pathway_evidence_template",
    "get_integration_template",
    "get_relationship_template",
    
    # Validation functions
    "validate_gene_data",
    "validate_evidence_data",
    "validate_episode_structure",
    "safe_episode_creation",
    
    # Utility functions
    "classify_therapeutic_priority",
    "determine_confidence_level",
    "calculate_evidence_breakdown",
    "derive_gene_rank",
    "generate_target_summary",
    "extract_dopamine_pathway_involvement",
    "assess_brain_tissue_specificity",
    "calculate_evidence_strength",
    "normalize_gene_symbol",
    "extract_relationships",
    "generate_episode_name",
    
    # Constants and configuration
    "PIPELINE_EVIDENCE_TYPES",
    "EVIDENCE_TYPE_WEIGHTS",
    "CONFIDENCE_THRESHOLDS",
    "LITERATURE_SCORING_WEIGHTS",
    "PATHWAY_SCORING_WEIGHTS",
    "INTEGRATION_SCORING_WEIGHTS",
    "PIPELINE_GENES",
    "ESTABLISHED_PD_GENES",
    "EMERGING_PD_TARGETS",
    "GENE_CLASSIFICATIONS",
    "DOPAMINE_PATHWAY_KEYWORDS",
    "NEURODEGENERATION_KEYWORDS",
    "BRAIN_TISSUE_KEYWORDS",
    "THERAPEUTIC_TARGET_KEYWORDS",
    "BIOMARKER_KEYWORDS",
    "DEFAULT_GROUP_ID",
    "DEFAULT_SOURCE",
    "DEFAULT_SOURCE_DESCRIPTION",
    "EPISODE_NAMING",
    "TISSUE_CLASSIFICATIONS",
    "SIGNIFICANCE_LEVELS",
    "DATA_QUALITY_THRESHOLDS",
    "VALIDATION_PARAMETERS",
    "SCHEMA_VERSION",
    "PIPELINE_VERSION"
]

# Convenience imports for common usage patterns
from .schema_constants import CONFIDENCE_THRESHOLDS, PIPELINE_GENES
from .graph_schema import EvidenceType, ConfidenceLevel, TherapeuticPriority
from .episode_templates import (
    get_gene_profile_template, 
    get_gwas_evidence_template,
    get_integration_template
)
from .schema_validation import validate_gene_data, safe_episode_creation
from .schema_utilities import (
    classify_therapeutic_priority,
    generate_target_summary,
    calculate_evidence_breakdown
)

# Package-level utility functions for common operations
def get_evidence_template(evidence_type: EvidenceType):
    """
    Get the appropriate episode template for a given evidence type.
    
    Args:
        evidence_type: The type of evidence (GWAS, eQTL, literature, pathway, integration)
        
    Returns:
        Dict containing the template structure for the specified evidence type
    """
    template_map = {
        EvidenceType.GWAS: get_gwas_evidence_template,
        EvidenceType.EQTL: get_eqtl_evidence_template,
        EvidenceType.LITERATURE: get_literature_evidence_template,
        EvidenceType.PATHWAY: get_pathway_evidence_template,
        EvidenceType.INTEGRATION: get_integration_template
    }
    
    if evidence_type not in template_map:
        raise ValueError(f"Unknown evidence type: {evidence_type}")
    
    return template_map[evidence_type]()

def validate_pipeline_gene(gene_symbol: str) -> bool:
    """
    Check if a gene symbol is part of the current pipeline gene set.
    
    Args:
        gene_symbol: Gene symbol to validate
        
    Returns:
        Boolean indicating if gene is in pipeline
    """
    return gene_symbol.upper() in [g.upper() for g in PIPELINE_GENES]

def get_gene_classification(gene_symbol: str) -> str:
    """
    Classify a gene as established, emerging, or novel based on PD research status.
    
    Args:
        gene_symbol: Gene symbol to classify
        
    Returns:
        String classification: 'established', 'emerging', or 'novel'
    """
    gene_upper = gene_symbol.upper()
    
    if gene_upper in [g.upper() for g in ESTABLISHED_PD_GENES]:
        return 'established'
    elif gene_upper in [g.upper() for g in EMERGING_PD_TARGETS]:
        return 'emerging'
    else:
        return 'novel'

def is_high_confidence_target(gene_data: dict) -> bool:
    """
    Determine if a gene qualifies as a high-confidence therapeutic target.
    
    Args:
        gene_data: Dictionary containing gene evidence data
        
    Returns:
        Boolean indicating high confidence status
    """
    enhanced_score = gene_data.get('enhanced_integrated_score', 0)
    evidence_types = gene_data.get('evidence_types', 0)
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    
    return (enhanced_score >= CONFIDENCE_THRESHOLDS['enhanced_score_high'] and
            evidence_types >= CONFIDENCE_THRESHOLDS['evidence_types_high'] and
            therapeutic_papers >= CONFIDENCE_THRESHOLDS['therapeutic_papers_medium'])

# Package-level constants for external reference
PACKAGE_INFO = {
    'name': 'knowledge_graph',
    'version': __version__,
    'description': __description__,
    'schema_version': SCHEMA_VERSION,
    'pipeline_version': PIPELINE_VERSION,
    'evidence_types_supported': len(PIPELINE_EVIDENCE_TYPES),
    'genes_in_pipeline': len(PIPELINE_GENES),
    'confidence_levels': len(ConfidenceLevel),
    'therapeutic_priorities': len(TherapeuticPriority)
}