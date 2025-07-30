# pd_target_identification/defs/knowledge_graph/schema_constants.py
"""
Configuration constants and thresholds for the PD target discovery knowledge graph.

This module defines evidence type classifications, confidence thresholds based on pipeline
data analysis, gene lists, and keyword sets used for biological pathway classification.
All thresholds are derived from actual pipeline results to ensure accurate evidence
assessment and therapeutic target prioritization.
"""

from enum import Enum

# ============================================================================
# EVIDENCE TYPE DEFINITIONS (Self-contained to avoid circular imports)
# ============================================================================

class EvidenceType(str, Enum):
    """Evidence types supported in the pipeline"""
    GWAS = "gwas"
    EQTL = "eqtl"
    LITERATURE = "literature"
    PATHWAY = "pathway"
    INTEGRATION = "integration"

# ============================================================================
# EVIDENCE TYPE DEFINITIONS
# ============================================================================

PIPELINE_EVIDENCE_TYPES = [
    EvidenceType.GWAS,
    EvidenceType.EQTL,
    EvidenceType.LITERATURE,
    EvidenceType.PATHWAY,
    EvidenceType.INTEGRATION
]

# Evidence type weights for scoring calculations
EVIDENCE_TYPE_WEIGHTS = {
    EvidenceType.GWAS: 0.4,
    EvidenceType.EQTL: 0.4,
    EvidenceType.LITERATURE: 0.5,
    EvidenceType.PATHWAY: 1.0,
    EvidenceType.INTEGRATION: 1.0
}

# ============================================================================
# CONFIDENCE THRESHOLDS (Based on Actual Pipeline Results)
# ============================================================================

CONFIDENCE_THRESHOLDS = {
    # Enhanced integrated score thresholds (based on SNCA=188.5, HLA-DRA=166.0, LRRK2=156.6)
    "enhanced_score_very_high": 150,    # Top tier targets
    "enhanced_score_high": 100,         # High priority (RIT2=128.1, BCKDK=110.8)
    "enhanced_score_medium": 60,        # Moderate priority
    "enhanced_score_low": 0,            # Any evidence
    
    # Literature evidence thresholds (based on SNCA=15, HLA-DRA=11, LRRK2=10)
    "therapeutic_papers_very_high": 10, # Extensive therapeutic focus
    "therapeutic_papers_high": 5,       # Strong therapeutic focus
    "therapeutic_papers_medium": 2,     # Moderate therapeutic focus
    "therapeutic_papers_low": 0,        # Any therapeutic evidence
    
    # Evidence convergence thresholds (based on max=4 evidence types)
    "evidence_types_max": 4,            # Maximum evidence types observed
    "evidence_types_high": 3,           # Strong multi-evidence support
    "evidence_types_medium": 2,         # Moderate evidence support
    "evidence_types_low": 1,            # Single evidence type
    
    # Statistical significance thresholds
    "gwas_significance": 5e-8,          # Genome-wide significance threshold
    "gwas_suggestive": 1e-6,            # Suggestive significance
    "eqtl_significance": 1e-5,          # Brain eQTL significance
    "pathway_significance": 0.05,       # Pathway enrichment FDR
    
    # Network analysis thresholds (based on MCCC1=8, LRRK2=7 interactions)
    "interaction_count_high": 6,        # Highly connected nodes
    "interaction_count_medium": 3,      # Moderately connected
    "interaction_count_low": 1,         # Any interaction evidence
    "string_confidence_high": 0.7,      # High confidence interactions
    "string_confidence_medium": 0.4,    # Medium confidence interactions
    
    # Pathway enrichment thresholds
    "pathway_count_high": 3,            # Multiple enriched pathways
    "pathway_count_medium": 1,          # Single enriched pathway
    "pathway_fdr_significant": 0.05,    # Significant pathway enrichment
    "pathway_fdr_suggestive": 0.1,      # Suggestive enrichment
    
    # Literature analysis thresholds
    "total_papers_high": 20,            # Extensively studied genes
    "total_papers_medium": 10,          # Moderately studied
    "total_papers_low": 3,              # Limited literature
    "recent_papers_threshold": 3,       # Recent publication activity
    "publication_recency_years": 3,     # Definition of "recent"
    
    # Quality assessment thresholds
    "data_completeness_high": 0.8,      # High data completeness
    "data_completeness_medium": 0.6,    # Moderate completeness
    "evidence_convergence_high": 0.8,   # Strong evidence agreement
    "evidence_convergence_medium": 0.6  # Moderate agreement
}

# ============================================================================
# GENE LISTS AND CLASSIFICATIONS (Must be defined before CONFIDENCE_THRESHOLDS)
# ============================================================================

# Current pipeline genes (14 total from multi_evidence_integrated)
PIPELINE_GENES = [
    'SNCA', 'HLA-DRA', 'LRRK2', 'RIT2', 'BCKDK',
    'INPP5F', 'DGKQ', 'NUCKS1', 'MCCC1', 'CCDC62',
    'STK39', 'DDRGK1', 'SIPA1L2', 'BST1'
]

# Known PD genes for validation and classification
ESTABLISHED_PD_GENES = [
    'SNCA', 'LRRK2', 'GBA1', 'PRKN', 'PINK1', 'PARK7',
    'VPS35', 'CHCHD2', 'TMEM175', 'MAPT', 'ATP13A2',
    'PLA2G6', 'FBXO7', 'DNAJC6', 'BST1', 'RIT2'
]

# Emerging PD targets identified by pipeline
EMERGING_PD_TARGETS = [
    'HLA-DRA', 'BCKDK', 'MCCC1', 'INPP5F', 'DGKQ',
    'NUCKS1', 'CCDC62', 'STK39', 'DDRGK1', 'SIPA1L2'
]

# Gene classification mapping
GENE_CLASSIFICATIONS = {
    'established': ESTABLISHED_PD_GENES,
    'emerging': EMERGING_PD_TARGETS,
    'novel': []  # Populated dynamically for genes not in other categories
}

# ============================================================================
# BIOLOGICAL PATHWAY KEYWORDS
# ============================================================================

# Dopamine-related pathway keywords (for pathway classification)
DOPAMINE_PATHWAY_KEYWORDS = [
    "dopamine", "dopaminergic", "tyrosine", "dopa", "catecholamine",
    "monoamine", "neurotransmitter", "tyrosine hydroxylase", "th",
    "dopamine receptor", "dopamine transporter", "dat", "slc6a3",
    "vesicular monoamine transporter", "vmat", "slc18a2",
    "aromatic l-amino acid decarboxylase", "aadc", "ddc"
]

# Neurodegeneration pathway keywords
NEURODEGENERATION_KEYWORDS = [
    "neurodegeneration", "neurodegenerative", "parkinson", "parkinsonian",
    "alpha-synuclein", "synuclein", "protein aggregation", "protein misfolding",
    "mitochondrial dysfunction", "oxidative stress", "autophagy",
    "lysosomal", "proteasome", "ubiquitin", "neuroinflammation",
    "microglial activation", "astrocyte", "neuronal death"
]

# Brain tissue keywords for tissue relevance
BRAIN_TISSUE_KEYWORDS = [
    "substantia nigra", "basal ganglia", "striatum", "putamen",
    "caudate nucleus", "globus pallidus", "midbrain", "brainstem",
    "dopaminergic neuron", "nigral", "striatal"
]

# Therapeutic target keywords for literature classification
THERAPEUTIC_TARGET_KEYWORDS = [
    "drug target", "therapeutic target", "treatment", "therapy",
    "medication", "inhibitor", "agonist", "antagonist", "modulator",
    "intervention", "clinical trial", "drug discovery", "pharmacological",
    "small molecule", "compound", "bioactive", "neuroprotective"
]

# Biomarker keywords for literature classification
BIOMARKER_KEYWORDS = [
    "biomarker", "diagnostic", "prognostic", "marker", "indicator",
    "detection", "screening", "monitor", "predict", "early detection",
    "disease progression", "clinical marker", "blood marker",
    "csf marker", "cerebrospinal fluid", "imaging marker"
]

# ============================================================================
# SCORING PARAMETERS
# ============================================================================

# Literature evidence scoring weights
LITERATURE_SCORING_WEIGHTS = {
    "therapeutic_target": 3.0,     # Highest weight for therapeutic relevance
    "clinical": 2.5,               # Clinical evidence weight
    "biomarker": 2.0,              # Biomarker evidence weight
    "pathogenesis": 1.5,           # Disease mechanism weight
    "functional": 1.0,             # Basic functional evidence weight
    "recency_bonus_recent": 2.0,   # Papers from last 1 year
    "recency_bonus_moderate": 1.5, # Papers from last 2-3 years
    "recency_bonus_old": 1.0       # Papers older than 3 years
}

# Pathway evidence scoring parameters
PATHWAY_SCORING_WEIGHTS = {
    "interaction_weight": 1.5,     # Protein interaction contribution
    "pathway_weight": 1.0,         # Pathway membership contribution
    "dopamine_bonus": 2.0,         # Bonus for dopamine pathway involvement
    "brain_tissue_bonus": 1.5,     # Bonus for brain-specific pathways
    "experimental_evidence": 1.2,  # Bonus for experimental validation
    "high_confidence_bonus": 1.3   # Bonus for high-confidence interactions
}

# Multi-evidence integration weights (based on pipeline scoring formula)
INTEGRATION_SCORING_WEIGHTS = {
    "gwas_eqtl_base": 0.4,         # Base genetic + regulatory evidence
    "interaction_multiplier": 1.5,  # Protein interaction contribution
    "pathway_multiplier": 1.0,     # Pathway evidence contribution
    "literature_multiplier": 0.5,  # Literature evidence contribution
    "therapeutic_paper_bonus": 3.0 # Bonus per therapeutic target paper
}

# ============================================================================
# DATA QUALITY PARAMETERS
# ============================================================================

# Missing data tolerance levels
DATA_QUALITY_THRESHOLDS = {
    "min_evidence_types": 1,        # Minimum evidence types required
    "max_missing_fields": 0.3,      # Maximum fraction of missing fields
    "min_confidence_score": 0.1,    # Minimum overall confidence
    "gene_symbol_required": True,   # Gene symbol is mandatory
    "ensembl_id_recommended": True, # Ensembl ID strongly recommended
    "literature_optional": True,    # Literature can be missing
    "pathway_optional": True       # Pathway data can be missing
}

# Validation parameters
VALIDATION_PARAMETERS = {
    "max_gene_symbol_length": 20,   # Maximum gene symbol length
    "min_p_value": 1e-50,           # Minimum p-value for calculations
    "max_p_value": 1.0,             # Maximum valid p-value
    "min_odds_ratio": 0.01,         # Minimum valid odds ratio
    "max_odds_ratio": 100.0,        # Maximum reasonable odds ratio
    "max_papers_per_gene": 100,     # Maximum papers for single gene
    "max_interactions_per_gene": 50 # Maximum interactions per gene
}

# ============================================================================
# DEFAULT VALUES AND CONFIGURATIONS
# ============================================================================

# Graphiti episode defaults
DEFAULT_GROUP_ID = "pd_target_discovery"
DEFAULT_SOURCE = "json"
DEFAULT_SOURCE_DESCRIPTION = "PD Target Discovery Multi-Omics Pipeline"

# Episode naming conventions
EPISODE_NAMING = {
    "gene_profile_prefix": "Gene_Profile",
    "gwas_evidence_prefix": "GWAS_Evidence",
    "eqtl_evidence_prefix": "eQTL_Evidence",
    "literature_evidence_prefix": "Literature_Evidence",
    "pathway_evidence_prefix": "Pathway_Evidence",
    "integration_prefix": "Multi_Evidence_Integration",
    "relationship_prefix": "Relationship",
    "separator": "_",
    "max_name_length": 100
}

# Tissue classification for brain relevance
TISSUE_CLASSIFICATIONS = {
    "substantia_nigra": {
        "relevance": "very_high",
        "keywords": ["substantia_nigra", "substantia nigra", "sn", "nigra"],
        "description": "Primary site of dopaminergic neuron loss in PD"
    },
    "basal_ganglia": {
        "relevance": "high", 
        "keywords": ["basal_ganglia", "putamen", "caudate", "striatum"],
        "description": "Brain circuit affected in PD motor symptoms"
    },
    "midbrain": {
        "relevance": "high",
        "keywords": ["midbrain", "mesencephalon", "ventral tegmental"],
        "description": "Contains dopaminergic nuclei relevant to PD"
    },
    "brain_general": {
        "relevance": "medium",
        "keywords": ["brain", "cerebral", "cortex", "hippocampus"],
        "description": "General brain tissues with potential PD relevance"
    }
}

# Statistical significance levels for display
SIGNIFICANCE_LEVELS = {
    "genome_wide": 5e-8,
    "suggestive": 1e-6, 
    "nominal": 0.05,
    "trend": 0.1
}

# Version and metadata
SCHEMA_VERSION = "1.0.0"
PIPELINE_VERSION = "1.0.0"
LAST_UPDATED = "2025-01-15"