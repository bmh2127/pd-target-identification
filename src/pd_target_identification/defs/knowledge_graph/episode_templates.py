# pd_target_identification/defs/knowledge_graph/episode_templates.py
"""
Episode template functions for Graphiti knowledge graph construction.

This module provides standardized templates for creating different types of
episodes that represent various forms of evidence in the Parkinson's disease
target discovery knowledge graph. Each template function returns a structured
dictionary that can be populated with actual data and converted to JSON
episodes for Graphiti ingestion.

The templates are designed to capture the full richness of multi-omics evidence
including genetic associations (GWAS), regulatory effects (eQTL), literature
validation, functional pathways, and integrated target assessments. Each template
includes comprehensive metadata, statistical measures, and cross-references to
enable sophisticated querying and analysis of therapeutic target evidence.

Templates follow a hierarchical structure where integration episodes synthesize
evidence from multiple sources, while individual evidence episodes provide
detailed source-specific information. This design supports both high-level
target prioritization and detailed evidence exploration.
"""

from typing import Dict, List, Any
from datetime import datetime
from .graph_schema import ConfidenceLevel, TherapeuticPriority, TissueRelevance

# ============================================================================
# CORE ENTITY TEMPLATES - Foundation Episodes
# ============================================================================

def get_gene_profile_template() -> Dict[str, Any]:
    """
    Comprehensive gene profile episode template for central gene entities.
    
    Creates the foundational gene entity episode that serves as the anchor
    point for all evidence types in the knowledge graph. Includes gene
    identity information, database mappings, integrated scoring results,
    and high-level evidence summaries that provide immediate target assessment.
    
    The gene profile synthesizes information from multiple pipeline stages
    including gene mapping, multi-evidence integration, and therapeutic
    prioritization to provide a complete gene-centric view of target potential.
    
    Returns:
        Dictionary containing complete gene profile structure with fields for:
        - Core gene identity and database mappings
        - Integrated scoring and ranking information
        - Evidence type counts and summary statistics
        - Key biological indicators and tissue specificity
        - Therapeutic assessment and confidence levels
        - Temporal metadata and data provenance
    """
    return {
        "gene": {
            # Core identity information from gene mapping
            "symbol": "",                           # Primary HGNC gene symbol
            "ensembl_id": "",                      # Ensembl gene identifier
            "entrez_id": "",                       # NCBI Entrez gene identifier
            "mapping_source": "",                  # Source of gene mapping
            "mapping_confidence": "",              # Quality of identifier mapping
            
            # Integration results from multi-evidence scoring
            "rank": None,                          # Gene ranking by integrated score
            "enhanced_integrated_score": 0.0,     # Primary composite score
            "integrated_score": 0.0,              # Base GWAS+eQTL score
            "evidence_types": 0,                   # Count of evidence types present
            
            # Evidence summary counts for quick assessment
            "gwas_variant_count": 0,               # Number of associated variants
            "eqtl_count": 0,                      # Number of brain eQTLs
            "literature_papers_count": 0,         # Total publications found
            "interaction_count": 0,               # Protein interaction count
            "pathway_count": 0,                   # Enriched pathway count
            
            # Key biological indicators for disease relevance
            "has_substantia_nigra_eqtl": False,   # SN-specific regulation
            "has_basal_ganglia_eqtl": False,      # BG-specific regulation
            "has_strong_literature_evidence": False, # Literature validation
            
            # Therapeutic assessment and prioritization
            "confidence_level": ConfidenceLevel.LOW.value,
            "therapeutic_priority": TherapeuticPriority.LOW_PRIORITY.value,
            
            # Metadata and provenance
            "last_updated": datetime.now().isoformat(),
            "data_completeness": 0.0,             # Fraction of available evidence
            "validation_status": "pending"        # Cross-validation status
        }
    }

# ============================================================================
# EVIDENCE-SPECIFIC TEMPLATES - Detailed Source Information
# ============================================================================

def get_gwas_evidence_template() -> Dict[str, Any]:
    """
    GWAS genetic association evidence episode template.
    
    Captures comprehensive genetic association data from genome-wide association
    studies, including individual variant details, population genetics information,
    and statistical significance measures. Designed to represent both summary
    statistics and detailed variant-level information for genetic target validation.
    
    The template accommodates multiple variants per gene, population-specific
    effects, and study-level metadata to enable sophisticated genetic analysis
    and cross-population validation of target associations.
    
    Returns:
        Dictionary containing GWAS evidence structure with fields for:
        - Gene identity and cross-references
        - Summary statistics (min p-value, max effect size)
        - Individual variant details with genomic coordinates
        - Population genetics and study metadata
        - Statistical significance assessment
        - Evidence quality and validation metrics
    """
    return {
        "genetic_association": {
            # Gene identity and cross-references
            "gene": "",                            # Target gene symbol
            "ensembl_gene_id": "",                # Gene database identifier
            
            # Summary statistics for quick assessment
            "min_p_value": None,                   # Most significant association
            "max_odds_ratio": None,                # Largest effect size
            "variant_count": 0,                    # Number of associated variants
            "genome_wide_significant": False,      # Meets 5e-8 threshold
            
            # Individual variant details
            "variants": [],                        # List of variant records
            "chromosomal_locations": [],           # Genomic coordinates
            "effect_alleles": [],                  # Risk-associated alleles
            
            # Population and study information
            "populations_studied": [],             # Ancestry populations
            "study_accessions": [],                # Study identifiers
            "total_sample_size": None,             # Combined sample size
            
            # Statistical assessment and classification
            "significance_level": "unknown",       # Genome-wide significance
            "max_effect_size": None,               # Maximum odds ratio
            "effect_consistency": "unknown",       # Cross-population consistency
            
            # Evidence quality and validation
            "genetic_evidence_strength": ConfidenceLevel.LOW.value,
            "replication_status": "unknown",       # Independent replication
            "population_diversity": "unknown",     # Multi-ancestry evidence
            
            # Summary and interpretation
            "evidence_summary": "",                # Human-readable summary
            "clinical_relevance": "unknown",       # Disease mechanism relevance
            "therapeutic_implications": ""         # Drug development insights
        }
    }

def get_eqtl_evidence_template() -> Dict[str, Any]:
    """
    eQTL regulatory evidence episode template for brain-specific gene regulation.
    
    Captures expression quantitative trait loci data specifically focused on
    brain tissues relevant to Parkinson's disease pathology. Includes detailed
    tissue specificity information, regulatory effect magnitudes, and clinical
    relevance assessments for understanding disease mechanisms.
    
    The template emphasizes brain tissue specificity with particular attention
    to substantia nigra and basal ganglia regions most affected in PD, enabling
    prioritization of targets with disease-relevant regulatory mechanisms.
    
    Returns:
        Dictionary containing eQTL evidence structure with fields for:
        - Gene identity and regulatory context
        - Brain tissue specificity and effect magnitudes
        - Individual eQTL details with statistical measures
        - Disease relevance and clinical implications
        - Regulatory strength assessment and validation
        - Temporal and methodological metadata
    """
    return {
        "regulatory_evidence": {
            # Gene identity and regulatory context
            "gene": "",                            # Target gene symbol
            "ensembl_gene_id": "",                # Gene identifier
            "base_ensembl_id": "",                # Base gene identifier
            
            # Summary statistics for regulatory assessment
            "eqtl_count": 0,                      # Number of brain eQTLs
            "tissue_count": 0,                    # Brain tissues with eQTLs
            "min_p_value": None,                  # Most significant eQTL
            "max_effect_size": None,              # Largest regulatory effect
            
            # Brain tissue specificity for PD relevance
            "tissues_with_eqtls": 0,              # Count of brain tissues
            "has_substantia_nigra_eqtl": False,   # Primary PD brain region
            "has_basal_ganglia_eqtl": False,      # Motor control region
            "brain_tissue_specificity": "",       # Specificity classification
            "pd_tissue_relevance": TissueRelevance.NON_BRAIN.value,
            
            # Effect characteristics and patterns
            "mean_effect_size": None,             # Average regulatory effect
            "effect_size_std": None,              # Effect size variability
            "unique_variants": 0,                 # Number of regulatory variants
            "eqtl_strength_score": 0.0,          # Composite regulatory strength
            
            # Individual eQTL details
            "brain_eqtls": [],                    # List of eQTL records
            "tissue_breakdown": {},               # Effects by tissue type
            "regulatory_patterns": {},            # Tissue-specific patterns
            
            # Disease relevance and clinical implications
            "brain_enriched": False,              # Brain-specific regulation
            "neurodegeneration_relevance": "unknown", # Neurodegen pathway involvement
            "dopaminergic_specificity": False,    # Dopamine system relevance
            
            # Regulatory assessment and validation
            "regulatory_strength": ConfidenceLevel.LOW.value,
            "tissue_specificity_score": 0.0,     # Specificity quantification
            "cross_tissue_consistency": "unknown", # Consistent effects
            
            # Summary and interpretation
            "evidence_summary": "",               # Human-readable summary
            "mechanism_hypothesis": "",           # Regulatory mechanism
            "therapeutic_relevance": ""           # Target validation insights
        }
    }

def get_literature_evidence_template() -> Dict[str, Any]:
    """
    Literature evidence episode template for publication-based target validation.
    
    Captures comprehensive literature analysis including publication counts,
    evidence classification, research trends, and therapeutic focus assessment.
    Designed to validate computational predictions with real-world research
    evidence and identify established versus emerging therapeutic targets.
    
    The template includes temporal analysis capabilities to track research
    momentum and identify targets with increasing or decreasing research focus,
    critical for understanding target maturity and competitive landscape.
    
    Returns:
        Dictionary containing literature evidence structure with fields for:
        - Publication counts by evidence type and recency
        - Research quality and breadth assessment
        - Therapeutic focus and clinical development status
        - Temporal trends and research momentum
        - Key publications and research highlights
        - Evidence validation and cross-referencing
    """
    return {
        "literature_evidence": {
            # Gene identity and literature scope
            "gene": "",                            # Target gene symbol
            
            # Publication counts by evidence type
            "total_papers": 0,                     # All publications found
            "therapeutic_target_papers": 0,        # Drug target papers
            "clinical_papers": 0,                  # Clinical studies
            "biomarker_papers": 0,                 # Diagnostic marker papers
            "recent_papers": 0,                    # Last 3 years
            
            # Literature scoring and assessment
            "literature_evidence_score": 0.0,      # Composite literature score
            "avg_evidence_score": 0.0,             # Average paper quality
            "max_evidence_score": 0.0,             # Best evidence score
            "has_strong_literature_evidence": False, # High-quality validation
            
            # Research quality indicators
            "top_evidence_paper_pmid": "",         # Best supporting paper
            "top_evidence_score": 0.0,             # Quality of best paper
            "journals_count": 0,                   # Research breadth
            "therapeutic_keywords_summary": "",     # Key therapeutic terms
            
            # Temporal analysis and research trends
            "most_recent_paper_year": None,        # Latest publication
            "publication_timeline": {},            # Papers by year
            "research_momentum": "stable",          # Trend assessment
            "peak_research_period": "",            # Period of highest activity
            
            # Evidence type breakdown
            "evidence_type_scores": {
                "therapeutic_target": 0.0,         # Target validation score
                "biomarker": 0.0,                  # Biomarker evidence
                "pathogenesis": 0.0,               # Disease mechanism
                "clinical": 0.0,                   # Clinical evidence
                "functional": 0.0                  # Functional studies
            },
            
            # Research focus and development status
            "primary_research_focus": "",          # Main research area
            "clinical_development_stage": "unknown", # Development progress
            "therapeutic_modalities": [],          # Drug approaches
            
            # Literature validation and cross-referencing
            "literature_strength": ConfidenceLevel.LOW.value,
            "citation_network_size": 0,           # Citation connections
            "research_community_size": 0,          # Active researchers
            
            # Summary and research insights
            "evidence_summary": "",               # Human-readable summary
            "research_gaps": [],                  # Identified knowledge gaps
            "future_directions": ""               # Recommended research
        }
    }

def get_pathway_evidence_template() -> Dict[str, Any]:
    """
    Pathway and functional evidence episode template for biological context.
    
    Captures protein interaction networks, pathway enrichments, and functional
    annotations that provide biological context for therapeutic targets.
    Includes detailed network topology, pathway membership, and disease relevance
    assessment to understand mechanism of action and therapeutic potential.
    
    The template emphasizes Parkinson's disease-relevant pathways including
    dopaminergic signaling, neurodegeneration, and mitochondrial function,
    enabling prioritization of targets with clear mechanistic relevance.
    
    Returns:
        Dictionary containing pathway evidence structure with fields for:
        - Protein interaction network statistics
        - Pathway membership and enrichment data
        - Functional annotation and mechanism insights
        - Disease pathway relevance assessment
        - Network topology and centrality measures
        - Cross-pathway connections and systems analysis
    """
    return {
        "functional_evidence": {
            # Gene identity and functional context
            "gene": "",                            # Target gene symbol
            
            # Network statistics and connectivity
            "interaction_count": 0,                # Total protein interactions
            "high_confidence_interactions": 0,     # Strong interactions (>0.7)
            "avg_interaction_confidence": 0.0,     # Mean confidence score
            "degree_centrality": 0.0,             # Network centrality measure
            "query_gene_interactions": 0,          # Within-target interactions
            
            # Pathway membership and enrichment
            "pathway_count": 0,                    # Enriched pathways
            "significant_pathways": 0,             # Statistically significant
            "pathway_evidence_score": 0.0,        # Composite pathway score
            "top_pathway": "",                     # Most significant pathway
            "pathways_list": [],                   # All enriched pathways
            
            # Detailed interaction data
            "protein_interactions": [],           # Structured interaction data
            "interaction_partners": [],           # Partner gene symbols
            "interaction_evidence": {},           # Evidence type breakdown
            
            # Evidence scoring by source type
            "evidence_scores": {
                "experimental_score": 0.0,        # Laboratory evidence
                "database_score": 0.0,            # Curated database
                "textmining_score": 0.0,          # Literature mining
                "coexpression_score": 0.0         # Expression correlation
            },
            
            # Pathway details and enrichment data
            "pathway_memberships": [],            # Detailed pathway data
            "go_term_enrichments": [],           # Gene Ontology terms
            "kegg_pathway_involvement": [],       # KEGG pathway membership
            
            # Disease relevance assessment
            "dopamine_pathway_involvement": False, # Dopaminergic signaling
            "neurodegeneration_pathways": False,  # Neurodegen mechanisms
            "pd_pathway_enrichment": False,       # PD-specific pathways
            "mitochondrial_involvement": False,   # Mitochondrial function
            
            # Network properties and system analysis
            "combined_pathway_score": 0.0,        # Integrated pathway score
            "functional_coherence": ConfidenceLevel.LOW.value,
            "network_centrality_rank": None,      # Relative importance
            "pathway_connectivity": 0.0,          # Cross-pathway connections
            
            # Functional assessment and mechanism insights
            "biological_process_focus": "",       # Primary biological role
            "molecular_function_class": "",       # Molecular mechanism
            "cellular_component_location": "",    # Subcellular localization
            
            # Summary and therapeutic implications
            "evidence_summary": "",               # Human-readable summary
            "mechanism_of_action": "",            # Therapeutic mechanism
            "druggability_assessment": "unknown"  # Drug development potential
        }
    }

def get_census_validation_template() -> Dict[str, Any]:
    """
    Census validation evidence episode template for single-cell expression validation.
    
    Captures single-cell RNA sequencing validation data from CellxGene Census
    specifically focused on Parkinson's disease brain tissue samples. Provides
    tissue-level biological evidence supporting genes as therapeutically relevant
    targets by confirming expression in disease-relevant cell types and brain regions.
    
    The template emphasizes validation metrics including cell counts, expression
    levels, and regional specificity to enable assessment of target expression
    in disease-relevant contexts, complementing genetic and literature evidence.
    
    Returns:
        Dictionary containing census validation structure with fields for:
        - Gene identity and validation context
        - Cell population statistics and expression metrics
        - Brain region and cell type specificity
        - Validation quality and confidence assessment
        - Integration with multi-evidence scoring
        - Cross-validation and quality metrics
    """
    return {
        "census_validation": {
            # Gene identity and validation context
            "gene": "",                            # Target gene symbol
            "ensembl_gene_id": "",                # Gene identifier
            
            # Validation status and outcome
            "validation_status": "pending",        # VALIDATED, NOT_DETECTED, INSUFFICIENT_DATA
            "expression_detected": False,          # Boolean validation result
            "validation_confidence": ConfidenceLevel.LOW.value,
            
            # Cell population statistics
            "total_pd_cells": 0,                  # Total PD brain cells analyzed
            "pd_cells_expressing": 0,             # Cells with detectable expression
            "expression_percentage": 0.0,         # Percentage of cells expressing
            "control_cells_analyzed": 0,          # Control brain cells (if available)
            "control_expression_percentage": 0.0, # Control expression rate
            
            # Expression level metrics
            "mean_expression_level": 0.0,         # Mean expression in expressing cells
            "median_expression_level": 0.0,       # Median expression level
            "expression_variance": 0.0,           # Expression variability
            "max_expression_level": 0.0,          # Peak expression observed
            "expression_threshold": 0.0,          # Detection threshold used
            
            # Tissue and cell type specificity
            "brain_regions": 0,                   # Number of brain regions detected
            "cell_types": 0,                      # Number of cell types detected
            "primary_brain_regions": [],          # Main regions with expression
            "primary_cell_types": [],             # Main cell types with expression
            
            # Disease relevance assessment
            "substantia_nigra_expression": False,  # Expression in SN (PD-relevant)
            "basal_ganglia_expression": False,    # Expression in BG (motor control)
            "dopaminergic_neuron_expression": False, # Dopamine neuron specificity
            "microglia_expression": False,        # Neuroinflammation relevance
            "astrocyte_expression": False,        # Glial cell involvement
            
            # Validation quality metrics
            "sample_size_adequacy": "unknown",    # Statistical power assessment
            "technical_replication": "unknown",  # Technical validation quality
            "cross_dataset_consistency": "unknown", # Consistency across studies
            "data_completeness": 0.0,            # Fraction of expected data present
            
            # Scoring and integration
            "scoring_bonus": 0,                   # Points added to integrated score
            "evidence_weight": 1.0,               # Weight in multi-evidence integration
            "validation_strength": "weak",       # Overall validation assessment
            "clinical_relevance": "unknown",     # Disease mechanism relevance
            
            # Comparative analysis
            "disease_vs_control_fold_change": 0.0, # PD vs control expression ratio
            "statistical_significance": None,     # P-value for expression difference
            "effect_size": 0.0,                  # Magnitude of expression difference
            "confidence_interval": [],           # Statistical confidence bounds
            
            # Technical metadata
            "census_version": "",                 # CellxGene Census version used
            "analysis_date": None,               # When validation was performed
            "sequencing_platform": "",          # Technology used (10X, etc.)
            "normalization_method": "",         # Expression normalization approach
            
            # Cross-validation and external support
            "external_validation_available": False, # Independent validation exists
            "literature_support": False,         # Published expression evidence
            "protein_atlas_concordance": "unknown", # HPA expression agreement
            "gtex_concordance": "unknown",       # GTEx bulk tissue agreement
            
            # Summary and interpretation
            "evidence_summary": "",              # Human-readable validation summary
            "validation_implications": "",       # Therapeutic target implications
            "research_recommendations": "",      # Suggested follow-up studies
            "limitations_noted": []              # Known validation limitations
        }
    }


def get_integration_template() -> Dict[str, Any]:
    """
    Multi-evidence integration episode template for comprehensive target assessment.
    
    Synthesizes evidence from all sources (GWAS, eQTL, literature, pathways) into
    a unified therapeutic target evaluation. Provides detailed breakdowns of how
    different evidence types contribute to the final target prioritization score
    and includes comprehensive therapeutic assessment metrics.
    
    This template represents the culmination of the multi-omics approach, providing
    both quantitative scores and qualitative assessments that guide therapeutic
    target selection and research prioritization decisions.
    
    Returns:
        Dictionary containing integration analysis structure with fields for:
        - Comprehensive target ranking and scoring
        - Evidence type contribution breakdown
        - Therapeutic indicators and development potential
        - Confidence assessment and validation metrics
        - Research recommendations and priority classification
        - Cross-evidence validation and quality assessment
    """
    return {
        "therapeutic_target": {
            # Core target identity and ranking
            "gene": "",                            # Target gene symbol
            "ensembl_gene_id": "",                # Gene identifier
            "rank": None,                          # Overall target ranking
            "enhanced_integrated_score": 0.0,     # Primary composite score
            "integrated_score": 0.0,              # Base GWAS+eQTL score
            "evidence_types": 0,                   # Count of evidence types
            
            # Evidence contribution breakdown
            "evidence_contributions": {
                "gwas_contribution": 0.0,          # Genetic evidence weight
                "eqtl_contribution": 0.0,          # Regulatory evidence weight
                "literature_contribution": 0.0,    # Publication evidence weight
                "pathway_contribution": 0.0        # Functional evidence weight
            },
            
            # Genetic evidence summary
            "genetic_evidence": {
                "min_p_value": None,               # Strongest association
                "max_odds_ratio": None,            # Largest effect size
                "variant_count": 0,                # Associated variants
                "genome_wide_significant": False,  # Significance threshold
                "population_diversity": "unknown"  # Multi-ancestry evidence
            },
            
            # Regulatory evidence summary
            "regulatory_evidence": {
                "eqtl_count": 0,                  # Brain eQTL count
                "tissue_count": 0,                # Tissue breadth
                "min_p_value": None,              # Strongest regulation
                "max_effect_size": None,          # Largest regulatory effect
                "has_substantia_nigra_eqtl": False, # SN specificity
                "has_basal_ganglia_eqtl": False,   # BG specificity
                "brain_specificity": "unknown"    # Brain enrichment
            },
            
            # Literature evidence summary
            "literature_evidence": {
                "total_papers": 0,                # All publications
                "therapeutic_target_papers": 0,   # Target-focused papers
                "clinical_papers": 0,             # Clinical studies
                "literature_evidence_score": 0.0, # Literature composite score
                "has_strong_literature_evidence": False, # Validation flag
                "research_momentum": "unknown"     # Publication trends
            },
            
            # Functional evidence summary
            "functional_evidence": {
                "interaction_count": 0,           # Protein interactions
                "pathway_count": 0,               # Enriched pathways
                "degree_centrality": 0.0,         # Network importance
                "pathway_evidence_score": 0.0,    # Pathway composite score
                "dopamine_pathway_involvement": False, # PD pathway relevance
                "functional_coherence": "unknown" # Biological consistency
            },
            
            # Therapeutic assessment indicators
            "therapeutic_indicators": {
                "has_therapeutic_literature": False,    # Target literature
                "has_clinical_evidence": False,         # Clinical validation
                "has_pathway_relevance": False,         # Mechanism clarity
                "has_brain_specificity": False,         # Tissue relevance
                "has_genetic_validation": False,        # GWAS support
                "druggability_potential": "unknown"     # Drug development
            },
            
            # Confidence and priority assessment
            "confidence_level": ConfidenceLevel.LOW.value,
            "therapeutic_priority": TherapeuticPriority.LOW_PRIORITY.value,
            "target_classification": "unknown",         # Novel/emerging/established
            "development_stage": "discovery",           # Research phase
            
            # Quality metrics and validation
            "evidence_convergence": 0.0,               # Cross-evidence agreement
            "data_completeness": 0.0,                  # Available evidence fraction
            "validation_strength": 0.0,                # Cross-validation score
            "statistical_robustness": "unknown",       # Statistical quality
            
            # Research and development guidance
            "target_summary": "",                      # Executive summary
            "key_evidence_highlights": [],             # Top supporting evidence
            "research_gaps": [],                       # Missing evidence types
            "next_steps_recommendation": "",           # Research priorities
            "competitive_landscape": "unknown",        # Development competition
            
            # Risk assessment and considerations
            "development_risks": [],                   # Potential challenges
            "safety_considerations": [],               # Known safety issues
            "intellectual_property": "unknown",        # Patent landscape
            
            # Temporal and strategic context
            "target_maturity": "unknown",              # Research maturity
            "strategic_value": "unknown",              # Portfolio fit
            "resource_requirements": "unknown"         # Development needs
        }
    }

def get_relationship_template() -> Dict[str, Any]:
    """
    Cross-evidence relationship episode template for entity connections.
    
    Captures relationships between different entities in the knowledge graph,
    including gene-gene interactions, evidence cross-validation, pathway
    co-membership, and literature co-citation patterns. These relationships
    enable graph traversal and discovery of indirect associations.
    
    The template supports both symmetric and asymmetric relationships with
    confidence scoring and temporal context to enable sophisticated network
    analysis and evidence correlation discovery.
    
    Returns:
        Dictionary containing relationship structure with fields for:
        - Entity identifiers and relationship type
        - Relationship strength and confidence metrics
        - Supporting evidence and validation data
        - Temporal context and relationship evolution
        - Bidirectional relationship properties
        - Cross-validation and quality assessment
    """
    return {
        "relationship": {
            # Core relationship definition
            "relationship_type": "",               # Type of relationship
            "source_entity": "",                   # Source entity identifier
            "target_entity": "",                   # Target entity identifier
            "relationship_strength": 0.0,         # Quantitative strength (0-1)
            "confidence_level": ConfidenceLevel.LOW.value,
            
            # Supporting evidence and validation
            "evidence_basis": {},                  # Supporting data
            "statistical_support": {},            # Statistical measures
            "validation_sources": [],             # Cross-validation data
            "replication_count": 0,               # Independent replications
            
            # Relationship characteristics
            "relationship_context": "",           # Descriptive context
            "bidirectional": False,               # Symmetric relationship
            "causal_direction": "unknown",        # Causal relationship
            "mechanism_basis": "",                # Biological mechanism
            
            # Temporal aspects and evolution
            "temporal_aspect": "",                # Time-dependent relationship
            "relationship_stability": "unknown",  # Consistency over time
            "discovery_date": None,               # When relationship identified
            "validation_timeline": [],           # Validation history
            
            # Network and graph properties
            "network_distance": None,             # Graph distance
            "common_neighbors": [],              # Shared connections
            "relationship_importance": 0.0,      # Network centrality
            "clustering_coefficient": 0.0,       # Local connectivity
            
            # Quality and reliability metrics
            "data_quality": "unknown",           # Relationship data quality
            "source_reliability": "unknown",     # Source credibility
            "cross_validation_score": 0.0,       # Multiple source agreement
            "annotation_confidence": "unknown",   # Annotation quality
            
            # Biological and therapeutic context
            "disease_relevance": "unknown",       # PD relevance
            "therapeutic_implications": "",       # Drug development impact
            "pathway_context": [],               # Associated pathways
            "functional_significance": "unknown"  # Biological importance
        }
    }

# ============================================================================
# TEMPLATE UTILITY FUNCTIONS - Template Access and Validation
# ============================================================================

def get_template_by_evidence_type(evidence_type: str) -> Dict[str, Any]:
    """
    Retrieve the appropriate episode template based on evidence type.
    
    Provides a centralized way to access episode templates based on the
    type of evidence being processed. Ensures consistent template usage
    across the episode generation pipeline and simplifies template
    management and updates.
    
    Args:
        evidence_type: Type of evidence ("gwas", "eqtl", "literature", 
                      "pathway", "integration", "relationship", "gene_profile")
    
    Returns:
        Dictionary containing the appropriate episode template
        
    Raises:
        ValueError: If evidence_type is not recognized
    """
    template_map = {
        "gene_profile": get_gene_profile_template,
        "gwas": get_gwas_evidence_template,
        "eqtl": get_eqtl_evidence_template,
        "literature": get_literature_evidence_template,
        "pathway": get_pathway_evidence_template,
        "census_validation": get_census_validation_template,
        "integration": get_integration_template,
        "relationship": get_relationship_template
    }
    
    if evidence_type not in template_map:
        raise ValueError(f"Unknown evidence type: {evidence_type}")
    
    return template_map[evidence_type]()

def get_all_template_types() -> List[str]:
    """
    Get list of all available template types.
    
    Returns:
        List of string identifiers for all available episode templates
    """
    return [
        "gene_profile",
        "gwas", 
        "eqtl",
        "literature",
        "pathway",
        "census_validation",
        "integration",
        "relationship"
    ]

def validate_template_structure(template: Dict[str, Any], template_type: str) -> bool:
    """
    Validate that a template has the expected structure for its type.
    
    Performs basic structural validation to ensure templates contain
    the required top-level keys and maintain consistency across the
    episode generation system.
    
    Args:
        template: Template dictionary to validate
        template_type: Expected template type identifier
        
    Returns:
        True if template structure is valid, False otherwise
    """
    expected_top_keys = {
        "gene_profile": ["gene"],
        "gwas": ["genetic_association"],
        "eqtl": ["regulatory_evidence"],
        "literature": ["literature_evidence"],
        "pathway": ["functional_evidence"],
        "census_validation": ["census_validation"],
        "integration": ["therapeutic_target"],
        "relationship": ["relationship"]
    }
    
    if template_type not in expected_top_keys:
        return False
    
    required_keys = expected_top_keys[template_type]
    return all(key in template for key in required_keys)