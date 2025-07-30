# pd_target_identification/defs/knowledge_graph/graph_schema.py
"""
Core schema definitions for the Parkinson's Disease target discovery knowledge graph.

This module defines the fundamental data structures, entity models, and type
enumerations that form the foundation of the knowledge graph representation.
It provides strongly-typed schemas for genes, variants, publications, pathways,
and the relationships between them, ensuring data consistency and type safety
across the entire knowledge graph implementation.

The schemas are designed to capture multi-omics evidence from GWAS associations,
brain eQTL data, literature analysis, and protein interaction networks, providing
a comprehensive representation of therapeutic target evidence.
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

# ============================================================================
# CORE ENUMERATIONS - Type Safety for Evidence Classification
# ============================================================================

class EvidenceType(str, Enum):
    """
    Classification of evidence types supported in the target discovery pipeline.
    
    Each evidence type represents a different aspect of therapeutic target validation,
    from genetic associations to functional annotations. The multi-evidence approach
    provides comprehensive target assessment by integrating across these dimensions.
    """
    GWAS = "gwas"                    # Genome-wide association studies
    EQTL = "eqtl"                   # Expression quantitative trait loci
    LITERATURE = "literature"        # Publication and citation analysis
    PATHWAY = "pathway"             # Protein interactions and functional pathways
    INTEGRATION = "integration"     # Multi-evidence synthesis and scoring

class ConfidenceLevel(str, Enum):
    """
    Standardized confidence levels for evidence assessment and target prioritization.
    
    These levels reflect the strength of evidence convergence across multiple data
    sources, with higher confidence indicating greater therapeutic target potential
    based on statistical significance, literature validation, and functional coherence.
    """
    LOW = "low"                     # Limited evidence or single source validation
    MEDIUM = "medium"               # Moderate evidence from multiple sources
    HIGH = "high"                   # Strong evidence convergence across sources
    VERY_HIGH = "very_high"         # Exceptional evidence with literature validation

class TherapeuticPriority(str, Enum):
    """
    Target prioritization categories based on integrated evidence scoring.
    
    Priority levels are determined by enhanced integrated scores, evidence type
    diversity, and therapeutic literature validation. These categories guide
    research resource allocation and drug development decision-making.
    """
    TOP_TIER = "top_tier"                   # Score >150, strong literature (SNCA-level)
    HIGH_PRIORITY = "high_priority"         # Score >100, multiple evidence types
    MODERATE_PRIORITY = "moderate_priority" # Score >60, some validation
    LOW_PRIORITY = "low_priority"           # Score ≤60, limited evidence

class TissueRelevance(str, Enum):
    """
    Brain tissue specificity classification for Parkinson's disease relevance.
    
    Tissue relevance reflects the anatomical specificity of gene expression
    and regulatory effects, with substantia nigra being the most relevant
    for PD pathology due to preferential dopaminergic neuron loss.
    """
    SUBSTANTIA_NIGRA = "substantia_nigra"   # Primary PD-affected brain region
    BASAL_GANGLIA = "basal_ganglia"         # Motor control circuit involvement
    BRAIN_GENERAL = "brain_general"         # General brain tissue expression
    NON_BRAIN = "non_brain"                 # No brain-specific evidence

class RelationshipType(str, Enum):
    """
    Types of relationships that can exist between entities in the knowledge graph.
    
    These relationships capture the connections between genes, evidence types,
    and biological processes, enabling graph traversal and discovery of
    indirect associations and evidence convergence patterns.
    """
    GENE_SUPPORTED_BY_EVIDENCE = "gene_supported_by_evidence"
    GENE_INTERACTS_WITH_GENE = "gene_interacts_with_gene"
    GENE_PARTICIPATES_IN_PATHWAY = "gene_participates_in_pathway"
    EVIDENCE_REINFORCES_EVIDENCE = "evidence_reinforces_evidence"
    VARIANT_REGULATES_GENE = "variant_regulates_gene"
    PUBLICATION_VALIDATES_TARGET = "publication_validates_target"

# ============================================================================
# ENTITY MODELS - Structured Data Representations
# ============================================================================

class GeneEntity(BaseModel):
    """
    Comprehensive gene entity model capturing identity, mappings, and metadata.
    
    Represents a gene as the central entity in the knowledge graph, including
    standardized identifiers from multiple databases, mapping confidence metrics,
    and basic functional annotations. This serves as the anchor point for all
    evidence types and cross-references.
    
    Attributes:
        gene_symbol: Primary HGNC gene symbol for human identification
        ensembl_id: Ensembl database gene identifier with version
        entrez_id: NCBI Entrez gene database identifier
        aliases: Alternative names and symbols for the gene
        chromosome: Chromosomal location (e.g., "4", "X", "MT")
        gene_type: Biotype classification (protein_coding, lncRNA, etc.)
        description: Functional description of the gene product
        mapping_confidence: Quality assessment of identifier mappings
    """
    gene_symbol: str = Field(description="Primary HGNC gene symbol")
    ensembl_id: Optional[str] = Field(default=None, description="Ensembl gene ID with version")
    entrez_id: Optional[str] = Field(default=None, description="NCBI Entrez gene ID")
    aliases: List[str] = Field(default_factory=list, description="Alternative gene names")
    chromosome: Optional[str] = Field(default=None, description="Chromosomal location")
    gene_type: str = Field(default="protein_coding", description="Gene biotype classification")
    description: Optional[str] = Field(default=None, description="Functional description")
    mapping_confidence: str = Field(default="unknown", description="Identifier mapping quality")

class VariantEntity(BaseModel):
    """
    Genetic variant entity model for GWAS associations and eQTL mapping.
    
    Represents genetic variants including SNPs, indels, and structural variants
    that show association with disease risk or gene expression regulation.
    Includes genomic coordinates, allele information, and population genetics data.
    
    Attributes:
        variant_id: Primary variant identifier (dbSNP rsID or coordinate-based)
        chromosome: Chromosomal location of the variant
        position: Genomic coordinate position (GRCh37/GRCh38)
        ref_allele: Reference allele sequence
        alt_allele: Alternative allele sequence
        variant_type: Classification (SNV, indel, CNV, etc.)
        population_frequency: Minor allele frequency in reference populations
    """
    variant_id: str = Field(description="Primary variant identifier")
    chromosome: str = Field(description="Chromosome number or name")
    position: int = Field(description="Genomic coordinate position")
    ref_allele: Optional[str] = Field(default=None, description="Reference allele")
    alt_allele: Optional[str] = Field(default=None, description="Alternative allele")
    variant_type: str = Field(default="SNV", description="Variant classification")
    population_frequency: Optional[float] = Field(default=None, description="Minor allele frequency")

class PublicationEntity(BaseModel):
    """
    Scientific publication entity model for literature evidence tracking.
    
    Represents peer-reviewed publications that provide evidence for therapeutic
    targets, including bibliographic metadata, research focus classification,
    and evidence quality metrics. Enables temporal analysis of research trends
    and validation of computational predictions.
    
    Attributes:
        pmid: PubMed identifier for unique publication reference
        title: Full publication title
        authors: Author list with first author emphasis
        journal: Publication venue and impact information
        publication_year: Year of publication for temporal analysis
        evidence_classification: Type of evidence provided (therapeutic, clinical, etc.)
        research_focus: Primary research area and methodology
    """
    pmid: str = Field(description="PubMed identifier")
    title: str = Field(description="Publication title")
    authors: Optional[str] = Field(default=None, description="Author information")
    journal: Optional[str] = Field(default=None, description="Journal name")
    publication_year: Optional[int] = Field(default=None, description="Publication year")
    evidence_classification: List[str] = Field(default_factory=list, description="Evidence types")
    research_focus: Optional[str] = Field(default=None, description="Primary research area")

class PathwayEntity(BaseModel):
    """
    Biological pathway entity model for functional annotation and enrichment.
    
    Represents biological pathways, molecular functions, and cellular processes
    from ontologies like Gene Ontology, KEGG, and Reactome. Includes statistical
    enrichment data and disease relevance annotations for pathway-based target
    prioritization.
    
    Attributes:
        pathway_id: Standardized pathway identifier (GO:XXXXXXX, KEGG, etc.)
        pathway_name: Human-readable pathway name
        pathway_type: Category (biological_process, molecular_function, etc.)
        description: Detailed pathway description and function
        gene_count: Number of genes annotated to this pathway
        disease_relevance: Relevance to neurodegeneration and PD pathology
        ontology_source: Source database and version information
    """
    pathway_id: str = Field(description="Standardized pathway identifier")
    pathway_name: str = Field(description="Human-readable pathway name")
    pathway_type: str = Field(description="Pathway category classification")
    description: Optional[str] = Field(default=None, description="Detailed description")
    gene_count: Optional[int] = Field(default=None, description="Associated gene count")
    disease_relevance: str = Field(default="unknown", description="PD relevance assessment")
    ontology_source: Optional[str] = Field(default=None, description="Source database")

# ============================================================================
# RELATIONSHIP MODELS - Entity Connections and Associations
# ============================================================================

class EvidenceRelationship(BaseModel):
    """
    Base model for evidence-based relationships between entities in the knowledge graph.
    
    Captures the fundamental structure of how different types of evidence connect
    genes, variants, pathways, and publications. Includes confidence metrics,
    statistical support, and temporal information for relationship validation.
    
    Attributes:
        source_entity: Identifier of the source entity in the relationship
        target_entity: Identifier of the target entity in the relationship
        relationship_type: Enumerated type of the relationship
        evidence_strength: Quantitative measure of relationship confidence (0-1)
        confidence_level: Categorical confidence assessment
        supporting_data: Additional metadata supporting the relationship
        temporal_context: Time-based information about the relationship
    """
    source_entity: str = Field(description="Source entity identifier")
    target_entity: str = Field(description="Target entity identifier")
    relationship_type: RelationshipType = Field(description="Type of relationship")
    evidence_strength: float = Field(ge=0.0, le=1.0, description="Relationship confidence score")
    confidence_level: ConfidenceLevel = Field(description="Categorical confidence")
    supporting_data: Dict[str, Any] = Field(default_factory=dict, description="Supporting metadata")
    temporal_context: Optional[str] = Field(default=None, description="Temporal information")

class GeneticAssociation(EvidenceRelationship):
    """
    Specialized relationship model for GWAS genetic associations.
    
    Extends the base evidence relationship to capture specific statistical
    measures from genome-wide association studies, including p-values,
    effect sizes, and population-specific information critical for
    genetic target validation.
    
    Attributes:
        p_value: Statistical significance of the genetic association
        odds_ratio: Effect size measure for risk association
        beta_coefficient: Linear effect size for quantitative traits
        population: Study population ancestry and demographics
        sample_size: Total sample size for statistical power assessment
        study_design: GWAS methodology and quality information
    """
    p_value: float = Field(description="Statistical significance p-value")
    odds_ratio: Optional[float] = Field(default=None, description="Risk association effect size")
    beta_coefficient: Optional[float] = Field(default=None, description="Linear effect size")
    population: str = Field(description="Study population ancestry")
    sample_size: Optional[int] = Field(default=None, description="Total sample size")
    study_design: Optional[str] = Field(default=None, description="GWAS methodology")

class RegulatoryRelationship(EvidenceRelationship):
    """
    Specialized relationship model for eQTL regulatory associations.
    
    Captures the regulatory effects of genetic variants on gene expression,
    particularly in brain tissues relevant to Parkinson's disease pathology.
    Includes tissue-specific information and directional effects critical
    for understanding disease mechanisms.
    
    Attributes:
        tissue_context: Specific tissue where regulation occurs
        effect_size: Magnitude of regulatory effect on expression
        direction: Direction of regulatory effect (positive/negative)
        tissue_specificity: Breadth of regulatory effect across tissues
        clinical_relevance: Relevance to disease pathology and symptoms
        regulatory_mechanism: Proposed mechanism of gene regulation
    """
    tissue_context: str = Field(description="Regulatory tissue context")
    effect_size: float = Field(description="Magnitude of regulatory effect")
    direction: str = Field(description="Direction of regulation")
    tissue_specificity: str = Field(description="Tissue-specific vs broad regulation")
    clinical_relevance: TissueRelevance = Field(description="Disease relevance")
    regulatory_mechanism: Optional[str] = Field(default=None, description="Regulation mechanism")

class FunctionalAssociation(EvidenceRelationship):
    """
    Specialized relationship model for protein interactions and pathway membership.
    
    Represents functional relationships between genes through protein-protein
    interactions, pathway co-membership, and functional enrichment patterns.
    Critical for understanding biological context and therapeutic mechanism
    of action for target validation.
    
    Attributes:
        interaction_type: Type of functional relationship
        confidence_score: Experimental or computational confidence
        evidence_sources: Supporting databases and experimental methods
        biological_context: Cellular and physiological context
        therapeutic_relevance: Potential for therapeutic intervention
        pathway_context: Associated biological pathways and processes
    """
    interaction_type: str = Field(description="Type of functional interaction")
    confidence_score: float = Field(description="Interaction confidence")
    evidence_sources: List[str] = Field(default_factory=list, description="Supporting evidence")
    biological_context: Optional[str] = Field(default=None, description="Biological context")
    therapeutic_relevance: str = Field(default="unknown", description="Therapeutic potential")
    pathway_context: List[str] = Field(default_factory=list, description="Associated pathways")