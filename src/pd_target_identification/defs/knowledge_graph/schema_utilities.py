# pd_target_identification/defs/knowledge_graph/schema_utilities.py
"""
Utility functions for gene classification, scoring, and analysis in the knowledge graph.
Provides therapeutic priority assessment, confidence determination, evidence breakdown
calculations, and summary generation for multi-omics target discovery data.
"""

from typing import Dict, List, Any, Optional
import math
import re
from datetime import datetime

from .graph_schema import ConfidenceLevel, TherapeuticPriority, TissueRelevance
from .schema_constants import (
    CONFIDENCE_THRESHOLDS,
    DOPAMINE_PATHWAY_KEYWORDS,
    NEURODEGENERATION_KEYWORDS
)


def classify_therapeutic_priority(gene_data: Dict[str, Any]) -> TherapeuticPriority:
    """
    Classifies therapeutic priority based on enhanced integrated score, evidence types,
    and therapeutic literature support using thresholds derived from actual pipeline results.
    """
    score = gene_data.get('enhanced_integrated_score', 0)
    evidence_types = gene_data.get('evidence_types', 0)
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    
    # Based on actual results: SNCA (188.5), HLA-DRA (166.0), LRRK2 (156.6)
    if score >= CONFIDENCE_THRESHOLDS["enhanced_score_very_high"] and evidence_types >= 3 and therapeutic_papers >= 5:
        return TherapeuticPriority.TOP_TIER
    elif score >= CONFIDENCE_THRESHOLDS["enhanced_score_high"] and evidence_types >= 3:
        return TherapeuticPriority.HIGH_PRIORITY
    elif score >= CONFIDENCE_THRESHOLDS["enhanced_score_medium"] and evidence_types >= 2:
        return TherapeuticPriority.MODERATE_PRIORITY
    else:
        return TherapeuticPriority.LOW_PRIORITY


def determine_confidence_level(gene_data: Dict[str, Any]) -> ConfidenceLevel:
    """
    Determines overall confidence level based on evidence convergence across multiple
    data types, with higher confidence for genes having both diverse evidence and validation.
    """
    evidence_types = gene_data.get('evidence_types', 0)
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    pathway_count = gene_data.get('pathway_count', 0)
    has_brain_eqtl = (
        gene_data.get('has_substantia_nigra_eqtl', False) or 
        gene_data.get('has_basal_ganglia_eqtl', False)
    )
    
    # Very high: 4+ evidence types OR 3+ with strong literature validation
    if evidence_types >= 4 or (evidence_types >= 3 and therapeutic_papers >= CONFIDENCE_THRESHOLDS["therapeutic_papers_high"]):
        return ConfidenceLevel.VERY_HIGH
    # High: 3+ evidence types with some validation (literature or brain specificity)
    elif evidence_types >= 3 and (therapeutic_papers > 0 or has_brain_eqtl):
        return ConfidenceLevel.HIGH
    # Medium: 2+ evidence types
    elif evidence_types >= 2:
        return ConfidenceLevel.MEDIUM
    else:
        return ConfidenceLevel.LOW


def calculate_evidence_breakdown(gene_data: Dict[str, Any]) -> Dict[str, float]:
    """
    Calculates the relative contribution of each evidence type to the final integrated
    score by approximating the scoring formula used in the pipeline integration.
    """
    enhanced_score = gene_data.get('enhanced_integrated_score', 0)
    if enhanced_score == 0:
        return {"gwas_eqtl": 0, "literature": 0, "pathway": 0, "other": 0}
    
    # Estimate contributions based on pipeline scoring formula
    base_score = gene_data.get('integrated_score', 0)  # GWAS + eQTL base
    literature_score = gene_data.get('literature_evidence_score', 0)
    pathway_score = gene_data.get('pathway_evidence_score', 0)
    interaction_count = gene_data.get('interaction_count', 0)
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    
    # Approximate the scoring formula from multi_evidence_integrated
    gwas_eqtl_contribution = base_score * 0.4
    literature_contribution = literature_score * 0.5 + therapeutic_papers * 3.0
    pathway_contribution = pathway_score * 1.0 + interaction_count * 1.5
    
    total_estimated = gwas_eqtl_contribution + literature_contribution + pathway_contribution
    
    if total_estimated > 0:
        return {
            "gwas_eqtl": gwas_eqtl_contribution / total_estimated,
            "literature": literature_contribution / total_estimated,
            "pathway": pathway_contribution / total_estimated,
            "other": max(0, 1 - (gwas_eqtl_contribution + literature_contribution + pathway_contribution) / total_estimated)
        }
    else:
        return {"gwas_eqtl": 0.25, "literature": 0.25, "pathway": 0.25, "other": 0.25}


def derive_gene_rank(enhanced_score: float, all_scores: List[float]) -> int:
    """
    Derives gene ranking position from enhanced integrated score by comparing against
    all other genes in the dataset, with ties broken by maintaining original order.
    """
    if not all_scores:
        return 1
    
    sorted_scores = sorted(all_scores, reverse=True)
    try:
        return sorted_scores.index(enhanced_score) + 1
    except ValueError:
        # If score not found, place at end
        return len(sorted_scores) + 1


def generate_target_summary(gene_data: Dict[str, Any]) -> str:
    """
    Generates a human-readable summary describing the gene's therapeutic potential,
    evidence profile, and key distinguishing characteristics for research prioritization.
    """
    gene = gene_data.get('gene_symbol', 'Unknown')
    rank = gene_data.get('rank', 'Unranked')
    score = gene_data.get('enhanced_integrated_score', 0)
    evidence_types = gene_data.get('evidence_types', 0)
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    
    summary = f"{gene} ranks #{rank} with an integrated score of {score:.1f} "
    summary += f"based on {evidence_types} evidence types. "
    
    if therapeutic_papers > 0:
        summary += f"Strong therapeutic focus with {therapeutic_papers} target papers. "
    
    if gene_data.get('has_substantia_nigra_eqtl', False):
        summary += "Shows brain-specific regulatory effects in substantia nigra. "
    
    pathway_count = gene_data.get('pathway_count', 0)
    if pathway_count > 0:
        summary += f"Participates in {pathway_count} enriched pathways. "
    
    priority = classify_therapeutic_priority(gene_data)
    summary += f"Classified as {priority.value.replace('_', ' ')} therapeutic target."
    
    return summary


def extract_dopamine_pathway_involvement(pathways_list: List[str]) -> bool:
    """
    Determines if a gene participates in dopamine-related biological pathways by
    searching pathway names for dopamine biosynthesis and neurotransmitter keywords.
    """
    if not pathways_list:
        return False
    
    for pathway in pathways_list:
        pathway_lower = pathway.lower()
        if any(keyword in pathway_lower for keyword in DOPAMINE_PATHWAY_KEYWORDS):
            return True
    return False


def extract_neurodegeneration_pathway_involvement(pathways_list: List[str]) -> bool:
    """
    Determines if a gene participates in neurodegeneration-related pathways by
    searching for keywords associated with protein misfolding and cellular dysfunction.
    """
    if not pathways_list:
        return False
    
    for pathway in pathways_list:
        pathway_lower = pathway.lower()
        if any(keyword in pathway_lower for keyword in NEURODEGENERATION_KEYWORDS):
            return True
    return False


def assess_brain_tissue_specificity(tissue_data: Dict[str, Any]) -> TissueRelevance:
    """
    Assesses the relevance of eQTL tissue specificity for Parkinson's disease research,
    prioritizing substantia nigra and basal ganglia over general brain regions.
    """
    has_sn = tissue_data.get('has_substantia_nigra_eqtl', False)
    has_bg = tissue_data.get('has_basal_ganglia_eqtl', False)
    tissue_count = tissue_data.get('eqtl_tissue_count', 0)
    
    if has_sn:
        return TissueRelevance.SUBSTANTIA_NIGRA
    elif has_bg:
        return TissueRelevance.BASAL_GANGLIA
    elif tissue_count > 0:  # Assuming brain tissues if eQTLs present in pipeline
        return TissueRelevance.BRAIN_GENERAL
    else:
        return TissueRelevance.NON_BRAIN


def calculate_evidence_convergence(gene_data: Dict[str, Any]) -> float:
    """
    Calculates how well different evidence types agree with each other, returning
    a score from 0-1 where higher values indicate stronger cross-evidence validation.
    """
    evidence_types = gene_data.get('evidence_types', 0)
    if evidence_types < 2:
        return 0.0
    
    # Check for cross-evidence validation patterns
    convergence_score = 0.0
    max_possible = 0.0
    
    # GWAS-eQTL convergence (same genomic regions)
    has_gwas = gene_data.get('gwas_variant_count', 0) > 0
    has_eqtl = gene_data.get('eqtl_count', 0) > 0
    if has_gwas and has_eqtl:
        convergence_score += 0.3
    max_possible += 0.3
    
    # Literature-evidence convergence (validation)
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    if therapeutic_papers > 0 and evidence_types >= 2:
        convergence_score += 0.4
    max_possible += 0.4
    
    # Pathway-network convergence (functional coherence)
    pathway_count = gene_data.get('pathway_count', 0)
    interaction_count = gene_data.get('interaction_count', 0)
    if pathway_count > 0 and interaction_count > 0:
        convergence_score += 0.3
    max_possible += 0.3
    
    return convergence_score / max_possible if max_possible > 0 else 0.0


def calculate_data_completeness(gene_data: Dict[str, Any]) -> float:
    """
    Calculates the fraction of available evidence types that have data for this gene,
    helping identify genes with comprehensive vs. sparse evidence profiles.
    """
    total_possible = 4  # GWAS, eQTL, literature, pathway
    available_count = 0
    
    if gene_data.get('gwas_variant_count', 0) > 0:
        available_count += 1
    if gene_data.get('eqtl_count', 0) > 0:
        available_count += 1
    if gene_data.get('literature_papers_count', 0) > 0:
        available_count += 1
    if gene_data.get('pathway_count', 0) > 0:
        available_count += 1
    
    return available_count / total_possible


def identify_research_gaps(gene_data: Dict[str, Any]) -> List[str]:
    """
    Identifies missing evidence types for a gene, helping prioritize future research
    directions by highlighting gaps in the current evidence profile.
    """
    gaps = []
    
    if gene_data.get('gwas_variant_count', 0) == 0:
        gaps.append("genetic_association")
    if gene_data.get('eqtl_count', 0) == 0:
        gaps.append("regulatory_evidence")
    if gene_data.get('literature_papers_count', 0) == 0:
        gaps.append("literature_validation")
    if gene_data.get('pathway_count', 0) == 0:
        gaps.append("functional_annotation")
    
    # Brain-specific gaps
    if not gene_data.get('has_substantia_nigra_eqtl', False):
        gaps.append("substantia_nigra_specificity")
    if gene_data.get('therapeutic_target_papers', 0) == 0:
        gaps.append("therapeutic_target_literature")
    
    return gaps


def generate_research_recommendations(gene_data: Dict[str, Any]) -> str:
    """
    Generates specific research recommendations based on the gene's evidence profile
    and identified gaps, prioritizing high-impact studies for target validation.
    """
    gaps = identify_research_gaps(gene_data)
    priority = classify_therapeutic_priority(gene_data)
    gene = gene_data.get('gene_symbol', 'Unknown')
    
    if not gaps:
        return f"Complete evidence profile for {gene}. Focus on functional validation and therapeutic development."
    
    recommendations = []
    
    if "genetic_association" in gaps:
        recommendations.append("GWAS validation in larger PD cohorts")
    if "regulatory_evidence" in gaps:
        recommendations.append("brain-specific eQTL analysis")
    if "substantia_nigra_specificity" in gaps:
        recommendations.append("substantia nigra tissue-specific expression studies")
    if "therapeutic_target_literature" in gaps:
        recommendations.append("therapeutic target validation studies")
    if "functional_annotation" in gaps:
        recommendations.append("pathway analysis and protein interaction studies")
    
    if priority in [TherapeuticPriority.TOP_TIER, TherapeuticPriority.HIGH_PRIORITY]:
        recommendations.append("drug target validation and lead compound screening")
    
    return f"Priority research areas for {gene}: " + "; ".join(recommendations[:3]) + "."


def classify_target_novelty(gene_data: Dict[str, Any]) -> str:
    """
    Classifies whether a therapeutic target is novel, emerging, or established based
    on literature volume and therapeutic focus in the research community.
    """
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    total_papers = gene_data.get('literature_papers_count', 0)
    
    if therapeutic_papers >= 10:
        return "established"
    elif therapeutic_papers >= 3 or total_papers >= 15:
        return "emerging"
    else:
        return "novel"


def normalize_gene_symbol(gene_symbol: str) -> str:
    """
    Normalizes gene symbols by removing whitespace, converting to uppercase, and
    handling common formatting variations to ensure consistent representation.
    """
    if not gene_symbol:
        return ""
    
    # Basic normalization
    normalized = str(gene_symbol).strip().upper()
    
    # Remove common prefixes/suffixes that cause inconsistency
    normalized = normalized.replace('ENSG00000', '').replace('_HUMAN', '')
    
    # Remove extra whitespace and special characters
    normalized = re.sub(r'[\r\n\s]+', '', normalized)
    
    return normalized


def calculate_research_momentum(gene_data: Dict[str, Any]) -> str:
    """
    Analyzes publication patterns to determine if research interest in a gene is
    increasing, stable, or decreasing based on recent vs. historical publication rates.
    """
    recent_papers = gene_data.get('recent_papers', 0)
    total_papers = gene_data.get('literature_papers_count', 0)
    most_recent_year = gene_data.get('most_recent_paper_year')
    
    if total_papers == 0:
        return "unknown"
    
    # Simple heuristic: if >50% of papers are recent, momentum is increasing
    recent_fraction = recent_papers / total_papers
    current_year = datetime.now().year
    
    if recent_fraction > 0.5 and most_recent_year and most_recent_year >= current_year - 2:
        return "increasing"
    elif recent_fraction > 0.3:
        return "stable"
    else:
        return "decreasing"