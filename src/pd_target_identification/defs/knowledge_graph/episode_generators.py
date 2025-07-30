# pd_target_identification/defs/knowledge_graph/episode_generators.py
"""
Episode generation functions for transforming pipeline data into Graphiti episodes.

This module provides the core functionality to convert multi-omics pipeline DataFrames
into properly structured Graphiti episodes for knowledge graph ingestion. Each function
handles a specific evidence type and ensures proper validation, error handling, and
JSON serialization compatibility.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime

from .episode_templates import (
    get_gene_profile_template,
    get_gwas_evidence_template,
    get_eqtl_evidence_template,
    get_literature_evidence_template,
    get_pathway_evidence_template
)
from .schema_validation import safe_episode_creation, validate_gene_data
from .schema_utilities import (
    classify_therapeutic_priority,
    determine_confidence_level,
    calculate_data_completeness,
    normalize_gene_symbol
)
from .schema_constants import DEFAULT_GROUP_ID
from .pathway_utilities import (
    extract_interaction_details,
    extract_pathway_details,
    calculate_interaction_evidence_scores,
    assess_dopamine_pathway_involvement,
    assess_mitochondrial_involvement,
    assess_neurodegeneration_pathway_involvement,
    assess_pd_pathway_enrichment,
    determine_functional_coherence,
    determine_biological_process_focus,
    determine_molecular_function_class,
    determine_cellular_component_location,
    generate_pathway_summary,
    generate_mechanism_hypothesis,
    assess_druggability_potential
)

# ============================================================================
# CORE EPISODE CREATORS - One per evidence type
# ============================================================================

def create_gene_profile_episode(
    gene_data: Dict[str, Any], 
    all_scores: List[float],
    gene_mapping_data: Optional[Dict[str, Any]] = None,
    group_id: str = DEFAULT_GROUP_ID
) -> Dict[str, Any]:
    """
    Create comprehensive gene profile episode from multi_evidence_integrated data.
    
    Transforms a single gene's integrated data into a complete gene profile episode
    that serves as the central entity in the knowledge graph. Includes all scoring
    results, evidence summaries, and derived insights.
    
    Args:
        gene_data: Row from multi_evidence_integrated DataFrame as dict
        all_scores: List of all enhanced_integrated_scores for ranking
        gene_mapping_data: Optional gene mapping info (ensembl_id, entrez_id)
        group_id: Graphiti group identifier
        
    Returns:
        Complete gene profile episode ready for Graphiti ingestion
    """
    # Get template and extract core data
    template = get_gene_profile_template()
    gene_symbol = normalize_gene_symbol(gene_data.get('gene_symbol', ''))
    
    if not gene_symbol:
        raise ValueError("Gene symbol is required for gene profile episode")
    
    # Validate input data
    is_valid, errors = validate_gene_data(gene_data)
    if not is_valid:
        print(f"Gene data validation warnings for {gene_symbol}: {errors}")
    
    # Populate core gene identity
    gene_profile = template['gene']
    gene_profile['symbol'] = gene_symbol
    
    # Add gene mapping data if available
    if gene_mapping_data:
        gene_profile['ensembl_id'] = gene_mapping_data.get('ensembl_id', '')
        gene_profile['entrez_id'] = str(gene_mapping_data.get('entrez_id', ''))
        gene_profile['mapping_source'] = gene_mapping_data.get('mapping_source', 'unknown')
        gene_profile['mapping_confidence'] = gene_mapping_data.get('mapping_confidence', 'unknown')
    
    # Core integration results - direct mapping from pipeline
    gene_profile['rank'] = int(gene_data.get('rank', 0))
    gene_profile['enhanced_integrated_score'] = float(gene_data.get('enhanced_integrated_score', 0.0))
    gene_profile['integrated_score'] = float(gene_data.get('integrated_score', 0.0))
    gene_profile['evidence_types'] = int(gene_data.get('evidence_types', 0))
    
    # Evidence summary counts for quick assessment
    gene_profile['gwas_variant_count'] = int(gene_data.get('gwas_variant_count', 0))
    gene_profile['eqtl_count'] = int(gene_data.get('eqtl_count', 0))
    gene_profile['literature_papers_count'] = int(gene_data.get('literature_papers_count', 0))
    gene_profile['interaction_count'] = int(gene_data.get('interaction_count', 0))
    gene_profile['pathway_count'] = int(gene_data.get('pathway_count', 0))
    
    # Key biological indicators from pipeline
    gene_profile['has_substantia_nigra_eqtl'] = bool(gene_data.get('has_substantia_nigra_eqtl', False))
    gene_profile['has_basal_ganglia_eqtl'] = bool(gene_data.get('has_basal_ganglia_eqtl', False))
    gene_profile['has_strong_literature_evidence'] = bool(gene_data.get('has_strong_literature_evidence', False))
    
    # Derived assessments using utility functions
    gene_profile['confidence_level'] = determine_confidence_level(gene_data).value
    gene_profile['therapeutic_priority'] = classify_therapeutic_priority(gene_data).value
    
    # Calculate data completeness and quality metrics
    gene_profile['data_completeness'] = calculate_data_completeness(gene_data)
    gene_profile['validation_status'] = "validated" if is_valid else "warnings"
    
    # Update timestamp
    gene_profile['last_updated'] = datetime.now().isoformat()
    
    # Create episode with comprehensive error handling
    episode_name = f"Gene_Profile_{gene_symbol}"
    source_description = f"Gene profile for {gene_symbol} from PD target discovery pipeline"
    
    return safe_episode_creation(
        episode_name=episode_name,
        episode_data=template,
        source_desc=source_description,
        group_id=group_id
    )


def create_gwas_evidence_episode(
    gene_symbol: str,
    gwas_data: List[Dict[str, Any]],
    group_id: str = DEFAULT_GROUP_ID
) -> Dict[str, Any]:
    """
    Create GWAS evidence episode from genetic association data.
    
    Transforms GWAS variant data for a specific gene into a comprehensive
    genetic evidence episode including variant details, statistical measures,
    and population information.
    
    Args:
        gene_symbol: Target gene symbol
        gwas_data: List of GWAS records for this gene
        group_id: Graphiti group identifier
        
    Returns:
        GWAS evidence episode ready for Graphiti ingestion
    """
    template = get_gwas_evidence_template()
    gene_symbol = normalize_gene_symbol(gene_symbol)
    
    if not gwas_data:
        print(f"No GWAS data available for {gene_symbol}")
        # Return minimal episode
        template['genetic_association']['gene'] = gene_symbol
        template['genetic_association']['evidence_summary'] = f"No GWAS associations found for {gene_symbol}"
    else:
        # Extract variant details
        variant_details = extract_variant_details(gwas_data)
        
        # Populate genetic association data
        genetic_assoc = template['genetic_association']
        genetic_assoc['gene'] = gene_symbol
        genetic_assoc['variant_count'] = len(gwas_data)
        
        # Calculate summary statistics
        p_values = [float(v.get('p_value', 1.0)) for v in gwas_data if v.get('p_value')]
        odds_ratios = [float(v.get('odds_ratio', 1.0)) for v in gwas_data if v.get('odds_ratio')]
        
        if p_values:
            genetic_assoc['min_p_value'] = min(p_values)
            genetic_assoc['genome_wide_significant'] = min(p_values) <= 5e-8
        
        if odds_ratios:
            genetic_assoc['max_odds_ratio'] = max(odds_ratios)
            genetic_assoc['max_effect_size'] = max(odds_ratios)
        
        # Store variant details
        genetic_assoc['variants'] = variant_details
        genetic_assoc['chromosomal_locations'] = [v.get('chromosome_position') for v in variant_details if v.get('chromosome_position')]
        genetic_assoc['effect_alleles'] = [v.get('effect_allele') for v in variant_details if v.get('effect_allele')]
        
        # Population and study info
        populations = list(set(v.get('population', 'EUR') for v in gwas_data))
        genetic_assoc['populations_studied'] = populations
        
        # Generate summary
        significance = "genome-wide significant" if genetic_assoc['genome_wide_significant'] else "suggestive"
        genetic_assoc['evidence_summary'] = f"{gene_symbol} shows {significance} associations with {len(gwas_data)} variants"
    
    episode_name = f"GWAS_Evidence_{gene_symbol}"
    source_description = f"GWAS genetic associations for {gene_symbol}"
    
    return safe_episode_creation(
        episode_name=episode_name,
        episode_data=template,
        source_desc=source_description,
        group_id=group_id
    )


def create_eqtl_evidence_episode(
    gene_symbol: str,
    eqtl_data: List[Dict[str, Any]],
    group_id: str = DEFAULT_GROUP_ID
) -> Dict[str, Any]:
    """
    Create eQTL evidence episode from brain regulatory data.
    
    Transforms brain eQTL data for a specific gene into a comprehensive
    regulatory evidence episode including tissue specificity, effect sizes,
    and disease relevance assessment.
    
    Args:
        gene_symbol: Target gene symbol
        eqtl_data: List of brain eQTL records for this gene
        group_id: Graphiti group identifier
        
    Returns:
        eQTL evidence episode ready for Graphiti ingestion
    """
    template = get_eqtl_evidence_template()
    gene_symbol = normalize_gene_symbol(gene_symbol)
    
    if not eqtl_data:
        print(f"No eQTL data available for {gene_symbol}")
        template['regulatory_evidence']['gene'] = gene_symbol
        template['regulatory_evidence']['evidence_summary'] = f"No brain eQTLs found for {gene_symbol}"
    else:
        # Extract eQTL details
        eqtl_details = extract_eqtl_details(eqtl_data)
        
        # Populate regulatory evidence
        reg_evidence = template['regulatory_evidence']
        reg_evidence['gene'] = gene_symbol
        reg_evidence['eqtl_count'] = len(eqtl_data)
        
        # Calculate tissue specificity
        tissues = [record.get('tissue_id', '') for record in eqtl_data if record.get('tissue_id')]
        unique_tissues = list(set(tissues))
        reg_evidence['tissue_count'] = len(unique_tissues)
        reg_evidence['tissues_with_eqtls'] = len(unique_tissues)
        
        # Check for PD-relevant tissues
        reg_evidence['has_substantia_nigra_eqtl'] = any('substantia_nigra' in tissue.lower() for tissue in tissues)
        reg_evidence['has_basal_ganglia_eqtl'] = any(
            any(bg_term in tissue.lower() for bg_term in ['basal_ganglia', 'putamen', 'caudate', 'striatum'])
            for tissue in tissues
        )
        
        # Calculate effect statistics
        effect_sizes = [float(record.get('effect_size', 0.0)) for record in eqtl_data if record.get('effect_size')]
        p_values = [float(record.get('p_value', 1.0)) for record in eqtl_data if record.get('p_value')]
        
        if effect_sizes:
            reg_evidence['mean_effect_size'] = sum(effect_sizes) / len(effect_sizes)
            reg_evidence['max_effect_size'] = max(abs(es) for es in effect_sizes)
            
        if p_values:
            reg_evidence['min_p_value'] = min(p_values)
        
        # Store detailed eQTL records
        reg_evidence['brain_eqtls'] = eqtl_details
        
        # Assess brain specificity
        reg_evidence['brain_enriched'] = len(unique_tissues) > 0
        
        # Generate summary
        tissue_summary = f"{len(unique_tissues)} brain tissues" if len(unique_tissues) > 1 else unique_tissues[0] if unique_tissues else "unknown tissues"
        reg_evidence['evidence_summary'] = f"{gene_symbol} shows regulatory effects in {tissue_summary} with {len(eqtl_data)} eQTLs"
    
    episode_name = f"eQTL_Evidence_{gene_symbol}"
    source_description = f"Brain eQTL regulatory evidence for {gene_symbol}"
    
    return safe_episode_creation(
        episode_name=episode_name,
        episode_data=template,
        source_desc=source_description,
        group_id=group_id
    )


def create_literature_evidence_episode(
    gene_symbol: str,
    literature_data: Dict[str, Any],
    group_id: str = DEFAULT_GROUP_ID
) -> Dict[str, Any]:
    """
    Create literature evidence episode from PubMed publication data.
    
    Transforms literature analysis data for a specific gene into a comprehensive
    publication evidence episode including paper counts, evidence classification,
    research trends, and therapeutic focus assessment.
    
    Args:
        gene_symbol: Target gene symbol
        literature_data: Dictionary containing literature analysis results
        group_id: Graphiti group identifier
        
    Returns:
        Literature evidence episode ready for Graphiti ingestion
    """
    template = get_literature_evidence_template()
    gene_symbol = normalize_gene_symbol(gene_symbol)
    
    if not literature_data:
        print(f"No literature data available for {gene_symbol}")
        template['literature_evidence']['gene'] = gene_symbol
        template['literature_evidence']['evidence_summary'] = f"No literature evidence found for {gene_symbol}"
    else:
        # Populate literature evidence data
        lit_evidence = template['literature_evidence']
        lit_evidence['gene'] = gene_symbol
        
        # Publication counts by evidence type
        lit_evidence['total_papers'] = int(literature_data.get('literature_papers_count', 0))
        lit_evidence['therapeutic_target_papers'] = int(literature_data.get('therapeutic_target_papers', 0))
        lit_evidence['clinical_papers'] = int(literature_data.get('clinical_papers', 0))
        lit_evidence['biomarker_papers'] = int(literature_data.get('biomarker_papers', 0))
        lit_evidence['recent_papers'] = int(literature_data.get('recent_papers', 0))
        
        # Literature scoring and assessment
        lit_evidence['literature_evidence_score'] = float(literature_data.get('literature_evidence_score', 0.0))
        lit_evidence['avg_evidence_score'] = float(literature_data.get('avg_evidence_score', 0.0))
        lit_evidence['max_evidence_score'] = float(literature_data.get('max_evidence_score', 0.0))
        lit_evidence['has_strong_literature_evidence'] = bool(literature_data.get('has_strong_literature_evidence', False))
        
        # Research quality indicators
        lit_evidence['top_evidence_paper_pmid'] = str(literature_data.get('top_evidence_paper_pmid', ''))
        lit_evidence['top_evidence_score'] = float(literature_data.get('top_evidence_score', 0.0))
        lit_evidence['journals_count'] = int(literature_data.get('journals_count', 0))
        lit_evidence['therapeutic_keywords_summary'] = str(literature_data.get('therapeutic_keywords_summary', ''))
        
        # Temporal analysis and research trends
        lit_evidence['most_recent_paper_year'] = literature_data.get('most_recent_paper_year')
        
        # Publication timeline - handle both dict and string formats
        pub_timeline = literature_data.get('publication_timeline', {})
        if isinstance(pub_timeline, str):
            try:
                import json
                pub_timeline = json.loads(pub_timeline)
            except:
                pub_timeline = {}
        lit_evidence['publication_timeline'] = pub_timeline
        
        # Research momentum assessment
        lit_evidence['research_momentum'] = calculate_research_momentum(literature_data)
        lit_evidence['peak_research_period'] = str(literature_data.get('peak_research_period', ''))
        
        # Evidence type breakdown
        evidence_scores = {
            'therapeutic_target': float(literature_data.get('therapeutic_target_score', 0.0)),
            'biomarker': float(literature_data.get('biomarker_score', 0.0)),
            'pathogenesis': float(literature_data.get('pathogenesis_score', 0.0)),
            'clinical': float(literature_data.get('clinical_score', 0.0)),
            'functional': float(literature_data.get('functional_score', 0.0))
        }
        lit_evidence['evidence_type_scores'] = evidence_scores
        
        # Research focus and development status
        lit_evidence['primary_research_focus'] = determine_primary_research_focus(evidence_scores)
        lit_evidence['clinical_development_stage'] = assess_clinical_development_stage(literature_data)
        
        # Extract therapeutic modalities if available
        therapeutic_modalities = literature_data.get('therapeutic_modalities', [])
        if isinstance(therapeutic_modalities, str):
            therapeutic_modalities = [therapeutic_modalities]
        lit_evidence['therapeutic_modalities'] = therapeutic_modalities
        
        # Literature validation and cross-referencing
        lit_evidence['literature_strength'] = determine_literature_strength(literature_data)
        lit_evidence['citation_network_size'] = int(literature_data.get('citation_network_size', 0))
        lit_evidence['research_community_size'] = int(literature_data.get('research_community_size', 0))
        
        # Generate comprehensive summary
        lit_evidence['evidence_summary'] = generate_literature_summary(gene_symbol, literature_data)
        
        # Research gaps and future directions
        lit_evidence['research_gaps'] = identify_literature_research_gaps(literature_data)
        lit_evidence['future_directions'] = generate_literature_future_directions(literature_data)
    
    episode_name = f"Literature_Evidence_{gene_symbol}"
    source_description = f"Literature evidence from PubMed analysis for {gene_symbol}"
    
    return safe_episode_creation(
        episode_name=episode_name,
        episode_data=template,
        source_desc=source_description,
        group_id=group_id
    )


# ============================================================================
# UTILITY FUNCTIONS - Data extraction and processing
# ============================================================================

def extract_variant_details(gwas_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract and structure variant details from GWAS records."""
    variant_details = []
    
    for record in gwas_records:
        variant = {
            'variant_id': record.get('variant_id', ''),
            'rsid': record.get('rsid', ''),
            'chromosome': record.get('chromosome', ''),
            'position': record.get('position'),
            'p_value': float(record.get('p_value', 1.0)),
            'odds_ratio': float(record.get('odds_ratio', 1.0)),
            'beta': float(record.get('beta', 0.0)),
            'effect_allele': record.get('effect_allele', ''),
            'population': record.get('population', 'EUR'),
            'study_accession': record.get('study_accession', ''),
            'sample_size': record.get('sample_size')
        }
        variant_details.append(variant)
    
    return variant_details


def extract_eqtl_details(eqtl_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract and structure eQTL details from brain eQTL records."""
    eqtl_details = []
    
    for record in eqtl_records:
        eqtl = {
            'tissue_id': record.get('tissue_id', ''),
            'variant_id': record.get('variant_id', ''),
            'snp_id': record.get('snp_id', ''),
            'p_value': float(record.get('p_value', 1.0)),
            'effect_size': float(record.get('effect_size', 0.0)),
            'maf': float(record.get('maf', 0.0)),
            'chromosome': record.get('chromosome', ''),
            'position': record.get('position'),
            'gene_symbol': record.get('gene_symbol', ''),
            'ensembl_gene_id': record.get('ensembl_gene_id', '')
        }
        eqtl_details.append(eqtl)
    
    return eqtl_details


def generate_episode_name(prefix: str, gene_symbol: str, suffix: str = "") -> str:
    """Generate standardized episode name following naming conventions."""
    clean_gene = normalize_gene_symbol(gene_symbol)
    name_parts = [prefix, clean_gene]
    if suffix:
        name_parts.append(suffix)
    return "_".join(name_parts)


# ============================================================================
# LITERATURE UTILITY FUNCTIONS - Supporting analysis functions
# ============================================================================

def calculate_research_momentum(literature_data: Dict[str, Any]) -> str:
    """
    Analyze publication patterns to determine research momentum.
    
    Args:
        literature_data: Literature analysis data
        
    Returns:
        Research momentum classification: "increasing", "stable", "decreasing", "unknown"
    """
    recent_papers = literature_data.get('recent_papers', 0)
    total_papers = literature_data.get('literature_papers_count', 0)
    most_recent_year = literature_data.get('most_recent_paper_year')
    
    # Return unknown for very minimal data or no data
    if total_papers == 0 or total_papers < 3:
        return "unknown"
    
    # Simple heuristic: if >50% of papers are recent, momentum is increasing
    recent_fraction = recent_papers / total_papers if total_papers > 0 else 0
    current_year = datetime.now().year
    
    if recent_fraction > 0.5 and most_recent_year and most_recent_year >= current_year - 2:
        return "increasing"
    elif recent_fraction > 0.3:
        return "stable"
    else:
        return "decreasing"


def determine_primary_research_focus(evidence_scores: Dict[str, float]) -> str:
    """
    Determine primary research focus based on evidence type scores.
    
    Args:
        evidence_scores: Dictionary of evidence type scores
        
    Returns:
        Primary research focus area
    """
    if not evidence_scores or all(score == 0.0 for score in evidence_scores.values()):
        return "unknown"
    
    # Find the evidence type with highest score
    max_evidence_type = max(evidence_scores.keys(), key=lambda k: evidence_scores[k])
    
    focus_mapping = {
        'therapeutic_target': 'drug_target_validation',
        'clinical': 'clinical_research',
        'biomarker': 'biomarker_development',
        'pathogenesis': 'disease_mechanism',
        'functional': 'functional_biology'
    }
    
    return focus_mapping.get(max_evidence_type, 'general_research')


def assess_clinical_development_stage(literature_data: Dict[str, Any]) -> str:
    """
    Assess clinical development stage based on literature patterns.
    
    Args:
        literature_data: Literature analysis data
        
    Returns:
        Clinical development stage classification
    """
    therapeutic_papers = literature_data.get('therapeutic_target_papers', 0)
    clinical_papers = literature_data.get('clinical_papers', 0)
    
    if clinical_papers >= 5:
        return "clinical_validation"
    elif therapeutic_papers >= 10:
        return "preclinical_development"
    elif therapeutic_papers >= 3:
        return "target_validation"
    elif therapeutic_papers > 0:
        return "early_discovery"
    else:
        return "basic_research"


def determine_literature_strength(literature_data: Dict[str, Any]) -> str:
    """
    Determine literature evidence strength based on multiple factors.
    
    Args:
        literature_data: Literature analysis data
        
    Returns:
        Literature strength classification from ConfidenceLevel enum
    """
    from .schema_utilities import determine_confidence_level
    
    # Use existing confidence level determination logic
    confidence = determine_confidence_level(literature_data)
    return confidence.value


def generate_literature_summary(gene_symbol: str, literature_data: Dict[str, Any]) -> str:
    """
    Generate human-readable literature evidence summary.
    
    Args:
        gene_symbol: Target gene symbol
        literature_data: Literature analysis data
        
    Returns:
        Comprehensive literature summary string
    """
    total_papers = literature_data.get('literature_papers_count', 0)
    therapeutic_papers = literature_data.get('therapeutic_target_papers', 0)
    clinical_papers = literature_data.get('clinical_papers', 0)
    
    if total_papers == 0:
        return f"No literature evidence found for {gene_symbol} in PubMed search"
    
    summary = f"{gene_symbol} has {total_papers} publications in PD research"
    
    if therapeutic_papers > 0:
        summary += f", including {therapeutic_papers} therapeutic target papers"
    
    if clinical_papers > 0:
        summary += f" and {clinical_papers} clinical studies"
    
    # Add research momentum
    momentum = calculate_research_momentum(literature_data)
    if momentum != "unknown":
        summary += f". Research momentum is {momentum}"
    
    # Add strength assessment
    if literature_data.get('has_strong_literature_evidence', False):
        summary += ". Strong literature validation present"
    
    summary += "."
    return summary


def identify_literature_research_gaps(literature_data: Dict[str, Any]) -> List[str]:
    """
    Identify research gaps based on literature analysis.
    
    Args:
        literature_data: Literature analysis data
        
    Returns:
        List of identified research gaps
    """
    gaps = []
    
    therapeutic_papers = literature_data.get('therapeutic_target_papers', 0)
    clinical_papers = literature_data.get('clinical_papers', 0)
    biomarker_papers = literature_data.get('biomarker_papers', 0)
    
    if therapeutic_papers == 0:
        gaps.append("therapeutic_target_validation")
    elif therapeutic_papers < 3:
        gaps.append("comprehensive_target_assessment")
    
    if clinical_papers == 0:
        gaps.append("clinical_validation")
    
    if biomarker_papers == 0:
        gaps.append("biomarker_development")
    
    # Check for recent research
    recent_papers = literature_data.get('recent_papers', 0)
    if recent_papers == 0:
        gaps.append("current_research_activity")
    
    return gaps


def generate_literature_future_directions(literature_data: Dict[str, Any]) -> str:
    """
    Generate research recommendations based on literature gaps.
    
    Args:
        literature_data: Literature analysis data
        
    Returns:
        Research recommendations string
    """
    gaps = identify_literature_research_gaps(literature_data)
    therapeutic_papers = literature_data.get('therapeutic_target_papers', 0)
    
    if not gaps:
        return "Comprehensive literature coverage. Focus on experimental validation and clinical development."
    
    recommendations = []
    
    if "therapeutic_target_validation" in gaps:
        recommendations.append("initiate therapeutic target validation studies")
    
    if "clinical_validation" in gaps and therapeutic_papers > 0:
        recommendations.append("pursue clinical validation and biomarker studies")
    
    if "current_research_activity" in gaps:
        recommendations.append("investigate with current research methodologies")
    
    if not recommendations:
        recommendations.append("expand research scope based on identified gaps")
    
    return "; ".join(recommendations[:3]) + "."


def create_pathway_evidence_episode(
    gene_symbol: str,
    pathway_data: Dict[str, Any],
    interaction_data: Optional[List[Dict[str, Any]]] = None,
    enrichment_data: Optional[List[Dict[str, Any]]] = None,
    group_id: str = DEFAULT_GROUP_ID
) -> Dict[str, Any]:
    """
    Create pathway evidence episode from protein interaction and functional enrichment data.
    
    Transforms pathway analysis data for a specific gene into a comprehensive
    functional evidence episode including protein interactions, pathway memberships,
    network topology, and disease relevance assessment.
    
    Args:
        gene_symbol: Target gene symbol
        pathway_data: Dictionary containing pathway summary data
        interaction_data: Optional list of protein interaction records
        enrichment_data: Optional list of pathway enrichment records
        group_id: Graphiti group identifier
        
    Returns:
        Pathway evidence episode ready for Graphiti ingestion
    """
    template = get_pathway_evidence_template()
    gene_symbol = normalize_gene_symbol(gene_symbol)
    
    if not pathway_data:
        print(f"No pathway data available for {gene_symbol}")
        template['functional_evidence']['gene'] = gene_symbol
        template['functional_evidence']['evidence_summary'] = f"No pathway evidence found for {gene_symbol}"
    else:
        # Populate functional evidence data
        func_evidence = template['functional_evidence']
        func_evidence['gene'] = gene_symbol
        
        # Network statistics and connectivity
        func_evidence['interaction_count'] = int(pathway_data.get('interaction_count', 0))
        func_evidence['high_confidence_interactions'] = int(pathway_data.get('high_confidence_interactions', 0))
        func_evidence['avg_interaction_confidence'] = float(pathway_data.get('avg_interaction_confidence', 0.0))
        func_evidence['degree_centrality'] = float(pathway_data.get('degree_centrality', 0.0))
        func_evidence['query_gene_interactions'] = int(pathway_data.get('query_gene_interactions', 0))
        
        # Pathway membership and enrichment
        func_evidence['pathway_count'] = int(pathway_data.get('pathway_count', 0))
        func_evidence['significant_pathways'] = int(pathway_data.get('significant_pathways', 0))
        func_evidence['pathway_evidence_score'] = float(pathway_data.get('pathway_evidence_score', 0.0))
        func_evidence['top_pathway'] = str(pathway_data.get('top_pathway', ''))
        
        # Extract pathways list - handle both string and list formats
        pathways_list = pathway_data.get('pathways_list', [])
        if isinstance(pathways_list, str):
            try:
                import json
                pathways_list = json.loads(pathways_list)
            except:
                pathways_list = [pathways_list] if pathways_list else []
        func_evidence['pathways_list'] = pathways_list
        
        # Detailed interaction data
        if interaction_data:
            func_evidence['protein_interactions'] = extract_interaction_details(interaction_data)
            func_evidence['interaction_partners'] = [
                interaction.get('partner_gene', '') for interaction in interaction_data
                if interaction.get('partner_gene')
            ]
            
            # Calculate evidence scores by source type
            evidence_scores = calculate_interaction_evidence_scores(interaction_data)
            func_evidence['evidence_scores'] = evidence_scores
        else:
            func_evidence['protein_interactions'] = []
            func_evidence['interaction_partners'] = []
            func_evidence['evidence_scores'] = {
                'experimental_score': 0.0,
                'database_score': 0.0,
                'textmining_score': 0.0,
                'coexpression_score': 0.0
            }
        
        # Pathway details and enrichment data
        if enrichment_data:
            func_evidence['pathway_memberships'] = extract_pathway_details(enrichment_data)
            func_evidence['go_term_enrichments'] = [
                pathway for pathway in enrichment_data 
                if pathway.get('pathway_id', '').startswith('GO:')
            ]
            func_evidence['kegg_pathway_involvement'] = [
                pathway for pathway in enrichment_data 
                if pathway.get('pathway_source', '').lower() == 'kegg'
            ]
        else:
            func_evidence['pathway_memberships'] = []
            func_evidence['go_term_enrichments'] = []
            func_evidence['kegg_pathway_involvement'] = []
        
        # Disease relevance assessment
        func_evidence['dopamine_pathway_involvement'] = assess_dopamine_pathway_involvement(pathways_list)
        func_evidence['neurodegeneration_pathways'] = assess_neurodegeneration_pathway_involvement(pathways_list)
        func_evidence['pd_pathway_enrichment'] = assess_pd_pathway_enrichment(pathways_list)
        func_evidence['mitochondrial_involvement'] = assess_mitochondrial_involvement(pathways_list)
        
        # Network properties and system analysis
        func_evidence['combined_pathway_score'] = float(pathway_data.get('combined_pathway_score', 0.0))
        func_evidence['functional_coherence'] = determine_functional_coherence(pathway_data)
        func_evidence['network_centrality_rank'] = pathway_data.get('network_centrality_rank')
        func_evidence['pathway_connectivity'] = float(pathway_data.get('pathway_connectivity', 0.0))
        
        # Functional assessment and mechanism insights
        func_evidence['biological_process_focus'] = determine_biological_process_focus(enrichment_data or [])
        func_evidence['molecular_function_class'] = determine_molecular_function_class(enrichment_data or [])
        func_evidence['cellular_component_location'] = determine_cellular_component_location(enrichment_data or [])
        
        # Generate comprehensive summary
        func_evidence['evidence_summary'] = generate_pathway_summary(gene_symbol, pathway_data, pathways_list)
        func_evidence['mechanism_of_action'] = generate_mechanism_hypothesis(gene_symbol, pathway_data, pathways_list)
        func_evidence['druggability_assessment'] = assess_druggability_potential(pathway_data)
    
    episode_name = f"Pathway_Evidence_{gene_symbol}"
    source_description = f"Pathway and functional evidence for {gene_symbol}"
    
    return safe_episode_creation(
        episode_name=episode_name,
        episode_data=template,
        source_desc=source_description,
        group_id=group_id
    )

# ============================================================================
# BATCH PROCESSING FUNCTIONS - For efficient episode generation
# ============================================================================

def generate_episodes_for_gene(
    gene_symbol: str, 
    all_pipeline_data: Dict[str, Any],
    group_id: str = DEFAULT_GROUP_ID
) -> List[Dict[str, Any]]:
    """
    Generate all episode types for a single gene from complete pipeline data.
    
    Creates a complete set of episodes (gene profile + all evidence types)
    for a single gene, ensuring all cross-references are consistent.
    
    Args:
        gene_symbol: Target gene to generate episodes for
        all_pipeline_data: Dictionary containing all pipeline DataFrames
        group_id: Graphiti group identifier
        
    Returns:
        List of all episodes for this gene
    """
    episodes = []
    gene_symbol = normalize_gene_symbol(gene_symbol)
    
    # Extract gene-specific data from each DataFrame
    multi_evidence = all_pipeline_data.get('multi_evidence_integrated')
    gene_mapping = all_pipeline_data.get('gene_mapping_table')
    gwas_data = all_pipeline_data.get('gwas_data_with_mappings')
    eqtl_data = all_pipeline_data.get('gtex_brain_eqtls')
    
    if multi_evidence is None:
        raise ValueError("multi_evidence_integrated DataFrame is required")
    
    # Get gene data
    gene_rows = multi_evidence[multi_evidence['gene_symbol'] == gene_symbol]
    if len(gene_rows) == 0:
        print(f"Warning: No data found for gene {gene_symbol} in multi_evidence_integrated")
        return episodes
    
    gene_data = gene_rows.iloc[0].to_dict()
    all_scores = multi_evidence['enhanced_integrated_score'].tolist()
    
    # Get gene mapping data
    gene_mapping_data = None
    if gene_mapping is not None:
        mapping_rows = gene_mapping[gene_mapping['gene_symbol'] == gene_symbol]
        if len(mapping_rows) > 0:
            gene_mapping_data = mapping_rows.iloc[0].to_dict()
    
    # 1. Create gene profile episode (foundation)
    try:
        gene_profile = create_gene_profile_episode(
            gene_data=gene_data,
            all_scores=all_scores,
            gene_mapping_data=gene_mapping_data,
            group_id=group_id
        )
        episodes.append(gene_profile)
    except Exception as e:
        print(f"Failed to create gene profile for {gene_symbol}: {e}")
    
    # 2. Create GWAS evidence episode if data available
    if gwas_data is not None:
        gene_gwas_data = gwas_data[gwas_data['nearest_gene'] == gene_symbol]
        if len(gene_gwas_data) > 0:
            try:
                gwas_episode = create_gwas_evidence_episode(
                    gene_symbol=gene_symbol,
                    gwas_data=gene_gwas_data.to_dict('records'),
                    group_id=group_id
                )
                episodes.append(gwas_episode)
            except Exception as e:
                print(f"Failed to create GWAS episode for {gene_symbol}: {e}")
    
    # 3. Create eQTL evidence episode if data available
    if eqtl_data is not None:
        gene_eqtl_data = eqtl_data[eqtl_data['gene_symbol'] == gene_symbol]
        if len(gene_eqtl_data) > 0:
            try:
                eqtl_episode = create_eqtl_evidence_episode(
                    gene_symbol=gene_symbol,
                    eqtl_data=gene_eqtl_data.to_dict('records'),
                    group_id=group_id
                )
                episodes.append(eqtl_episode)
            except Exception as e:
                print(f"Failed to create eQTL episode for {gene_symbol}: {e}")
    
    return episodes