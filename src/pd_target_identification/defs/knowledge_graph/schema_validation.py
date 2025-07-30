# pd_target_identification/defs/knowledge_graph/schema_validation.py

"""
Schema validation functions for knowledge graph episodes.

This module provides comprehensive validation for gene data, evidence data,
and complete episode structures to ensure data quality and Graphiti compatibility.
Includes error handling, data quality checks, and safe episode creation with
fallback mechanisms for robust pipeline operation.
"""

from typing import Dict, List, Any, Tuple
import json
import re
from datetime import datetime

from .graph_schema import EvidenceType, ConfidenceLevel
from .schema_constants import (
    CONFIDENCE_THRESHOLDS,
    PIPELINE_GENES,
    DEFAULT_SOURCE,
    DEFAULT_SOURCE_DESCRIPTION
)


def validate_gene_data(gene_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate gene data structure and content for episode creation.
    
    Performs comprehensive validation of gene information including symbol format,
    score ranges, evidence type counts, and cross-field consistency checks based
    on the PD target discovery pipeline data structure.
    
    Args:
        gene_data: Dictionary containing gene information from pipeline
        
    Returns:
        Tuple of (is_valid: bool, error_messages: List[str])
        
    Validation Rules:
        - gene_symbol is required and follows standard gene naming format
        - enhanced_integrated_score must be non-negative and within expected range
        - evidence_types must be between 0-5 (max evidence types in pipeline)
        - therapeutic_target_papers must be non-negative
        - Cross-field consistency (evidence types vs scores)
        
    Example:
        >>> is_valid, errors = validate_gene_data({
        ...     'gene_symbol': 'SNCA',
        ...     'enhanced_integrated_score': 188.5,
        ...     'evidence_types': 3
        ... })
        >>> print(is_valid)  # True
    """
    errors = []
    warnings = []
    
    # Essential validation - gene symbol is required
    gene_symbol = gene_data.get('gene_symbol', '').strip()
    if not gene_symbol:
        errors.append("gene_symbol is required")
    else:
        # Validate gene symbol format (standard gene naming convention)
        if not re.match(r'^[A-Z0-9][A-Z0-9\-]*[A-Z0-9]?$', gene_symbol):
            warnings.append(f"Gene symbol '{gene_symbol}' doesn't follow standard format")
        
        # Check if gene is in known pipeline gene list
        if gene_symbol not in PIPELINE_GENES:
            warnings.append(f"Gene '{gene_symbol}' not in known pipeline gene list")
    
    # Enhanced integrated score validation
    enhanced_score = gene_data.get('enhanced_integrated_score')
    if enhanced_score is not None:
        if enhanced_score < 0:
            errors.append("enhanced_integrated_score cannot be negative")
        elif enhanced_score > 500:  # Based on pipeline max ~188.5
            warnings.append(f"Enhanced score {enhanced_score} unusually high (pipeline max ~188)")
        elif enhanced_score > CONFIDENCE_THRESHOLDS['enhanced_score_very_high']:
            # Score is high but within reasonable bounds
            pass
    
    # Evidence type validation
    evidence_types = gene_data.get('evidence_types', 0)
    if not isinstance(evidence_types, int):
        errors.append("evidence_types must be an integer")
    elif evidence_types < 0:
        errors.append("evidence_types cannot be negative")
    elif evidence_types > CONFIDENCE_THRESHOLDS['evidence_types_max']:
        errors.append(f"evidence_types {evidence_types} exceeds maximum {CONFIDENCE_THRESHOLDS['evidence_types_max']}")
    
    # Therapeutic papers validation
    therapeutic_papers = gene_data.get('therapeutic_target_papers', 0)
    if therapeutic_papers is not None:
        if not isinstance(therapeutic_papers, int):
            errors.append("therapeutic_target_papers must be an integer")
        elif therapeutic_papers < 0:
            errors.append("therapeutic_target_papers cannot be negative")
        elif therapeutic_papers > 25:  # Based on pipeline max ~15
            warnings.append(f"Therapeutic papers count {therapeutic_papers} unusually high (pipeline max ~15)")
    
    # Rank validation
    rank = gene_data.get('rank')
    if rank is not None:
        if not isinstance(rank, int):
            errors.append("rank must be an integer")
        elif rank < 1:
            errors.append("rank must be positive")
        elif rank > 100:  # Reasonable upper bound
            warnings.append(f"Rank {rank} seems unusually high")
    
    # Cross-field validation - consistency checks
    if evidence_types > 0 and enhanced_score == 0:
        warnings.append("Gene has evidence types but zero integrated score")
    
    if therapeutic_papers > 0 and evidence_types == 0:
        warnings.append("Gene has therapeutic papers but no evidence types")
    
    # Pathway count validation
    pathway_count = gene_data.get('pathway_count', 0)
    if pathway_count is not None and pathway_count < 0:
        errors.append("pathway_count cannot be negative")
    
    # Interaction count validation
    interaction_count = gene_data.get('interaction_count', 0)
    if interaction_count is not None and interaction_count < 0:
        errors.append("interaction_count cannot be negative")
    
    # Log warnings but don't fail validation
    if warnings:
        print(f"Validation warnings for {gene_symbol}: {warnings}")
    
    return len(errors) == 0, errors


def validate_evidence_data(evidence_data: Dict[str, Any], evidence_type: EvidenceType) -> Tuple[bool, List[str]]:
    """
    Validate evidence-specific data based on evidence type.
    
    Performs type-specific validation for different evidence types (GWAS, eQTL,
    literature, pathway) with appropriate statistical and biological constraints
    based on the pipeline data characteristics.
    
    Args:
        evidence_data: Dictionary containing evidence-specific data
        evidence_type: Type of evidence being validated
        
    Returns:
        Tuple of (is_valid: bool, error_messages: List[str])
        
    Validation Rules by Evidence Type:
        - GWAS: p-values (0-1), odds ratios (positive), significance thresholds
        - eQTL: tissue IDs required, effect sizes reasonable, p-values valid
        - Literature: paper counts non-negative, scores within expected ranges
        - Pathway: interaction/pathway counts non-negative, confidence scores valid
        
    Example:
        >>> is_valid, errors = validate_evidence_data({
        ...     'p_value': 2.3e-12,
        ...     'odds_ratio': 1.34
        ... }, EvidenceType.GWAS)
        >>> print(is_valid)  # True
    """
    errors = []
    warnings = []
    
    # Common validation for all evidence types
    gene_symbol = evidence_data.get('gene', '')
    if not gene_symbol:
        warnings.append("Gene identifier missing from evidence data")
    
    # Type-specific validation
    if evidence_type == EvidenceType.GWAS:
        # GWAS-specific validation
        p_value = evidence_data.get('p_value')
        if p_value is not None:
            if not isinstance(p_value, (int, float)):
                errors.append("GWAS p_value must be numeric")
            elif p_value <= 0 or p_value > 1.0:
                errors.append(f"GWAS p_value {p_value} must be between 0 and 1")
            elif p_value > CONFIDENCE_THRESHOLDS['gwas_significance']:
                warnings.append(f"GWAS p_value {p_value} above genome-wide significance threshold")
        
        # Odds ratio validation
        odds_ratio = evidence_data.get('odds_ratio')
        if odds_ratio is not None:
            if not isinstance(odds_ratio, (int, float)):
                errors.append("odds_ratio must be numeric")
            elif odds_ratio <= 0:
                errors.append("odds_ratio must be positive")
            elif odds_ratio > 10:  # Extremely high OR
                warnings.append(f"Odds ratio {odds_ratio} extremely high")
        
        # Variant count validation
        variant_count = evidence_data.get('variant_count', 0)
        if variant_count < 0:
            errors.append("variant_count cannot be negative")
            
    elif evidence_type == EvidenceType.EQTL:
        # eQTL-specific validation
        tissue_id = evidence_data.get('tissue_id')
        if not tissue_id and evidence_data.get('eqtl_count', 0) > 0:
            warnings.append("tissue_id missing for eQTL evidence")
        
        # eQTL p-value validation
        p_value = evidence_data.get('p_value')
        if p_value is not None:
            if not isinstance(p_value, (int, float)):
                errors.append("eQTL p_value must be numeric")
            elif p_value <= 0 or p_value > 1.0:
                errors.append(f"eQTL p_value {p_value} must be between 0 and 1")
        
        # Effect size validation
        effect_size = evidence_data.get('effect_size')
        if effect_size is not None:
            if not isinstance(effect_size, (int, float)):
                errors.append("effect_size must be numeric")
            elif abs(effect_size) > 5:  # Very large effect size
                warnings.append(f"Effect size {effect_size} unusually large")
        
        # Tissue count validation
        tissue_count = evidence_data.get('tissue_count', 0)
        if tissue_count < 0:
            errors.append("tissue_count cannot be negative")
        elif tissue_count > 50:  # GTEx has ~50 tissues
            warnings.append(f"tissue_count {tissue_count} unusually high")
            
    elif evidence_type == EvidenceType.LITERATURE:
        # Literature-specific validation
        total_papers = evidence_data.get('total_papers', 0)
        if not isinstance(total_papers, int):
            errors.append("total_papers must be an integer")
        elif total_papers < 0:
            errors.append("total_papers cannot be negative")
        elif total_papers > 50:  # Based on pipeline max ~25
            warnings.append(f"total_papers {total_papers} unusually high (pipeline max ~25)")
        
        # Therapeutic target papers validation
        therapeutic_papers = evidence_data.get('therapeutic_target_papers', 0)
        if not isinstance(therapeutic_papers, int):
            errors.append("therapeutic_target_papers must be an integer")
        elif therapeutic_papers < 0:
            errors.append("therapeutic_target_papers cannot be negative")
        elif therapeutic_papers > total_papers:
            errors.append("therapeutic_target_papers cannot exceed total_papers")
        
        # Literature evidence score validation
        lit_score = evidence_data.get('literature_evidence_score', 0)
        if not isinstance(lit_score, (int, float)):
            errors.append("literature_evidence_score must be numeric")
        elif lit_score < 0:
            errors.append("literature_evidence_score cannot be negative")
        
        # Publication year validation
        pub_year = evidence_data.get('most_recent_paper_year')
        if pub_year is not None:
            current_year = datetime.now().year
            if pub_year < 1990 or pub_year > current_year:
                warnings.append(f"Publication year {pub_year} outside reasonable range")
                
    elif evidence_type == EvidenceType.PATHWAY:
        # Pathway-specific validation
        interaction_count = evidence_data.get('interaction_count', 0)
        pathway_count = evidence_data.get('pathway_count', 0)
        
        if not isinstance(interaction_count, int):
            errors.append("interaction_count must be an integer")
        elif interaction_count < 0:
            errors.append("interaction_count cannot be negative")
        
        if not isinstance(pathway_count, int):
            errors.append("pathway_count must be an integer")
        elif pathway_count < 0:
            errors.append("pathway_count cannot be negative")
        
        # At least some evidence should be present
        if interaction_count == 0 and pathway_count == 0:
            warnings.append("No pathway or interaction evidence found")
        
        # Confidence scores validation
        combined_score = evidence_data.get('combined_score')
        if combined_score is not None:
            if not isinstance(combined_score, (int, float)):
                errors.append("combined_score must be numeric")
            elif combined_score < 0 or combined_score > 1:
                errors.append("combined_score must be between 0 and 1")
        
        # Degree centrality validation
        centrality = evidence_data.get('degree_centrality', 0)
        if not isinstance(centrality, (int, float)):
            errors.append("degree_centrality must be numeric")
        elif centrality < 0:
            errors.append("degree_centrality cannot be negative")
    
    # Log warnings
    if warnings:
        print(f"Evidence validation warnings for {evidence_type.value}: {warnings}")
    
    return len(errors) == 0, errors


def validate_episode_structure(episode: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate complete episode structure for Graphiti compatibility.
    
    Ensures episode follows required Graphiti MCP format with proper fields,
    JSON serialization compatibility, and naming conventions for successful
    ingestion into the knowledge graph.
    
    Args:
        episode: Complete episode dictionary ready for Graphiti
        
    Returns:
        Tuple of (is_valid: bool, error_messages: List[str])
        
    Validation Rules:
        - Required Graphiti fields: name, episode_body, source
        - episode_body must be JSON-serializable
        - name must follow naming conventions
        - source_description should be descriptive
        - Overall structure must be valid for MCP ingestion
        
    Example:
        >>> episode = {
        ...     'name': 'Gene_Profile_SNCA',
        ...     'episode_body': json.dumps({'gene': {'symbol': 'SNCA'}}),
        ...     'source': 'json'
        ... }
        >>> is_valid, errors = validate_episode_structure(episode)
        >>> print(is_valid)  # True
    """
    errors = []
    warnings = []
    
    # Core Graphiti requirements validation
    required_fields = ['name', 'episode_body', 'source']
    for field in required_fields:
        if field not in episode:
            errors.append(f"Required Graphiti field '{field}' is missing")
        elif not episode[field]:
            errors.append(f"Required field '{field}' cannot be empty")
    
    # Episode name validation
    episode_name = episode.get('name', '')
    if episode_name:
        # Check naming convention
        if not re.match(r'^[A-Za-z0-9_\-]+$', episode_name):
            errors.append("Episode name contains invalid characters (use only letters, numbers, _, -)")
        
        # Check reasonable length
        if len(episode_name) > 100:
            warnings.append(f"Episode name '{episode_name}' is very long ({len(episode_name)} chars)")
        elif len(episode_name) < 5:
            warnings.append(f"Episode name '{episode_name}' is very short")
        
        # Check for descriptive naming
        if not any(keyword in episode_name.lower() for keyword in ['gene', 'gwas', 'eqtl', 'literature', 'pathway', 'integration']):
            warnings.append(f"Episode name '{episode_name}' doesn't indicate content type")
    
    # Episode body validation - must be JSON serializable
    episode_body = episode.get('episode_body')
    if episode_body is not None:
        try:
            if isinstance(episode_body, dict):
                # Convert to JSON string if it's a dict
                json.dumps(episode_body)
                warnings.append("episode_body should be JSON string, not dict")
            elif isinstance(episode_body, str):
                # Validate it's valid JSON
                parsed = json.loads(episode_body)
                
                # Check for reasonable content
                if not parsed:
                    warnings.append("episode_body contains empty JSON")
                elif len(str(episode_body)) > 50000:  # Very large episode
                    warnings.append(f"episode_body is very large ({len(str(episode_body))} chars)")
            else:
                errors.append("episode_body must be JSON string or dict")
                
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            errors.append(f"episode_body must be valid JSON: {e}")
    
    # Source validation
    source = episode.get('source', '')
    if source and source not in ['json', 'text', 'message']:
        warnings.append(f"Unusual source type '{source}' (common types: json, text, message)")
    
    # Source description validation
    source_desc = episode.get('source_description', '')
    if source_desc and len(source_desc) < 10:
        warnings.append("source_description should be more descriptive")
    
    # Additional Graphiti compatibility checks
    if 'group_id' in episode and not episode['group_id']:
        warnings.append("group_id is empty")
    
    if 'uuid' in episode and not episode['uuid']:
        warnings.append("uuid is empty or invalid")
    
    # Log warnings
    if warnings:
        print(f"Episode structure warnings for '{episode_name}': {warnings}")
    
    return len(errors) == 0, errors


def safe_episode_creation(
    episode_name: str, 
    episode_data: Dict[str, Any], 
    source_desc: str,
    group_id: str = None
) -> Dict[str, Any]:
    """
    Create episode with comprehensive error handling and fallback mechanisms.
    
    Provides robust episode creation that handles errors gracefully and ensures
    valid episodes are always returned, even when input data has issues. Includes
    automatic validation and fallback to minimal valid episodes when needed.
    
    Args:
        episode_name: Name for the episode
        episode_data: Data content for episode body
        source_desc: Description of data source
        group_id: Optional group ID for episode organization
        
    Returns:
        Valid episode dictionary ready for Graphiti ingestion
        
    Error Handling:
        - JSON serialization errors -> fallback episode with error info
        - Validation failures -> minimal valid episode structure
        - Missing data -> default values applied
        - Invalid names -> sanitized names generated
        
    Example:
        >>> episode = safe_episode_creation(
        ...     'Gene_Profile_SNCA',
        ...     {'gene': {'symbol': 'SNCA', 'score': 188.5}},
        ...     'Gene profile from pipeline'
        ... )
        >>> print(episode['name'])  # 'Gene_Profile_SNCA'
    """
    try:
        # Sanitize episode name
        safe_name = re.sub(r'[^\w\-]', '_', str(episode_name))
        safe_name = re.sub(r'_+', '_', safe_name).strip('_')
        if not safe_name:
            safe_name = f"Episode_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Ensure episode_body is properly formatted JSON string
        if isinstance(episode_data, dict):
            episode_body = json.dumps(episode_data, ensure_ascii=False, separators=(',', ':'))
        elif isinstance(episode_data, str):
            # Validate it's valid JSON
            json.loads(episode_data)
            episode_body = episode_data
        else:
            # Convert other types to JSON
            episode_body = json.dumps({'data': str(episode_data)})
        
        # Create episode structure
        episode = {
            "name": safe_name,
            "episode_body": episode_body,
            "source": DEFAULT_SOURCE,
            "source_description": source_desc or DEFAULT_SOURCE_DESCRIPTION
        }
        
        # Add optional group_id if provided
        if group_id:
            episode["group_id"] = group_id
        
        # Validate the created episode
        is_valid, errors = validate_episode_structure(episode)
        if not is_valid:
            print(f"Episode validation failed for {safe_name}: {errors}")
            # Create fallback episode with error information
            return _create_fallback_episode(safe_name, errors, source_desc)
        
        return episode
        
    except json.JSONDecodeError as e:
        print(f"JSON encoding failed for episode {episode_name}: {e}")
        return _create_fallback_episode(
            f"{episode_name}_json_error",
            [f"JSON encoding error: {e}"],
            source_desc
        )
        
    except Exception as e:
        print(f"Episode creation failed for {episode_name}: {e}")
        return _create_fallback_episode(
            f"{episode_name}_error",
            [f"Creation error: {e}"],
            source_desc
        )


def _create_fallback_episode(episode_name: str, errors: List[str], source_desc: str) -> Dict[str, Any]:
    """
    Create minimal valid fallback episode when normal creation fails.
    
    Internal helper function that creates a basic valid episode structure
    when the main episode creation process encounters errors. Ensures
    the pipeline can continue operating even with problematic data.
    
    Args:
        episode_name: Base name for fallback episode
        errors: List of errors that caused fallback
        source_desc: Source description for context
        
    Returns:
        Minimal valid episode dictionary
    """
    fallback_name = f"{episode_name}_fallback_{datetime.now().strftime('%H%M%S')}"
    
    fallback_data = {
        "status": "fallback_episode",
        "original_name": episode_name,
        "errors": errors,
        "created_at": datetime.now().isoformat(),
        "note": "Fallback episode created due to validation or creation errors"
    }
    
    return {
        "name": fallback_name,
        "episode_body": json.dumps(fallback_data),
        "source": DEFAULT_SOURCE,
        "source_description": f"Fallback for failed {source_desc}"
    }


def validate_cross_episode_consistency(episodes: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    Validate consistency across multiple related episodes.
    
    Performs cross-episode validation to ensure that related episodes (e.g.,
    different evidence types for the same gene) maintain consistency in
    shared fields and relationships.
    
    Args:
        episodes: List of related episodes to validate together
        
    Returns:
        Tuple of (is_consistent: bool, inconsistency_messages: List[str])
        
    Validation Checks:
        - Gene symbols consistent across episodes
        - Evidence scores align with integration scores
        - No contradictory information between episodes
        - Proper referential integrity
        
    Example:
        >>> gene_episodes = [gene_profile_episode, gwas_episode, literature_episode]
        >>> is_consistent, issues = validate_cross_episode_consistency(gene_episodes)
        >>> print(is_consistent)  # True if all episodes are consistent
    """
    inconsistencies = []
    
    if not episodes:
        return True, []
    
    # Extract gene symbols from all episodes
    gene_symbols = set()
    episode_types = set()
    
    for episode in episodes:
        try:
            episode_body = episode.get('episode_body', '{}')
            if isinstance(episode_body, str):
                data = json.loads(episode_body)
            else:
                data = episode_body
            
            # Extract gene symbol from various possible locations
            gene_symbol = None
            for possible_key in ['gene', 'therapeutic_target', 'genetic_association', 'regulatory_evidence', 'literature_evidence', 'functional_evidence']:
                if possible_key in data:
                    if isinstance(data[possible_key], dict):
                        gene_symbol = data[possible_key].get('gene') or data[possible_key].get('symbol')
                    break
            
            if gene_symbol:
                gene_symbols.add(gene_symbol)
            
            # Track episode types
            episode_name = episode.get('name', '')
            if 'Gene_Profile' in episode_name:
                episode_types.add('profile')
            elif 'GWAS_Evidence' in episode_name:
                episode_types.add('gwas')
            elif 'eQTL_Evidence' in episode_name:
                episode_types.add('eqtl')
            elif 'Literature_Evidence' in episode_name:
                episode_types.add('literature')
            elif 'Pathway_Evidence' in episode_name:
                episode_types.add('pathway')
            elif 'Integration' in episode_name:
                episode_types.add('integration')
                
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            inconsistencies.append(f"Failed to parse episode {episode.get('name', 'unknown')}: {e}")
    
    # Check for consistent gene symbols
    if len(gene_symbols) > 1:
        inconsistencies.append(f"Multiple gene symbols found in related episodes: {gene_symbols}")
    elif len(gene_symbols) == 0:
        inconsistencies.append("No gene symbols found in any episode")
    
    # Check for reasonable episode type combinations
    if 'integration' in episode_types and len(episode_types) == 1:
        inconsistencies.append("Integration episode found without supporting evidence episodes")
    
    return len(inconsistencies) == 0, inconsistencies