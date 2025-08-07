# pd_target_identification/defs/knowledge_graph/assets.py
"""
Dagster assets for knowledge graph episode generation from pipeline data.

This module provides Dagster assets that transform multi-omics pipeline DataFrames
into complete sets of Graphiti episodes ready for knowledge graph ingestion. Each
asset processes specific evidence types and generates properly validated episodes
with comprehensive error handling and quality metrics.
"""

import pandas as pd
import json
import numpy as np
from typing import Dict, Any
from dagster import asset, AssetExecutionContext
import hashlib
import os
from pathlib import Path
from datetime import datetime
from ..shared.episode_config_yaml import get_episode_types_for_export

from .episode_generators import (
    create_gene_profile_episode,
    create_gwas_evidence_episode,
    create_eqtl_evidence_episode,
    create_literature_evidence_episode,
    create_pathway_evidence_episode
)
from .schema_constants import DEFAULT_GROUP_ID


# ============================================================================
# INDIVIDUAL EVIDENCE TYPE ASSETS - One per evidence type
# ============================================================================

@asset(
    deps=["multi_evidence_integrated"],
    description="Generate gene profile episodes from integrated target ranking data"
)
def gene_profile_episodes(
    context: AssetExecutionContext,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create gene profile episodes for all genes in the integrated analysis.
    
    Transforms each gene's comprehensive data from multi_evidence_integrated
    into a gene profile episode that serves as the central entity in the 
    knowledge graph.
    
    Returns:
        DataFrame with columns: gene_symbol, episode_name, episode_data, validation_status
    """
    context.log.info(f"Creating gene profile episodes for {len(multi_evidence_integrated)} genes")
    
    episodes = []
    all_scores = multi_evidence_integrated['enhanced_integrated_score'].tolist()
    successful_genes = 0
    failed_genes = 0
    
    for idx, gene_row in multi_evidence_integrated.iterrows():
        gene_symbol = gene_row['gene_symbol']
        
        try:
            # Gene mapping data will be handled dynamically when needed
            # No static mapping table dependency
            
            # Create gene profile episode
            episode = create_gene_profile_episode(
                gene_data=gene_row.to_dict(),
                all_scores=all_scores,
                gene_mapping_data=None,  # Dynamic mapping handled in episode generators
                group_id=DEFAULT_GROUP_ID
            )
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': episode['name'],
                'episode_data': episode,
                'validation_status': 'success',
                'episode_type': 'gene_profile',
                'data_completeness': _calculate_episode_completeness(episode),
                'created_timestamp': pd.Timestamp.now()
            })
            
            successful_genes += 1
            
        except Exception as e:
            context.log.error(f"Failed to create gene profile episode for {gene_symbol}: {e}")
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': f"Gene_Profile_{gene_symbol}_FAILED",
                'episode_data': None,
                'validation_status': 'failed',
                'episode_type': 'gene_profile',
                'error_message': str(e),
                'data_completeness': 0.0,
                'created_timestamp': pd.Timestamp.now()
            })
            
            failed_genes += 1
    
    context.log.info(f"Gene profile episode generation complete: {successful_genes} successful, {failed_genes} failed")
    
    # Add metadata
    context.add_output_metadata({
        "total_genes": len(multi_evidence_integrated),
        "successful_episodes": successful_genes,
        "failed_episodes": failed_genes,
        "success_rate": f"{(successful_genes / len(multi_evidence_integrated)) * 100:.1f}%",
        "top_ranked_gene": multi_evidence_integrated.iloc[0]['gene_symbol'],
        "score_range": f"{multi_evidence_integrated['enhanced_integrated_score'].min():.1f} - {multi_evidence_integrated['enhanced_integrated_score'].max():.1f}"
    })
    
    return pd.DataFrame(episodes)


@asset(
    deps=["raw_gwas_data", "multi_evidence_integrated"],
    description="Generate GWAS evidence episodes from genetic association data"
)
def gwas_evidence_episodes(
    context: AssetExecutionContext,
    raw_gwas_data: pd.DataFrame,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create GWAS evidence episodes for genes with genetic associations.
    
    Transforms GWAS variant data into comprehensive genetic evidence episodes
    including variant details, statistical measures, and population information.
    
    Returns:
        DataFrame with GWAS evidence episodes for genes with variant associations
    """
    context.log.info(f"Creating GWAS evidence episodes from {len(raw_gwas_data)} variants")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    # Get genes that have GWAS data and are in the integrated analysis
    target_genes = set(multi_evidence_integrated['gene_symbol'].tolist())
    genes_with_gwas = raw_gwas_data['nearest_gene'].unique()
    genes_to_process = [gene for gene in genes_with_gwas if gene in target_genes]
    
    context.log.info(f"Processing GWAS evidence for {len(genes_to_process)} genes")
    
    for gene_symbol in genes_to_process:
        try:
            # Get GWAS data for this gene
            gene_gwas_data = raw_gwas_data[
                raw_gwas_data['nearest_gene'] == gene_symbol
            ]
            
            if len(gene_gwas_data) == 0:
                continue
                
            # Create GWAS evidence episode
            episode = create_gwas_evidence_episode(
                gene_symbol=gene_symbol,
                gwas_data=gene_gwas_data.to_dict('records'),
                group_id=DEFAULT_GROUP_ID
            )
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': episode['name'],
                'episode_data': episode,
                'validation_status': 'success',
                'episode_type': 'gwas_evidence',
                'variant_count': len(gene_gwas_data),
                'min_p_value': float(gene_gwas_data['p_value'].min()),
                'genome_wide_significant': bool(gene_gwas_data['p_value'].min() <= 5e-8),
                'data_completeness': _calculate_episode_completeness(episode),
                'created_timestamp': pd.Timestamp.now()
            })
            
            successful_episodes += 1
            
        except Exception as e:
            context.log.error(f"Failed to create GWAS episode for {gene_symbol}: {e}")
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': f"GWAS_Evidence_{gene_symbol}_FAILED",
                'episode_data': None,
                'validation_status': 'failed',
                'episode_type': 'gwas_evidence',
                'error_message': str(e),
                'data_completeness': 0.0,
                'created_timestamp': pd.Timestamp.now()
            })
            
            failed_episodes += 1
    
    context.log.info(f"GWAS episode generation complete: {successful_episodes} successful, {failed_episodes} failed")
    
    # Add metadata
    context.add_output_metadata({
        "total_variants": len(raw_gwas_data),
        "genes_with_gwas_data": len(genes_to_process),
        "successful_episodes": successful_episodes,
        "failed_episodes": failed_episodes,
        "genome_wide_significant_genes": sum(1 for ep in episodes if ep.get('genome_wide_significant', False)),
        "min_p_value_overall": float(raw_gwas_data['p_value'].min()) if len(raw_gwas_data) > 0 else None
    })
    
    return pd.DataFrame(episodes)


@asset(
    deps=["gtex_brain_eqtls", "multi_evidence_integrated"],
    description="Generate eQTL evidence episodes from brain regulatory data"
)
def eqtl_evidence_episodes(
    context: AssetExecutionContext,
    gtex_brain_eqtls: pd.DataFrame,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create eQTL evidence episodes for genes with brain regulatory effects.
    
    Transforms brain eQTL data into comprehensive regulatory evidence episodes
    including tissue specificity, effect sizes, and disease relevance.
    
    Returns:
        DataFrame with eQTL evidence episodes for genes with brain regulatory effects
    """
    context.log.info(f"Creating eQTL evidence episodes from {len(gtex_brain_eqtls)} brain eQTLs")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    # Get genes that have eQTL data and are in the integrated analysis
    target_genes = set(multi_evidence_integrated['gene_symbol'].tolist())
    genes_with_eqtls = gtex_brain_eqtls['gene_symbol'].unique()
    genes_to_process = [gene for gene in genes_with_eqtls if gene in target_genes]
    
    context.log.info(f"Processing eQTL evidence for {len(genes_to_process)} genes")
    
    for gene_symbol in genes_to_process:
        try:
            # Get eQTL data for this gene
            gene_eqtl_data = gtex_brain_eqtls[
                gtex_brain_eqtls['gene_symbol'] == gene_symbol
            ]
            
            if len(gene_eqtl_data) == 0:
                continue
                
            # Create eQTL evidence episode
            episode = create_eqtl_evidence_episode(
                gene_symbol=gene_symbol,
                eqtl_data=gene_eqtl_data.to_dict('records'),
                group_id=DEFAULT_GROUP_ID
            )
            
            # Calculate tissue metrics
            tissues = gene_eqtl_data['tissue_id'].unique()
            has_sn_eqtl = any('substantia_nigra' in tissue.lower() for tissue in tissues)
            has_bg_eqtl = any(
                any(bg_term in tissue.lower() for bg_term in ['basal_ganglia', 'putamen', 'caudate', 'striatum'])
                for tissue in tissues
            )
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': episode['name'],
                'episode_data': episode,
                'validation_status': 'success',
                'episode_type': 'eqtl_evidence',
                'eqtl_count': len(gene_eqtl_data),
                'tissue_count': len(tissues),
                'has_substantia_nigra_eqtl': has_sn_eqtl,
                'has_basal_ganglia_eqtl': has_bg_eqtl,
                'min_p_value': float(gene_eqtl_data['p_value'].min()),
                'data_completeness': _calculate_episode_completeness(episode),
                'created_timestamp': pd.Timestamp.now()
            })
            
            successful_episodes += 1
            
        except Exception as e:
            context.log.error(f"Failed to create eQTL episode for {gene_symbol}: {e}")
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': f"eQTL_Evidence_{gene_symbol}_FAILED",
                'episode_data': None,
                'validation_status': 'failed',
                'episode_type': 'eqtl_evidence',
                'error_message': str(e),
                'data_completeness': 0.0,
                'created_timestamp': pd.Timestamp.now()
            })
            
            failed_episodes += 1
    
    context.log.info(f"eQTL episode generation complete: {successful_episodes} successful, {failed_episodes} failed")
    
    # Add metadata
    context.add_output_metadata({
        "total_eqtls": len(gtex_brain_eqtls),
        "genes_with_eqtl_data": len(genes_to_process),
        "successful_episodes": successful_episodes,
        "failed_episodes": failed_episodes,
        "substantia_nigra_genes": sum(1 for ep in episodes if ep.get('has_substantia_nigra_eqtl', False)),
        "basal_ganglia_genes": sum(1 for ep in episodes if ep.get('has_basal_ganglia_eqtl', False)),
        "unique_tissues": len(gtex_brain_eqtls['tissue_id'].unique()) if len(gtex_brain_eqtls) > 0 else 0
    })
    
    return pd.DataFrame(episodes)


@asset(
    deps=["literature_analysis", "multi_evidence_integrated"],
    description="Generate literature evidence episodes from publication analysis"
)
def literature_evidence_episodes(
    context: AssetExecutionContext,
    literature_analysis: pd.DataFrame,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create literature evidence episodes for genes with publication evidence.
    
    Transforms literature analysis data into comprehensive publication evidence
    episodes including paper counts, research trends, and therapeutic focus.
    
    Returns:
        DataFrame with literature evidence episodes for genes with publication data
    """
    context.log.info(f"Creating literature evidence episodes from {len(literature_analysis)} gene literature profiles")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    # Get genes that have literature data and are in the integrated analysis
    target_genes = set(multi_evidence_integrated['gene_symbol'].tolist())
    genes_with_literature = literature_analysis['gene_symbol'].unique() if 'gene_symbol' in literature_analysis.columns else []
    genes_to_process = [gene for gene in genes_with_literature if gene in target_genes]
    
    context.log.info(f"Processing literature evidence for {len(genes_to_process)} genes")
    
    for gene_symbol in genes_to_process:
        try:
            # Get literature data for this gene
            gene_lit_data = literature_analysis[
                literature_analysis['gene_symbol'] == gene_symbol
            ]
            
            if len(gene_lit_data) == 0:
                continue
                
            # Convert to dict for episode creation
            literature_data = gene_lit_data.iloc[0].to_dict()
            
            # Create literature evidence episode
            episode = create_literature_evidence_episode(
                gene_symbol=gene_symbol,
                literature_data=literature_data,
                group_id=DEFAULT_GROUP_ID
            )
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': episode['name'],
                'episode_data': episode,
                'validation_status': 'success',
                'episode_type': 'literature_evidence',
                'total_papers': int(literature_data.get('literature_papers_count', 0)),
                'therapeutic_papers': int(literature_data.get('therapeutic_target_papers', 0)),
                'has_strong_evidence': bool(literature_data.get('has_strong_literature_evidence', False)),
                'literature_score': float(literature_data.get('literature_evidence_score', 0.0)),
                'data_completeness': _calculate_episode_completeness(episode),
                'created_timestamp': pd.Timestamp.now()
            })
            
            successful_episodes += 1
            
        except Exception as e:
            context.log.error(f"Failed to create literature episode for {gene_symbol}: {e}")
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': f"Literature_Evidence_{gene_symbol}_FAILED",
                'episode_data': None,
                'validation_status': 'failed',
                'episode_type': 'literature_evidence',
                'error_message': str(e),
                'data_completeness': 0.0,
                'created_timestamp': pd.Timestamp.now()
            })
            
            failed_episodes += 1
    
    context.log.info(f"Literature episode generation complete: {successful_episodes} successful, {failed_episodes} failed")
    
    # Add metadata
    context.add_output_metadata({
        "genes_with_literature_data": len(genes_to_process),
        "successful_episodes": successful_episodes,
        "failed_episodes": failed_episodes,
        "genes_with_therapeutic_papers": sum(1 for ep in episodes if ep.get('therapeutic_papers', 0) > 0),
        "genes_with_strong_evidence": sum(1 for ep in episodes if ep.get('has_strong_evidence', False)),
        "total_papers_analyzed": sum(ep.get('total_papers', 0) for ep in episodes if ep.get('total_papers'))
    })
    
    return pd.DataFrame(episodes)


def sanitize_dict_for_json(obj):
    """
    Sanitize dictionaries and lists to ensure all numpy types are converted to Python native types.
    """
    if obj is None:
        return None
    elif isinstance(obj, (bool, int, float, str)):
        return obj
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.integer, np.floating, np.number)):
        return obj.item()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {key: sanitize_dict_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [sanitize_dict_for_json(item) for item in obj]
    elif hasattr(obj, 'tolist'):  # pandas Series, etc.
        return obj.tolist()
    elif hasattr(obj, 'item') and callable(obj.item):  # numpy scalars
        return obj.item()
    else:
        # For any other type, convert to string as fallback
        return str(obj)

@asset(
    deps=["pathway_network_summary", "string_protein_interactions", "string_functional_enrichment", "multi_evidence_integrated"],
    description="Generate pathway evidence episodes from functional and interaction data"
)
def pathway_evidence_episodes(
    context: AssetExecutionContext,
    pathway_network_summary: pd.DataFrame,
    string_protein_interactions: pd.DataFrame,
    string_functional_enrichment: pd.DataFrame,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create pathway evidence episodes for genes with functional annotation data.
    
    Transforms pathway analysis, protein interactions, and functional enrichment
    data into comprehensive functional evidence episodes.
    
    Returns:
        DataFrame with pathway evidence episodes for genes with functional data
    """
    context.log.info(f"Creating pathway evidence episodes from {len(pathway_network_summary)} gene pathway profiles")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    # Get genes that have pathway data and are in the integrated analysis
    target_genes = set(multi_evidence_integrated['gene_symbol'].tolist())
    genes_with_pathways = pathway_network_summary['gene_symbol'].unique() if 'gene_symbol' in pathway_network_summary.columns else []
    genes_to_process = [gene for gene in genes_with_pathways if gene in target_genes]
    
    context.log.info(f"Processing pathway evidence for {len(genes_to_process)} genes")
    
    for gene_symbol in genes_to_process:
        try:
            # Get pathway summary data for this gene
            gene_pathway_data = pathway_network_summary[
                pathway_network_summary['gene_symbol'] == gene_symbol
            ]
            
            if len(gene_pathway_data) == 0:
                continue
                
            # Convert to dict and sanitize numpy arrays
            pathway_data = gene_pathway_data.iloc[0].to_dict()
            pathway_data = sanitize_dict_for_json(pathway_data)
            
            # Get interaction data for this gene
            interaction_data = None
            if len(string_protein_interactions) > 0 and 'gene_symbol' in string_protein_interactions.columns:
                gene_interactions = string_protein_interactions[
                    string_protein_interactions['gene_symbol'] == gene_symbol
                ]
                if len(gene_interactions) > 0:
                    interaction_data = gene_interactions.to_dict('records')
                    interaction_data = sanitize_dict_for_json(interaction_data)
            
            # Get enrichment data for this gene
            enrichment_data = None
            if len(string_functional_enrichment) > 0 and 'gene_symbol' in string_functional_enrichment.columns:
                gene_enrichments = string_functional_enrichment[
                    string_functional_enrichment['gene_symbol'] == gene_symbol
                ]
                if len(gene_enrichments) > 0:
                    enrichment_data = gene_enrichments.to_dict('records')
                    enrichment_data = sanitize_dict_for_json(enrichment_data)
            
            # Create pathway evidence episode
            episode = create_pathway_evidence_episode(
                gene_symbol=gene_symbol,
                pathway_data=pathway_data,
                interaction_data=interaction_data,
                enrichment_data=enrichment_data,
                group_id=DEFAULT_GROUP_ID
            )
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': episode['name'],
                'episode_data': episode,
                'validation_status': 'success',
                'episode_type': 'pathway_evidence',
                'interaction_count': int(pathway_data.get('interaction_count', 0)),
                'pathway_count': int(pathway_data.get('pathway_count', 0)),
                'pathway_score': float(pathway_data.get('pathway_evidence_score', 0.0)),
                'has_interaction_data': interaction_data is not None,
                'has_enrichment_data': enrichment_data is not None,
                'data_completeness': _calculate_episode_completeness(episode),
                'created_timestamp': pd.Timestamp.now(),
                'error_message': None  # No error for successful episodes
            })
            
            successful_episodes += 1
            
        except Exception as e:
            context.log.error(f"Failed to create pathway episode for {gene_symbol}: {e}")
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': f"Pathway_Evidence_{gene_symbol}_FAILED",
                'episode_data': None,
                'validation_status': 'failed',
                'episode_type': 'pathway_evidence',
                'interaction_count': 0,  # Default value for failed episodes
                'pathway_count': 0,      # Default value for failed episodes  
                'pathway_score': 0.0,    # Default value for failed episodes
                'has_interaction_data': False,  # Default value for failed episodes
                'has_enrichment_data': False,   # Default value for failed episodes
                'data_completeness': 0.0,
                'created_timestamp': pd.Timestamp.now(),
                'error_message': str(e)  # Keep error message as additional info
            })
            
            failed_episodes += 1
    
    context.log.info(f"Pathway episode generation complete: {successful_episodes} successful, {failed_episodes} failed")
    
    # Add metadata
    context.add_output_metadata({
        "genes_with_pathway_data": len(genes_to_process),
        "successful_episodes": successful_episodes,
        "failed_episodes": failed_episodes,
        "genes_with_interactions": sum(1 for ep in episodes if ep.get('has_interaction_data', False)),
        "genes_with_enrichments": sum(1 for ep in episodes if ep.get('has_enrichment_data', False)),
        "total_interactions": len(string_protein_interactions) if len(string_protein_interactions) > 0 else 0,
        "total_enrichments": len(string_functional_enrichment) if len(string_functional_enrichment) > 0 else 0
    })
    
    return pd.DataFrame(episodes)


# ============================================================================
# INTEGRATION AND FINAL ASSEMBLY ASSETS
# ============================================================================

@asset(
    deps=["multi_evidence_integrated"],
    description="Generate integration episodes synthesizing all evidence types",
)
def integration_episodes(
    context: AssetExecutionContext,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create integration episodes that synthesize all evidence types.
    
    Uses the get_integration_template to create comprehensive therapeutic
    target assessments based on multi-evidence integration results.
    
    Returns:
        DataFrame with integration episodes for all target genes
    """
    from .episode_templates import get_integration_template
    from .schema_validation import safe_episode_creation
    
    context.log.info(f"Creating integration episodes for {len(multi_evidence_integrated)} target genes")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    for idx, gene_row in multi_evidence_integrated.iterrows():
        gene_symbol = gene_row['gene_symbol']
        
        try:
            # Get integration template and populate with comprehensive data
            template = get_integration_template()
            integration_data = template['therapeutic_target']
            
            # Populate with complete gene data
            gene_data = gene_row.to_dict()
            
            # Core target identity and ranking
            integration_data['gene'] = gene_symbol
            integration_data['rank'] = int(gene_data.get('rank', 0))
            integration_data['enhanced_integrated_score'] = float(gene_data.get('enhanced_integrated_score', 0.0))
            integration_data['integrated_score'] = float(gene_data.get('integrated_score', 0.0))
            integration_data['evidence_types'] = int(gene_data.get('evidence_types', 0))
            
            # Evidence summary sections (populate from gene_data)
            integration_data['genetic_evidence'] = {
                'min_p_value': gene_data.get('gwas_min_p_value'),
                'max_odds_ratio': gene_data.get('gwas_max_odds_ratio'),
                'variant_count': int(gene_data.get('gwas_variant_count', 0)),
                'genome_wide_significant': bool(gene_data.get('gwas_genome_wide_significant', False)),
                'population_diversity': 'EUR'  # Default from pipeline
            }
            
            integration_data['regulatory_evidence'] = {
                'eqtl_count': int(gene_data.get('eqtl_count', 0)),
                'tissue_count': int(gene_data.get('eqtl_tissue_count', 0)),
                'min_p_value': gene_data.get('eqtl_min_p_value'),
                'max_effect_size': gene_data.get('eqtl_max_effect_size'),
                'has_substantia_nigra_eqtl': bool(gene_data.get('has_substantia_nigra_eqtl', False)),
                'has_basal_ganglia_eqtl': bool(gene_data.get('has_basal_ganglia_eqtl', False)),
                'brain_specificity': 'brain_enriched' if gene_data.get('has_substantia_nigra_eqtl', False) else 'unknown'
            }
            
            integration_data['literature_evidence'] = {
                'total_papers': int(gene_data.get('literature_papers_count', 0)),
                'therapeutic_target_papers': int(gene_data.get('therapeutic_target_papers', 0)),
                'clinical_papers': int(gene_data.get('clinical_papers', 0)),
                'literature_evidence_score': float(gene_data.get('literature_evidence_score', 0.0)),
                'has_strong_literature_evidence': bool(gene_data.get('has_strong_literature_evidence', False)),
                'research_momentum': 'unknown'  # Would need temporal analysis
            }
            
            integration_data['functional_evidence'] = {
                'interaction_count': int(gene_data.get('interaction_count', 0)),
                'pathway_count': int(gene_data.get('pathway_count', 0)),
                'degree_centrality': float(gene_data.get('degree_centrality', 0.0)),
                'pathway_evidence_score': float(gene_data.get('pathway_evidence_score', 0.0)),
                'dopamine_pathway_involvement': bool(gene_data.get('dopamine_pathway_involvement', False)),
                'functional_coherence': 'unknown'  # Would need pathway analysis
            }
            
            # Generate episode
            episode = safe_episode_creation(
                episode_name=f"Integration_{gene_symbol}",
                episode_data=template,
                source_desc=f"Multi-evidence integration for {gene_symbol}",
                group_id=DEFAULT_GROUP_ID
            )
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': episode['name'],
                'episode_data': episode,
                'validation_status': 'success',
                'episode_type': 'integration',
                'rank': int(gene_data.get('rank', 0)),
                'enhanced_score': float(gene_data.get('enhanced_integrated_score', 0.0)),
                'evidence_types': int(gene_data.get('evidence_types', 0)),
                'data_completeness': _calculate_episode_completeness(episode),
                'created_timestamp': pd.Timestamp.now()
            })
            
            successful_episodes += 1
            
        except Exception as e:
            context.log.error(f"Failed to create integration episode for {gene_symbol}: {e}")
            
            episodes.append({
                'gene_symbol': gene_symbol,
                'episode_name': f"Integration_{gene_symbol}_FAILED",
                'episode_data': None,
                'validation_status': 'failed',
                'episode_type': 'integration',
                'error_message': str(e),
                'data_completeness': 0.0,
                'created_timestamp': pd.Timestamp.now()
            })
            
            failed_episodes += 1
    
    context.log.info(f"Integration episode generation complete: {successful_episodes} successful, {failed_episodes} failed")
    
    # Add metadata
    context.add_output_metadata({
        "total_genes": len(multi_evidence_integrated),
        "successful_episodes": successful_episodes,
        "failed_episodes": failed_episodes,
        "top_tier_targets": sum(1 for ep in episodes if ep.get('enhanced_score', 0) > 150),
        "high_priority_targets": sum(1 for ep in episodes if ep.get('enhanced_score', 0) > 100),
        "multi_evidence_targets": sum(1 for ep in episodes if ep.get('evidence_types', 0) >= 3)
    })
    
    return pd.DataFrame(episodes)


@asset(
    deps=["gene_profile_episodes", "gwas_evidence_episodes", "eqtl_evidence_episodes", 
          "literature_evidence_episodes", "pathway_evidence_episodes", "integration_episodes", 
          "census_validation_episodes"],
    description="Combine all episode types into complete knowledge graph dataset"
)
def complete_knowledge_graph_episodes(
    context: AssetExecutionContext,
    gene_profile_episodes: pd.DataFrame,
    gwas_evidence_episodes: pd.DataFrame,
    eqtl_evidence_episodes: pd.DataFrame,
    literature_evidence_episodes: pd.DataFrame,
    pathway_evidence_episodes: pd.DataFrame,
    integration_episodes: pd.DataFrame,
    census_validation_episodes: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine all episode types into a complete knowledge graph dataset.
    
    Aggregates all generated episodes into a single comprehensive DataFrame
    ready for Graphiti ingestion with complete cross-referencing and validation.
    
    Returns:
        DataFrame containing all episodes across all evidence types
    """
    context.log.info("Combining all episode types into complete knowledge graph dataset")
    
    # Census validation episodes are already a DataFrame
    if len(census_validation_episodes) > 0:
        context.log.info(f"Adding {len(census_validation_episodes)} Census validation episodes")
    
    # Combine all episode DataFrames
    all_episodes = pd.concat([
        gene_profile_episodes,
        gwas_evidence_episodes,
        eqtl_evidence_episodes,
        literature_evidence_episodes,
        pathway_evidence_episodes,
        integration_episodes,
        census_validation_episodes  # Add census episodes directly
    ], ignore_index=True)
    
    # Add global episode numbering
    all_episodes['episode_id'] = range(1, len(all_episodes) + 1)
    
    # Calculate episode type distribution
    episode_type_counts = all_episodes['episode_type'].value_counts().to_dict()
    
    # Calculate success rates by episode type
    success_rates = {}
    for episode_type in all_episodes['episode_type'].unique():
        type_episodes = all_episodes[all_episodes['episode_type'] == episode_type]
        successful = len(type_episodes[type_episodes['validation_status'] == 'success'])
        total = len(type_episodes)
        success_rates[episode_type] = f"{(successful/total)*100:.1f}%" if total > 0 else "0%"
    
    # Calculate data completeness statistics
    successful_episodes = all_episodes[all_episodes['validation_status'] == 'success']
    avg_completeness = successful_episodes['data_completeness'].mean() if len(successful_episodes) > 0 else 0.0
    
    # Get gene coverage statistics
    unique_genes = all_episodes['gene_symbol'].nunique()
    genes_with_multiple_evidence = all_episodes[
        all_episodes['validation_status'] == 'success'
    ].groupby('gene_symbol').size()
    genes_with_3_plus_evidence = sum(genes_with_multiple_evidence >= 3)
    
    context.log.info(f"Knowledge graph assembly complete: {len(all_episodes)} total episodes")
    context.log.info(f"Episode type distribution: {episode_type_counts}")
    context.log.info(f"Gene coverage: {unique_genes} genes, {genes_with_3_plus_evidence} with 3+ evidence types")
    
    # Add comprehensive metadata
    context.add_output_metadata({
        "total_episodes": len(all_episodes),
        "successful_episodes": len(successful_episodes),
        "failed_episodes": len(all_episodes) - len(successful_episodes),
        "overall_success_rate": f"{(len(successful_episodes)/len(all_episodes))*100:.1f}%",
        "episode_type_counts": episode_type_counts,
        "success_rates_by_type": success_rates,
        "unique_genes_covered": unique_genes,
        "genes_with_multiple_evidence": int(genes_with_3_plus_evidence),
        "average_data_completeness": f"{avg_completeness:.3f}",
        "ready_for_graphiti_ingestion": len(successful_episodes) > 0,
        "top_genes_by_evidence": genes_with_multiple_evidence.sort_values(ascending=False).head(5).to_dict()
    })
    
    return all_episodes


# ============================================================================
# UTILITY FUNCTIONS - Supporting functions for episode processing
# ============================================================================

def _calculate_episode_completeness(episode: Dict[str, Any]) -> float:
    """
    Calculate data completeness score for an episode.
    
    Analyzes the episode content to determine what fraction of possible
    fields are populated with meaningful data.
    
    Args:
        episode: Episode dictionary from episode generator
        
    Returns:
        Completeness score from 0.0 to 1.0
    """
    if not episode or 'episode_body' not in episode:
        return 0.0
    
    try:
        episode_data = json.loads(episode['episode_body']) if isinstance(episode['episode_body'], str) else episode['episode_body']
        
        # Count non-empty fields recursively
        total_fields = 0
        populated_fields = 0
        
        def count_fields(obj, path=""):
            nonlocal total_fields, populated_fields
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, (dict, list)):
                        count_fields(value, f"{path}.{key}" if path else key)
                    else:
                        total_fields += 1
                        # Consider field populated if it's not None, empty string, or 0
                        if value is not None and value != "" and value != 0 and value != []:
                            populated_fields += 1
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    if isinstance(item, (dict, list)):
                        count_fields(item, f"{path}[{i}]")
                    else:
                        total_fields += 1
                        if item is not None and item != "" and item != 0:
                            populated_fields += 1
        
        count_fields(episode_data)
        
        return populated_fields / total_fields if total_fields > 0 else 0.0
        
    except Exception:
        return 0.0


def _validate_episode_cross_references(all_episodes: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate cross-references between episodes for the same gene.
    
    Ensures that different episode types for the same gene maintain
    consistent gene identifiers and cross-reference properly.
    
    Args:
        all_episodes: Combined episodes DataFrame
        
    Returns:
        Dictionary with validation results and any inconsistencies found
    """
    validation_results = {
        'total_genes': 0,
        'genes_with_multiple_episodes': 0,
        'cross_reference_errors': [],
        'orphaned_episodes': [],
        'gene_episode_counts': {}
    }
    
    try:
        # Group episodes by gene
        gene_groups = all_episodes.groupby('gene_symbol')
        validation_results['total_genes'] = len(gene_groups)
        
        for gene_symbol, gene_episodes in gene_groups:
            episode_types = gene_episodes['episode_type'].tolist()
            validation_results['gene_episode_counts'][gene_symbol] = len(episode_types)
            
            if len(episode_types) > 1:
                validation_results['genes_with_multiple_episodes'] += 1
            
            # Check for gene profile episode (should exist for all genes)
            if 'gene_profile' not in episode_types:
                validation_results['orphaned_episodes'].append({
                    'gene_symbol': gene_symbol,
                    'episode_types': episode_types,
                    'issue': 'missing_gene_profile'
                })
        
    except Exception as e:
        validation_results['validation_error'] = str(e)
    
    return validation_results


# ============================================================================
# GRAPHITI INTEGRATION ASSET - Final preparation for knowledge graph
# ============================================================================

@asset(
    deps=["complete_knowledge_graph_episodes"],
    description="Prepare episodes for Graphiti MCP ingestion with final validation"
)
def graphiti_ready_episodes(
    context: AssetExecutionContext,
    complete_knowledge_graph_episodes: pd.DataFrame
) -> pd.DataFrame:
    """
    Final preparation of episodes for Graphiti MCP ingestion.
    
    Performs final validation, formatting, and quality checks to ensure
    all episodes are ready for knowledge graph ingestion via Graphiti MCP.
    
    Returns:
        DataFrame with validated episodes ready for Graphiti ingestion
    """
    context.log.info(f"Preparing {len(complete_knowledge_graph_episodes)} episodes for Graphiti ingestion")
    
    # Filter to only successful episodes
    successful_episodes = complete_knowledge_graph_episodes[
        complete_knowledge_graph_episodes['validation_status'] == 'success'
    ].copy()
    
    context.log.info(f"Filtered to {len(successful_episodes)} successful episodes")
    
    # Perform cross-reference validation
    validation_results = _validate_episode_cross_references(successful_episodes)
    
    # Add final metadata for Graphiti compatibility
    successful_episodes['graphiti_ready'] = True
    successful_episodes['ingestion_timestamp'] = pd.Timestamp.now()
    successful_episodes['knowledge_graph_version'] = "1.0.0"
    
    # Sort episodes by gene and episode type for consistent ingestion order
    successful_episodes = successful_episodes.sort_values(['gene_symbol', 'episode_type'])
    
    # Final episode count by type for ingestion planning
    final_episode_counts = successful_episodes['episode_type'].value_counts().to_dict()
    
    context.log.info("Final episode preparation complete")
    context.log.info(f"Ready for ingestion: {final_episode_counts}")
    
    # Add comprehensive final metadata
    context.add_output_metadata({
        "episodes_ready_for_ingestion": len(successful_episodes),
        "final_episode_type_counts": final_episode_counts,
        "cross_reference_validation": validation_results,
        "genes_with_complete_profiles": validation_results.get('genes_with_multiple_episodes', 0),
        "average_episodes_per_gene": len(successful_episodes) / validation_results.get('total_genes', 1),
        "data_quality_score": f"{successful_episodes['data_completeness'].mean():.3f}",
        "knowledge_graph_version": "1.0.0",
        "ingestion_ready": True,
        "recommended_ingestion_order": ["gene_profile", "gwas_evidence", "eqtl_evidence", 
                                       "literature_evidence", "pathway_evidence", "integration"]
    })
    
    return successful_episodes

@asset(
    deps=["graphiti_ready_episodes"],
    description="Export validated episodes to structured JSON files for Graphiti ingestion service",
    io_manager_key="default_io_manager"
)
def graphiti_export(
    context: AssetExecutionContext,
    graphiti_ready_episodes: pd.DataFrame
) -> Dict[str, Any]:
    """
    Export 81 validated episodes to structured JSON files for external Graphiti service.
    
    Creates a clean handoff between Dagster's data processing and Graphiti's knowledge
    graph construction by exporting episodes in the proper format and order with
    comprehensive validation and integrity checks.
    
    Returns:
        Export summary with file locations, checksums, and validation results
    """
    context.log.info(f"Starting export of {len(graphiti_ready_episodes)} episodes to JSON files")
    
    # Create timestamped export directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_base_dir = Path("exports") / f"graphiti_episodes_{timestamp}"
    export_base_dir.mkdir(parents=True, exist_ok=True)
    
    context.log.info(f"Export directory created: {export_base_dir}")
    
    # Get recommended ingestion order from configuration
    recommended_order = get_episode_types_for_export()
    
    export_summary = {
        'export_timestamp': timestamp,
        'export_directory': str(export_base_dir),
        'total_episodes': len(graphiti_ready_episodes),
        'episodes_by_type': {},
        'files_created': [],
        'checksums': {},
        'validation_results': {},
        'ingestion_order': recommended_order,
        'errors': []
    }
    
    # Group episodes by type and export each type to separate files
    for episode_type in recommended_order:
        type_episodes = graphiti_ready_episodes[
            graphiti_ready_episodes['episode_type'] == episode_type
        ].copy()
        
        if len(type_episodes) == 0:
            context.log.info(f"No episodes found for type: {episode_type}")
            continue
            
        context.log.info(f"Exporting {len(type_episodes)} {episode_type} episodes")
        
        # Create type-specific directory
        type_dir = export_base_dir / "episodes" / episode_type
        type_dir.mkdir(parents=True, exist_ok=True)
        
        # Export episodes for this type
        type_summary = _export_episode_type(
            context, 
            type_episodes, 
            episode_type, 
            type_dir
        )
        
        export_summary['episodes_by_type'][episode_type] = type_summary
        export_summary['files_created'].extend(type_summary['files'])
        export_summary['checksums'].update(type_summary['checksums'])
        
        if type_summary['errors']:
            export_summary['errors'].extend(type_summary['errors'])
    
    # Create master manifest file
    manifest_file = export_base_dir / "manifest.json"
    manifest_data = _create_export_manifest(export_summary, graphiti_ready_episodes)
    
    with open(manifest_file, 'w', encoding='utf-8') as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)
    
    export_summary['manifest_file'] = str(manifest_file)
    export_summary['manifest_checksum'] = _calculate_file_checksum(manifest_file)
    
    # Create validation summary
    validation_file = export_base_dir / "validation.json" 
    validation_data = _create_validation_summary(export_summary, graphiti_ready_episodes)
    
    with open(validation_file, 'w', encoding='utf-8') as f:
        json.dump(validation_data, f, indent=2, ensure_ascii=False)
    
    export_summary['validation_file'] = str(validation_file)
    
    # Calculate overall statistics
    total_files = len(export_summary['files_created'])
    total_errors = len(export_summary['errors'])
    success_rate = ((total_files - total_errors) / max(total_files, 1)) * 100
    
    context.log.info(f"Export complete: {total_files} files created, {total_errors} errors, {success_rate:.1f}% success rate")
    
    # Add comprehensive metadata for monitoring
    context.add_output_metadata({
        "export_timestamp": timestamp,
        "export_directory": str(export_base_dir),
        "total_episodes_exported": len(graphiti_ready_episodes),
        "episode_types_exported": len([t for t in export_summary['episodes_by_type'].keys()]),
        "files_created": total_files,
        "total_file_size_mb": f"{sum(os.path.getsize(f) for f in export_summary['files_created'] if os.path.exists(f)) / 1024 / 1024:.2f}",
        "export_success_rate": f"{success_rate:.1f}%",
        "manifest_file": str(manifest_file),
        "validation_file": str(validation_file),
        "ready_for_graphiti_ingestion": total_errors == 0,
        "recommended_ingestion_order": recommended_order,
        "next_step": "Run external Graphiti ingestion service on exported files"
    })
    
    return export_summary


def _export_episode_type(
    context: AssetExecutionContext,
    episodes_df: pd.DataFrame,
    episode_type: str,
    output_dir: Path
) -> Dict[str, Any]:
    """
    Export episodes of a specific type to individual JSON files.
    
    Args:
        context: Dagster execution context
        episodes_df: Episodes DataFrame for this type
        episode_type: Type of episodes being exported
        output_dir: Directory to write files to
        
    Returns:
        Summary of export results for this type
    """
    type_summary = {
        'episode_type': episode_type,
        'total_episodes': len(episodes_df),
        'files': [],
        'checksums': {},
        'errors': [],
        'genes_included': []
    }
    
    # Sort episodes by gene symbol for consistent ordering
    episodes_df = episodes_df.sort_values('gene_symbol')
    
    for idx, episode_row in episodes_df.iterrows():
        try:
            gene_symbol = episode_row['gene_symbol']
            episode_name = episode_row['episode_name']
            episode_data = episode_row['episode_data']
            
            # Create standardized filename
            filename = f"{gene_symbol}_{episode_type}.json"
            file_path = output_dir / filename
            
            # Prepare episode for export with additional metadata
            export_episode = {
                'episode_metadata': {
                    'gene_symbol': gene_symbol,
                    'episode_name': episode_name,
                    'episode_type': episode_type,
                    'export_timestamp': datetime.now().isoformat(),
                    'validation_status': episode_row.get('validation_status', 'unknown'),
                    'data_completeness': float(episode_row.get('data_completeness', 0.0))
                },
                'graphiti_episode': episode_data  # Original episode data for Graphiti
            }
            
            # Write episode to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(export_episode, f, indent=2, ensure_ascii=False)
            
            # Calculate checksum
            checksum = _calculate_file_checksum(file_path)
            
            # Update summary
            type_summary['files'].append(str(file_path))
            type_summary['checksums'][filename] = checksum
            type_summary['genes_included'].append(gene_symbol)
            
            context.log.debug(f"Exported {episode_type} episode for {gene_symbol}")
            
        except Exception as e:
            error_msg = f"Failed to export {episode_type} episode for {episode_row.get('gene_symbol', 'unknown')}: {str(e)}"
            type_summary['errors'].append(error_msg)
            context.log.error(error_msg)
    
    context.log.info(f"Exported {len(type_summary['files'])} {episode_type} episodes with {len(type_summary['errors'])} errors")
    return type_summary


def _create_export_manifest(
    export_summary: Dict[str, Any], 
    original_episodes: pd.DataFrame
) -> Dict[str, Any]:
    """
    Create comprehensive manifest file for the export.
    
    Args:
        export_summary: Summary of export process
        original_episodes: Original episodes DataFrame
        
    Returns:
        Manifest data structure
    """
    manifest = {
        'export_info': {
            'timestamp': export_summary['export_timestamp'],
            'directory': export_summary['export_directory'],
            'dagster_asset': 'graphiti_export',
            'pipeline_version': '1.0.0'
        },
        'episode_summary': {
            'total_episodes': export_summary['total_episodes'],
            'episodes_by_type': {
                ep_type: len(data['files']) 
                for ep_type, data in export_summary['episodes_by_type'].items()
            },
            'genes_included': sorted(original_episodes['gene_symbol'].unique().tolist()),
            'total_genes': original_episodes['gene_symbol'].nunique()
        },
        'ingestion_instructions': {
            'recommended_order': export_summary['ingestion_order'],
            'file_format': 'json',
            'encoding': 'utf-8',
            'episode_structure': {
                'episode_metadata': 'Export and validation metadata',
                'graphiti_episode': 'Original episode data for Graphiti ingestion'
            }
        },
        'validation': {
            'total_files': len(export_summary['files_created']),
            'total_errors': len(export_summary['errors']),
            'success_rate': ((len(export_summary['files_created']) - len(export_summary['errors'])) / max(len(export_summary['files_created']), 1)) * 100,
            'checksums_available': True
        },
        'next_steps': [
            "Run Graphiti ingestion service on this export directory",
            "Process episodes in the recommended order",
            "Validate checksums before ingestion",
            "Monitor ingestion progress and error rates"
        ]
    }
    
    return manifest


def _create_validation_summary(
    export_summary: Dict[str, Any],
    original_episodes: pd.DataFrame  
) -> Dict[str, Any]:
    """
    Create detailed validation summary for troubleshooting.
    
    Args:
        export_summary: Summary of export process
        original_episodes: Original episodes DataFrame
        
    Returns:
        Validation data structure
    """
    validation = {
        'export_validation': {
            'timestamp': datetime.now().isoformat(),
            'total_episodes_in_source': len(original_episodes),
            'total_files_exported': len(export_summary['files_created']),
            'export_complete': len(export_summary['errors']) == 0
        },
        'episode_type_validation': {},
        'gene_coverage': {
            'genes_in_source': sorted(original_episodes['gene_symbol'].unique().tolist()),
            'genes_exported': [],
            'missing_genes': []
        },
        'file_integrity': {
            'all_files_have_checksums': True,
            'checksum_algorithm': 'sha256',
            'total_export_size_bytes': 0
        },
        'errors_and_warnings': export_summary['errors']
    }
    
    # Validate each episode type
    for episode_type, type_data in export_summary['episodes_by_type'].items():
        source_count = len(original_episodes[original_episodes['episode_type'] == episode_type])
        exported_count = len(type_data['files'])
        
        validation['episode_type_validation'][episode_type] = {
            'episodes_in_source': source_count,
            'files_exported': exported_count,
            'export_complete': source_count == exported_count,
            'genes_for_type': type_data['genes_included']
        }
        
        validation['gene_coverage']['genes_exported'].extend(type_data['genes_included'])
    
    # Remove duplicates and sort
    validation['gene_coverage']['genes_exported'] = sorted(list(set(validation['gene_coverage']['genes_exported'])))
    
    # Find missing genes
    source_genes = set(original_episodes['gene_symbol'].unique())
    exported_genes = set(validation['gene_coverage']['genes_exported'])
    validation['gene_coverage']['missing_genes'] = sorted(list(source_genes - exported_genes))
    
    # Calculate total file size
    total_size = 0
    for file_path in export_summary['files_created']:
        if os.path.exists(file_path):
            total_size += os.path.getsize(file_path)
    
    validation['file_integrity']['total_export_size_bytes'] = total_size
    validation['file_integrity']['total_export_size_mb'] = round(total_size / 1024 / 1024, 2)
    
    return validation


def _calculate_file_checksum(file_path: Path) -> str:
    """
    Calculate SHA256 checksum for a file.
    
    Args:
        file_path: Path to file
        
    Returns:
        Hex string of SHA256 checksum
    """
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


# ============================================================================
# GRAPHITI SERVICE INTEGRATION ASSET
# ============================================================================

@asset(
    deps=["graphiti_export"],
    description="Integrate with Graphiti service to ingest episodes and construct knowledge graph",
    required_resource_keys={"graphiti_service"},
    io_manager_key="default_io_manager"  # Use filesystem IO manager for dict outputs
)
def graphiti_knowledge_graph_ingestion(
    context: AssetExecutionContext,
    graphiti_export: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Bridge asset between Dagster data processing and Graphiti knowledge graph construction.
    
    This asset:
    - Takes the validated episode export from graphiti_export
    - Triggers ingestion through the Graphiti service API
    - Polls for completion with robust error handling
    - Returns comprehensive results and statistics
    
    Args:
        context: Dagster execution context with logging
        graphiti_export: Export summary from graphiti_export asset
        
    Returns:
        Complete ingestion results including operation tracking and final graph statistics
    """
    context.log.info("Starting Graphiti knowledge graph ingestion")
    
    # Get the Graphiti service resource
    graphiti_service = context.resources.graphiti_service
    
    # Extract export directory from previous asset
    export_directory = graphiti_export.get('export_directory')
    if not export_directory:
        raise ValueError("No export directory found in graphiti_export results")
    
    context.log.info(f"Ingesting export directory: {export_directory}")
    
    # Log export summary for tracking
    context.log.info(f"Export summary: {graphiti_export.get('total_episodes', 0)} episodes across {len(graphiti_export.get('episodes_by_type', {}))} types")
    
    try:
        # Step 1: Pre-ingestion validation and health check
        context.log.info("Performing pre-ingestion health check...")
        health_status = graphiti_service.health_check()
        
        if health_status["status"] != "healthy":
            error_msg = f"Graphiti service health check failed: {health_status.get('error', 'Unknown error')}"
            context.log.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "health_status": health_status,
                "export_info": graphiti_export
            }
        
        context.log.info(f"Service health check passed: {health_status['service_url']}")
        
        # Step 2: Get pre-ingestion statistics
        context.log.info("Getting pre-ingestion graph statistics...")
        pre_stats = graphiti_service.get_graph_statistics()
        
        # Step 3: Trigger ingestion with comprehensive error handling
        context.log.info("Starting knowledge graph ingestion...")
        
        # Use recommended ingestion order if available
        episode_types_filter = graphiti_export.get('ingestion_order')
        context.log.info(f"Episode types filter: {episode_types_filter}")
        
        # Perform complete ingestion workflow with detailed progress tracking
        try:
            ingestion_result = graphiti_service.ingest_and_wait(
                export_directory=export_directory,
                validate_files=True,  # Always validate for production
                force_reingest=False,  # Don't re-ingest unless explicitly needed
                episode_types_filter=episode_types_filter
            )
            
            # Log detailed ingestion progress
            if ingestion_result.get("operation_id"):
                context.log.info(f"Ingestion operation ID: {ingestion_result['operation_id']}")
            
            if ingestion_result.get("polling_result"):
                polling_info = ingestion_result["polling_result"]
                context.log.info(f"Polling completed in {polling_info.get('elapsed_time', 0):.1f} seconds")
                
                if polling_info.get("consecutive_errors", 0) > 0:
                    context.log.warning(f"Polling had {polling_info['consecutive_errors']} consecutive errors but succeeded")
            
        except Exception as e:
            context.log.error(f"Exception during ingestion workflow: {str(e)}")
            # Try to get service status for debugging
            try:
                service_status = graphiti_service.get_service_status()
                context.log.info(f"Service status during error: {service_status}")
            except Exception as status_e:
                context.log.warning(f"Could not get service status: {status_e}")
            
            return {
                "success": False,
                "error": f"Ingestion workflow exception: {str(e)}",
                "error_type": type(e).__name__,
                "health_status": health_status,
                "pre_ingestion_stats": pre_stats,
                "export_info": graphiti_export
            }
        
        if not ingestion_result.get("success"):
            error_msg = f"Knowledge graph ingestion failed: {ingestion_result.get('error', 'Unknown error')}"
            context.log.error(error_msg)
            
            # Log additional debugging information
            polling_result = ingestion_result.get("polling_result", {})
            if polling_result.get("operation_id"):
                context.log.error(f"Failed operation ID: {polling_result['operation_id']}")
            if polling_result.get("last_status"):
                context.log.error(f"Last known status: {polling_result['last_status']}")
            if polling_result.get("consecutive_errors"):
                context.log.error(f"Consecutive errors during polling: {polling_result['consecutive_errors']}")
            
            return {
                "success": False,
                "error": error_msg,
                "ingestion_result": ingestion_result,
                "health_status": health_status,
                "pre_ingestion_stats": pre_stats,
                "export_info": graphiti_export
            }
        
        # Step 4: Log successful completion details
        operation_id = ingestion_result.get("operation_id")
        elapsed_time = ingestion_result.get("total_elapsed_time", 0)
        
        context.log.info("Knowledge graph ingestion completed successfully!")
        context.log.info(f"Operation ID: {operation_id}")
        context.log.info(f"Total processing time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
        
        # Step 5: Compare pre/post statistics
        post_stats = ingestion_result.get("final_statistics", {})
        stats_comparison = _calculate_ingestion_impact(pre_stats, post_stats)
        
        context.log.info(f"Graph growth: {str(stats_comparison)}")
        
        # Step 6: Validate ingestion completeness
        validation_results = _validate_ingestion_completeness(
            context, 
            graphiti_export, 
            ingestion_result, 
            post_stats
        )
        
        # Prepare comprehensive results
        final_result = {
            "success": True,
            "operation_id": operation_id,
            "processing_time_seconds": elapsed_time,
            "processing_time_minutes": elapsed_time / 60,
            
            # Core results
            "ingestion_result": ingestion_result,
            "health_status": health_status,
            "validation_results": validation_results,
            
            # Statistics tracking
            "pre_ingestion_stats": pre_stats,
            "post_ingestion_stats": post_stats,
            "stats_comparison": stats_comparison,
            
            # Export context
            "export_info": graphiti_export,
            "episodes_ingested": graphiti_export.get('total_episodes', 0),
            "episode_types": list(graphiti_export.get('episodes_by_type', {}).keys()),
            
            # Status flags
            "validation_passed": validation_results.get("all_validations_passed", False),
            "ready_for_queries": validation_results.get("ready_for_queries", False)
        }
        
        # Add comprehensive metadata for monitoring
        context.add_output_metadata({
            "operation_id": operation_id,
            "ingestion_success": True,
            "processing_time_minutes": f"{elapsed_time/60:.1f}",
            "episodes_ingested": graphiti_export.get('total_episodes', 0),
            "episode_types_count": len(graphiti_export.get('episodes_by_type', {})),
            "graph_nodes_after": post_stats.get("graph_stats", {}).get("knowledge_graph_statistics", {}).get("total_nodes") if post_stats.get("success") else "unknown",
            "graph_edges_after": post_stats.get("graph_stats", {}).get("knowledge_graph_statistics", {}).get("total_relationships") if post_stats.get("success") else "unknown",
            "validation_passed": validation_results.get("all_validations_passed", False),
            "ready_for_research_queries": validation_results.get("ready_for_queries", False),
            "graphiti_service_url": health_status.get("service_url"),
            "export_directory": export_directory
        })
        
        context.log.info("Graphiti knowledge graph ingestion completed successfully with full validation")
        return final_result
        
    except Exception as e:
        # Comprehensive error logging
        context.log.error(f"Unexpected error during knowledge graph ingestion: {e}")
        
        # Attempt to get current service status for debugging
        try:
            service_status = graphiti_service.get_service_status()
        except Exception:
            service_status = {"error": "Could not retrieve service status"}
        
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "export_info": graphiti_export,
            "service_status": service_status,
            "troubleshooting_hint": "Check Graphiti service logs and ensure service is running and accessible"
        }


def _calculate_ingestion_impact(pre_stats: Dict[str, Any], post_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate the impact of ingestion on graph statistics"""
    
    if not pre_stats.get("success") or not post_stats.get("success"):
        return {"error": "Could not calculate impact due to missing statistics"}
    
    pre_data = pre_stats.get("graph_stats", {}).get("knowledge_graph_statistics", {})
    post_data = post_stats.get("graph_stats", {}).get("knowledge_graph_statistics", {})
    
    if not pre_data or not post_data:
        return {"error": "Statistics data not available"}
    
    pre_nodes = pre_data.get("total_nodes", 0)
    post_nodes = post_data.get("total_nodes", 0)
    pre_edges = pre_data.get("total_relationships", 0)
    post_edges = post_data.get("total_relationships", 0)
    
    return {
        "nodes_added": post_nodes - pre_nodes,
        "edges_added": post_edges - pre_edges,
        "nodes_before": pre_nodes,
        "nodes_after": post_nodes,
        "edges_before": pre_edges,
        "edges_after": post_edges,
        "growth_percentage": {
            "nodes": ((post_nodes - pre_nodes) / max(pre_nodes, 1)) * 100,
            "edges": ((post_edges - pre_edges) / max(pre_edges, 1)) * 100
        }
    }


def _validate_ingestion_completeness(
    context: AssetExecutionContext,
    export_info: Dict[str, Any],
    ingestion_result: Dict[str, Any],
    post_stats: Dict[str, Any]
) -> Dict[str, Any]:
    """Validate that ingestion completed successfully and all data is present"""
    
    validation_results = {
        "all_validations_passed": True,
        "individual_validations": {},
        "warnings": [],
        "errors": []
    }
    
    # Validation 1: Check if ingestion operation completed successfully
    final_status = ingestion_result.get("polling_result", {}).get("final_status")
    validation_results["individual_validations"]["operation_completed"] = final_status in ["completed", "success"]
    
    if not validation_results["individual_validations"]["operation_completed"]:
        error_msg = f"Ingestion operation did not complete successfully: {final_status}"
        validation_results["errors"].append(error_msg)
        validation_results["all_validations_passed"] = False
        context.log.error(error_msg)
    
    # Validation 2: Check if graph statistics are available
    graph_stats_available = post_stats.get("success", False) and bool(post_stats.get("graph_stats"))
    validation_results["individual_validations"]["graph_stats_available"] = graph_stats_available
    
    if not graph_stats_available:
        error_msg = "Knowledge graph statistics not available after ingestion"
        validation_results["errors"].append(error_msg)
        validation_results["all_validations_passed"] = False
        context.log.error(error_msg)
    
    # Validation 3: Check expected episode count (if ingestion details available)
    expected_episodes = export_info.get("total_episodes", 0)
    validation_results["individual_validations"]["expected_episode_count"] = expected_episodes > 0
    
    if expected_episodes == 0:
        warning_msg = "No episodes were expected to be ingested"
        validation_results["warnings"].append(warning_msg)
        context.log.warning(warning_msg)
    
    # Validation 4: Check if all episode types were processed
    expected_types = list(export_info.get("episodes_by_type", {}).keys())
    validation_results["individual_validations"]["all_episode_types_present"] = len(expected_types) > 0
    
    if len(expected_types) == 0:
        warning_msg = "No episode types found in export"
        validation_results["warnings"].append(warning_msg)
        context.log.warning(warning_msg)
    else:
        context.log.info(f"Expected episode types: {expected_types}")
    
    # Validation 5: Basic graph health check
    if graph_stats_available:
        graph_data = post_stats.get("graph_stats", {}).get("knowledge_graph_statistics", {})
        total_nodes = graph_data.get("total_nodes", 0)
        total_edges = graph_data.get("total_relationships", 0)
        
        graph_has_content = total_nodes > 0 and total_edges > 0
        validation_results["individual_validations"]["graph_has_content"] = graph_has_content
        
        if not graph_has_content:
            error_msg = f"Knowledge graph appears empty: {total_nodes} nodes, {total_edges} edges"
            validation_results["errors"].append(error_msg)
            validation_results["all_validations_passed"] = False
            context.log.error(error_msg)
        else:
            context.log.info(f"Knowledge graph populated: {total_nodes} nodes, {total_edges} edges")
    
    # Determine if ready for queries
    validation_results["ready_for_queries"] = (
        validation_results["all_validations_passed"] and 
        validation_results["individual_validations"].get("graph_has_content", False)
    )
    
    # Log validation summary
    if validation_results["all_validations_passed"]:
        context.log.info("✅ All ingestion validations passed")
    else:
        context.log.warning(f"⚠️ {len(validation_results['errors'])} validation errors found")
    
    if validation_results["warnings"]:
        context.log.warning(f"⚠️ {len(validation_results['warnings'])} validation warnings found")
    
    return validation_results