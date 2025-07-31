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
from typing import Dict, List, Any
from dagster import asset, AssetExecutionContext

from .episode_generators import (
    create_gene_profile_episode,
    create_gwas_evidence_episode,
    create_eqtl_evidence_episode,
    create_literature_evidence_episode,
    create_pathway_evidence_episode,
    generate_episodes_for_gene
)
from .schema_constants import DEFAULT_GROUP_ID


# ============================================================================
# INDIVIDUAL EVIDENCE TYPE ASSETS - One per evidence type
# ============================================================================

@asset(
    deps=["multi_evidence_integrated", "gene_mapping_table"],
    description="Generate gene profile episodes from integrated target ranking data"
)
def gene_profile_episodes(
    context: AssetExecutionContext,
    multi_evidence_integrated: pd.DataFrame,
    gene_mapping_table: pd.DataFrame
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
            # Get gene mapping data if available
            gene_mapping_data = None
            if len(gene_mapping_table) > 0:
                mapping_rows = gene_mapping_table[gene_mapping_table['gene_symbol'] == gene_symbol]
                if len(mapping_rows) > 0:
                    gene_mapping_data = mapping_rows.iloc[0].to_dict()
            
            # Create gene profile episode
            episode = create_gene_profile_episode(
                gene_data=gene_row.to_dict(),
                all_scores=all_scores,
                gene_mapping_data=gene_mapping_data,
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
    deps=["gwas_data_with_mappings", "multi_evidence_integrated"],
    description="Generate GWAS evidence episodes from genetic association data"
)
def gwas_evidence_episodes(
    context: AssetExecutionContext,
    gwas_data_with_mappings: pd.DataFrame,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create GWAS evidence episodes for genes with genetic associations.
    
    Transforms GWAS variant data into comprehensive genetic evidence episodes
    including variant details, statistical measures, and population information.
    
    Returns:
        DataFrame with GWAS evidence episodes for genes with variant associations
    """
    context.log.info(f"Creating GWAS evidence episodes from {len(gwas_data_with_mappings)} variants")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    # Get genes that have GWAS data and are in the integrated analysis
    target_genes = set(multi_evidence_integrated['gene_symbol'].tolist())
    genes_with_gwas = gwas_data_with_mappings['nearest_gene'].unique()
    genes_to_process = [gene for gene in genes_with_gwas if gene in target_genes]
    
    context.log.info(f"Processing GWAS evidence for {len(genes_to_process)} genes")
    
    for gene_symbol in genes_to_process:
        try:
            # Get GWAS data for this gene
            gene_gwas_data = gwas_data_with_mappings[
                gwas_data_with_mappings['nearest_gene'] == gene_symbol
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
        "total_variants": len(gwas_data_with_mappings),
        "genes_with_gwas_data": len(genes_to_process),
        "successful_episodes": successful_episodes,
        "failed_episodes": failed_episodes,
        "genome_wide_significant_genes": sum(1 for ep in episodes if ep.get('genome_wide_significant', False)),
        "min_p_value_overall": float(gwas_data_with_mappings['p_value'].min()) if len(gwas_data_with_mappings) > 0 else None
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
    deps=["literature_gene_summary", "multi_evidence_integrated"],
    description="Generate literature evidence episodes from publication analysis"
)
def literature_evidence_episodes(
    context: AssetExecutionContext,
    literature_gene_summary: pd.DataFrame,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """
    Create literature evidence episodes for genes with publication evidence.
    
    Transforms literature analysis data into comprehensive publication evidence
    episodes including paper counts, research trends, and therapeutic focus.
    
    Returns:
        DataFrame with literature evidence episodes for genes with publication data
    """
    context.log.info(f"Creating literature evidence episodes from {len(literature_gene_summary)} gene literature profiles")
    
    episodes = []
    successful_episodes = 0
    failed_episodes = 0
    
    # Get genes that have literature data and are in the integrated analysis
    target_genes = set(multi_evidence_integrated['gene_symbol'].tolist())
    genes_with_literature = literature_gene_summary['gene_symbol'].unique() if 'gene_symbol' in literature_gene_summary.columns else []
    genes_to_process = [gene for gene in genes_with_literature if gene in target_genes]
    
    context.log.info(f"Processing literature evidence for {len(genes_to_process)} genes")
    
    for gene_symbol in genes_to_process:
        try:
            # Get literature data for this gene
            gene_lit_data = literature_gene_summary[
                literature_gene_summary['gene_symbol'] == gene_symbol
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
    description="Generate integration episodes synthesizing all evidence types"
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
          "literature_evidence_episodes", "pathway_evidence_episodes", "integration_episodes"],
    description="Combine all episode types into complete knowledge graph dataset"
)
def complete_knowledge_graph_episodes(
    context: AssetExecutionContext,
    gene_profile_episodes: pd.DataFrame,
    gwas_evidence_episodes: pd.DataFrame,
    eqtl_evidence_episodes: pd.DataFrame,
    literature_evidence_episodes: pd.DataFrame,
    pathway_evidence_episodes: pd.DataFrame,
    integration_episodes: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine all episode types into a complete knowledge graph dataset.
    
    Aggregates all generated episodes into a single comprehensive DataFrame
    ready for Graphiti ingestion with complete cross-referencing and validation.
    
    Returns:
        DataFrame containing all episodes across all evidence types
    """
    context.log.info("Combining all episode types into complete knowledge graph dataset")
    
    # Combine all episode DataFrames
    all_episodes = pd.concat([
        gene_profile_episodes,
        gwas_evidence_episodes,
        eqtl_evidence_episodes,
        literature_evidence_episodes,
        pathway_evidence_episodes,
        integration_episodes
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
    
    context.log.info(f"Final episode preparation complete")
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