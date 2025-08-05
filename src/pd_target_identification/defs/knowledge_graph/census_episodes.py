from dagster import asset, AssetExecutionContext
import pandas as pd

@asset(
    deps=["census_expression_validation"],
    group_name="knowledge_graph", 
    compute_kind="python",
    tags={"data_type": "episodes", "source": "census"}
)
def census_validation_episodes(
    context: AssetExecutionContext,
    census_expression_validation: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate Census validation episodes for knowledge graph ingestion.
    Creates separate episodes for single-cell validation evidence.
    """
    context.log.info("Generating Census validation episodes")
    
    episodes = []
    validated_count = 0
    
    for _, row in census_expression_validation.iterrows():
        if row['expression_detected']:  # Only create episodes for validated genes
            episode_name = f"{row['gene_symbol']}_census_validation"
            
            episode_content = f"""
Gene: {row['gene_symbol']}
Evidence Type: Single Cell RNA Expression Validation
Data Source: CellxGene Census (Parkinson's disease brain tissue)

Validation Results:
- PD Brain Cells Analyzed: {int(row['total_pd_cells'])}
- Cells Expressing Gene: {int(row['pd_cells_expressing'])} ({row['pd_cells_expressing']/row['total_pd_cells']*100:.1f}%)
- Mean Expression Level: {row['mean_pd_expression']:.3f}
- Brain Regions Detected: {int(row['brain_regions'])}
- Cell Types Detected: {int(row['cell_types'])}
- Validation Status: VALIDATED

Single-cell RNA sequencing validation confirms {row['gene_symbol']} expression 
in Parkinson's disease brain tissue. This provides tissue-level biological evidence 
supporting the gene as a therapeutically relevant target, complementing genetic 
association and literature evidence with direct molecular validation.

Census Validation adds +{10 if row['expression_detected'] else 0} points to integrated scoring.
"""
            
            # Create episode in same format as other episode assets
            episode_data = {
                'name': episode_name,
                'episode_body': episode_content.strip(),
                'source': 'census_validation',
                'source_description': 'CellxGene Census single-cell validation',
                'group_id': 'pd_target_identification'
            }
            
            episodes.append({
                'gene_symbol': row['gene_symbol'],
                'episode_name': episode_name,
                'episode_data': episode_data,
                'validation_status': 'success',
                'episode_type': 'census_validation',
                'data_completeness': 1.0,  # Census episodes are always complete
                'created_timestamp': pd.Timestamp.now()
            })
            
            validated_count += 1
    
    context.log.info(f"Generated {len(episodes)} Census validation episodes")
    context.log.info(f"Validated genes: {validated_count}/{len(census_expression_validation)}")
    
    # Add execution metadata
    context.add_output_metadata({
        "total_episodes": len(episodes),
        "validated_genes": validated_count,
        "total_genes_tested": len(census_expression_validation),
        "validation_rate": f"{(validated_count/len(census_expression_validation)*100):.1f}%" if len(census_expression_validation) > 0 else "0%",
        "episode_type": "census_validation"
    })
    
    return pd.DataFrame(episodes)