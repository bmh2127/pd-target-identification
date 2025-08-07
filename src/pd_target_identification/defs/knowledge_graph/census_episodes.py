from dagster import asset, AssetExecutionContext
import pandas as pd
from .episode_generators import create_census_validation_episode
from .schema_constants import DEFAULT_GROUP_ID

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
            # Prepare census data for the generator function
            census_data = {
                'expression_detected': row['expression_detected'],
                'total_pd_cells': row['total_pd_cells'],
                'pd_cells_expressing': row['pd_cells_expressing'],
                'mean_pd_expression': row['mean_pd_expression'],
                'brain_regions': row['brain_regions'],
                'cell_types': row['cell_types'],
                'scoring_bonus': 10 if row['expression_detected'] else 0
            }
            
            # Create episode using the standardized generator function
            episode_data = create_census_validation_episode(
                gene_symbol=row['gene_symbol'],
                census_data=census_data,
                group_id=DEFAULT_GROUP_ID
            )
            
            episode_name = episode_data['name']
            
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