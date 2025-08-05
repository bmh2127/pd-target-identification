from dagster import asset, AssetExecutionContext
import pandas as pd

@asset(
    deps=["multi_evidence_integrated", "census_expression_validation"],
    group_name="integration",
    compute_kind="python"
)
def enhanced_scoring_with_census(
    context: AssetExecutionContext,
    multi_evidence_integrated: pd.DataFrame,
    census_expression_validation: pd.DataFrame
) -> pd.DataFrame:
    """
    Add Census validation bonuses to existing enhanced integrated scores.
    Creates the final target ranking with 5 evidence types.
    """
    context.log.info("Adding Census validation bonuses to target scores")
    
    # Merge validation data
    enhanced_df = multi_evidence_integrated.merge(
        census_expression_validation,
        on='gene_symbol',
        how='left'
    )
    
    # Calculate census validation bonus
    enhanced_df['census_validation_bonus'] = 0.0
    
    # Apply bonuses (following your established scoring logic)
    validated_mask = enhanced_df['expression_detected'].fillna(False)
    enhanced_df.loc[validated_mask, 'census_validation_bonus'] += 10.0
    
    well_expressed_mask = enhanced_df['pd_cells_expressing'].fillna(0) > 100
    enhanced_df.loc[well_expressed_mask, 'census_validation_bonus'] += 5.0
    
    # Update final scores
    enhanced_df['enhanced_integrated_score_final'] = (
        enhanced_df['enhanced_integrated_score'] + 
        enhanced_df['census_validation_bonus']
    )
    
    # Update evidence types count
    enhanced_df['evidence_types_final'] = enhanced_df['evidence_types'] + validated_mask.astype(int)
    
    # Re-rank with validation
    enhanced_df = enhanced_df.sort_values('enhanced_integrated_score_final', ascending=False).reset_index(drop=True)
    enhanced_df['final_rank'] = range(1, len(enhanced_df) + 1)
    
    # Log impact
    context.log.info("Census validation impact:")
    for _, row in enhanced_df.head(5).iterrows():
        bonus = row['census_validation_bonus']
        context.log.info(f"  {row['gene_symbol']}: {row['enhanced_integrated_score']:.1f} → "
                        f"{row['enhanced_integrated_score_final']:.1f} (+{bonus})")
    
    return enhanced_df