from dagster import asset, AssetExecutionContext
import pandas as pd
from ...shared.census_resource import CellxGeneCensusResource

@asset(
    deps=["multi_evidence_integrated"],  # Your existing final integration
    group_name="validation",
    compute_kind="api",
    tags={"source": "cellxgene_census", "data_type": "validation"}
)
def census_expression_validation(
    context: AssetExecutionContext,
    multi_evidence_integrated: pd.DataFrame,
    census: CellxGeneCensusResource
) -> pd.DataFrame:
    """
    Validate expression of multi-evidence targets in PD brain tissue.
    Adds expression validation as 5th evidence type.
    """
    context.log.info("Validating target expression using CellxGene Census")
    
    # Get all genes from your integrated evidence
    target_genes = multi_evidence_integrated['gene_symbol'].unique().tolist()
    context.log.info(f"Validating expression for {len(target_genes)} targets")
    
    # Get validation results
    validation_df = census.validate_target_expression(target_genes)
    
    # Log validation summary
    validated_count = validation_df['expression_detected'].sum()
    context.log.info(f"✅ {validated_count}/{len(target_genes)} targets validated in PD brain")
    
    # Log top validated targets
    top_validated = validation_df[validation_df['expression_detected']].nlargest(5, 'mean_pd_expression')
    for _, row in top_validated.iterrows():
        context.log.info(f"  {row['gene_symbol']}: {row['pd_cells_expressing']} cells, "
                        f"mean expr: {row['mean_pd_expression']:.3f}")
    
    return validation_df