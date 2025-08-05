from dagster import asset, AssetExecutionContext
import pandas as pd
import time
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
    Adds expression validation as 5th evidence type with detailed progress tracking.
    """
    start_time = time.time()
    
    context.log.info("🔬 Starting CellxGene Census expression validation")
    context.log.info(f"📊 Census version: {census.census_version}")
    context.log.info(f"🔧 Sample mode: {'Testing (50K cells)' if census.use_sample_range else 'Production (1.8M cells)'}")
    
    # Get all genes from your integrated evidence
    target_genes = multi_evidence_integrated['gene_symbol'].unique().tolist()
    context.log.info(f"🎯 Preparing to validate {len(target_genes)} target genes")
    context.log.info(f"📋 Genes: {', '.join(target_genes[:10])}{'...' if len(target_genes) > 10 else ''}")
    
    # Log query configuration
    context.log.info("🔍 Census query configuration:")
    context.log.info(f"  • Validation threshold: {census.validation_threshold}")
    context.log.info(f"  • Max cells per query: {census.max_cells_per_query}")
    context.log.info(f"  • Timeout: {census.timeout_seconds}s")
    
    # Execute validation with progress tracking
    context.log.info("⏳ Executing Census validation query...")
    query_start = time.time()
    
    def progress_callback(message: str):
        """Forward progress messages from the resource to the asset logger"""
        context.log.info(f"    {message}")
    
    try:
        validation_df = census.validate_target_expression(target_genes, progress_callback=progress_callback)
        query_time = time.time() - query_start
        
        context.log.info(f"✅ Census query completed in {query_time:.1f}s")
        
        # Process and log detailed results
        context.log.info("📈 Processing validation results...")
        
        if len(validation_df) > 0:
            # Log validation summary
            validated_count = validation_df['expression_detected'].sum()
            total_genes = len(validation_df)
            validation_rate = (validated_count / total_genes) * 100
            
            context.log.info(f"🎯 Validation Summary:")
            context.log.info(f"  • {validated_count}/{total_genes} genes validated ({validation_rate:.1f}%)")
            
            if validated_count > 0:
                # Log expression statistics
                validated_genes = validation_df[validation_df['expression_detected']]
                avg_expression = validated_genes['mean_pd_expression'].mean()
                avg_cell_count = validated_genes['pd_cells_expressing'].mean()
                total_cells = validation_df['total_pd_cells'].iloc[0] if len(validation_df) > 0 else 0
                
                context.log.info(f"  • Average expression level: {avg_expression:.3f}")
                context.log.info(f"  • Average expressing cells: {avg_cell_count:.0f}")
                context.log.info(f"  • Total PD cells analyzed: {total_cells:,}")
                
                # Log top validated targets
                context.log.info("🏆 Top validated targets:")
                top_validated = validated_genes.nlargest(5, 'mean_pd_expression')
                for i, (_, row) in enumerate(top_validated.iterrows(), 1):
                    expr_rate = (row['pd_cells_expressing'] / row['total_pd_cells']) * 100
                    context.log.info(f"  {i}. {row['gene_symbol']}: "
                                   f"{row['pd_cells_expressing']:,} cells ({expr_rate:.1f}%), "
                                   f"mean expr: {row['mean_pd_expression']:.3f}")
                
                # Log genes that weren't detected
                not_detected = validation_df[~validation_df['expression_detected']]
                if len(not_detected) > 0:
                    context.log.info(f"⚠️  {len(not_detected)} genes not detected in PD brain:")
                    context.log.info(f"   {', '.join(not_detected['gene_symbol'].tolist())}")
            else:
                context.log.warning("⚠️  No genes showed significant expression in PD brain tissue")
                
        else:
            context.log.warning("⚠️  No validation results returned from Census")
            
    except Exception as e:
        query_time = time.time() - query_start
        context.log.error(f"❌ Census validation failed after {query_time:.1f}s: {str(e)}")
        # Return empty dataframe with proper structure for graceful degradation
        validation_df = pd.DataFrame(columns=[
            'gene_symbol', 'pd_cells_expressing', 'mean_pd_expression', 
            'max_pd_expression', 'expression_detected', 'total_pd_cells',
            'brain_regions', 'cell_types'
        ])
    
    # Final timing and metadata
    total_time = time.time() - start_time
    context.log.info(f"⏱️  Census validation completed in {total_time:.1f}s total")
    
    # Add execution metadata
    context.add_output_metadata({
        "validation_rate": f"{(validation_df['expression_detected'].sum() / len(validation_df) * 100):.1f}%" if len(validation_df) > 0 else "0%",
        "total_genes_tested": len(target_genes),
        "genes_validated": int(validation_df['expression_detected'].sum()) if len(validation_df) > 0 else 0,
        "query_time_seconds": f"{query_time:.1f}",
        "total_execution_time": f"{total_time:.1f}",
        "census_version": census.census_version,
        "sample_mode": census.use_sample_range,
        "total_pd_cells": int(validation_df['total_pd_cells'].iloc[0]) if len(validation_df) > 0 else 0
    })
    
    return validation_df