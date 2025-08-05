from dagster import ConfigurableResource
from pydantic import Field
from typing import List
import cellxgene_census
import pandas as pd

class CellxGeneCensusResource(ConfigurableResource):
    """
    CellxGene Census resource for PD target expression validation.
    Follows the same pattern as GTExResource and STRINGResource.
    """
    census_version: str = Field(default="2025-01-30", description="Census data version")
    max_cells_per_query: int = Field(default=50000, description="Max cells per validation query")
    validation_threshold: float = Field(default=0.1, description="Min expression for validation")
    timeout_seconds: int = Field(default=120, description="Query timeout")
    use_sample_range: bool = Field(default=True, description="Use soma_joinid sampling for testing")
    
    def validate_target_expression(self, gene_symbols: List[str]) -> pd.DataFrame:
        """
        Validate expression of target genes in PD brain tissue.
        Returns expression validation data for scoring enhancement.
        """
        try:
            with cellxgene_census.open_soma(census_version=self.census_version) as census:
                # Build query filter - use sampling for testing, full range for production
                if self.use_sample_range:
                    obs_filter = ("disease == 'Parkinson disease' and tissue_general == 'brain' "
                                "and soma_joinid >= 1000000 and soma_joinid < 1050000")
                else:
                    obs_filter = "disease == 'Parkinson disease' and tissue_general == 'brain'"
                
                # Query PD brain expression
                adata = cellxgene_census.get_anndata(
                    census=census,
                    organism="Homo sapiens",
                    obs_value_filter=obs_filter,
                    var_value_filter=f"feature_name in {gene_symbols}",
                    obs_column_names=["disease", "tissue", "cell_type", "dataset_id"]
                )
                
                # Calculate validation metrics
                validation_data = []
                for gene in gene_symbols:
                    if gene in adata.var['feature_name'].values:
                        # Find gene in the data
                        gene_mask = adata.var['feature_name'] == gene
                        gene_idx = gene_mask.idxmax()
                        gene_col_idx = adata.var.index.get_loc(gene_idx)
                        
                        expression_values = adata.X[:, gene_col_idx].toarray().flatten()
                        
                        validation_data.append({
                            'gene_symbol': gene,
                            'pd_cells_expressing': (expression_values > 0).sum(),
                            'mean_pd_expression': float(expression_values.mean()),
                            'max_pd_expression': float(expression_values.max()),
                            'expression_detected': float(expression_values.max()) > self.validation_threshold,
                            'total_pd_cells': len(expression_values),
                            'brain_regions': adata.obs['tissue'].nunique(),
                            'cell_types': adata.obs['cell_type'].nunique()
                        })
                    else:
                        validation_data.append({
                            'gene_symbol': gene,
                            'pd_cells_expressing': 0,
                            'mean_pd_expression': 0.0,
                            'max_pd_expression': 0.0,
                            'expression_detected': False,
                            'total_pd_cells': 0,
                            'brain_regions': 0,
                            'cell_types': 0
                        })
                
                return pd.DataFrame(validation_data)
                
        except Exception as e:
            # Return empty validation if Census fails (graceful degradation)
            return pd.DataFrame([{
                'gene_symbol': gene,
                'pd_cells_expressing': 0,
                'mean_pd_expression': 0.0,
                'max_pd_expression': 0.0,
                'expression_detected': False,
                'total_pd_cells': 0,
                'brain_regions': 0,
                'cell_types': 0,
                'validation_error': str(e)
            } for gene in gene_symbols])
    
    def calculate_validation_bonus(self, gene_symbol: str, current_score: float) -> float:
        """
        Calculate modest validation bonus for enhanced integrated scoring.
        Follows the same pattern as your existing evidence contributions.
        """
        validation_df = self.validate_target_expression([gene_symbol])
        
        if len(validation_df) == 0:
            return current_score
        
        gene_data = validation_df.iloc[0]
        bonus = 0.0
        
        # Modest bonuses (following your existing scoring pattern)
        if gene_data['expression_detected']:
            bonus += 10.0  # Expression validation bonus
        
        if gene_data['pd_cells_expressing'] > 100:  # Well-expressed
            bonus += 5.0   # Strong expression bonus
            
        if gene_data['brain_regions'] > 1:  # Multiple brain regions
            bonus += 3.0   # Regional distribution bonus
        
        return current_score + bonus