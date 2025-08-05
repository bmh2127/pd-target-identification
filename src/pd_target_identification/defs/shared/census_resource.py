from dagster import ConfigurableResource
from pydantic import Field
from typing import List, Optional, Callable
import cellxgene_census
import pandas as pd
import time

class CellxGeneCensusResource(ConfigurableResource):
    """
    CellxGene Census resource for PD target expression validation.
    Follows the same pattern as GTExResource and STRINGResource.
    """
    census_version: str = Field(default="2025-01-30", description="Census data version")
    max_cells_per_query: int = Field(default=50000, description="Max cells per validation query")
    validation_threshold: float = Field(default=0.1, description="Min expression for validation")
    timeout_seconds: int = Field(default=600, description="Query timeout (10 minutes for production)")
    use_sample_range: bool = Field(default=True, description="Use soma_joinid sampling for testing")
    
    # Batch processing configuration
    enable_batch_processing: bool = Field(default=True, description="Enable batch processing for production queries")
    genes_per_batch: int = Field(default=5, description="Number of genes to process per batch")
    batch_timeout: int = Field(default=180, description="Timeout per batch (3 minutes)")
    
    def validate_target_expression(self, gene_symbols: List[str], 
                                  progress_callback: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
        """
        Validate expression of target genes in PD brain tissue.
        Returns expression validation data for scoring enhancement.
        
        Args:
            gene_symbols: List of gene symbols to validate
            progress_callback: Optional callback function for progress updates
        """
        def log_progress(message: str):
            """Internal progress logging"""
            if progress_callback:
                progress_callback(message)
        
        # Decide whether to use batch processing
        if (self.enable_batch_processing and 
            not self.use_sample_range and 
            len(gene_symbols) > self.genes_per_batch):
            log_progress(f"🔄 Using batch processing: {len(gene_symbols)} genes in batches of {self.genes_per_batch}")
            return self._validate_in_batches(gene_symbols, log_progress)
        else:
            if len(gene_symbols) <= self.genes_per_batch:
                log_progress(f"📝 Small gene set ({len(gene_symbols)} genes) - using single query")
            elif self.use_sample_range:
                log_progress(f"🧪 Testing mode - using single query for {len(gene_symbols)} genes")
            else:
                log_progress(f"⚡ Batch processing disabled - using single query for {len(gene_symbols)} genes")
            return self._validate_single_query(gene_symbols, log_progress)
    
    def _validate_in_batches(self, gene_symbols: List[str], log_progress: Callable[[str], None]) -> pd.DataFrame:
        """
        Validate genes using batch processing for better reliability and progress tracking.
        """
        total_genes = len(gene_symbols)
        batch_size = self.genes_per_batch
        total_batches = (total_genes + batch_size - 1) // batch_size  # Ceiling division
        
        log_progress(f"🔢 Processing {total_genes} genes in {total_batches} batches of {batch_size} genes each")
        log_progress(f"⏱️  Batch timeout: {self.batch_timeout}s per batch")
        
        all_validation_data = []
        start_time = time.time()
        
        for batch_num in range(total_batches):
            batch_start_idx = batch_num * batch_size
            batch_end_idx = min(batch_start_idx + batch_size, total_genes)
            batch_genes = gene_symbols[batch_start_idx:batch_end_idx]
            
            log_progress(f"📦 Batch {batch_num + 1}/{total_batches}: Processing genes {batch_start_idx + 1}-{batch_end_idx}")
            log_progress(f"   Genes: {', '.join(batch_genes)}")
            
            batch_start_time = time.time()
            
            try:
                # Create a temporary resource with batch timeout
                batch_resource = CellxGeneCensusResource(
                    census_version=self.census_version,
                    validation_threshold=self.validation_threshold,
                    timeout_seconds=self.batch_timeout,
                    use_sample_range=self.use_sample_range,
                    enable_batch_processing=False  # Prevent recursion
                )
                
                # Process this batch
                batch_validation_df = batch_resource._validate_single_query(
                    batch_genes, 
                    lambda msg: log_progress(f"     {msg}")
                )
                
                batch_time = time.time() - batch_start_time
                log_progress(f"✅ Batch {batch_num + 1} completed in {batch_time:.1f}s ({len(batch_validation_df)} genes processed)")
                
                # Collect results
                if len(batch_validation_df) > 0:
                    all_validation_data.append(batch_validation_df)
                
                # Progress summary
                genes_processed = batch_end_idx
                total_elapsed = time.time() - start_time
                avg_time_per_gene = total_elapsed / genes_processed
                estimated_remaining = avg_time_per_gene * (total_genes - genes_processed)
                
                log_progress(f"📊 Progress: {genes_processed}/{total_genes} genes ({(genes_processed/total_genes)*100:.1f}%)")
                if genes_processed < total_genes:
                    log_progress(f"⏱️  Estimated remaining time: {estimated_remaining:.1f}s")
                
            except Exception as e:
                log_progress(f"❌ Batch {batch_num + 1} failed: {str(e)}")
                # Create empty results for failed batch
                empty_batch_data = []
                for gene in batch_genes:
                    empty_batch_data.append({
                        'gene_symbol': gene,
                        'pd_cells_expressing': 0,
                        'mean_pd_expression': 0.0,
                        'max_pd_expression': 0.0,
                        'expression_detected': False,
                        'total_pd_cells': 0,
                        'brain_regions': 0,
                        'cell_types': 0,
                        'batch_error': str(e)
                    })
                all_validation_data.append(pd.DataFrame(empty_batch_data))
        
        # Combine all batch results
        if all_validation_data:
            final_df = pd.concat(all_validation_data, ignore_index=True)
            total_time = time.time() - start_time
            log_progress(f"🎯 Batch processing complete! {len(final_df)} genes processed in {total_time:.1f}s total")
            log_progress(f"📈 Average time per batch: {total_time/total_batches:.1f}s")
            return final_df
        else:
            log_progress("⚠️  No validation data collected from any batch")
            return pd.DataFrame()
    
    def _validate_single_query(self, gene_symbols: List[str], log_progress: Callable[[str], None]) -> pd.DataFrame:
        """
        Validate genes using a single Census query (original method).
        """
        
        try:
            log_progress("🔗 Opening CellxGene Census connection...")
            start_time = time.time()
            
            with cellxgene_census.open_soma(census_version=self.census_version) as census:
                connection_time = time.time() - start_time
                log_progress(f"✅ Census connected in {connection_time:.1f}s")
                
                # Build query filter - use sampling for testing, full range for production
                if self.use_sample_range:
                    obs_filter = ("disease == 'Parkinson disease' and tissue_general == 'brain' "
                                "and soma_joinid >= 1000000 and soma_joinid < 1050000")
                    cell_description = "~50K sampled PD brain cells"
                else:
                    obs_filter = "disease == 'Parkinson disease' and tissue_general == 'brain'"
                    cell_description = "~1.8M PD brain cells"
                
                log_progress(f"🧠 Querying {cell_description} for {len(gene_symbols)} genes...")
                log_progress(f"⏱️  Query timeout set to {self.timeout_seconds}s - monitoring progress...")
                query_start = time.time()
                
                # Enhanced query with timeout monitoring
                try:
                    # Log intermediate progress every 30 seconds for long queries
                    import signal
                    
                    def timeout_handler(signum, frame):
                        raise TimeoutError(f"Census query exceeded {self.timeout_seconds}s timeout")
                    
                    # Set timeout alarm (only on Unix systems)
                    if hasattr(signal, 'alarm'):
                        signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(self.timeout_seconds)
                    
                    # Monitor query progress with periodic updates
                    progress_thread = None
                    if query_start:
                        def log_intermediate_progress():
                            elapsed = 0
                            while elapsed < self.timeout_seconds:
                                time.sleep(30)  # Check every 30 seconds
                                elapsed = time.time() - query_start
                                if elapsed < self.timeout_seconds:
                                    log_progress(f"⏳ Query in progress... {elapsed:.0f}s elapsed (timeout at {self.timeout_seconds}s)")
                        
                        import threading
                        progress_thread = threading.Thread(target=log_intermediate_progress, daemon=True)
                        progress_thread.start()
                    
                    # Execute the actual Census query
                    adata = cellxgene_census.get_anndata(
                        census=census,
                        organism="Homo sapiens",
                        obs_value_filter=obs_filter,
                        var_value_filter=f"feature_name in {gene_symbols}",
                        obs_column_names=["disease", "tissue", "cell_type", "dataset_id"]
                    )
                    
                    # Clear timeout alarm
                    if hasattr(signal, 'alarm'):
                        signal.alarm(0)
                    
                    query_time = time.time() - query_start
                    log_progress(f"📊 Data retrieved successfully in {query_time:.1f}s: {adata.shape[0]:,} cells x {adata.shape[1]} genes")
                    
                except TimeoutError as e:
                    log_progress(f"⏰ Census query timed out after {self.timeout_seconds}s")
                    raise e
                except Exception as e:
                    query_time = time.time() - query_start
                    log_progress(f"❌ Census query failed after {query_time:.1f}s: {str(e)}")
                    raise e
                
                # Calculate validation metrics
                log_progress("🧮 Processing expression data for each gene...")
                validation_data = []
                total_genes = len(gene_symbols)
                
                for i, gene in enumerate(gene_symbols, 1):
                    if gene in adata.var['feature_name'].values:
                        # Find gene in the data
                        gene_mask = adata.var['feature_name'] == gene
                        gene_idx = gene_mask.idxmax()
                        gene_col_idx = adata.var.index.get_loc(gene_idx)
                        
                        expression_values = adata.X[:, gene_col_idx].toarray().flatten()
                        expressing_cells = (expression_values > 0).sum()
                        mean_expr = float(expression_values.mean())
                        
                        validation_data.append({
                            'gene_symbol': gene,
                            'pd_cells_expressing': expressing_cells,
                            'mean_pd_expression': mean_expr,
                            'max_pd_expression': float(expression_values.max()),
                            'expression_detected': float(expression_values.max()) > self.validation_threshold,
                            'total_pd_cells': len(expression_values),
                            'brain_regions': adata.obs['tissue'].nunique(),
                            'cell_types': adata.obs['cell_type'].nunique()
                        })
                        
                        if i % 5 == 0 or i == total_genes:  # Progress every 5 genes or at end
                            log_progress(f"  📈 Processed {i}/{total_genes} genes | {gene}: {expressing_cells:,} cells expressing")
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
                        
                        if i % 5 == 0 or i == total_genes:  # Progress every 5 genes or at end
                            log_progress(f"  ⚠️  Processed {i}/{total_genes} genes | {gene}: not found in data")
                
                log_progress("✅ Expression validation processing complete")
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