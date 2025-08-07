# pd_target_identification/defs/ingestion/gwas/assets.py
from dagster import asset, AssetExecutionContext
import pandas as pd
import numpy as np
from ...shared.resources import GWASCatalogResource
from ...shared.configs import GWASConfig
from ..gene_mapping.assets import map_genes_dynamically, clean_gene_symbol


@asset(
    group_name="data_acquisition",
    compute_kind="api",
    tags={"source": "gwas_catalog", "data_type": "genetic"}
)
def raw_gwas_data(
    context: AssetExecutionContext, 
    config: GWASConfig,
    gwas_catalog: GWASCatalogResource
) -> pd.DataFrame:
    """
    Fetch real GWAS data from GWAS Catalog API with dynamic gene mapping
    """
    # Use configuration values from config parameter instead of hardcoded ones
    p_value_threshold = config.p_value_threshold
    max_variants = config.max_variants
    
    context.log.info(f"Fetching PD GWAS data with p-value threshold: {p_value_threshold}")
    context.log.info(f"Max variants to retrieve: {max_variants}")
    
    try:
        # Fetch GWAS data using enhanced resource with config values
        df = gwas_catalog.get_pd_associations(p_value_threshold, max_variants=max_variants)
        context.log.info(f"Retrieved {len(df)} associations from GWAS Catalog")
        
        if len(df) > 0:
            # Clean gene symbols first
            context.log.info("Cleaning gene symbols...")
            df['nearest_gene'] = df['nearest_gene'].apply(clean_gene_symbol)
            
            # Get unique genes for dynamic mapping
            unique_genes = df['nearest_gene'].unique().tolist()
            unique_genes = [g for g in unique_genes if g and g != '']  # Remove empty
            
            context.log.info(f"Found {len(unique_genes)} unique genes for mapping")
            context.log.info(f"Unique genes: {unique_genes}")
            
            # Perform dynamic gene mapping
            context.log.info("Performing dynamic gene mapping...")
            gene_mapping_df = map_genes_dynamically(unique_genes, context)
            
            # Create lookup dictionaries from mapping results
            symbol_to_ensembl = dict(zip(gene_mapping_df['gene_symbol'], gene_mapping_df['ensembl_gene_id']))
            symbol_to_entrez = dict(zip(gene_mapping_df['gene_symbol'], gene_mapping_df['entrez_gene_id']))
            
            # Add gene mapping information to GWAS data
            context.log.info("Adding gene mapping information to GWAS data...")
            df['ensembl_gene_id'] = df['nearest_gene'].map(symbol_to_ensembl).fillna('N/A')
            df['entrez_gene_id'] = df['nearest_gene'].map(symbol_to_entrez).fillna('N/A')
            
            # Add mapping status flags
            df['has_ensembl_mapping'] = df['ensembl_gene_id'] != 'N/A'
            df['has_entrez_mapping'] = df['entrez_gene_id'] != 'N/A'
            df['has_complete_mapping'] = df['has_ensembl_mapping'] & df['has_entrez_mapping']
            
            # Calculate mapping statistics
            total_variants = len(df)
            unique_genes_count = len(unique_genes)
            genes_with_ensembl = gene_mapping_df[gene_mapping_df['has_ensembl_mapping']].shape[0]
            genes_with_entrez = gene_mapping_df[gene_mapping_df['has_entrez_mapping']].shape[0]
            genes_with_complete = gene_mapping_df[gene_mapping_df['has_complete_mapping']].shape[0]
            
            # Calculate variant-level statistics
            variants_with_complete = len(df[df['has_complete_mapping']])
            
            context.log.info("Gene mapping results:")
            context.log.info(f"  Total unique genes: {unique_genes_count}")
            context.log.info(f"  Genes with Ensembl ID: {genes_with_ensembl}/{unique_genes_count} ({genes_with_ensembl/unique_genes_count*100:.1f}%)")
            context.log.info(f"  Genes with Entrez ID: {genes_with_entrez}/{unique_genes_count} ({genes_with_entrez/unique_genes_count*100:.1f}%)")
            context.log.info(f"  Genes with complete mapping: {genes_with_complete}/{unique_genes_count} ({genes_with_complete/unique_genes_count*100:.1f}%)")
            context.log.info(f"  Variants with complete mapping: {variants_with_complete}/{total_variants} ({variants_with_complete/total_variants*100:.1f}%)")
            
            # Log unmapped genes for debugging
            unmapped_genes = gene_mapping_df[~gene_mapping_df['has_complete_mapping']]['gene_symbol'].tolist()
            if unmapped_genes:
                context.log.warning(f"Genes without complete mapping: {unmapped_genes}")
            
            # Filter to variants with complete gene mapping
            context.log.info("Filtering to variants with complete gene mapping...")
            complete_mapping_data = df[df['has_complete_mapping']].copy()
            
            # Sort by p-value (most significant first)
            complete_mapping_data = complete_mapping_data.sort_values('p_value').reset_index(drop=True)
            
            # Add derived fields
            complete_mapping_data['neg_log10_p'] = -np.log10(complete_mapping_data['p_value'])
            complete_mapping_data['log_odds_ratio'] = np.log(complete_mapping_data['odds_ratio'])
            
            # Calculate significance tiers
            complete_mapping_data['significance_tier'] = complete_mapping_data['p_value'].apply(
                lambda p: 'genome_wide' if p < 5e-8 else 'suggestive' if p < 1e-5 else 'nominal'
            )
            
            # Calculate final statistics
            filtered_count = len(complete_mapping_data)
            filtered_genes = complete_mapping_data['nearest_gene'].nunique()
            
            context.log.info(f"Filtered data: {filtered_count}/{total_variants} variants ({filtered_count/total_variants*100:.1f}%)")
            context.log.info(f"Filtered genes: {filtered_genes}/{unique_genes_count} genes ({filtered_genes/unique_genes_count*100:.1f}%)")
            
            context.add_output_metadata({
                "num_variants": total_variants,
                "num_unique_genes": unique_genes_count,
                "genes_with_ensembl_mapping": genes_with_ensembl,
                "genes_with_entrez_mapping": genes_with_entrez, 
                "genes_with_complete_mapping": genes_with_complete,
                "variants_with_complete_mapping": variants_with_complete,
                "gene_ensembl_mapping_rate": f"{genes_with_ensembl/unique_genes_count*100:.1f}%",
                "gene_entrez_mapping_rate": f"{genes_with_entrez/unique_genes_count*100:.1f}%",
                "gene_complete_mapping_rate": f"{genes_with_complete/unique_genes_count*100:.1f}%",
                "variant_complete_mapping_rate": f"{variants_with_complete/total_variants*100:.1f}%",
                "filtered_variant_count": filtered_count,
                "filtered_gene_count": filtered_genes,
                "retention_rate": f"{filtered_count/total_variants*100:.1f}%",
                "gene_retention_rate": f"{filtered_genes/unique_genes_count*100:.1f}%",
                "genome_wide_significant": len(complete_mapping_data[complete_mapping_data['significance_tier'] == 'genome_wide']),
                "mean_neg_log10_p": float(complete_mapping_data['neg_log10_p'].mean()) if filtered_count > 0 else 0,
                "top_gene": complete_mapping_data.iloc[0]['nearest_gene'] if filtered_count > 0 else "None",
                "data_source": "GWAS Catalog API + dynamic biorosetta mapping",
                "unmapped_gene_count": len(unmapped_genes)
            })
            
        else:
            context.log.warning("No GWAS associations retrieved, falling back to mock data")
            raise Exception("No data from API")
        
        return complete_mapping_data
        
    except Exception as e:
        context.log.warning(f"API call failed: {e}, falling back to enhanced mock data")
        
        # Generate enhanced mock GWAS data with mapping
        np.random.seed(42)
        
        # Use a broader set of known PD genes for mock data
        pd_genes = ['LRRK2', 'SNCA', 'GBA1', 'PARK7', 'PINK1', 'PRKN', 'VPS35', 'MAPT', 'BST1', 'GAK']
        
        gwas_data = []
        for i, gene in enumerate(pd_genes):
            gwas_data.append({
                'variant_id': f'rs100000{i}',
                'rsid': f'rs100000{i}',
                'chromosome': str(np.random.choice([1, 4, 6, 12, 17])),
                'position': np.random.randint(1000000, 50000000),
                'p_value': np.random.exponential(1e-10),
                'odds_ratio': np.random.uniform(1.1, 2.0),
                'beta': np.random.normal(0, 0.2),
                'nearest_gene': gene,
                'population': 'EUR',
                'study_accession': f'GCST{90000000 + i}',
                'sample_size': np.random.randint(10000, 100000),
                'effect_allele': np.random.choice(['A', 'T', 'G', 'C'])
            })
        
        df = pd.DataFrame(gwas_data)
        
        # Apply dynamic mapping to mock data too
        context.log.info("Applying dynamic mapping to mock data...")
        unique_genes = df['nearest_gene'].unique().tolist()
        gene_mapping_df = map_genes_dynamically(unique_genes, context)
        
        # Create lookup dictionaries
        symbol_to_ensembl = dict(zip(gene_mapping_df['gene_symbol'], gene_mapping_df['ensembl_gene_id']))
        symbol_to_entrez = dict(zip(gene_mapping_df['gene_symbol'], gene_mapping_df['entrez_gene_id']))
        
        # Add mapping information
        df['ensembl_gene_id'] = df['nearest_gene'].map(symbol_to_ensembl).fillna('N/A')
        df['entrez_gene_id'] = df['nearest_gene'].map(symbol_to_entrez).fillna('N/A')
        df['has_ensembl_mapping'] = df['ensembl_gene_id'] != 'N/A'
        df['has_entrez_mapping'] = df['entrez_gene_id'] != 'N/A'
        df['has_complete_mapping'] = df['has_ensembl_mapping'] & df['has_entrez_mapping']
        
        # Calculate mock mapping statistics
        total_variants = len(df)
        unique_genes_count = len(unique_genes)
        genes_with_complete = gene_mapping_df[gene_mapping_df['has_complete_mapping']].shape[0]
        variants_with_complete = len(df[df['has_complete_mapping']])
        
        # Filter to variants with complete gene mapping (same as real data)
        complete_mapping_data = df[df['has_complete_mapping']].copy()
        complete_mapping_data = complete_mapping_data.sort_values('p_value').reset_index(drop=True)
        
        # Add derived fields
        complete_mapping_data['neg_log10_p'] = -np.log10(complete_mapping_data['p_value'])
        complete_mapping_data['log_odds_ratio'] = np.log(complete_mapping_data['odds_ratio'])
        complete_mapping_data['significance_tier'] = complete_mapping_data['p_value'].apply(
            lambda p: 'genome_wide' if p < 5e-8 else 'suggestive' if p < 1e-5 else 'nominal'
        )
        
        # Calculate final statistics
        filtered_count = len(complete_mapping_data)
        filtered_genes = complete_mapping_data['nearest_gene'].nunique()
        
        context.add_output_metadata({
            "num_variants": total_variants,
            "num_unique_genes": unique_genes_count,
            "genes_with_complete_mapping": genes_with_complete,
            "variants_with_complete_mapping": variants_with_complete,
            "gene_complete_mapping_rate": f"{genes_with_complete/unique_genes_count*100:.1f}%",
            "variant_complete_mapping_rate": f"{variants_with_complete/total_variants*100:.1f}%",
            "filtered_variant_count": filtered_count,
            "filtered_gene_count": filtered_genes,
            "retention_rate": f"{filtered_count/total_variants*100:.1f}%",
            "data_source": "Mock data with dynamic biorosetta mapping (API fallback)"
        })
        
        return complete_mapping_data

