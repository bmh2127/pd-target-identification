# pd_target_identification/defs/ingestion/gene_mapping/assets.py
from dagster import asset, AssetExecutionContext
import pandas as pd
import biorosetta as br
from typing import List
import re


def clean_gene_symbol(gene_symbol: str) -> str:
    """Clean gene symbols by removing whitespace and special characters"""
    if pd.isna(gene_symbol):
        return gene_symbol
    
    # Remove carriage returns, newlines, and extra whitespace
    cleaned = re.sub(r'[\r\n]+', '', str(gene_symbol))
    cleaned = cleaned.strip()
    
    return cleaned

@asset(
    group_name="data_processing",
    compute_kind="python",
    tags={"data_type": "mapping", "source": "biorosetta"}
)
def dynamic_gene_mapping(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Create dynamic gene mapping for any list of genes using biorosetta
    
    This will be used by GWAS asset to map all discovered genes
    """
    context.log.info("Dynamic gene mapping asset created - will be called by other assets")
    
    # This is a placeholder - the actual mapping happens in map_genes_dynamically function
    return pd.DataFrame(columns=[
        'gene_symbol', 'ensembl_gene_id', 'entrez_gene_id', 
        'mapping_source'
    ])

def map_genes_dynamically(gene_symbols: List[str], context: AssetExecutionContext) -> pd.DataFrame:
    """
    Map any list of gene symbols to Ensembl and Entrez IDs using biorosetta
    
    Args:
        gene_symbols: List of gene symbols to map
        context: Dagster execution context for logging
        
    Returns:
        DataFrame with mapping results
    """
    context.log.info(f"Dynamic mapping for {len(gene_symbols)} genes")
    
    # Clean gene symbols first
    cleaned_symbols = [clean_gene_symbol(symbol) for symbol in gene_symbols]
    cleaned_symbols = [s for s in cleaned_symbols if s and s != '']  # Remove empty
    
    context.log.info(f"After cleaning: {len(cleaned_symbols)} valid gene symbols")
    
    try:
        # Initialize IDMapper
        idmap = br.IDMapper('all')
        
        # Batch convert all genes
        context.log.info("Converting symbols to Ensembl IDs...")
        ensembl_ids = idmap.convert(cleaned_symbols, 'symb', 'ensg')
        
        context.log.info("Converting symbols to Entrez IDs...")
        entrez_ids = idmap.convert(cleaned_symbols, 'symb', 'entr')
        
        # Create mapping dataframe
        mapping_data = []
        for i, gene_symbol in enumerate(cleaned_symbols):
            mapping_data.append({
                'gene_symbol': gene_symbol,
                'original_symbol': gene_symbols[i] if i < len(gene_symbols) else gene_symbol,
                'ensembl_gene_id': ensembl_ids[i] if i < len(ensembl_ids) else 'N/A',
                'entrez_gene_id': entrez_ids[i] if i < len(entrez_ids) else 'N/A',
                'mapping_source': 'biorosetta_dynamic'
            })
        
        df = pd.DataFrame(mapping_data)
        
        # Add mapping success flags
        df['has_ensembl_mapping'] = df['ensembl_gene_id'] != 'N/A'
        df['has_entrez_mapping'] = df['entrez_gene_id'] != 'N/A'
        df['has_complete_mapping'] = df['has_ensembl_mapping'] & df['has_entrez_mapping']
        
        # Log success rates
        total_genes = len(df)
        ensembl_success = len(df[df['has_ensembl_mapping']])
        entrez_success = len(df[df['has_entrez_mapping']])
        complete_success = len(df[df['has_complete_mapping']])
        
        context.log.info(f"Dynamic mapping results:")
        context.log.info(f"  Total genes: {total_genes}")
        context.log.info(f"  Ensembl success: {ensembl_success}/{total_genes} ({ensembl_success/total_genes*100:.1f}%)")
        context.log.info(f"  Entrez success: {entrez_success}/{total_genes} ({entrez_success/total_genes*100:.1f}%)")
        context.log.info(f"  Complete mapping: {complete_success}/{total_genes} ({complete_success/total_genes*100:.1f}%)")
        
        # Log failed mappings
        failed_genes = df[~df['has_complete_mapping']]['gene_symbol'].tolist()
        if failed_genes:
            context.log.warning(f"Genes without complete mapping: {failed_genes}")
            
        return df
        
    except Exception as e:
        context.log.error(f"Dynamic gene mapping failed: {e}")
        
        # Return minimal dataframe with original symbols
        fallback_data = []
        for symbol in cleaned_symbols:
            fallback_data.append({
                'gene_symbol': symbol,
                'original_symbol': symbol,
                'ensembl_gene_id': 'N/A',
                'entrez_gene_id': 'N/A',
                'mapping_source': 'fallback',
                'has_ensembl_mapping': False,
                'has_entrez_mapping': False,
                'has_complete_mapping': False
            })
        
        return pd.DataFrame(fallback_data)

