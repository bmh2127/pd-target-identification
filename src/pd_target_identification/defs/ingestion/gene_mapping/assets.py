# pd_target_identification/defs/shared/gene_mapping.py
from dagster import asset, AssetExecutionContext
import pandas as pd
import biorosetta as br
from typing import List, Dict, Any
import re

@asset(
    group_name="data_processing",
    compute_kind="python",
    tags={"data_type": "mapping", "source": "biorosetta"}
)
def gene_mapping_table(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Create a comprehensive gene mapping table using biorosetta
    
    Maps between gene symbols, Ensembl gene IDs, and Entrez IDs
    Uses multiple sources with priority fallback for robustness
    """
    context.log.info("Creating gene mapping table with biorosetta")
    
    # Known Parkinson's disease genes for testing
    pd_genes = [
        'LRRK2', 'SNCA', 'GBA1', 'PARK7', 'PINK1', 'PRKN', 
        'VPS35', 'CHCHD2', 'TMEM175', 'MCCC1', 'MAPT', 
        'ATP13A2', 'PLA2G6', 'FBXO7', 'DNAJC6'
    ]
    
    try:
        # Initialize IDMapper with all sources and priority order
        # Priority: local sources (reproducible) -> remote sources (current)
        idmap = br.IDMapper('all')  # Uses EnsemblBiomart, HGNCBiomart, MyGene
        
        context.log.info(f"Mapping {len(pd_genes)} PD genes")
        
        # Convert gene symbols to Ensembl gene IDs
        context.log.info("Converting symbols to Ensembl IDs...")
        ensembl_ids = idmap.convert(pd_genes, 'symb', 'ensg')
        
        # Convert gene symbols to Entrez IDs  
        context.log.info("Converting symbols to Entrez IDs...")
        entrez_ids = idmap.convert(pd_genes, 'symb', 'entr')
        
        # Convert Ensembl IDs back to symbols (verification)
        context.log.info("Verifying Ensembl->Symbol conversion...")
        # Filter out 'N/A' values for reverse mapping
        valid_ensembl = [eid for eid in ensembl_ids if eid != 'N/A']
        if valid_ensembl:
            reverse_symbols = idmap.convert(valid_ensembl, 'ensg', 'symb')
        else:
            reverse_symbols = []
        
        # Create mapping dataframe
        mapping_data = []
        for i, gene_symbol in enumerate(pd_genes):
            mapping_data.append({
                'gene_symbol': gene_symbol,
                'ensembl_gene_id': ensembl_ids[i] if i < len(ensembl_ids) else 'N/A',
                'entrez_gene_id': entrez_ids[i] if i < len(entrez_ids) else 'N/A',
                'mapping_source': 'biorosetta_multi'
            })
        
        df = pd.DataFrame(mapping_data)
        
        # Data quality checks
        successful_ensembl = len(df[df['ensembl_gene_id'] != 'N/A'])
        successful_entrez = len(df[df['entrez_gene_id'] != 'N/A'])
        
        context.log.info(f"Mapping success rates:")
        context.log.info(f"  Ensembl: {successful_ensembl}/{len(pd_genes)} ({successful_ensembl/len(pd_genes)*100:.1f}%)")
        context.log.info(f"  Entrez: {successful_entrez}/{len(pd_genes)} ({successful_entrez/len(pd_genes)*100:.1f}%)")
        
        # Log any failed mappings for debugging
        failed_ensembl = df[df['ensembl_gene_id'] == 'N/A']['gene_symbol'].tolist()
        failed_entrez = df[df['entrez_gene_id'] == 'N/A']['gene_symbol'].tolist()
        
        if failed_ensembl:
            context.log.warning(f"Failed Ensembl mappings: {failed_ensembl}")
        if failed_entrez:
            context.log.warning(f"Failed Entrez mappings: {failed_entrez}")
        
        context.add_output_metadata({
            "total_genes": len(pd_genes),
            "ensembl_success_rate": f"{successful_ensembl/len(pd_genes)*100:.1f}%",
            "entrez_success_rate": f"{successful_entrez/len(pd_genes)*100:.1f}%",
            "failed_ensembl_count": len(failed_ensembl),
            "failed_entrez_count": len(failed_entrez),
            "mapping_sources": "ensembl_biomart,hgnc_biomart,mygene"
        })
        
        return df
        
    except Exception as e:
        context.log.error(f"Gene mapping failed: {e}")
        
        # Fallback: create minimal mapping with just symbols
        context.log.info("Creating fallback mapping table")
        fallback_data = []
        for gene_symbol in pd_genes:
            fallback_data.append({
                'gene_symbol': gene_symbol,
                'ensembl_gene_id': 'N/A',
                'entrez_gene_id': 'N/A', 
                'mapping_source': 'fallback'
            })
        
        df = pd.DataFrame(fallback_data)
        
        context.add_output_metadata({
            "total_genes": len(pd_genes),
            "mapping_status": "fallback_mode",
            "error": str(e)
        })
        
        return df

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

@asset(
    group_name="data_processing", 
    compute_kind="python",
    io_manager_key="default_io_manager"  # Use default I/O manager for dict
)
def gene_mapping_lookup(
    context: AssetExecutionContext,
    gene_mapping_table: pd.DataFrame
) -> Dict[str, Dict[str, str]]:
    """
    Create lookup dictionaries for fast gene ID conversion
    
    Returns nested dict structure for easy programmatic access
    """
    context.log.info("Creating gene mapping lookup dictionaries")
    
    # Create bidirectional lookup structures
    lookup = {
        'symbol_to_ensembl': {},
        'symbol_to_entrez': {},
        'ensembl_to_symbol': {},
        'entrez_to_symbol': {}
    }
    
    for _, row in gene_mapping_table.iterrows():
        symbol = row['gene_symbol']
        ensembl = row['ensembl_gene_id']
        entrez = row['entrez_gene_id']
        
        # Symbol as key
        if ensembl != 'N/A':
            lookup['symbol_to_ensembl'][symbol] = ensembl
            lookup['ensembl_to_symbol'][ensembl] = symbol
            
        if entrez != 'N/A':
            lookup['symbol_to_entrez'][symbol] = entrez
            lookup['entrez_to_symbol'][entrez] = symbol
    
    context.add_output_metadata({
        "symbol_to_ensembl_mappings": len(lookup['symbol_to_ensembl']),
        "symbol_to_entrez_mappings": len(lookup['symbol_to_entrez']),
        "ensembl_to_symbol_mappings": len(lookup['ensembl_to_symbol']),
        "entrez_to_symbol_mappings": len(lookup['entrez_to_symbol'])
    })
    
    return lookup

# Helper function for use in other assets
def get_ensembl_id(gene_symbol: str, gene_mapping_lookup: Dict[str, Dict[str, str]]) -> str:
    """Helper function to get Ensembl ID from gene symbol"""
    return gene_mapping_lookup['symbol_to_ensembl'].get(gene_symbol, 'N/A')

def get_entrez_id(gene_symbol: str, gene_mapping_lookup: Dict[str, Dict[str, str]]) -> str:
    """Helper function to get Entrez ID from gene symbol"""
    return gene_mapping_lookup['symbol_to_entrez'].get(gene_symbol, 'N/A')

def get_gene_symbol_from_ensembl(ensembl_id: str, gene_mapping_lookup: Dict[str, Dict[str, str]]) -> str:
    """Helper function to get gene symbol from Ensembl ID"""
    return gene_mapping_lookup['ensembl_to_symbol'].get(ensembl_id, 'N/A')