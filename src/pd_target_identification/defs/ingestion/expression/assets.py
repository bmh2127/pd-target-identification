# pd_target_identification/defs/ingestion/expression/assets.py
from dagster import asset, AssetExecutionContext
import pandas as pd
import numpy as np
from typing import Dict, List
from ...shared.resources import GTExResource


@asset(
    deps=["gwas_data_with_mappings"],
    group_name="data_acquisition",
    compute_kind="api", 
    tags={"source": "gtex", "data_type": "eqtl"}
)
def gtex_gene_version_mapping(
    context: AssetExecutionContext,
    gwas_data_with_mappings: pd.DataFrame,
    gtex: GTExResource
) -> pd.DataFrame:
    """
    Find the correct versioned Ensembl IDs for our GWAS genes in GTEx
    """
    context.log.info("Finding correct GTEx gene versions for GWAS genes")
    
    # Get unique base Ensembl IDs from GWAS data (without versions)
    base_ensembl_ids = gwas_data_with_mappings['ensembl_gene_id'].unique()
    base_ensembl_ids = [eid for eid in base_ensembl_ids if eid != 'N/A']
    
    context.log.info(f"Testing version formats for {len(base_ensembl_ids)} genes")
    
    version_mapping = []
    successful_genes = 0
    
    for base_id in base_ensembl_ids:
        context.log.info(f"Testing versions for {base_id}...")
        
        # Test different version numbers
        version_results = gtex.test_gene_with_version(base_id)
        
        if version_results['working_versions']:
            successful_genes += 1
            best_version_info = max(version_results['working_versions'], 
                                  key=lambda x: x['eqtl_count'])
            
            version_mapping.append({
                'base_ensembl_id': base_id,
                'gtex_ensembl_id': best_version_info['version'],
                'gene_symbol': best_version_info['gene_symbol'],
                'eqtl_count': best_version_info['eqtl_count'],
                'tissue_count': best_version_info['tissues'],
                'has_gtex_data': True
            })
            
            context.log.info(f"  ✅ {base_id} -> {best_version_info['version']} "
                            f"({best_version_info['eqtl_count']} eQTLs, "
                            f"{best_version_info['tissues']} tissues)")
        else:
            version_mapping.append({
                'base_ensembl_id': base_id,
                'gtex_ensembl_id': None,
                'gene_symbol': None,
                'eqtl_count': 0,
                'tissue_count': 0,
                'has_gtex_data': False
            })
            
            context.log.warning(f"  ❌ {base_id}: No working version found")
    
    df = pd.DataFrame(version_mapping)
    
    success_rate = successful_genes / len(base_ensembl_ids) if base_ensembl_ids else 0
    
    context.log.info(f"Version mapping results:")
    context.log.info(f"  Successful: {successful_genes}/{len(base_ensembl_ids)} ({success_rate*100:.1f}%)")
    
    if successful_genes > 0:
        total_eqtls = df[df['has_gtex_data']]['eqtl_count'].sum()
        avg_tissues = df[df['has_gtex_data']]['tissue_count'].mean()
        context.log.info(f"  Total eQTLs found: {total_eqtls}")
        context.log.info(f"  Average tissues per gene: {avg_tissues:.1f}")
    
    context.add_output_metadata({
        "genes_tested": len(base_ensembl_ids),
        "successful_mappings": successful_genes,
        "success_rate": f"{success_rate*100:.1f}%",
        "total_eqtls": int(df[df['has_gtex_data']]['eqtl_count'].sum()),
        "genes_with_eqtls": list(df[df['has_gtex_data']]['gene_symbol'].dropna())
    })
    
    return df

@asset(
    deps=["gtex_gene_version_mapping"],
    group_name="data_acquisition",
    compute_kind="api",
    tags={"source": "gtex", "data_type": "eqtl"}
)
def gtex_brain_eqtls(
    context: AssetExecutionContext,
    gtex_gene_version_mapping: pd.DataFrame,
    gtex: GTExResource
) -> pd.DataFrame:
    """
    Fetch brain eQTL data from GTEx for mapped genes
    """
    context.log.info("Fetching brain eQTL data from GTEx")
    
    # Get genes with successful GTEx mappings
    successful_genes = gtex_gene_version_mapping[gtex_gene_version_mapping['has_gtex_data']]
    
    if len(successful_genes) == 0:
        context.log.warning("No genes with GTEx mappings found")
        return pd.DataFrame()
    
    gtex_ensembl_ids = successful_genes['gtex_ensembl_id'].tolist()
    
    context.log.info(f"Fetching brain eQTLs for {len(gtex_ensembl_ids)} genes")
    context.log.info(f"GTEx IDs: {gtex_ensembl_ids}")
    
    try:
        # Get brain eQTL data
        df = gtex.get_brain_eqtls(gtex_ensembl_ids)
        
        if len(df) > 0:
            # Add base Ensembl IDs back for easy joining
            version_mapping = dict(zip(successful_genes['gtex_ensembl_id'], 
                                     successful_genes['base_ensembl_id']))
            df['base_ensembl_id'] = df['ensembl_gene_id'].map(version_mapping)
            
            # Calculate summary statistics
            total_eqtls = len(df)
            unique_genes = df['ensembl_gene_id'].nunique()
            unique_tissues = df['tissue_id'].nunique()
            unique_variants = df['variant_id'].nunique()
            
            # Find substantia nigra specifically
            sn_eqtls = df[df['tissue_id'].str.contains('Substantia_nigra', case=False, na=False)]
            
            context.log.info(f"Brain eQTL results:")
            context.log.info(f"  Total brain eQTLs: {total_eqtls}")
            context.log.info(f"  Unique genes: {unique_genes}")
            context.log.info(f"  Unique brain tissues: {unique_tissues}")
            context.log.info(f"  Unique variants: {unique_variants}")
            context.log.info(f"  Substantia nigra eQTLs: {len(sn_eqtls)}")
            
            # Log brain tissues found
            brain_tissues = sorted(df['tissue_id'].unique())
            context.log.info(f"Brain tissues with eQTLs: {brain_tissues}")
            
            # Log top genes by eQTL count
            gene_eqtl_counts = df.groupby('gene_symbol').size().sort_values(ascending=False)
            context.log.info(f"Top genes by brain eQTL count: {gene_eqtl_counts.head().to_dict()}")
            
            context.add_output_metadata({
                "total_brain_eqtls": total_eqtls,
                "unique_genes": unique_genes,
                "unique_brain_tissues": unique_tissues,
                "unique_variants": unique_variants,
                "substantia_nigra_eqtls": len(sn_eqtls),
                "brain_tissues": brain_tissues,
                "data_source": "GTEx Portal API v2"
            })
            
        else:
            context.log.warning("No brain eQTL data retrieved")
            
        return df
        
    except Exception as e:
        context.log.error(f"Failed to fetch brain eQTLs: {e}")
        
        # Return empty DataFrame with correct structure
        return pd.DataFrame(columns=[
            'ensembl_gene_id', 'gene_symbol', 'tissue_id', 'variant_id', 
            'snp_id', 'p_value', 'effect_size', 'maf', 'chromosome', 
            'position', 'base_ensembl_id'
        ])

@asset(
    deps=["gtex_brain_eqtls"],
    group_name="data_processing",
    compute_kind="python",
    tags={"data_type": "eqtl", "processing": "analyze"}
)
def gtex_eqtl_summary(
    context: AssetExecutionContext,
    gtex_brain_eqtls: pd.DataFrame
) -> pd.DataFrame:
    """
    Summarize eQTL data by gene and tissue
    """
    context.log.info("Summarizing GTEx eQTL data")
    
    if len(gtex_brain_eqtls) == 0:
        context.log.warning("No eQTL data to summarize")
        return pd.DataFrame()
    
    # Summarize by gene
    gene_summary = gtex_brain_eqtls.groupby('gene_symbol').agg({
        'tissue_id': ['count', 'nunique'],
        'p_value': 'min',
        'effect_size': ['mean', 'max', 'std'],
        'variant_id': 'nunique'
    }).round(4)
    
    # Flatten column names
    gene_summary.columns = [
        'total_eqtls', 'tissues_with_eqtls', 'min_p_value',
        'mean_effect_size', 'max_effect_size', 'effect_size_std', 'unique_variants'
    ]
    
    gene_summary = gene_summary.reset_index()
    
    # Add tissue-specific indicators
    gene_summary['has_substantia_nigra_eqtl'] = gene_summary['gene_symbol'].isin(
        gtex_brain_eqtls[gtex_brain_eqtls['tissue_id'].str.contains('Substantia_nigra', case=False, na=False)]['gene_symbol']
    )
    
    gene_summary['has_basal_ganglia_eqtl'] = gene_summary['gene_symbol'].isin(
        gtex_brain_eqtls[gtex_brain_eqtls['tissue_id'].str.contains('basal_ganglia', case=False, na=False)]['gene_symbol']
    )
    
    # Calculate eQTL strength score (combination of effect size and significance)

    gene_summary['eqtl_strength_score'] = gene_summary.apply(
        lambda row: robust_eqtl_score(row['min_p_value'], row['max_effect_size']), 
        axis=1
    )
    
    # Sort by eQTL strength
    gene_summary = gene_summary.sort_values('eqtl_strength_score', ascending=False).reset_index(drop=True)
    
    context.log.info(f"eQTL summary results:")
    context.log.info(f"  Total genes with brain eQTLs: {len(gene_summary)}")
    
    sn_genes = len(gene_summary[gene_summary['has_substantia_nigra_eqtl']])
    bg_genes = len(gene_summary[gene_summary['has_basal_ganglia_eqtl']])
    
    context.log.info(f"  Genes with substantia nigra eQTLs: {sn_genes}")
    context.log.info(f"  Genes with basal ganglia eQTLs: {bg_genes}")
    
    # Log top genes
    if len(gene_summary) > 0:
        top_genes = gene_summary.head(5)[['gene_symbol', 'eqtl_strength_score', 'tissues_with_eqtls']]
        context.log.info(f"Top genes by eQTL strength: {top_genes.to_dict('records')}")
    
    context.add_output_metadata({
        "genes_with_brain_eqtls": len(gene_summary),
        "genes_with_substantia_nigra_eqtls": sn_genes,
        "genes_with_basal_ganglia_eqtls": bg_genes,
        "mean_eqtls_per_gene": float(gene_summary['total_eqtls'].mean()),
        "mean_tissues_per_gene": float(gene_summary['tissues_with_eqtls'].mean()),
        "top_gene_by_strength": gene_summary.iloc[0]['gene_symbol'] if len(gene_summary) > 0 else "None"
    })
    
    return gene_summary

@asset(
    deps=["gtex_brain_eqtls", "gwas_data_with_mappings"],
    group_name="data_integration",
    compute_kind="python",
    tags={"data_type": "integrated"}
)
def gwas_eqtl_integrated(
    context: AssetExecutionContext,
    gtex_brain_eqtls: pd.DataFrame,
    gwas_data_with_mappings: pd.DataFrame
) -> pd.DataFrame:
    """
    Integrate GWAS and eQTL data for comprehensive gene analysis
    """
    context.log.info("Integrating GWAS and eQTL data")
    
    if len(gtex_brain_eqtls) == 0:
        context.log.warning("No eQTL data available for integration")
        return pd.DataFrame()
    
    # Get genes with both GWAS and eQTL data
    gwas_genes = set(gwas_data_with_mappings['ensembl_gene_id'].unique())
    eqtl_genes = set(gtex_brain_eqtls['base_ensembl_id'].unique())
    overlapping_genes = gwas_genes.intersection(eqtl_genes)
    
    context.log.info(f"Gene overlap: {len(overlapping_genes)} genes have both GWAS and eQTL data")
    
    integrated_data = []
    
    for ensembl_id in overlapping_genes:
        # Get GWAS data
        gwas_subset = gwas_data_with_mappings[gwas_data_with_mappings['ensembl_gene_id'] == ensembl_id]
        
        # Get eQTL data
        eqtl_subset = gtex_brain_eqtls[gtex_brain_eqtls['base_ensembl_id'] == ensembl_id]
        
        if len(gwas_subset) > 0 and len(eqtl_subset) > 0:
            gene_symbol = gwas_subset['nearest_gene'].iloc[0]
            
            # GWAS summary
            min_gwas_p = gwas_subset['p_value'].min()
            max_gwas_or = gwas_subset['odds_ratio'].max()
            gwas_variant_count = len(gwas_subset)
            
            # eQTL summary
            eqtl_count = len(eqtl_subset)
            eqtl_tissues = eqtl_subset['tissue_id'].nunique()
            min_eqtl_p = eqtl_subset['p_value'].min()
            max_effect_size = eqtl_subset['effect_size'].abs().max()
            
            # Tissue-specific eQTL flags
            has_sn_eqtl = any(eqtl_subset['tissue_id'].str.contains('Substantia_nigra', case=False, na=False))
            has_bg_eqtl = any(eqtl_subset['tissue_id'].str.contains('basal_ganglia', case=False, na=False))
            
            # Composite score: genetic association strength × genetic regulation strength
            integrated_score = (
                -np.log10(min_gwas_p) * -np.log10(min_eqtl_p) * max_effect_size
            )
            
            integrated_data.append({
                'ensembl_gene_id': ensembl_id,
                'gene_symbol': gene_symbol,
                'gwas_min_p_value': min_gwas_p,
                'gwas_max_odds_ratio': max_gwas_or,
                'gwas_variant_count': gwas_variant_count,
                'eqtl_count': eqtl_count,
                'eqtl_tissue_count': eqtl_tissues,
                'eqtl_min_p_value': min_eqtl_p,
                'eqtl_max_effect_size': max_effect_size,
                'has_substantia_nigra_eqtl': has_sn_eqtl,
                'has_basal_ganglia_eqtl': has_bg_eqtl,
                'evidence_types': 2,  # GWAS + eQTL
                'integrated_score': integrated_score
            })
    
    df = pd.DataFrame(integrated_data)
    
    if len(df) > 0:
        # Sort by integrated score
        df = df.sort_values('integrated_score', ascending=False).reset_index(drop=True)
        
        context.log.info(f"Integration results:")
        context.log.info(f"  Genes with both GWAS and eQTL: {len(df)}")
        
        sn_genes = len(df[df['has_substantia_nigra_eqtl']])
        context.log.info(f"  Genes with substantia nigra eQTLs: {sn_genes}")
        
        if len(df) >= 5:
            top_genes = df.head(5)[['gene_symbol', 'integrated_score']].to_dict('records')
            context.log.info(f"Top integrated genes: {top_genes}")
    
    context.add_output_metadata({
        "genes_with_both_data": len(df),
        "gwas_only_genes": len(gwas_genes - eqtl_genes),
        "eqtl_only_genes": len(eqtl_genes - gwas_genes),
        "genes_with_substantia_nigra_eqtls": len(df[df['has_substantia_nigra_eqtl']]) if len(df) > 0 else 0,
        "top_integrated_gene": df.iloc[0]['gene_symbol'] if len(df) > 0 else "None",
        "max_integrated_score": float(df['integrated_score'].max()) if len(df) > 0 else 0
    })
    
    return df

def robust_eqtl_score(p_value, effect_size, cap_threshold=1e-100):
    capped_p = np.maximum(p_value, cap_threshold)
    log_p_score = -np.log(capped_p)
    return log_p_score * np.abs(effect_size)