# pd_target_identification/defs/ingestion/expression/assets.py
from dagster import asset, AssetExecutionContext
import pandas as pd
import numpy as np
from typing import Dict, List
from ...shared.resources import GTExResource



@asset(
    deps=["raw_gwas_data"],
    group_name="data_acquisition",
    compute_kind="api",
    tags={"source": "gtex", "data_type": "eqtl"}
)
def gtex_brain_eqtls(
    context: AssetExecutionContext,
    raw_gwas_data: pd.DataFrame,
    gtex: GTExResource
) -> pd.DataFrame:
    """
    Fetch brain eQTL data from GTEx for GWAS genes with integrated version mapping
    """
    context.log.info("Fetching brain eQTL data from GTEx with version mapping")
    
    # Get unique base Ensembl IDs from GWAS data (without versions)
    base_ensembl_ids = raw_gwas_data['ensembl_gene_id'].unique()
    base_ensembl_ids = [eid for eid in base_ensembl_ids if eid != 'N/A']
    
    if len(base_ensembl_ids) == 0:
        context.log.warning("No Ensembl IDs found in GWAS data")
        return pd.DataFrame()
    
    context.log.info(f"Resolving GTEx versions for {len(base_ensembl_ids)} genes")
    
    # Internal version mapping - find working GTEx gene versions
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
                'eqtl_count': best_version_info['eqtl_count']
            })
            
            context.log.info(f"  ✅ {base_id} -> {best_version_info['version']} "
                            f"({best_version_info['eqtl_count']} eQTLs)")
        else:
            context.log.warning(f"  ❌ {base_id}: No working version found")
    
    if successful_genes == 0:
        context.log.warning("No genes found working GTEx versions")
        return pd.DataFrame()
    
    # Extract successful GTEx IDs for eQTL fetching
    gtex_ensembl_ids = [vm['gtex_ensembl_id'] for vm in version_mapping]
    
    context.log.info(f"Fetching brain eQTLs for {len(gtex_ensembl_ids)} genes")
    context.log.info(f"Version mapping success: {successful_genes}/{len(base_ensembl_ids)} genes")
    
    try:
        # Get brain eQTL data
        df = gtex.get_brain_eqtls(gtex_ensembl_ids)
        
        if len(df) > 0:
            # Add base Ensembl IDs back for easy joining
            gtex_to_base_mapping = {vm['gtex_ensembl_id']: vm['base_ensembl_id'] 
                                   for vm in version_mapping}
            df['base_ensembl_id'] = df['ensembl_gene_id'].map(gtex_to_base_mapping)
            
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
    deps=["gtex_brain_eqtls", "raw_gwas_data"],
    group_name="data_integration",
    compute_kind="python",
    tags={"data_type": "integrated"}
)
def gwas_eqtl_integrated(
    context: AssetExecutionContext,
    gtex_brain_eqtls: pd.DataFrame,
    raw_gwas_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Integrate GWAS and eQTL data for comprehensive gene analysis
    """
    context.log.info("Integrating GWAS and eQTL data")
    
    if len(gtex_brain_eqtls) == 0:
        context.log.warning("No eQTL data available for integration")
        return pd.DataFrame()
    
    # Get all GWAS genes (left join approach - keep all GWAS genes even without eQTL)
    gwas_genes = set(raw_gwas_data['ensembl_gene_id'].unique())
    eqtl_genes = set(gtex_brain_eqtls['base_ensembl_id'].unique())
    overlapping_genes = gwas_genes.intersection(eqtl_genes)
    
    context.log.info(f"GWAS genes: {len(gwas_genes)}, eQTL genes: {len(eqtl_genes)}")
    context.log.info(f"Gene overlap: {len(overlapping_genes)} genes have both GWAS and eQTL data")
    context.log.info(f"GWAS-only genes: {len(gwas_genes - eqtl_genes)} genes have GWAS data without eQTL")
    
    integrated_data = []
    
    # Process ALL GWAS genes (whether they have eQTL data or not)
    for ensembl_id in gwas_genes:
        # Get GWAS data
        gwas_subset = raw_gwas_data[raw_gwas_data['ensembl_gene_id'] == ensembl_id]
        
        # Get eQTL data
        eqtl_subset = gtex_brain_eqtls[gtex_brain_eqtls['base_ensembl_id'] == ensembl_id]
        
        if len(gwas_subset) > 0:  # Process all genes with GWAS data
            gene_symbol = gwas_subset['nearest_gene'].iloc[0]
            
            # GWAS summary (always available)
            min_gwas_p = gwas_subset['p_value'].min()
            max_gwas_or = gwas_subset['odds_ratio'].max()
            gwas_variant_count = len(gwas_subset)
            
            # eQTL summary (conditional on eQTL data availability)
            if len(eqtl_subset) > 0:
                # Gene has both GWAS and eQTL data
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
                evidence_types = 2  # GWAS + eQTL
            else:
                # Gene has GWAS data only (no eQTL data available)
                eqtl_count = 0
                eqtl_tissues = 0
                min_eqtl_p = None
                max_effect_size = 0.0
                has_sn_eqtl = False
                has_bg_eqtl = False
                
                # Score based on GWAS data only
                integrated_score = -np.log10(min_gwas_p) * max_gwas_or  # GWAS significance × effect size
                evidence_types = 1  # GWAS only
            
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
                'evidence_types': evidence_types,
                'integrated_score': integrated_score
            })
    
    df = pd.DataFrame(integrated_data)
    
    if len(df) > 0:
        # Sort by integrated score
        df = df.sort_values('integrated_score', ascending=False).reset_index(drop=True)
        
        context.log.info(f"Integration results:")
        context.log.info(f"  Total integrated genes: {len(df)}")
        
        gwas_eqtl_genes = len(df[df['evidence_types'] >= 2])
        gwas_only_genes = len(df[df['evidence_types'] == 1])
        context.log.info(f"  Genes with both GWAS and eQTL: {gwas_eqtl_genes}")
        context.log.info(f"  Genes with GWAS only: {gwas_only_genes}")
        
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

