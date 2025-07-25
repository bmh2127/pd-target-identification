import requests
import pandas as pd
import json
import time

def check_substantia_nigra_across_genes():
    """Check substantia nigra eQTL availability across multiple genes"""
    
    # Test genes with different characteristics
    test_genes = {
        'SNCA': 'ENSG00000145335.15',      # Major PD gene from your data
        'LRRK2': 'ENSG00000188906.14',     # Another major PD gene  
        'DGKQ': 'ENSG00000145214.13',      # Had most brain eQTLs in your data
        'APOE': 'ENSG00000130203.11',      # Well-studied brain gene (test version)
        'TH': 'ENSG00000180176.12'         # Tyrosine hydroxylase - dopamine synthesis
    }
    
    sn_results = []
    total_brain_results = []
    
    for gene_name, ensembl_id in test_genes.items():
        print(f"\nTesting {gene_name} ({ensembl_id})...")
        
        url = "https://gtexportal.org/api/v2/association/independentEqtl"
        params = {
            'gencodeId': ensembl_id,
            'datasetId': 'gtex_v8',
            'itemsPerPage': 1000
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data:
                    all_eqtls = pd.DataFrame(data['data'])
                    
                    # Count brain eQTLs
                    brain_eqtls = all_eqtls[all_eqtls['tissueSiteDetailId'].str.contains('Brain', case=False, na=False)]
                    
                    # Count substantia nigra specifically
                    sn_eqtls = all_eqtls[all_eqtls['tissueSiteDetailId'].str.contains('Substantia_nigra', case=False, na=False)]
                    
                    total_eqtls = len(all_eqtls)
                    brain_count = len(brain_eqtls)
                    sn_count = len(sn_eqtls)
                    
                    print(f"  Total eQTLs: {total_eqtls}")
                    print(f"  Brain eQTLs: {brain_count} ({brain_count/total_eqtls*100:.1f}%)")
                    print(f"  Substantia nigra eQTLs: {sn_count}")
                    
                    # Store results
                    sn_results.append({
                        'gene': gene_name,
                        'ensembl_id': ensembl_id,
                        'total_eqtls': total_eqtls,
                        'brain_eqtls': brain_count,
                        'sn_eqtls': sn_count,
                        'brain_percentage': brain_count/total_eqtls*100 if total_eqtls > 0 else 0
                    })
                    
                    # Show brain tissue distribution
                    if brain_count > 0:
                        brain_tissues = brain_eqtls['tissueSiteDetailId'].value_counts()
                        print(f"  Brain tissues: {list(brain_tissues.index)}")
                        
                        # Check if substantia nigra appears
                        sn_tissues = [t for t in brain_tissues.index if 'Substantia_nigra' in t]
                        if sn_tissues:
                            print(f"  Substantia nigra tissues found: {sn_tissues}")
                else:
                    print(f"  No eQTL data found")
                    sn_results.append({
                        'gene': gene_name,
                        'ensembl_id': ensembl_id,
                        'total_eqtls': 0,
                        'brain_eqtls': 0,
                        'sn_eqtls': 0,
                        'brain_percentage': 0
                    })
            else:
                print(f"  API error: {response.status_code}")
                
        except Exception as e:
            print(f"  Error: {e}")
        
        time.sleep(0.5)  # Rate limiting
    
    # Summary analysis
    print("\n" + "="*60)
    print("SUBSTANTIA NIGRA ANALYSIS SUMMARY")
    print("="*60)
    
    results_df = pd.DataFrame(sn_results)
    print(results_df.to_string(index=False))
    
    total_genes_tested = len(results_df)
    genes_with_sn = len(results_df[results_df['sn_eqtls'] > 0])
    
    print(f"\nGenes with substantia nigra eQTLs: {genes_with_sn}/{total_genes_tested}")
    print(f"Substantia nigra eQTL rate: {genes_with_sn/total_genes_tested*100:.1f}%")
    
    if genes_with_sn > 0:
        avg_sn_eqtls = results_df[results_df['sn_eqtls'] > 0]['sn_eqtls'].mean()
        print(f"Average SN eQTLs per gene (when present): {avg_sn_eqtls:.1f}")

def check_tissue_sample_sizes():
    """Check sample sizes for all brain tissues"""
    print("\n" + "="*60)
    print("BRAIN TISSUE SAMPLE SIZE ANALYSIS")
    print("="*60)
    
    url = "https://gtexportal.org/api/v2/dataset/tissueSample"
    response = requests.get(url, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        if 'data' in data:
            df = pd.DataFrame(data['data'])
            
            # Filter to brain tissues
            brain_df = df[df['tissueSiteDetailId'].str.contains('Brain', case=False, na=False)]
            
            # Extract sample sizes (assuming rnaSeqSampleSummary contains count)
            print("Brain tissue sample information:")
            for _, row in brain_df.iterrows():
                tissue = row['tissueSiteDetailId']
                sample_info = row.get('rnaSeqSampleSummary', 'Unknown')
                print(f"  {tissue}: {sample_info}")
                
                # Look specifically for substantia nigra
                if 'Substantia_nigra' in tissue:
                    print(f"    *** SUBSTANTIA NIGRA FOUND: {sample_info} ***")

if __name__ == "__main__":
    check_substantia_nigra_across_genes()
    check_tissue_sample_sizes()