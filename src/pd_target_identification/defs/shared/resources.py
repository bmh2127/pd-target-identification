# pd_target_identification/defs/shared/resources.py
from dagster import ConfigurableResource
import requests
import pandas as pd
from typing import Optional, Dict, Any, List
import time

class GWASCatalogResource(ConfigurableResource):
    """Enhanced resource for accessing GWAS Catalog data with complete variant information"""
    base_url: str = "https://www.ebi.ac.uk/gwas/rest/api"
    
    def _get_variant_details(self, snp_link: str) -> Dict[str, Any]:
        """Get detailed variant information from SNP link"""
        try:
            response = requests.get(snp_link, timeout=10)
            if response.status_code == 200:
                snp_data = response.json()
                
                # Extract locations (can have multiple)
                locations = snp_data.get('locations', [])
                if locations:
                    loc = locations[0]  # Take first location
                    return {
                        'chromosome': loc.get('chromosomeName', 'Unknown'),
                        'position': loc.get('chromosomePosition'),
                        'rsid': snp_data.get('rsId'),
                        'merge_status': snp_data.get('merged', 0)
                    }
        except Exception as e:
            print(f"Failed to get variant details: {e}")
        
        return {
            'chromosome': 'Unknown',
            'position': None,
            'rsid': None,
            'merge_status': 0
        }
    
    def get_pd_associations(self, p_value_threshold: float = 5e-8, max_variants: int = 100) -> pd.DataFrame:
        """
        Fetch Parkinson's disease associations from GWAS Catalog with complete variant data
        """
        search_url = f"{self.base_url}/efoTraits/MONDO_0005180/associations"
        params = {
            'size': 1000
        }
        
        response = requests.get(search_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        associations = []
        variant_count = 0
        
        if '_embedded' in data and 'associations' in data['_embedded']:
            raw_associations = data['_embedded']['associations']
            
            for assoc in raw_associations:
                if variant_count >= max_variants:
                    break
                
                # Extract p-value
                try:
                    mantissa = assoc.get('pvalueMantissa', 1)
                    exponent = assoc.get('pvalueExponent', 0)
                    p_value = float(mantissa) * (10 ** float(exponent))
                except (ValueError, TypeError):
                    p_value = 1.0
                
                if p_value > p_value_threshold:
                    continue
                
                # Extract odds ratio
                try:
                    odds_ratio = float(assoc.get('orPerCopyNum', 1.0))
                    if odds_ratio == 0:
                        odds_ratio = 1.0
                except (ValueError, TypeError):
                    odds_ratio = 1.0
                
                # Extract beta coefficient
                try:
                    beta = float(assoc.get('betaNum', 0.0))
                except (ValueError, TypeError):
                    beta = 0.0
                
                # Process loci
                loci = assoc.get('loci', [])
                for locus in loci:
                    strongest_alleles = locus.get('strongestRiskAlleles', [])
                    if not strongest_alleles:
                        continue
                        
                    allele = strongest_alleles[0]
                    variant_id = allele.get('riskAlleleName', '')
                    if not variant_id:
                        continue
                    
                    # Get gene information
                    reported_genes = locus.get('authorReportedGenes', [])
                    if not reported_genes:
                        continue
                        
                    gene = reported_genes[0]
                    nearest_gene = gene.get('geneName', '')
                    if not nearest_gene:
                        continue
                    
                    # Get variant details from SNP link
                    variant_details = {'chromosome': 'Unknown', 'position': None, 'rsid': None}
                    snp_links = allele.get('_links', {})
                    if 'snp' in snp_links:
                        snp_url = snp_links['snp']['href']
                        variant_details = self._get_variant_details(snp_url)
                        time.sleep(0.1)  # Rate limiting
                    
                    # Extract study information
                    study_links = assoc.get('_links', {}).get('study', {})
                    study_info = {
                        'study_accession': 'Unknown',
                        'sample_size': None,
                        'ancestry': 'EUR'  # Default assumption
                    }
                    
                    associations.append({
                        'variant_id': variant_id,
                        'rsid': variant_details.get('rsid', variant_id),
                        'chromosome': variant_details['chromosome'],
                        'position': variant_details['position'],
                        'p_value': p_value,
                        'odds_ratio': odds_ratio,
                        'beta': beta,
                        'nearest_gene': nearest_gene,
                        'population': study_info['ancestry'],
                        'study_accession': study_info['study_accession'],
                        'sample_size': study_info['sample_size'],
                        'effect_allele': allele.get('riskAlleleName', '').split('-')[-1] if '-' in allele.get('riskAlleleName', '') else 'Unknown'
                    })
                    
                    variant_count += 1
                    if variant_count >= max_variants:
                        break
        
        df = pd.DataFrame(associations)
        
        # Data quality filtering
        if len(df) > 0:
            df = df[df['variant_id'].notna() & (df['variant_id'] != '')]
            df = df[df['nearest_gene'].notna() & (df['nearest_gene'] != '')]
            df = df[df['p_value'] <= p_value_threshold]
            
            # Sort by p-value (most significant first)
            df = df.sort_values('p_value').reset_index(drop=True)
        
        return df

class GTExResource(ConfigurableResource):
    """Resource for accessing GTEx Portal API for eQTL data with version caching"""
    base_url: str = "https://gtexportal.org/api/v2"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # In-memory cache for successful gene version mappings
        self._version_cache: Dict[str, str] = {}
        # Cache for failed versions to avoid retesting
        self._failed_cache: set = set()
        
    def get_independent_eqtls(self, gencode_id: str, dataset_id: str = 'gtex_v8') -> List[Dict[str, Any]]:
        """
        Get independent eQTL data for a single gene
        
        Args:
            gencode_id: Full versioned Ensembl gene ID (e.g., 'ENSG00000132693.12')
            dataset_id: GTEx dataset ID (default: 'gtex_v8')
            
        Returns:
            List of eQTL records
        """
        url = f"{self.base_url}/association/independentEqtl"
        
        params = {
            'gencodeId': gencode_id,
            'datasetId': dataset_id,
            'itemsPerPage': 1000  # Get all eQTLs for this gene
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get('data', [])
            
        except Exception as e:
            print(f"Failed to get eQTLs for {gencode_id}: {e}")
            return []
    
    def get_cached_gene_version(self, base_ensembl_id: str) -> Optional[str]:
        """
        Get cached working version for a gene, or find and cache it
        
        Args:
            base_ensembl_id: Base Ensembl ID without version
            
        Returns:
            Working versioned Ensembl ID or None if none found
        """
        # Check cache first
        if base_ensembl_id in self._version_cache:
            return self._version_cache[base_ensembl_id]
        
        # Check if we've already determined this gene has no working version
        if base_ensembl_id in self._failed_cache:
            return None
        
        # Test versions to find working one
        version_results = self.test_gene_with_version(base_ensembl_id)
        
        if version_results['working_versions']:
            # Cache the best version (most eQTLs)
            best_version = max(version_results['working_versions'], 
                             key=lambda x: x['eqtl_count'])['version']
            self._version_cache[base_ensembl_id] = best_version
            return best_version
        else:
            # Cache that this gene has no working version
            self._failed_cache.add(base_ensembl_id)
            return None
    
    def test_gene_with_version(self, base_ensembl_id: str) -> Dict[str, Any]:
        """
        Test different version numbers for an Ensembl ID to find working format
        
        Args:
            base_ensembl_id: Base Ensembl ID without version (e.g., 'ENSG00000188906')
            
        Returns:
            Dict with test results
        """
        # Optimized version order: most common versions first
        versions_to_try = ['.10', '.11', '.12', '.13', '.9', '.14', '.15', '.8', '.7', '']
        
        results = {
            'base_id': base_ensembl_id,
            'working_versions': [],
            'failed_versions': [],
            'best_version': None,
            'max_eqtls': 0
        }
        
        for version in versions_to_try:
            test_id = f"{base_ensembl_id}{version}" if version else base_ensembl_id
            
            try:
                # Quick test with small page size first
                url = f"{self.base_url}/association/independentEqtl"
                params = {
                    'gencodeId': test_id,
                    'datasetId': 'gtex_v8',
                    'itemsPerPage': 10  # Just test if any data exists
                }
                
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    eqtls = data.get('data', [])
                    
                    if eqtls:
                        # Get full count for comparison
                        full_eqtls = self.get_independent_eqtls(test_id)
                        
                        results['working_versions'].append({
                            'version': test_id,
                            'eqtl_count': len(full_eqtls),
                            'tissues': len(set(e.get('tissueSiteDetailId') for e in full_eqtls)),
                            'gene_symbol': full_eqtls[0].get('geneSymbol') if full_eqtls else None
                        })
                        
                        if len(full_eqtls) > results['max_eqtls']:
                            results['max_eqtls'] = len(full_eqtls)
                            results['best_version'] = test_id
                        
                        # If we found a version with good data, we can stop early
                        if len(full_eqtls) > 50:  # Arbitrary threshold
                            break
                    else:
                        results['failed_versions'].append(test_id)
                else:
                    results['failed_versions'].append(f"{test_id} (HTTP {response.status_code})")
                    
            except Exception as e:
                results['failed_versions'].append(f"{test_id} (error: {e})")
            
            # Rate limiting between version tests
            time.sleep(0.1)
        
        return results
    
    def get_eqtls_batch(self, ensembl_ids: List[str]) -> pd.DataFrame:
        """
        Get eQTL data for multiple genes using cached versions
        
        Args:
            ensembl_ids: List of base Ensembl gene IDs (will find versions automatically)
            
        Returns:
            DataFrame with eQTL data
        """
        eqtl_data = []
        successful_genes = 0
        
        for base_ensembl_id in ensembl_ids:
            # Get cached or find working version
            working_version = self.get_cached_gene_version(base_ensembl_id)
            
            if working_version:
                gene_eqtls = self.get_independent_eqtls(working_version)
                successful_genes += 1
                
                for eqtl_record in gene_eqtls:
                    eqtl_data.append({
                        'ensembl_gene_id': working_version,
                        'base_ensembl_id': base_ensembl_id,  # Keep original for joining
                        'gene_symbol': eqtl_record.get('geneSymbol'),
                        'tissue_id': eqtl_record.get('tissueSiteDetailId'),
                        'variant_id': eqtl_record.get('variantId'),
                        'snp_id': eqtl_record.get('snpId'),
                        'p_value': float(eqtl_record.get('pValue', 1.0)),
                        'effect_size': float(eqtl_record.get('nes', 0.0)),  # Normalized Effect Size
                        'maf': float(eqtl_record.get('maf', 0.0)),  # Minor Allele Frequency
                        'chromosome': eqtl_record.get('chromosome'),
                        'position': eqtl_record.get('pos')
                    })
                
                # Rate limiting between genes
                time.sleep(0.2)
            else:
                print(f"No working GTEx version found for {base_ensembl_id}")
        
        print(f"Successfully retrieved eQTLs for {successful_genes}/{len(ensembl_ids)} genes")
        return pd.DataFrame(eqtl_data)
    
    def get_brain_eqtls(self, ensembl_ids: List[str]) -> pd.DataFrame:
        """
        Get eQTL data filtered to brain tissues only
        """
        all_eqtls = self.get_eqtls_batch(ensembl_ids)
        
        if len(all_eqtls) == 0:
            return all_eqtls
        
        # Filter to brain tissues
        brain_eqtls = all_eqtls[all_eqtls['tissue_id'].str.contains('Brain', case=False, na=False)]
        
        return brain_eqtls
    
    def get_substantia_nigra_eqtls(self, ensembl_ids: List[str]) -> pd.DataFrame:
        """
        Get eQTL data for substantia nigra specifically
        """
        all_eqtls = self.get_eqtls_batch(ensembl_ids)
        
        if len(all_eqtls) == 0:
            return all_eqtls
        
        # Filter to substantia nigra
        sn_eqtls = all_eqtls[all_eqtls['tissue_id'].str.contains('Substantia_nigra', case=False, na=False)]
        
        return sn_eqtls
    
    def clear_cache(self):
        """Clear version cache (useful for testing or if versions change)"""
        self._version_cache.clear()
        self._failed_cache.clear()
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics for monitoring"""
        return {
            'cached_successful_versions': len(self._version_cache),
            'cached_failed_genes': len(self._failed_cache),
            'total_cached_genes': len(self._version_cache) + len(self._failed_cache)
        }
    
# class GTExResource(ConfigurableResource):
#     """Resource for accessing GTEx Portal API for gene expression data"""
#     base_url: str = "https://gtexportal.org/api/v2"
    
#     def get_service_info(self) -> Dict[str, Any]:
#         """Test API connection and get service info"""
#         url = f"{self.base_url}/dataset/serviceInfo"
        
#         try:
#             response = requests.get(url, timeout=30)
#             response.raise_for_status()
#             return response.json()
#         except Exception as e:
#             print(f"Failed to get service info: {e}")
#             return {}
    
#     def get_tissues_dynamically(self) -> List[Dict[str, Any]]:
#         """
#         Get available tissues dynamically by querying eQTL genes endpoint
#         This extracts tissue IDs from actual GTEx data
#         """
#         url = f"{self.base_url}/association/eqtlGenes"
        
#         # Use a minimal query to get tissue data
#         params = {
#             'datasetId': 'gtex_v8',
#             'itemsPerPage': 1000  # Get enough to see all tissues
#         }
        
#         try:
#             response = requests.get(url, params=params, timeout=30)
#             response.raise_for_status()
            
#             data = response.json()
#             eqtl_genes = data.get('eqtlGenes', [])
            
#             # Extract unique tissues from the response
#             tissues_seen = set()
#             tissue_info = []
            
#             for gene_record in eqtl_genes:
#                 tissue_id = gene_record.get('tissueSiteDetailId')
#                 ontology_id = gene_record.get('ontologyId')
                
#                 if tissue_id and tissue_id not in tissues_seen:
#                     tissues_seen.add(tissue_id)
#                     tissue_info.append({
#                         'tissue_id': tissue_id,
#                         'tissue_name': tissue_id.replace('_', ' '),
#                         'ontology_id': ontology_id,
#                         'is_brain_tissue': 'Brain' in tissue_id
#                     })
            
#             # Sort tissues alphabetically
#             tissue_info.sort(key=lambda x: x['tissue_id'])
            
#             return tissue_info
            
#         except Exception as e:
#             print(f"Failed to get tissues dynamically: {e}")
#             return []
    
#     def get_brain_tissues_dynamically(self) -> List[str]:
#         """Get brain tissue IDs dynamically"""
#         all_tissues = self.get_tissues_dynamically()
#         brain_tissues = [t['tissue_id'] for t in all_tissues if t['is_brain_tissue']]
#         return brain_tissues
    
#     def get_median_gene_expression(self, gencode_id: str, dataset_id: str = 'gtex_v8') -> List[Dict[str, Any]]:
#         """
#         Get median gene expression data for a single gene across all tissues
        
#         Args:
#             gencode_id: Ensembl gene ID (e.g., 'ENSG00000188906.10')
#             dataset_id: GTEx dataset ID (default: 'gtex_v8')
            
#         Returns:
#             List of expression records
#         """
#         url = f"{self.base_url}/expression/medianGeneExpression"
        
#         params = {
#             'gencodeId': gencode_id,
#             'datasetId': dataset_id
#         }
        
#         try:
#             response = requests.get(url, params=params, timeout=30)
#             response.raise_for_status()
            
#             data = response.json()
#             return data.get('medianGeneExpression', [])
            
#         except Exception as e:
#             print(f"Failed to get expression for {gencode_id}: {e}")
#             return []
    
#     def get_gene_expression_batch(self, ensembl_ids: List[str], tissue_ids: List[str] = None) -> pd.DataFrame:
#         """
#         Get median gene expression data from GTEx for multiple genes
        
#         Args:
#             ensembl_ids: List of Ensembl gene IDs (ENSG format)
#             tissue_ids: List of tissue IDs to filter (if None, gets brain tissues dynamically)
            
#         Returns:
#             DataFrame with expression data
#         """
#         if tissue_ids is None:
#             tissue_ids = self.get_brain_tissues_dynamically()
        
#         expression_data = []
        
#         for ensembl_id in ensembl_ids:
#             # Get expression data for this gene
#             gene_expression = self.get_median_gene_expression(ensembl_id)
            
#             for expr_record in gene_expression:
#                 tissue_id = expr_record.get('tissueSiteDetailId')
                
#                 # Filter to requested tissues
#                 if tissue_id in tissue_ids:
#                     expression_data.append({
#                         'ensembl_gene_id': ensembl_id,
#                         'tissue_id': tissue_id,
#                         'median_tpm': float(expr_record.get('median', 0.0)),
#                         'unit': expr_record.get('unit', 'TPM'),
#                         'sample_count': expr_record.get('numSamples', 0)
#                     })
            
#             # Rate limiting - GTEx recommends not overwhelming their servers
#             time.sleep(0.2)
        
#         return pd.DataFrame(expression_data)
    
#     def test_single_gene(self, test_gene: str = 'ENSG00000188906') -> Dict[str, Any]:
#         """
#         Test API with a single well-known gene (LRRK2)
#         """
#         try:
#             expression_data = self.get_median_gene_expression(test_gene)
#             return {
#                 'gene': test_gene,
#                 'num_tissues': len(expression_data),
#                 'sample_data': expression_data[:3] if expression_data else [],
#                 'success': len(expression_data) > 0
#             }
#         except Exception as e:
#             return {
#                 'gene': test_gene,
#                 'error': str(e),
#                 'success': False
#             }
    
#     def test_tissue_discovery(self) -> Dict[str, Any]:
#         """
#         Test the dynamic tissue discovery
#         """
#         try:
#             tissues = self.get_tissues_dynamically()
#             brain_tissues = [t for t in tissues if t['is_brain_tissue']]
            
#             return {
#                 'total_tissues': len(tissues),
#                 'brain_tissues': len(brain_tissues),
#                 'sample_tissues': [t['tissue_id'] for t in tissues[:5]],
#                 'brain_tissue_names': [t['tissue_id'] for t in brain_tissues],
#                 'success': len(tissues) > 0
#             }
#         except Exception as e:
#             return {
#                 'error': str(e),
#                 'success': False
#             }