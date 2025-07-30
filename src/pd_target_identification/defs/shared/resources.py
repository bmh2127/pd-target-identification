# pd_target_identification/defs/shared/resources.py
from dagster import ConfigurableResource
import requests
import pandas as pd
from typing import Optional, Dict, Any, List
import time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
    
class STRINGResource(ConfigurableResource):
    """Resource wrapping STRING MCP functionality"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        import sys
        import asyncio
        from pathlib import Path

        # FIXED: Use correct absolute path
        string_mcp_path = Path("/Users/brandonhager/Documents/pd-discovery-platform/mcp_servers")
        
        if not string_mcp_path.exists():
            raise RuntimeError(f"MCP servers path not found: {string_mcp_path}")
            
        sys.path.insert(0, str(string_mcp_path))

        # Import the async functions
        from string_mcp.server import map_proteins, get_network, functional_enrichment
        
        # Store as async functions
        self._map_proteins_async = map_proteins
        self._get_network_async = get_network  
        self._functional_enrichment_async = functional_enrichment
    
    def get_protein_interactions(self, gene_symbols: List[str]) -> pd.DataFrame:
        """Get protein interaction network for genes"""
        
        async def _get_interactions():
            mapped = await self._map_proteins_async(gene_symbols)
            if 'mapped_proteins' in mapped:
                network = await self._get_network_async(mapped['mapped_proteins'])
                return network.get('network_data', {}).get('interactions', [])
            return []
        
        # Run the async function
        import asyncio
        interactions = asyncio.run(_get_interactions())
        return pd.DataFrame(interactions)
    
    def get_functional_enrichment(self, gene_symbols: List[str]) -> pd.DataFrame:
        """Get functional enrichment analysis for genes"""
        
        async def _get_enrichment():
            mapped = await self._map_proteins_async(gene_symbols)
            if 'mapped_proteins' in mapped:
                enrichment = await self._functional_enrichment_async(mapped['mapped_proteins'])
                return enrichment.get('enrichment_results', [])
            return []
        
        # Run the async function
        import asyncio
        enrichment_results = asyncio.run(_get_enrichment())
        return pd.DataFrame(enrichment_results)
    
class PubMedResource(ConfigurableResource):
    """Resource for accessing PubMed/NCBI E-utilities API for literature mining"""
    
    base_url: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    email: str = os.getenv("NCBI_EMAIL", "")  # Required by NCBI
    api_key: str = os.getenv("NCBI_API_KEY", "")  # Optional: for higher rate limits
    tool_name: str = os.getenv("NCBI_TOOL", "pd-target-identification")  # Tool identifier
    
    @property
    def rate_limit_delay(self) -> float:
        """Get rate limit delay based on API key availability"""
        # Correct rate limiting: 10 req/sec with API key, 3 req/sec without
        return 0.1 if self.api_key else 0.34
    
    def _log_configuration(self):
        """Log configuration information"""
        if not self.email:
            print("Warning: NCBI_EMAIL not set in environment variables. This is required by NCBI.")
        if not self.api_key:
            print("Info: NCBI_API_KEY not set. Rate limited to 3 requests/second.")
        else:
            print("Info: Using NCBI API key. Rate limit: 10 requests/second.")
    
    def _get_common_params(self) -> Dict[str, str]:
        """Get common parameters required for all E-utilities requests"""
        params = {
            'tool': self.tool_name,
            'email': self.email
        }
        if self.api_key:
            params['api_key'] = self.api_key
        return params
    
    def search_pubmed(
        self, 
        query: str, 
        max_results: int = 100,
        years_back: int = 5
    ) -> List[str]:
        """
        Search PubMed and return list of PMIDs
        
        Args:
            query: Search query string
            max_results: Maximum number of results to return (up to 10,000)
            years_back: Only include papers from last N years
            
        Returns:
            List of PubMed IDs (PMIDs)
        """
        # Calculate date range for recent papers
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years_back * 365)
        date_filter = f"({start_date.year}[Date - Publication] : {end_date.year}[Date - Publication])"
        
        # Combine query with date filter
        full_query = f"({query}) AND {date_filter}"
        
        search_params = self._get_common_params()
        search_params.update({
            'db': 'pubmed',
            'term': full_query,
            'retmax': min(max_results, 10000),  # E-utilities limit
            'retmode': 'json',
            'sort': 'relevance'
        })
            
        try:
            response = requests.get(
                f"{self.base_url}/esearch.fcgi", 
                params=search_params,
                timeout=30
            )
            response.raise_for_status()
            
            # Handle API rate limit errors
            if response.headers.get('content-type', '').startswith('application/json'):
                data = response.json()
                if 'error' in data and 'rate limit exceeded' in str(data.get('error', '')).lower():
                    print(f"API rate limit exceeded for query '{query}'. Consider reducing request frequency.")
                    return []
                    
                pmid_list = data.get('esearchresult', {}).get('idlist', [])
            else:
                # Fallback to XML parsing if needed
                pmid_list = []
                print(f"Unexpected response format for query '{query}'")
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return pmid_list
            
        except Exception as e:
            print(f"PubMed search failed for query '{query}': {e}")
            return []
    
    def fetch_abstracts(self, pmid_list: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch abstracts for a list of PMIDs
        
        Args:
            pmid_list: List of PubMed IDs
            
        Returns:
            List of paper details with abstracts
        """
        if not pmid_list:
            return []
            
        # Batch fetch abstracts (max 200 at a time)
        papers = []
        batch_size = 50  # Conservative batch size
        
        for i in range(0, len(pmid_list), batch_size):
            batch_pmids = pmid_list[i:i + batch_size]
            
            fetch_params = self._get_common_params()
            fetch_params['db'] = 'pubmed'
            fetch_params['id'] = ','.join(batch_pmids)
            fetch_params['retmode'] = 'xml'
            fetch_params['rettype'] = 'abstract'
            
            try:
                response = requests.get(
                    f"{self.base_url}/efetch.fcgi",
                    params=fetch_params,
                    timeout=60
                )
                response.raise_for_status()
                
                # Parse XML response
                batch_papers = self._parse_pubmed_xml(response.text)
                papers.extend(batch_papers)
                
                # Rate limiting between batches
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                print(f"Failed to fetch abstracts for batch {i//batch_size + 1}: {e}")
                continue
                
        return papers
    
    def _parse_pubmed_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse PubMed XML response and extract paper details"""
        papers = []
        
        try:
            root = ET.fromstring(xml_content)
            
            for article in root.findall('.//PubmedArticle'):
                paper = {}
                
                # Extract PMID
                pmid_elem = article.find('.//PMID')
                paper['pmid'] = pmid_elem.text if pmid_elem is not None else ''
                
                # Extract title
                title_elem = article.find('.//ArticleTitle')
                paper['title'] = title_elem.text if title_elem is not None else ''
                
                # Extract abstract
                abstract_parts = []
                for abstract_text in article.findall('.//AbstractText'):
                    if abstract_text.text:
                        abstract_parts.append(abstract_text.text)
                paper['abstract'] = ' '.join(abstract_parts)
                
                # Extract publication year
                year_elem = article.find('.//PubDate/Year')
                paper['publication_year'] = int(year_elem.text) if year_elem is not None else None
                
                # Extract journal
                journal_elem = article.find('.//Journal/Title')
                paper['journal'] = journal_elem.text if journal_elem is not None else ''
                
                # Extract authors (first author only for simplicity)
                first_author_elem = article.find('.//Author[1]/LastName')
                paper['first_author'] = first_author_elem.text if first_author_elem is not None else ''
                
                if paper['pmid']:  # Only add if we have a valid PMID
                    papers.append(paper)
                    
        except ET.ParseError as e:
            print(f"Error parsing PubMed XML: {e}")
            
        return papers
    
    def search_gene_literature(
        self, 
        gene_symbol: str, 
        max_papers: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search for literature about a specific gene and Parkinson's disease
        
        Args:
            gene_symbol: Gene symbol to search for
            max_papers: Maximum number of papers to retrieve
            
        Returns:
            List of papers mentioning the gene and PD
        """
        # Construct search query for gene + Parkinson's disease
        query = f'({gene_symbol}[Title/Abstract]) AND (Parkinson*[Title/Abstract] OR PD[Title/Abstract])'
        
        print(f"🔍 Searching PubMed for: {gene_symbol} + Parkinson's disease")
        
        # Search for PMIDs
        pmid_list = self.search_pubmed(query, max_results=max_papers)
        
        if not pmid_list:
            print(f"  No papers found for {gene_symbol}")
            return []
            
        print(f"  Found {len(pmid_list)} papers for {gene_symbol}")
        
        # Fetch abstracts
        papers = self.fetch_abstracts(pmid_list)
        
        # Add gene context to each paper
        for paper in papers:
            paper['target_gene'] = gene_symbol
            paper['search_query'] = query
            
        return papers
    
    def batch_gene_literature_search(
        self, 
        gene_symbols: List[str], 
        max_papers_per_gene: int = 30
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Search literature for multiple genes efficiently
        
        Args:
            gene_symbols: List of gene symbols to search
            max_papers_per_gene: Max papers to retrieve per gene
            
        Returns:
            Dictionary mapping gene symbols to their literature
        """
        literature_by_gene = {}
        
        print(f"📚 Starting literature search for {len(gene_symbols)} genes")
        
        for i, gene_symbol in enumerate(gene_symbols, 1):
            print(f"  [{i}/{len(gene_symbols)}] Searching {gene_symbol}...")
            
            try:
                papers = self.search_gene_literature(gene_symbol, max_papers_per_gene)
                literature_by_gene[gene_symbol] = papers
                
                # Progress logging
                if papers:
                    print(f"    ✅ Found {len(papers)} papers")
                else:
                    print(f"    ⚠️ No papers found")
                    
            except Exception as e:
                print(f"    ❌ Search failed: {e}")
                literature_by_gene[gene_symbol] = []
                
            # Rate limiting between genes
            time.sleep(self.rate_limit_delay * 2)  # Extra conservative
            
        total_papers = sum(len(papers) for papers in literature_by_gene.values())
        print(f"📊 Literature search complete: {total_papers} total papers found")
        
        return literature_by_gene