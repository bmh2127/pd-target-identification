# Expected Behaviors for PubMedResource
# Test these behaviors before integrating into the full pipeline

"""
CRITICAL BEHAVIORS FOR PubMedResource:

1. **API Rate Limiting Compliance**
   - MUST respect 3 requests/second without API key (10 with key), 10/second with key
   - MUST include proper delays between requests
   - SHOULD handle rate limiting errors gracefully

2. **Search Query Construction**
   - MUST construct proper E-utilities search queries
   - SHOULD limit searches to recent papers (last 5 years)
   - MUST combine gene symbols with Parkinson's disease terms

3. **Error Handling & Resilience**
   - MUST handle network timeouts gracefully
   - MUST handle malformed API responses
   - SHOULD provide meaningful fallback data when API fails
   - MUST log errors without crashing the pipeline

4. **Data Quality & Validation**
   - MUST return structured data with required fields
   - SHOULD filter out papers without abstracts
   - MUST validate PMIDs are numeric strings
   - SHOULD handle missing/incomplete paper metadata

5. **Performance & Efficiency**
   - SHOULD batch requests when possible
   - MUST avoid duplicate API calls for same gene
   - SHOULD cache results during development
   - MUST limit results to reasonable numbers (20-50 papers/gene)

6. **Integration with Pipeline**
   - MUST accept gene symbols from multi_evidence_integrated
   - MUST return DataFrame-compatible data
   - SHOULD provide progress logging for long searches
   - MUST work with Dagster asset execution context
"""

# Test script to validate PubMedResource behaviors
def test_pubmed_resource_behaviors():
    """
    Test script to validate all expected behaviors before pipeline integration
    """
    from pd_target_identification.defs.shared.resources import PubMedResource
    import time
    
    print("🧪 Testing PubMedResource Behaviors\n")
    
    # Initialize resource
    pubmed = PubMedResource()
    
    # Test 1: Basic search functionality
    print("Test 1: Basic Search Functionality")
    test_genes = ['LRRK2', 'SNCA']  # Known PD genes
    
    for gene in test_genes:
        print(f"  Testing search for {gene}...")
        start_time = time.time()
        
        try:
            papers = pubmed.search_gene_literature(gene, max_papers=5)
            elapsed = time.time() - start_time
            
            print(f"    ✅ Found {len(papers)} papers in {elapsed:.2f}s")
            
            if papers:
                sample_paper = papers[0]
                print(f"    Sample paper keys: {list(sample_paper.keys())}")
                print(f"    Sample PMID: {sample_paper.get('pmid', 'MISSING')}")
                print(f"    Has abstract: {'abstract' in sample_paper and len(sample_paper['abstract']) > 0}")
            
        except Exception as e:
            print(f"    ❌ Search failed: {e}")
    
    # Test 2: Rate limiting behavior
    print("\nTest 2: Rate Limiting Compliance")
    start_time = time.time()
    
    try:
        # Make 3 quick searches to test rate limiting
        for i in range(3):
            pmids = pubmed.search_pubmed(f"LRRK2 AND Parkinson's disease", max_results=5)
            print(f"    Request {i+1}: {len(pmids)} PMIDs found")
        
        elapsed = time.time() - start_time
        print(f"    Total time for 3 requests: {elapsed:.2f}s")
        
        if elapsed >= 1.0:  # Should take at least 1 second due to rate limiting
            print(f"    ✅ Rate limiting appears to be working")
        else:
            print(f"    ⚠️ Rate limiting may be too aggressive or not working")
            
    except Exception as e:
        print(f"    ❌ Rate limiting test failed: {e}")
    
    # Test 3: Error handling
    print("\nTest 3: Error Handling")
    
    try:
        # Test with invalid query
        result = pubmed.search_pubmed("", max_results=5)
        print(f"    Empty query handled: {len(result)} results")
        
        # Test with non-existent gene
        papers = pubmed.search_gene_literature("FAKEGENE123", max_papers=5)
        print(f"    Non-existent gene handled: {len(papers)} papers")
        
        print(f"    ✅ Error handling appears robust")
        
    except Exception as e:
        print(f"    ❌ Error handling test failed: {e}")
    
    # Test 4: Data structure validation
    print("\nTest 4: Data Structure Validation")
    
    try:
        papers = pubmed.search_gene_literature('SNCA', max_papers=3)
        
        if papers:
            required_fields = ['pmid', 'title', 'abstract', 'target_gene']
            sample_paper = papers[0]
            
            missing_fields = [field for field in required_fields if field not in sample_paper]
            
            if not missing_fields:
                print(f"    ✅ All required fields present")
                print(f"    Sample paper structure: {list(sample_paper.keys())}")
            else:
                print(f"    ❌ Missing required fields: {missing_fields}")
                
            # Validate data types
            if isinstance(sample_paper.get('pmid'), str) and sample_paper['pmid'].isdigit():
                print(f"    ✅ PMID format valid")
            else:
                print(f"    ❌ PMID format invalid: {sample_paper.get('pmid')}")
                
        else:
            print(f"    ⚠️ No papers returned for validation")
            
    except Exception as e:
        print(f"    ❌ Data structure validation failed: {e}")
    
    # Test 5: Batch processing
    print("\nTest 5: Batch Processing")
    
    try:
        test_genes = ['LRRK2', 'SNCA', 'GBA1']
        start_time = time.time()
        
        literature_by_gene = pubmed.batch_gene_literature_search(test_genes, max_papers_per_gene=3)
        elapsed = time.time() - start_time
        
        print(f"    Batch search completed in {elapsed:.2f}s")
        print(f"    Genes processed: {len(literature_by_gene)}")
        
        total_papers = sum(len(papers) for papers in literature_by_gene.values())
        print(f"    Total papers retrieved: {total_papers}")
        
        if len(literature_by_gene) == len(test_genes):
            print(f"    ✅ Batch processing successful")
        else:
            print(f"    ❌ Batch processing incomplete")
            
    except Exception as e:
        print(f"    ❌ Batch processing test failed: {e}")
    
    print("\n🏁 PubMedResource behavior testing complete!")

# Expected behavior checklist for integration
PUBMED_INTEGRATION_CHECKLIST = """
Before integrating PubMedResource into the pipeline, verify:

□ Basic search returns structured data with required fields
□ Rate limiting prevents API overuse
□ Error handling doesn't crash on invalid inputs
□ Batch processing works for multiple genes
□ Results are reasonable (not empty, not too many)
□ Data types are correct (PMIDs as strings, years as integers)
□ Search queries are properly formatted for E-utilities
□ Recent papers are prioritized (last 5 years)
□ Abstracts are non-empty when available
□ Gene symbols are preserved in results

KNOWN LIMITATIONS TO DOCUMENT:
- PubMed API has usage limits (3 req/sec without key)
- Some papers may not have abstracts available
- Search results depend on exact gene symbol matching
- API may be temporarily unavailable
- Older papers may have incomplete metadata
"""