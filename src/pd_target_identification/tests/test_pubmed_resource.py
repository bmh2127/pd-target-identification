# Create this file: tests/test_pubmed_resource.py

import pytest
import requests
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# Import the resource to test
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from pd_target_identification.defs.shared.resources import PubMedResource


class TestPubMedResource:
    """Comprehensive tests for PubMedResource"""
    
    @pytest.fixture
    def pubmed_resource(self):
        """Create a PubMedResource instance for testing"""
        return PubMedResource()
    
    @pytest.fixture
    def pubmed_resource_with_key(self):
        """Create a PubMedResource instance with API key for testing"""
        return PubMedResource(api_key="test_key", email="test@example.com")
    
    @pytest.fixture
    def mock_search_response(self):
        """Mock successful PubMed search response"""
        return {
            "esearchresult": {
                "idlist": ["12345678", "87654321", "11111111"],
                "count": "3",
                "retmax": "3"
            }
        }
    
    @pytest.fixture
    def mock_abstract_xml(self):
        """Mock PubMed XML response with abstracts"""
        return """<?xml version="1.0"?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <PMID Version="1">12345678</PMID>
                    <Article>
                        <Journal>
                            <Title>Nature Neuroscience</Title>
                        </Journal>
                        <ArticleTitle>LRRK2 mutations in Parkinson's disease</ArticleTitle>
                        <Abstract>
                            <AbstractText>LRRK2 is a major genetic risk factor for Parkinson's disease. This study investigates therapeutic targets.</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
                <PubmedData>
                    <ArticleIdList>
                        <ArticleId IdType="pubmed">12345678</ArticleId>
                    </ArticleIdList>
                </PubmedData>
            </PubmedArticle>
            <PubmedArticle>
                <MedlineCitation>
                    <PMID Version="1">87654321</PMID>
                    <Article>
                        <Journal>
                            <Title>Journal of Neurochemistry</Title>
                        </Journal>
                        <ArticleTitle>SNCA alpha-synuclein aggregation in PD</ArticleTitle>
                        <Abstract>
                            <AbstractText>Alpha-synuclein protein aggregation is a key pathological feature of Parkinson's disease.</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
                <PubmedData>
                    <ArticleIdList>
                        <ArticleId IdType="pubmed">87654321</ArticleId>
                    </ArticleIdList>
                </PubmedData>
            </PubmedArticle>
        </PubmedArticleSet>"""

    # Test 1: Resource Initialization
    def test_resource_initialization_default(self, pubmed_resource):
        """Test default resource initialization"""
        assert pubmed_resource.base_url == "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        # Email may be set from environment variables
        assert isinstance(pubmed_resource.email, str)  # Just check it's a string
        assert isinstance(pubmed_resource.api_key, str)  # Just check it's a string
        # Rate limit depends on whether API key is set
        expected_delay = 0.1 if pubmed_resource.api_key else 0.34
        assert pubmed_resource.rate_limit_delay == expected_delay
    
    def test_resource_initialization_with_credentials(self, pubmed_resource_with_key):
        """Test resource initialization with API credentials"""
        assert pubmed_resource_with_key.api_key == "test_key"
        assert pubmed_resource_with_key.email == "test@example.com"
        assert pubmed_resource_with_key.rate_limit_delay == 0.1  # With API key

    # Test 2: PubMed Search Functionality
    @patch('requests.get')
    def test_search_pubmed_success(self, mock_get, pubmed_resource, mock_search_response):
        """Test successful PubMed search"""
        # Mock the API response
        mock_response = Mock()
        mock_response.json.return_value = mock_search_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test search
        query = "LRRK2 Parkinson"
        pmids = pubmed_resource.search_pubmed(query, max_results=10)
        
        # Assertions
        assert len(pmids) == 3
        assert pmids == ["12345678", "87654321", "11111111"]
        
        # Verify API call was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "esearch.fcgi" in call_args[0][0]
        assert call_args[1]['params']['term'].startswith("(LRRK2 Parkinson)")
        assert call_args[1]['params']['db'] == 'pubmed'
        assert call_args[1]['params']['retmax'] == 10

    @patch('requests.get')
    def test_search_pubmed_with_date_filtering(self, mock_get, pubmed_resource, mock_search_response):
        """Test PubMed search includes proper date filtering"""
        mock_response = Mock()
        mock_response.json.return_value = mock_search_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pmids = pubmed_resource.search_pubmed("LRRK2", years_back=3)
        
        # Check that date filter is included
        call_args = mock_get.call_args
        query_term = call_args[1]['params']['term']
        current_year = datetime.now().year
        expected_start_year = current_year - 3
        assert f"{expected_start_year}[Date - Publication]" in query_term
        assert f"{current_year}[Date - Publication]" in query_term

    @patch('requests.get')
    def test_search_pubmed_with_api_key(self, mock_get, pubmed_resource_with_key, mock_search_response):
        """Test PubMed search includes API key when available"""
        mock_response = Mock()
        mock_response.json.return_value = mock_search_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pmids = pubmed_resource_with_key.search_pubmed("LRRK2")
        
        # Check API key and email are included
        call_args = mock_get.call_args
        params = call_args[1]['params']
        assert params['api_key'] == "test_key"
        assert params['email'] == "test@example.com"

    @patch('requests.get')
    def test_search_pubmed_network_error(self, mock_get, pubmed_resource):
        """Test PubMed search handles network errors gracefully"""
        mock_get.side_effect = requests.RequestException("Network error")
        
        pmids = pubmed_resource.search_pubmed("LRRK2")
        
        assert pmids == []  # Should return empty list on error

    @patch('requests.get')
    def test_search_pubmed_empty_results(self, mock_get, pubmed_resource):
        """Test PubMed search handles empty results"""
        mock_response = Mock()
        mock_response.json.return_value = {"esearchresult": {"idlist": []}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pmids = pubmed_resource.search_pubmed("NONEXISTENT_GENE")
        
        assert pmids == []

    # Test 3: Abstract Fetching
    @patch('requests.get')
    def test_fetch_abstracts_success(self, mock_get, pubmed_resource, mock_abstract_xml):
        """Test successful abstract fetching"""
        mock_response = Mock()
        mock_response.text = mock_abstract_xml
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pmids = ["12345678", "87654321"]
        papers = pubmed_resource.fetch_abstracts(pmids)
        
        # Assertions
        assert len(papers) == 2
        
        # Check first paper
        paper1 = papers[0]
        assert paper1['pmid'] == "12345678"
        assert "LRRK2 mutations" in paper1['title']
        assert "therapeutic targets" in paper1['abstract']
        assert paper1['journal'] == "Nature Neuroscience"
        
        # Check second paper
        paper2 = papers[1]
        assert paper2['pmid'] == "87654321"
        assert "SNCA alpha-synuclein" in paper2['title']
        assert "pathological feature" in paper2['abstract']

    def test_fetch_abstracts_empty_list(self, pubmed_resource):
        """Test fetching abstracts with empty PMID list"""
        papers = pubmed_resource.fetch_abstracts([])
        assert papers == []

    @patch('requests.get')
    def test_fetch_abstracts_batch_processing(self, mock_get, pubmed_resource, mock_abstract_xml):
        """Test that large PMID lists are batched correctly"""
        mock_response = Mock()
        mock_response.text = mock_abstract_xml
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Create a list larger than batch size
        large_pmid_list = [f"1234567{i}" for i in range(75)]  # 75 PMIDs (batch size is 50)
        
        papers = pubmed_resource.fetch_abstracts(large_pmid_list)
        
        # Should make 2 API calls (2 batches)
        assert mock_get.call_count == 2

    # Test 4: XML Parsing
    def test_parse_pubmed_xml_valid(self, pubmed_resource, mock_abstract_xml):
        """Test XML parsing with valid XML"""
        papers = pubmed_resource._parse_pubmed_xml(mock_abstract_xml)
        
        assert len(papers) == 2
        assert papers[0]['pmid'] == "12345678"
        assert papers[1]['pmid'] == "87654321"

    def test_parse_pubmed_xml_invalid(self, pubmed_resource):
        """Test XML parsing with invalid XML"""
        invalid_xml = "<invalid><unclosed>tag"
        papers = pubmed_resource._parse_pubmed_xml(invalid_xml)
        
        assert papers == []  # Should return empty list for invalid XML

    def test_parse_pubmed_xml_empty(self, pubmed_resource):
        """Test XML parsing with empty content"""
        papers = pubmed_resource._parse_pubmed_xml("")
        assert papers == []

    # Test 5: Gene-Specific Literature Search
    @patch.object(PubMedResource, 'search_pubmed')
    @patch.object(PubMedResource, 'fetch_abstracts')
    def test_search_gene_literature_success(self, mock_fetch, mock_search, pubmed_resource):
        """Test gene-specific literature search"""
        # Mock search results
        mock_search.return_value = ["12345678", "87654321"]
        
        # Mock abstract fetch results
        mock_papers = [
            {
                'pmid': '12345678',
                'title': 'LRRK2 in Parkinson disease',
                'abstract': 'LRRK2 mutations cause familial PD',
                'journal': 'Nature',
                'publication_year': 2023
            }
        ]
        mock_fetch.return_value = mock_papers
        
        # Test gene search
        papers = pubmed_resource.search_gene_literature("LRRK2", max_papers=50)
        
        # Assertions
        assert len(papers) == 1
        assert papers[0]['target_gene'] == "LRRK2"
        assert papers[0]['pmid'] == '12345678'
        assert "LRRK2" in papers[0]['search_query']
        assert "Parkinson" in papers[0]['search_query']
        
        # Verify search was called with correct query
        mock_search.assert_called_once()
        search_query = mock_search.call_args[0][0]
        assert "LRRK2[Title/Abstract]" in search_query
        assert "Parkinson*[Title/Abstract]" in search_query

    @patch.object(PubMedResource, 'search_pubmed')
    def test_search_gene_literature_no_results(self, mock_search, pubmed_resource):
        """Test gene search with no results"""
        mock_search.return_value = []
        
        papers = pubmed_resource.search_gene_literature("FAKE_GENE")
        
        assert papers == []

    # Test 6: Batch Gene Literature Search
    @patch.object(PubMedResource, 'search_gene_literature')
    @patch('time.sleep')  # Mock sleep to speed up tests
    def test_batch_gene_literature_search(self, mock_sleep, mock_gene_search, pubmed_resource):
        """Test batch literature search for multiple genes"""
        # Mock individual gene searches
        def mock_search_side_effect(gene_symbol, max_papers):
            if gene_symbol == "LRRK2":
                return [{'pmid': '123', 'title': 'LRRK2 paper', 'target_gene': 'LRRK2'}]
            elif gene_symbol == "SNCA":
                return [{'pmid': '456', 'title': 'SNCA paper', 'target_gene': 'SNCA'}]
            else:
                return []
        
        mock_gene_search.side_effect = mock_search_side_effect
        
        # Test batch search
        gene_list = ["LRRK2", "SNCA", "FAKE_GENE"]
        results = pubmed_resource.batch_gene_literature_search(gene_list, max_papers_per_gene=20)
        
        # Assertions
        assert len(results) == 3
        assert len(results["LRRK2"]) == 1
        assert len(results["SNCA"]) == 1
        assert len(results["FAKE_GENE"]) == 0
        
        # Verify all genes were searched
        assert mock_gene_search.call_count == 3

    # Test 7: Rate Limiting
    @patch('time.sleep')
    @patch('requests.get')
    def test_rate_limiting_applied(self, mock_get, mock_sleep, pubmed_resource):
        """Test that rate limiting is applied correctly"""
        mock_response = Mock()
        mock_response.json.return_value = {"esearchresult": {"idlist": []}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pubmed_resource.search_pubmed("test")
        
        # Verify sleep was called with correct delay based on API key availability
        expected_delay = 0.1 if pubmed_resource.api_key else 0.34
        mock_sleep.assert_called_with(expected_delay)

    @patch('time.sleep')
    @patch('requests.get')
    def test_rate_limiting_with_api_key(self, mock_get, mock_sleep, pubmed_resource_with_key):
        """Test faster rate limiting with API key"""
        mock_response = Mock()
        mock_response.json.return_value = {"esearchresult": {"idlist": []}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pubmed_resource_with_key.search_pubmed("test")
        
        # Verify faster rate limit is used
        mock_sleep.assert_called_with(0.1)

    # Test 8: Error Handling and Edge Cases
    @patch('requests.get')
    def test_malformed_json_response(self, mock_get, pubmed_resource):
        """Test handling of malformed JSON responses"""
        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        pmids = pubmed_resource.search_pubmed("test")
        
        assert pmids == []

    @patch('requests.get')
    def test_timeout_handling(self, mock_get, pubmed_resource):
        """Test timeout handling"""
        mock_get.side_effect = requests.Timeout("Request timed out")
        
        pmids = pubmed_resource.search_pubmed("test")
        
        assert pmids == []

    @patch('requests.get')
    def test_http_error_handling(self, mock_get, pubmed_resource):
        """Test HTTP error handling"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        pmids = pubmed_resource.search_pubmed("test")
        
        assert pmids == []

    # Test 9: Integration Test
    @patch('requests.get')
    def test_end_to_end_workflow(self, mock_get, pubmed_resource, mock_search_response, mock_abstract_xml):
        """Test complete end-to-end workflow"""
        # Mock search response
        search_response = Mock()
        search_response.json.return_value = mock_search_response
        search_response.raise_for_status.return_value = None
        
        # Mock abstract response
        abstract_response = Mock()
        abstract_response.text = mock_abstract_xml
        abstract_response.raise_for_status.return_value = None
        
        # Configure mock to return different responses for different calls
        mock_get.side_effect = [search_response, abstract_response]
        
        # Run complete workflow
        papers = pubmed_resource.search_gene_literature("LRRK2", max_papers=10)
        
        # Verify complete workflow
        assert len(papers) == 2
        assert papers[0]['target_gene'] == "LRRK2"
        assert papers[0]['pmid'] == "12345678"
        assert "LRRK2" in papers[0]['search_query']
        
        # Verify both API calls were made
        assert mock_get.call_count == 2


# Test 10: Performance and Load Testing
class TestPubMedResourcePerformance:
    """Performance-focused tests for PubMedResource"""
    
    @pytest.fixture
    def pubmed_resource(self):
        return PubMedResource()
    
    @patch('time.sleep')  # Mock sleep to speed up tests
    @patch.object(PubMedResource, 'search_gene_literature')
    def test_batch_search_performance(self, mock_gene_search, mock_sleep, pubmed_resource):
        """Test that batch search scales reasonably with gene count"""
        # Mock fast responses
        mock_gene_search.return_value = [{'pmid': '123', 'title': 'test'}]
        
        # Test with different gene list sizes
        for gene_count in [1, 5, 10, 20]:
            gene_list = [f"GENE{i}" for i in range(gene_count)]
            
            start_time = time.time()
            results = pubmed_resource.batch_gene_literature_search(gene_list)
            end_time = time.time()
            
            # Should complete quickly with mocked responses
            assert (end_time - start_time) < 1.0  # Less than 1 second
            assert len(results) == gene_count


# Test 11: Configuration and Setup Tests
class TestPubMedResourceConfiguration:
    """Test resource configuration options"""
    
    def test_custom_email_configuration(self):
        """Test resource with custom email"""
        resource = PubMedResource(email="researcher@university.edu")
        assert resource.email == "researcher@university.edu"
    
    def test_custom_api_key_configuration(self):
        """Test resource with custom API key"""
        resource = PubMedResource(api_key="custom_key_123")
        assert resource.api_key == "custom_key_123"
        assert resource.rate_limit_delay == 0.1  # Should use faster rate limit
    
    def test_custom_base_url_configuration(self):
        """Test resource with custom base URL"""
        custom_url = "https://custom.eutils.api.com/entrez/eutils"
        resource = PubMedResource(base_url=custom_url)
        assert resource.base_url == custom_url


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])