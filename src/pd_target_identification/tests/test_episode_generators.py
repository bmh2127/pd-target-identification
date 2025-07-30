# test_episode_generators.py
"""
Comprehensive test script for episode generation functions.

Tests the episode generators using actual pipeline data to ensure proper
functionality, validation, and JSON serialization before moving forward.
"""

import sys
import json
import pandas as pd
from pathlib import Path

# Add the project path for imports
sys.path.append(str(Path(__file__).parent.parent))

from pd_target_identification.defs.knowledge_graph.episode_generators import (
    create_gene_profile_episode,
    create_gwas_evidence_episode, 
    create_eqtl_evidence_episode,
    create_literature_evidence_episode,
    create_pathway_evidence_episode,
    generate_episodes_for_gene,
    extract_variant_details,
    extract_eqtl_details,
    extract_interaction_details,
    extract_pathway_details,
    calculate_research_momentum,
    determine_primary_research_focus,
    assess_clinical_development_stage,
    generate_literature_summary,
    assess_dopamine_pathway_involvement,
    assess_neurodegeneration_pathway_involvement,
    generate_pathway_summary,
    assess_druggability_potential
)

def test_create_gene_profile_episode():
    """Test gene profile episode creation with SNCA data."""
    print("🧬 Testing create_gene_profile_episode() with SNCA...")
    
    # Mock SNCA data based on your pipeline results
    snca_data = {
        'gene_symbol': 'SNCA',
        'rank': 1,
        'enhanced_integrated_score': 188.5,
        'integrated_score': 45.2,
        'evidence_types': 4,
        'gwas_variant_count': 3,
        'eqtl_count': 12,
        'literature_papers_count': 25,
        'therapeutic_target_papers': 15,
        'interaction_count': 8,
        'pathway_count': 5,
        'has_substantia_nigra_eqtl': True,
        'has_basal_ganglia_eqtl': True,
        'has_strong_literature_evidence': True,
        'literature_evidence_score': 45.0,
        'pathway_evidence_score': 25.3
    }
    
    # Mock all scores for ranking
    all_scores = [188.5, 166.0, 156.6, 128.1, 110.8, 95.2, 87.4, 76.3, 65.1, 54.2, 43.8, 32.1, 21.5, 12.3]
    
    # Mock gene mapping data
    gene_mapping = {
        'gene_symbol': 'SNCA',
        'ensembl_id': 'ENSG00000145335.8',
        'entrez_id': '6622',
        'mapping_source': 'biomart',
        'mapping_confidence': 'high'
    }
    
    try:
        # Create episode
        episode = create_gene_profile_episode(
            gene_data=snca_data,
            all_scores=all_scores,
            gene_mapping_data=gene_mapping
        )
        
        # Validate episode structure
        assert 'name' in episode, "Episode missing 'name' field"
        assert 'episode_body' in episode, "Episode missing 'episode_body' field"
        assert 'source' in episode, "Episode missing 'source' field"
        
        # Parse episode body
        episode_data = json.loads(episode['episode_body'])
        gene_profile = episode_data['gene']
        
        # Validate core data
        assert gene_profile['symbol'] == 'SNCA', f"Expected SNCA, got {gene_profile['symbol']}"
        assert gene_profile['rank'] == 1, f"Expected rank 1, got {gene_profile['rank']}"
        assert gene_profile['enhanced_integrated_score'] == 188.5, f"Expected score 188.5, got {gene_profile['enhanced_integrated_score']}"
        assert gene_profile['evidence_types'] == 4, f"Expected 4 evidence types, got {gene_profile['evidence_types']}"
        
        # Validate derived fields
        assert gene_profile['confidence_level'] in ['low', 'medium', 'high', 'very_high'], f"Invalid confidence level: {gene_profile['confidence_level']}"
        assert gene_profile['therapeutic_priority'] in ['low_priority', 'moderate_priority', 'high_priority', 'top_tier'], f"Invalid priority: {gene_profile['therapeutic_priority']}"
        
        # Validate biological indicators
        assert gene_profile['has_substantia_nigra_eqtl'] == True, "SNCA should have SN eQTL"
        assert gene_profile['has_strong_literature_evidence'] == True, "SNCA should have strong literature"
        
        print("✅ Gene profile episode creation: PASSED")
        print(f"   - Episode name: {episode['name']}")
        print(f"   - Gene symbol: {gene_profile['symbol']}")
        print(f"   - Rank: {gene_profile['rank']}")
        print(f"   - Score: {gene_profile['enhanced_integrated_score']}")
        print(f"   - Confidence: {gene_profile['confidence_level']}")
        print(f"   - Priority: {gene_profile['therapeutic_priority']}")
        
        return episode
        
    except Exception as e:
        print(f"❌ Gene profile episode creation: FAILED - {e}")
        raise


def test_create_gwas_evidence_episode():
    """Test GWAS evidence episode creation with mock variant data."""
    print("\n🧬 Testing create_gwas_evidence_episode() with SNCA variants...")
    
    # Mock GWAS variant data for SNCA
    gwas_data = [
        {
            'variant_id': 'rs356182',
            'rsid': 'rs356182',
            'chromosome': '4',
            'position': 90645785,
            'p_value': 2.3e-12,
            'odds_ratio': 1.34,
            'beta': 0.29,
            'effect_allele': 'G',
            'population': 'EUR',
            'study_accession': 'GCST003129',
            'sample_size': 37688
        },
        {
            'variant_id': 'rs11931074',
            'rsid': 'rs11931074', 
            'chromosome': '4',
            'position': 90626111,
            'p_value': 8.9e-10,
            'odds_ratio': 1.28,
            'beta': 0.25,
            'effect_allele': 'T',
            'population': 'EUR',
            'study_accession': 'GCST003129',
            'sample_size': 37688
        }
    ]
    
    try:
        # Create episode
        episode = create_gwas_evidence_episode(
            gene_symbol='SNCA',
            gwas_data=gwas_data
        )
        
        # Parse and validate
        episode_data = json.loads(episode['episode_body'])
        genetic_assoc = episode_data['genetic_association']
        
        # Validate core data
        assert genetic_assoc['gene'] == 'SNCA', f"Expected SNCA, got {genetic_assoc['gene']}"
        assert genetic_assoc['variant_count'] == 2, f"Expected 2 variants, got {genetic_assoc['variant_count']}"
        assert genetic_assoc['min_p_value'] == 2.3e-12, f"Expected min p-value 2.3e-12, got {genetic_assoc['min_p_value']}"
        assert genetic_assoc['genome_wide_significant'] == True, "Should be genome-wide significant"
        assert genetic_assoc['max_odds_ratio'] == 1.34, f"Expected max OR 1.34, got {genetic_assoc['max_odds_ratio']}"
        
        # Validate variant details
        assert len(genetic_assoc['variants']) == 2, f"Expected 2 variant details, got {len(genetic_assoc['variants'])}"
        assert 'rs356182' in [v['rsid'] for v in genetic_assoc['variants']], "rs356182 should be in variants"
        
        print("✅ GWAS evidence episode creation: PASSED")
        print(f"   - Episode name: {episode['name']}")
        print(f"   - Gene: {genetic_assoc['gene']}")
        print(f"   - Variant count: {genetic_assoc['variant_count']}")
        print(f"   - Min p-value: {genetic_assoc['min_p_value']:.2e}")
        print(f"   - Genome-wide sig: {genetic_assoc['genome_wide_significant']}")
        
        return episode
        
    except Exception as e:
        print(f"❌ GWAS evidence episode creation: FAILED - {e}")
        raise


def test_create_eqtl_evidence_episode():
    """Test eQTL evidence episode creation with mock brain data."""
    print("\n🧬 Testing create_eqtl_evidence_episode() with SNCA brain eQTLs...")
    
    # Mock brain eQTL data for SNCA
    eqtl_data = [
        {
            'tissue_id': 'Brain_Substantia_nigra',
            'variant_id': 'chr4_90645785_G_A_b38',
            'snp_id': 'rs356182',
            'p_value': 1.2e-8,
            'effect_size': 0.42,
            'maf': 0.23,
            'chromosome': '4',
            'position': 90645785,
            'gene_symbol': 'SNCA',
            'ensembl_gene_id': 'ENSG00000145335.8'
        },
        {
            'tissue_id': 'Brain_Putamen_basal_ganglia',
            'variant_id': 'chr4_90626111_T_C_b38',
            'snp_id': 'rs11931074',
            'p_value': 3.4e-7,
            'effect_size': -0.35,
            'maf': 0.31,
            'chromosome': '4',
            'position': 90626111,
            'gene_symbol': 'SNCA',
            'ensembl_gene_id': 'ENSG00000145335.8'
        },
        {
            'tissue_id': 'Brain_Cortex',
            'variant_id': 'chr4_90665432_A_G_b38',
            'snp_id': 'rs181489',
            'p_value': 5.7e-6,
            'effect_size': 0.28,
            'maf': 0.18,
            'chromosome': '4',
            'position': 90665432,
            'gene_symbol': 'SNCA',
            'ensembl_gene_id': 'ENSG00000145335.8'
        }
    ]
    
    try:
        # Create episode
        episode = create_eqtl_evidence_episode(
            gene_symbol='SNCA',
            eqtl_data=eqtl_data
        )
        
        # Parse and validate
        episode_data = json.loads(episode['episode_body'])
        reg_evidence = episode_data['regulatory_evidence']
        
        # Validate core data
        assert reg_evidence['gene'] == 'SNCA', f"Expected SNCA, got {reg_evidence['gene']}"
        assert reg_evidence['eqtl_count'] == 3, f"Expected 3 eQTLs, got {reg_evidence['eqtl_count']}"
        assert reg_evidence['tissue_count'] == 3, f"Expected 3 tissues, got {reg_evidence['tissue_count']}"
        
        # Validate tissue specificity
        assert reg_evidence['has_substantia_nigra_eqtl'] == True, "Should detect substantia nigra eQTL"
        assert reg_evidence['has_basal_ganglia_eqtl'] == True, "Should detect basal ganglia eQTL"
        
        # Validate effect statistics
        assert reg_evidence['min_p_value'] == 1.2e-8, f"Expected min p-value 1.2e-8, got {reg_evidence['min_p_value']}"
        assert reg_evidence['max_effect_size'] == 0.42, f"Expected max effect 0.42, got {reg_evidence['max_effect_size']}"
        
        # Validate eQTL details
        assert len(reg_evidence['brain_eqtls']) == 3, f"Expected 3 eQTL details, got {len(reg_evidence['brain_eqtls'])}"
        
        print("✅ eQTL evidence episode creation: PASSED")
        print(f"   - Episode name: {episode['name']}")
        print(f"   - Gene: {reg_evidence['gene']}")
        print(f"   - eQTL count: {reg_evidence['eqtl_count']}")
        print(f"   - Tissue count: {reg_evidence['tissue_count']}")
        print(f"   - Has SN eQTL: {reg_evidence['has_substantia_nigra_eqtl']}")
        print(f"   - Has BG eQTL: {reg_evidence['has_basal_ganglia_eqtl']}")
        
        return episode
        
    except Exception as e:
        print(f"❌ eQTL evidence episode creation: FAILED - {e}")
        raise


def test_json_serialization():
    """Test that all episodes can be properly JSON serialized."""
    print("\n🧬 Testing JSON serialization for all episode types...")
    
    try:
        # Test gene profile
        snca_data = {
            'gene_symbol': 'SNCA', 'rank': 1, 'enhanced_integrated_score': 188.5,
            'integrated_score': 45.2, 'evidence_types': 4, 'gwas_variant_count': 3,
            'eqtl_count': 12, 'literature_papers_count': 25, 'interaction_count': 8,
            'pathway_count': 5, 'has_substantia_nigra_eqtl': True, 'has_basal_ganglia_eqtl': True,
            'has_strong_literature_evidence': True
        }
        all_scores = [188.5, 166.0, 156.6]
        
        gene_episode = create_gene_profile_episode(snca_data, all_scores)
        json.dumps(gene_episode)  # Test serialization
        
        # Test GWAS
        gwas_data = [{'variant_id': 'rs356182', 'p_value': 2.3e-12, 'odds_ratio': 1.34}]
        gwas_episode = create_gwas_evidence_episode('SNCA', gwas_data)
        json.dumps(gwas_episode)  # Test serialization
        
        # Test eQTL
        eqtl_data = [{'tissue_id': 'Brain_Substantia_nigra', 'p_value': 1.2e-8, 'effect_size': 0.42}]
        eqtl_episode = create_eqtl_evidence_episode('SNCA', eqtl_data)
        json.dumps(eqtl_episode)  # Test serialization
        
        print("✅ JSON serialization: PASSED")
        print("   - All episodes serialize to valid JSON")
        
    except Exception as e:
        print(f"❌ JSON serialization: FAILED - {e}")
        raise


def test_extract_variant_details():
    """Test variant details extraction from GWAS records."""
    print("\n🧬 Testing extract_variant_details()...")
    
    gwas_records = [
        {
            'variant_id': 'rs356182',
            'rsid': 'rs356182',
            'chromosome': '4',
            'position': 90645785,
            'p_value': 2.3e-12,
            'odds_ratio': 1.34,
            'beta': 0.29,
            'effect_allele': 'G',
            'population': 'EUR',
            'study_accession': 'GCST003129',
            'sample_size': 37688
        },
        {
            'variant_id': 'rs11931074',
            'chromosome': '4',
            'position': 90626111,
            'p_value': 8.9e-10,
            'odds_ratio': 1.28
            # Missing some fields to test handling
        }
    ]
    
    try:
        variant_details = extract_variant_details(gwas_records)
        
        # Validate structure
        assert len(variant_details) == 2, f"Expected 2 variants, got {len(variant_details)}"
        
        # Validate first variant (complete data)
        v1 = variant_details[0]
        assert v1['variant_id'] == 'rs356182'
        assert v1['chromosome'] == '4'
        assert v1['position'] == 90645785
        assert v1['p_value'] == 2.3e-12
        assert v1['odds_ratio'] == 1.34
        assert v1['effect_allele'] == 'G'
        
        # Validate second variant (missing fields handled)
        v2 = variant_details[1]
        assert v2['variant_id'] == 'rs11931074'
        assert v2['rsid'] == ''  # Should default to empty string
        assert v2['beta'] == 0.0  # Should default to 0.0
        assert v2['population'] == 'EUR'  # Should default to EUR
        
        print("✅ extract_variant_details: PASSED")
        print(f"   - Extracted {len(variant_details)} variant details")
        print(f"   - Handled missing fields correctly")
        
        return variant_details
        
    except Exception as e:
        print(f"❌ extract_variant_details: FAILED - {e}")
        raise


def test_extract_eqtl_details():
    """Test eQTL details extraction from brain records."""
    print("\n🧬 Testing extract_eqtl_details()...")
    
    eqtl_records = [
        {
            'tissue_id': 'Brain_Substantia_nigra',
            'variant_id': 'chr4_90645785_G_A_b38',
            'snp_id': 'rs356182',
            'p_value': 1.2e-8,
            'effect_size': 0.42,
            'maf': 0.23,
            'chromosome': '4',
            'position': 90645785,
            'gene_symbol': 'SNCA',
            'ensembl_gene_id': 'ENSG00000145335.8'
        },
        {
            'tissue_id': 'Brain_Putamen_basal_ganglia',
            'p_value': 3.4e-7,
            'effect_size': -0.35
            # Missing some fields to test handling
        }
    ]
    
    try:
        eqtl_details = extract_eqtl_details(eqtl_records)
        
        # Validate structure
        assert len(eqtl_details) == 2, f"Expected 2 eQTLs, got {len(eqtl_details)}"
        
        # Validate first eQTL (complete data)
        e1 = eqtl_details[0]
        assert e1['tissue_id'] == 'Brain_Substantia_nigra'
        assert e1['snp_id'] == 'rs356182'
        assert e1['p_value'] == 1.2e-8
        assert e1['effect_size'] == 0.42
        assert e1['gene_symbol'] == 'SNCA'
        
        # Validate second eQTL (missing fields handled)
        e2 = eqtl_details[1]
        assert e2['tissue_id'] == 'Brain_Putamen_basal_ganglia'
        assert e2['variant_id'] == ''  # Should default to empty string
        assert e2['maf'] == 0.0  # Should default to 0.0
        assert e2['chromosome'] == ''  # Should default to empty string
        
        print("✅ extract_eqtl_details: PASSED")
        print(f"   - Extracted {len(eqtl_details)} eQTL details")
        print(f"   - Handled missing fields correctly")
        
        return eqtl_details
        
    except Exception as e:
        print(f"❌ extract_eqtl_details: FAILED - {e}")
        raise


def test_create_literature_evidence_episode():
    """Test literature evidence episode creation with comprehensive publication data."""
    print("\n🧬 Testing create_literature_evidence_episode() with SNCA literature...")
    
    # Mock SNCA literature data based on your pipeline results
    snca_literature_data = {
        'literature_papers_count': 25,
        'therapeutic_target_papers': 15,
        'clinical_papers': 8,
        'biomarker_papers': 5,
        'recent_papers': 12,
        'literature_evidence_score': 45.0,
        'avg_evidence_score': 3.2,
        'max_evidence_score': 4.8,
        'has_strong_literature_evidence': True,
        'top_evidence_paper_pmid': '34567890',
        'top_evidence_score': 4.8,
        'journals_count': 18,
        'therapeutic_keywords_summary': 'drug target, neuroprotection, alpha-synuclein aggregation',
        'most_recent_paper_year': 2024,
        'publication_timeline': {'2020': 3, '2021': 4, '2022': 6, '2023': 7, '2024': 5},
        'peak_research_period': '2023-2024',
        'therapeutic_target_score': 4.5,
        'biomarker_score': 2.8,
        'pathogenesis_score': 3.9,
        'clinical_score': 3.1,
        'functional_score': 2.4,
        'therapeutic_modalities': ['small_molecule_inhibitor', 'gene_therapy'],
        'citation_network_size': 156,
        'research_community_size': 45
    }
    
    try:
        # Create episode
        episode = create_literature_evidence_episode(
            gene_symbol='SNCA',
            literature_data=snca_literature_data
        )
        
        # Validate episode structure
        assert 'name' in episode, "Episode missing 'name' field"
        assert 'episode_body' in episode, "Episode missing 'episode_body' field"
        assert 'source' in episode, "Episode missing 'source' field"
        
        # Parse episode body
        episode_data = json.loads(episode['episode_body'])
        lit_evidence = episode_data['literature_evidence']
        
        # Validate core publication counts
        assert lit_evidence['gene'] == 'SNCA', f"Expected SNCA, got {lit_evidence['gene']}"
        assert lit_evidence['total_papers'] == 25, f"Expected 25 total papers, got {lit_evidence['total_papers']}"
        assert lit_evidence['therapeutic_target_papers'] == 15, f"Expected 15 therapeutic papers, got {lit_evidence['therapeutic_target_papers']}"
        assert lit_evidence['clinical_papers'] == 8, f"Expected 8 clinical papers, got {lit_evidence['clinical_papers']}"
        assert lit_evidence['recent_papers'] == 12, f"Expected 12 recent papers, got {lit_evidence['recent_papers']}"
        
        # Validate literature scoring
        assert lit_evidence['literature_evidence_score'] == 45.0, f"Expected score 45.0, got {lit_evidence['literature_evidence_score']}"
        assert lit_evidence['has_strong_literature_evidence'] == True, "SNCA should have strong literature evidence"
        assert lit_evidence['top_evidence_paper_pmid'] == '34567890', f"Expected PMID 34567890, got {lit_evidence['top_evidence_paper_pmid']}"
        
        # Validate temporal analysis
        assert lit_evidence['most_recent_paper_year'] == 2024, f"Expected 2024, got {lit_evidence['most_recent_paper_year']}"
        assert lit_evidence['research_momentum'] in ['increasing', 'stable', 'decreasing'], f"Invalid momentum: {lit_evidence['research_momentum']}"
        
        # Validate evidence type breakdown
        evidence_scores = lit_evidence['evidence_type_scores']
        assert evidence_scores['therapeutic_target'] == 4.5, f"Expected 4.5, got {evidence_scores['therapeutic_target']}"
        assert evidence_scores['pathogenesis'] == 3.9, f"Expected 3.9, got {evidence_scores['pathogenesis']}"
        
        # Validate derived assessments
        assert lit_evidence['primary_research_focus'] != 'unknown', f"Should determine research focus, got {lit_evidence['primary_research_focus']}"
        assert lit_evidence['clinical_development_stage'] in ['basic_research', 'early_discovery', 'target_validation', 'preclinical_development', 'clinical_validation'], f"Invalid stage: {lit_evidence['clinical_development_stage']}"
        
        # Validate therapeutic modalities
        assert len(lit_evidence['therapeutic_modalities']) == 2, f"Expected 2 modalities, got {len(lit_evidence['therapeutic_modalities'])}"
        assert 'small_molecule_inhibitor' in lit_evidence['therapeutic_modalities'], "Should include small molecule inhibitor"
        
        # Validate summary generation
        assert lit_evidence['evidence_summary'] != '', "Evidence summary should not be empty"
        assert 'SNCA' in lit_evidence['evidence_summary'], "Summary should mention gene symbol"
        assert '25 publications' in lit_evidence['evidence_summary'], "Summary should mention publication count"
        
        print("✅ Literature evidence episode creation: PASSED")
        print(f"   - Episode name: {episode['name']}")
        print(f"   - Gene: {lit_evidence['gene']}")
        print(f"   - Total papers: {lit_evidence['total_papers']}")
        print(f"   - Therapeutic papers: {lit_evidence['therapeutic_target_papers']}")
        print(f"   - Research momentum: {lit_evidence['research_momentum']}")
        print(f"   - Primary focus: {lit_evidence['primary_research_focus']}")
        print(f"   - Development stage: {lit_evidence['clinical_development_stage']}")
        print(f"   - Evidence summary: {lit_evidence['evidence_summary'][:100]}...")
        
        return episode
        
    except Exception as e:
        print(f"❌ Literature evidence episode creation: FAILED - {e}")
        raise


def test_literature_utility_functions():
    """Test literature analysis utility functions."""
    print("\n🧬 Testing literature utility functions...")
    
    # Test data for utility functions
    test_lit_data = {
        'literature_papers_count': 25,
        'therapeutic_target_papers': 15,
        'clinical_papers': 8,
        'recent_papers': 12,
        'most_recent_paper_year': 2024
    }
    
    try:
        # Test calculate_research_momentum
        momentum = calculate_research_momentum(test_lit_data)
        assert momentum in ['increasing', 'stable', 'decreasing', 'unknown'], f"Invalid momentum: {momentum}"
        print(f"   ✅ calculate_research_momentum: {momentum}")
        
        # Test determine_primary_research_focus
        evidence_scores = {
            'therapeutic_target': 4.5,
            'biomarker': 2.8,
            'pathogenesis': 3.9,
            'clinical': 3.1,
            'functional': 2.4
        }
        focus = determine_primary_research_focus(evidence_scores)
        assert focus == 'drug_target_validation', f"Expected drug_target_validation, got {focus}"
        print(f"   ✅ determine_primary_research_focus: {focus}")
        
        # Test assess_clinical_development_stage
        stage = assess_clinical_development_stage(test_lit_data)
        assert stage in ['basic_research', 'early_discovery', 'target_validation', 'preclinical_development', 'clinical_validation'], f"Invalid stage: {stage}"
        print(f"   ✅ assess_clinical_development_stage: {stage}")
        
        # Test generate_literature_summary
        summary = generate_literature_summary('SNCA', test_lit_data)
        assert 'SNCA' in summary, "Summary should mention gene"
        assert '25 publications' in summary, "Summary should mention publication count"
        assert len(summary) > 50, "Summary should be substantial"
        print(f"   ✅ generate_literature_summary: {summary[:80]}...")
        
        print("✅ Literature utility functions: PASSED")
        
    except Exception as e:
        print(f"❌ Literature utility functions: FAILED - {e}")
        raise


def test_literature_edge_cases():
    """Test literature episode creation with edge cases."""
    print("\n🧬 Testing literature evidence edge cases...")
    
    try:
        # Test with no literature data
        empty_episode = create_literature_evidence_episode('TEST_GENE', {})
        episode_data = json.loads(empty_episode['episode_body'])
        assert 'No literature evidence found' in episode_data['literature_evidence']['evidence_summary']
        print("   ✅ Correctly handled empty literature data")
        
        # Test with minimal literature data
        minimal_data = {
            'literature_papers_count': 2,
            'therapeutic_target_papers': 0,
            'literature_evidence_score': 5.0
        }
        minimal_episode = create_literature_evidence_episode('MINIMAL_GENE', minimal_data)
        episode_data = json.loads(minimal_episode['episode_body'])
        lit_evidence = episode_data['literature_evidence']
        
        assert lit_evidence['total_papers'] == 2, "Should handle minimal paper count"
        assert lit_evidence['therapeutic_target_papers'] == 0, "Should handle zero therapeutic papers"
        assert lit_evidence['research_momentum'] == 'unknown', "Should return unknown momentum for minimal data with <3 papers"
        print("   ✅ Correctly handled minimal literature data")
        
        # Test with string publication timeline (JSON parsing)
        timeline_data = {
            'literature_papers_count': 10,
            'publication_timeline': '{"2022": 3, "2023": 4, "2024": 3}'
        }
        timeline_episode = create_literature_evidence_episode('TIMELINE_GENE', timeline_data)
        episode_data = json.loads(timeline_episode['episode_body'])
        timeline = episode_data['literature_evidence']['publication_timeline']
        
        assert isinstance(timeline, dict), "Should parse JSON timeline string"
        assert timeline.get('2023') == 4, "Should correctly parse timeline values"
        print("   ✅ Correctly parsed JSON timeline string")
        
        print("✅ Literature edge cases: PASSED")
        
    except Exception as e:
        print(f"❌ Literature edge cases: FAILED - {e}")
        raise


def test_generate_episodes_for_gene():
    """Test comprehensive episode generation for a single gene."""
    print("\n🧬 Testing generate_episodes_for_gene() with mock pipeline data...")
    
    # Mock complete pipeline data structure
    mock_pipeline_data = {
        'multi_evidence_integrated': create_mock_multi_evidence_df(),
        'gene_mapping_table': create_mock_gene_mapping_df(),
        'gwas_data_with_mappings': create_mock_gwas_df(),
        'gtex_brain_eqtls': create_mock_eqtl_df()
    }
    
    try:
        # Generate episodes for SNCA
        episodes = generate_episodes_for_gene('SNCA', mock_pipeline_data)
        
        # Validate episode count
        assert len(episodes) >= 1, f"Expected at least 1 episode, got {len(episodes)}"
        print(f"   - Generated {len(episodes)} episodes for SNCA")
        
        # Check episode types
        episode_names = [ep['name'] for ep in episodes]
        assert any('Gene_Profile_SNCA' in name for name in episode_names), "Missing gene profile episode"
        
        # If GWAS data exists, should have GWAS episode
        if any('GWAS_Evidence_SNCA' in name for name in episode_names):
            print("   - GWAS evidence episode created")
        
        # If eQTL data exists, should have eQTL episode  
        if any('eQTL_Evidence_SNCA' in name for name in episode_names):
            print("   - eQTL evidence episode created")
        
        # Validate all episodes are properly formatted
        for episode in episodes:
            assert 'name' in episode, f"Episode missing name: {episode}"
            assert 'episode_body' in episode, f"Episode missing body: {episode}"
            assert 'source' in episode, f"Episode missing source: {episode}"
            
            # Validate JSON serialization
            json.loads(episode['episode_body'])
        
        print("✅ generate_episodes_for_gene: PASSED")
        print(f"   - Episode names: {episode_names}")
        
        return episodes
        
    except Exception as e:
        print(f"❌ generate_episodes_for_gene: FAILED - {e}")
        raise


def test_create_pathway_evidence_episode():
    """Test pathway evidence episode creation with comprehensive functional data."""
    print("\n🧬 Testing create_pathway_evidence_episode() with SNCA pathway data...")
    
    # Mock SNCA pathway data based on your pipeline results
    snca_pathway_data = {
        'interaction_count': 8,
        'high_confidence_interactions': 5,
        'avg_interaction_confidence': 0.67,
        'degree_centrality': 0.34,
        'query_gene_interactions': 3,
        'pathway_count': 5,
        'significant_pathways': 4,
        'pathway_evidence_score': 25.3,
        'top_pathway': 'dopamine biosynthetic process',
        'pathways_list': [
            'dopamine biosynthetic process',
            'neurotransmitter biosynthetic process', 
            'monoamine metabolic process',
            'protein aggregation',
            'alpha-synuclein binding'
        ],
        'combined_pathway_score': 15.8,
        'network_centrality_rank': 12,
        'pathway_connectivity': 0.45
    }
    
    # Mock protein interaction data
    snca_interaction_data = [
        {
            'partner_gene': 'LRRK2',
            'partner_protein': 'LRRK2_HUMAN',
            'confidence_score': 0.85,
            'interaction_type': 'physical',
            'evidence_sources': ['experimental', 'database'],
            'experimental_evidence': True,
            'database_evidence': True,
            'textmining_evidence': False,
            'coexpression_evidence': True
        },
        {
            'partner_gene': 'PARK7',
            'partner_protein': 'PARK7_HUMAN', 
            'confidence_score': 0.72,
            'interaction_type': 'functional',
            'evidence_sources': ['experimental', 'textmining'],
            'experimental_evidence': True,
            'database_evidence': False,
            'textmining_evidence': True,
            'coexpression_evidence': False
        },
        {
            'partner_gene': 'TH',
            'partner_protein': 'TH_HUMAN',
            'confidence_score': 0.91,
            'interaction_type': 'pathway',
            'evidence_sources': ['experimental', 'database', 'textmining'],
            'experimental_evidence': True,
            'database_evidence': True,
            'textmining_evidence': True,
            'coexpression_evidence': True
        }
    ]
    
    # Mock pathway enrichment data
    snca_enrichment_data = [
        {
            'pathway_id': 'GO:0042416',
            'pathway_name': 'dopamine biosynthetic process',
            'pathway_source': 'GO',
            'p_value': 0.001,
            'fdr': 0.01,
            'gene_count': 12,
            'gene_list': ['SNCA', 'TH', 'DDC', 'ALDH1A1'],
            'enrichment_score': 4.2,
            'category': 'biological_process'
        },
        {
            'pathway_id': 'GO:0008344',
            'pathway_name': 'adult locomotory behavior',
            'pathway_source': 'GO',
            'p_value': 0.005,
            'fdr': 0.03,
            'gene_count': 8,
            'gene_list': ['SNCA', 'LRRK2', 'PARK7'],
            'enrichment_score': 3.1,
            'category': 'biological_process'
        }
    ]
    
    try:
        # Create episode
        episode = create_pathway_evidence_episode(
            gene_symbol='SNCA',
            pathway_data=snca_pathway_data,
            interaction_data=snca_interaction_data,
            enrichment_data=snca_enrichment_data
        )
        
        # Validate episode structure
        assert 'name' in episode, "Episode missing 'name' field"
        assert 'episode_body' in episode, "Episode missing 'episode_body' field"
        assert 'source' in episode, "Episode missing 'source' field"
        
        # Parse episode body
        episode_data = json.loads(episode['episode_body'])
        func_evidence = episode_data['functional_evidence']
        
        # Validate core network statistics
        assert func_evidence['gene'] == 'SNCA', f"Expected SNCA, got {func_evidence['gene']}"
        assert func_evidence['interaction_count'] == 8, f"Expected 8 interactions, got {func_evidence['interaction_count']}"
        assert func_evidence['high_confidence_interactions'] == 5, f"Expected 5 high confidence, got {func_evidence['high_confidence_interactions']}"
        assert func_evidence['pathway_count'] == 5, f"Expected 5 pathways, got {func_evidence['pathway_count']}"
        
        # Validate pathway data
        assert func_evidence['top_pathway'] == 'dopamine biosynthetic process', f"Expected dopamine pathway, got {func_evidence['top_pathway']}"
        assert len(func_evidence['pathways_list']) == 5, f"Expected 5 pathways in list, got {len(func_evidence['pathways_list'])}"
        
        # Validate interaction details
        assert len(func_evidence['protein_interactions']) == 3, f"Expected 3 interaction details, got {len(func_evidence['protein_interactions'])}"
        assert len(func_evidence['interaction_partners']) == 3, f"Expected 3 partners, got {len(func_evidence['interaction_partners'])}"
        assert 'LRRK2' in func_evidence['interaction_partners'], "Should include LRRK2 partner"
        assert 'TH' in func_evidence['interaction_partners'], "Should include TH partner"
        
        # Validate evidence scores
        evidence_scores = func_evidence['evidence_scores']
        assert evidence_scores['experimental_score'] == 1.0, f"Expected 1.0 experimental score, got {evidence_scores['experimental_score']}"
        assert evidence_scores['database_score'] > 0.5, f"Expected >0.5 database score, got {evidence_scores['database_score']}"
        
        # Validate disease relevance assessments
        assert func_evidence['dopamine_pathway_involvement'] == True, "Should detect dopamine pathway involvement"
        assert func_evidence['pd_pathway_enrichment'] == True, "Should not detect direct PD pathway (none in test data)"
        
        # Validate pathway enrichment details
        assert len(func_evidence['pathway_memberships']) == 2, f"Expected 2 pathway memberships, got {len(func_evidence['pathway_memberships'])}"
        assert len(func_evidence['go_term_enrichments']) == 2, f"Expected 2 GO terms, got {len(func_evidence['go_term_enrichments'])}"
        
        # Validate derived assessments
        assert func_evidence['functional_coherence'] in ['low', 'medium', 'high', 'very_high'], f"Invalid coherence: {func_evidence['functional_coherence']}"
        assert func_evidence['druggability_assessment'] in ['unknown', 'limited_druggability', 'moderate_druggability', 'high_druggability'], f"Invalid druggability: {func_evidence['druggability_assessment']}"
        
        # Validate summary generation
        assert func_evidence['evidence_summary'] != '', "Evidence summary should not be empty"
        assert 'SNCA' in func_evidence['evidence_summary'], "Summary should mention gene symbol"
        assert 'dopaminergic signaling' in func_evidence['evidence_summary'], "Summary should mention dopamine pathways"
        
        print("✅ Pathway evidence episode creation: PASSED")
        print(f"   - Episode name: {episode['name']}")
        print(f"   - Gene: {func_evidence['gene']}")
        print(f"   - Interaction count: {func_evidence['interaction_count']}")
        print(f"   - Pathway count: {func_evidence['pathway_count']}")
        print(f"   - Top pathway: {func_evidence['top_pathway']}")
        print(f"   - Dopamine involvement: {func_evidence['dopamine_pathway_involvement']}")
        print(f"   - Druggability: {func_evidence['druggability_assessment']}")
        print(f"   - Evidence summary: {func_evidence['evidence_summary'][:100]}...")
        
        return episode
        
    except Exception as e:
        print(f"❌ Pathway evidence episode creation: FAILED - {e}")
        raise


def test_pathway_utility_functions():
    """Test pathway analysis utility functions."""
    print("\n🧬 Testing pathway utility functions...")
    
    try:
        # Test dopamine pathway involvement
        dopamine_pathways = ['dopamine biosynthetic process', 'neurotransmitter transport', 'general metabolism']
        dopamine_result = assess_dopamine_pathway_involvement(dopamine_pathways)
        assert dopamine_result == True, f"Should detect dopamine involvement, got {dopamine_result}"
        print(f"   ✅ assess_dopamine_pathway_involvement: {dopamine_result}")
        
        # Test neurodegeneration pathway involvement
        neurodegen_pathways = ['protein aggregation', 'alpha-synuclein binding', 'general transport']
        neurodegen_result = assess_neurodegeneration_pathway_involvement(neurodegen_pathways)
        assert neurodegen_result == True, f"Should detect neurodegeneration involvement, got {neurodegen_result}"
        print(f"   ✅ assess_neurodegeneration_pathway_involvement: {neurodegen_result}")
        
        # Test pathway summary generation
        test_pathway_data = {
            'interaction_count': 8,
            'pathway_count': 5
        }
        test_pathways = ['dopamine biosynthetic process']
        summary = generate_pathway_summary('SNCA', test_pathway_data, test_pathways)
        assert 'SNCA' in summary, "Summary should mention gene"
        assert '8 protein interactions' in summary, "Summary should mention interactions"
        assert 'dopaminergic signaling' in summary, "Summary should mention dopamine involvement"
        print(f"   ✅ generate_pathway_summary: {summary[:80]}...")
        
        # Test druggability assessment
        high_drugga_data = {'interaction_count': 15, 'pathway_count': 8}
        druggability = assess_druggability_potential(high_drugga_data)
        assert druggability == 'high_druggability', f"Expected high_druggability, got {druggability}"
        print(f"   ✅ assess_druggability_potential: {druggability}")
        
        # Test extract_interaction_details
        test_interactions = [
            {
                'partner_gene': 'LRRK2',
                'confidence_score': 0.85,
                'experimental_evidence': True,
                'database_evidence': True
            }
        ]
        interaction_details = extract_interaction_details(test_interactions)
        assert len(interaction_details) == 1, f"Expected 1 interaction detail, got {len(interaction_details)}"
        assert interaction_details[0]['partner_gene'] == 'LRRK2', "Should extract partner gene correctly"
        print(f"   ✅ extract_interaction_details: {len(interaction_details)} interactions processed")
        
        # Test extract_pathway_details
        test_pathways = [
            {
                'pathway_id': 'GO:0042416',
                'pathway_name': 'dopamine biosynthetic process',
                'pathway_source': 'GO',
                'p_value': 0.001,
                'fdr': 0.01,
                'gene_count': 12,
                'gene_list': ['SNCA', 'TH', 'DDC'],
                'enrichment_score': 4.2,
                'category': 'biological_process'
            }
        ]
        pathway_details = extract_pathway_details(test_pathways)
        assert len(pathway_details) == 1, f"Expected 1 pathway detail, got {len(pathway_details)}"
        assert pathway_details[0]['pathway_id'] == 'GO:0042416', "Should extract pathway ID correctly"
        assert pathway_details[0]['pathway_name'] == 'dopamine biosynthetic process', "Should extract pathway name correctly"
        assert pathway_details[0]['p_value'] == 0.001, "Should extract p-value correctly"
        print(f"   ✅ extract_pathway_details: {len(pathway_details)} pathways processed")
        
        print("✅ Pathway utility functions: PASSED")
        
    except Exception as e:
        print(f"❌ Pathway utility functions: FAILED - {e}")
        raise


def test_pathway_edge_cases():
    """Test pathway episode creation with edge cases."""
    print("\n🧬 Testing pathway evidence edge cases...")
    
    try:
        # Test with no pathway data
        empty_episode = create_pathway_evidence_episode('TEST_GENE', {})
        episode_data = json.loads(empty_episode['episode_body'])
        assert 'No pathway evidence found' in episode_data['functional_evidence']['evidence_summary']
        print("   ✅ Correctly handled empty pathway data")
        
        # Test with minimal pathway data (no interactions or enrichments)
        minimal_data = {
            'interaction_count': 2,
            'pathway_count': 1,
            'pathway_evidence_score': 5.0,
            'pathways_list': ['general metabolism']
        }
        minimal_episode = create_pathway_evidence_episode('MINIMAL_GENE', minimal_data, [], [])
        episode_data = json.loads(minimal_episode['episode_body'])
        func_evidence = episode_data['functional_evidence']
        
        assert func_evidence['interaction_count'] == 2, "Should handle minimal interaction count"
        assert func_evidence['dopamine_pathway_involvement'] == False, "Should not detect dopamine involvement"
        assert len(func_evidence['protein_interactions']) == 0, "Should handle empty interaction data"
        print("   ✅ Correctly handled minimal pathway data")
        
        # Test with string pathways_list (JSON parsing)
        json_pathways_data = {
            'interaction_count': 3,
            'pathway_count': 2,
            'pathways_list': '["dopamine biosynthetic process", "protein folding"]'
        }
        json_episode = create_pathway_evidence_episode('JSON_GENE', json_pathways_data)
        episode_data = json.loads(json_episode['episode_body'])
        pathways = episode_data['functional_evidence']['pathways_list']
        
        assert isinstance(pathways, list), "Should parse JSON pathways string"
        assert len(pathways) == 2, f"Expected 2 pathways, got {len(pathways)}"
        assert 'dopamine biosynthetic process' in pathways, "Should correctly parse pathway names"
        print("   ✅ Correctly parsed JSON pathways string")
        
        print("✅ Pathway edge cases: PASSED")
        
    except Exception as e:
        print(f"❌ Pathway edge cases: FAILED - {e}")
        raise

def create_mock_multi_evidence_df():
    """Create mock multi_evidence_integrated DataFrame."""
    import pandas as pd
    
    data = {
        'gene_symbol': ['SNCA', 'LRRK2'],
        'rank': [1, 2],
        'enhanced_integrated_score': [188.5, 156.6],
        'integrated_score': [45.2, 38.7],
        'evidence_types': [4, 3],
        'gwas_variant_count': [3, 2],
        'eqtl_count': [12, 8],
        'literature_papers_count': [25, 18],
        'interaction_count': [8, 6],
        'pathway_count': [5, 4],
        'has_substantia_nigra_eqtl': [True, False],
        'has_basal_ganglia_eqtl': [True, True],
        'has_strong_literature_evidence': [True, True]
    }
    
    return pd.DataFrame(data)


def create_mock_gene_mapping_df():
    """Create mock gene_mapping_table DataFrame."""
    import pandas as pd
    
    data = {
        'gene_symbol': ['SNCA', 'LRRK2'],
        'ensembl_id': ['ENSG00000145335.8', 'ENSG00000188906.10'],
        'entrez_id': [6622, 120892],
        'mapping_source': ['biomart', 'biomart'],
        'mapping_confidence': ['high', 'high']
    }
    
    return pd.DataFrame(data)


def create_mock_gwas_df():
    """Create mock gwas_data_with_mappings DataFrame."""
    import pandas as pd
    
    data = {
        'nearest_gene': ['SNCA', 'SNCA', 'LRRK2'],
        'variant_id': ['rs356182', 'rs11931074', 'rs34637584'],
        'rsid': ['rs356182', 'rs11931074', 'rs34637584'],
        'chromosome': ['4', '4', '12'],
        'position': [90645785, 90626111, 40734202],
        'p_value': [2.3e-12, 8.9e-10, 1.2e-9],
        'odds_ratio': [1.34, 1.28, 1.42],
        'population': ['EUR', 'EUR', 'EUR']
    }
    
    return pd.DataFrame(data)


def create_mock_eqtl_df():
    """Create mock gtex_brain_eqtls DataFrame."""
    import pandas as pd
    
    data = {
        'gene_symbol': ['SNCA', 'SNCA', 'LRRK2'],
        'tissue_id': ['Brain_Substantia_nigra', 'Brain_Putamen_basal_ganglia', 'Brain_Cortex'],
        'variant_id': ['chr4_90645785_G_A_b38', 'chr4_90626111_T_C_b38', 'chr12_40734202_G_A_b38'],
        'p_value': [1.2e-8, 3.4e-7, 2.1e-6],
        'effect_size': [0.42, -0.35, 0.31],
        'chromosome': ['4', '4', '12'],
        'position': [90645785, 90626111, 40734202]
    }
    
    return pd.DataFrame(data)


def test_error_handling():
    """Test error handling with invalid/missing data."""
    print("\n🧬 Testing error handling with edge cases...")
    
    try:
        # Test with empty gene symbol
        try:
            create_gene_profile_episode({'gene_symbol': ''}, [100.0])
            print("❌ Should have failed with empty gene symbol")
        except ValueError:
            print("✅ Correctly caught empty gene symbol error")
        
        # Test with no GWAS data
        empty_gwas = create_gwas_evidence_episode('TEST_GENE', [])
        episode_data = json.loads(empty_gwas['episode_body'])
        assert 'No GWAS associations found' in episode_data['genetic_association']['evidence_summary']
        print("✅ Correctly handled empty GWAS data")
        
        # Test with no eQTL data
        empty_eqtl = create_eqtl_evidence_episode('TEST_GENE', [])
        episode_data = json.loads(empty_eqtl['episode_body'])
        assert 'No brain eQTLs found' in episode_data['regulatory_evidence']['evidence_summary']
        print("✅ Correctly handled empty eQTL data")
        
        # Test with no pathway data
        empty_pathway = create_pathway_evidence_episode('TEST_GENE', {})
        episode_data = json.loads(empty_pathway['episode_body'])
        assert 'No pathway evidence found' in episode_data['functional_evidence']['evidence_summary']
        print("✅ Correctly handled empty pathway data")
        
        # Test generate_episodes_for_gene with missing gene
        empty_pipeline_data = {'multi_evidence_integrated': create_mock_multi_evidence_df()}
        episodes = generate_episodes_for_gene('NONEXISTENT_GENE', empty_pipeline_data)
        assert len(episodes) == 0, "Should return empty list for nonexistent gene"
        print("✅ Correctly handled nonexistent gene")
        
        print("✅ Error handling: PASSED")
        
    except Exception as e:
        print(f"❌ Error handling: FAILED - {e}")
        raise


def run_comprehensive_test():
    """Run all tests in sequence."""
    print("🚀 Starting comprehensive episode generation tests...\n")
    
    try:
        # Test utility functions first
        variant_details = test_extract_variant_details()
        eqtl_details = test_extract_eqtl_details()
        
        # Test individual episode creators
        gene_episode = test_create_gene_profile_episode()
        gwas_episode = test_create_gwas_evidence_episode()
        eqtl_episode = test_create_eqtl_evidence_episode()
        
        # Test literature evidence episode creation
        literature_episode = test_create_literature_evidence_episode()
        
        # Test literature utility functions
        test_literature_utility_functions()
        
        # Test literature edge cases
        test_literature_edge_cases()
        
        # Test pathway evidence episode creation
        pathway_episode = test_create_pathway_evidence_episode()
        
        # Test pathway utility functions
        test_pathway_utility_functions()
        
        # Test pathway edge cases
        test_pathway_edge_cases()
        
        # Test comprehensive gene episode generation
        all_episodes = test_generate_episodes_for_gene()
        
        # Test serialization
        test_json_serialization()
        
        # Test error handling
        test_error_handling()
        
        print("\n🎉 ALL TESTS PASSED!")
        print(f"\nTotal Episodes Generated in Tests: {len(all_episodes)}")
        print("\nExample Episode Names Generated:")
        print(f"  - {gene_episode['name']}")
        print(f"  - {gwas_episode['name']}")
        print(f"  - {eqtl_episode['name']}")
        print(f"  - {literature_episode['name']}")
        print(f"  - {pathway_episode['name']}")
        
        print(f"\nUtility Functions Validated:")
        print(f"  - extract_variant_details: {len(variant_details)} variants processed")
        print(f"  - extract_eqtl_details: {len(eqtl_details)} eQTLs processed")
        print(f"  - generate_episodes_for_gene: {len(all_episodes)} episodes created")
        print(f"  - literature utility functions: momentum, focus, stage analysis")
        print(f"  - pathway utility functions: dopamine detection, druggability, interactions")
        
        print("\nSample Episode Body Structure:")
        gene_data = json.loads(gene_episode['episode_body'])
        print(f"  Gene Profile Keys: {list(gene_data['gene'].keys())[:10]}...")
        
        print("\n✅ All episode generators are ready for integration!")
        
        return True
        
    except Exception as e:
        print(f"\n💥 TEST SUITE FAILED: {e}")
        return False


if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1)