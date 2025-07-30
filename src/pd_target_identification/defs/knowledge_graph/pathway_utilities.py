from typing import List, Dict, Any

def extract_interaction_details(interaction_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract and structure protein interaction details."""
    interaction_details = []
    
    for interaction in interaction_data:
        detail = {
            'partner_gene': interaction.get('partner_gene', ''),
            'partner_protein': interaction.get('partner_protein', ''),
            'confidence_score': float(interaction.get('confidence_score', 0.0)),
            'interaction_type': interaction.get('interaction_type', 'unknown'),
            'evidence_sources': interaction.get('evidence_sources', []),
            'experimental_evidence': bool(interaction.get('experimental_evidence', False)),
            'database_evidence': bool(interaction.get('database_evidence', False)),
            'textmining_evidence': bool(interaction.get('textmining_evidence', False)),
            'coexpression_evidence': bool(interaction.get('coexpression_evidence', False))
        }
        interaction_details.append(detail)
    
    return interaction_details


def extract_pathway_details(enrichment_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract and structure pathway enrichment details."""
    pathway_details = []
    
    for pathway in enrichment_data:
        detail = {
            'pathway_id': pathway.get('pathway_id', ''),
            'pathway_name': pathway.get('pathway_name', ''),
            'pathway_source': pathway.get('pathway_source', ''),
            'p_value': float(pathway.get('p_value', 1.0)),
            'fdr': float(pathway.get('fdr', 1.0)),
            'gene_count': int(pathway.get('gene_count', 0)),
            'gene_list': pathway.get('gene_list', []),
            'enrichment_score': float(pathway.get('enrichment_score', 0.0)),
            'category': pathway.get('category', 'unknown')
        }
        pathway_details.append(detail)
    
    return pathway_details


def calculate_interaction_evidence_scores(interaction_data: List[Dict[str, Any]]) -> Dict[str, float]:
    """Calculate evidence scores by source type from interaction data."""
    if not interaction_data:
        return {
            'experimental_score': 0.0,
            'database_score': 0.0,
            'textmining_score': 0.0,
            'coexpression_score': 0.0
        }
    
    experimental_count = sum(1 for i in interaction_data if i.get('experimental_evidence', False))
    database_count = sum(1 for i in interaction_data if i.get('database_evidence', False))
    textmining_count = sum(1 for i in interaction_data if i.get('textmining_evidence', False))
    coexpression_count = sum(1 for i in interaction_data if i.get('coexpression_evidence', False))
    
    total_interactions = len(interaction_data)
    
    return {
        'experimental_score': experimental_count / total_interactions if total_interactions > 0 else 0.0,
        'database_score': database_count / total_interactions if total_interactions > 0 else 0.0,
        'textmining_score': textmining_count / total_interactions if total_interactions > 0 else 0.0,
        'coexpression_score': coexpression_count / total_interactions if total_interactions > 0 else 0.0
    }


def assess_dopamine_pathway_involvement(pathways_list: List[str]) -> bool:
    """Assess if gene participates in dopamine-related pathways."""
    from .schema_constants import DOPAMINE_PATHWAY_KEYWORDS
    
    if not pathways_list:
        return False
    
    for pathway in pathways_list:
        pathway_lower = pathway.lower()
        if any(keyword in pathway_lower for keyword in DOPAMINE_PATHWAY_KEYWORDS):
            return True
    return False


def assess_neurodegeneration_pathway_involvement(pathways_list: List[str]) -> bool:
    """Assess if gene participates in neurodegeneration-related pathways."""
    from .schema_constants import NEURODEGENERATION_KEYWORDS
    
    if not pathways_list:
        return False
    
    for pathway in pathways_list:
        pathway_lower = pathway.lower()
        if any(keyword in pathway_lower for keyword in NEURODEGENERATION_KEYWORDS):
            return True
    return False


def assess_pd_pathway_enrichment(pathways_list: List[str]) -> bool:
    """Assess if gene shows Parkinson's disease pathway enrichment."""
    pd_keywords = ['parkinson', 'parkinsonian', 'alpha-synuclein', 'synuclein', 'lewy body']
    
    if not pathways_list:
        return False
    
    for pathway in pathways_list:
        pathway_lower = pathway.lower()
        if any(keyword in pathway_lower for keyword in pd_keywords):
            return True
    return False


def assess_mitochondrial_involvement(pathways_list: List[str]) -> bool:
    """Assess if gene participates in mitochondrial pathways."""
    mito_keywords = ['mitochondrial', 'mitochondria', 'oxidative phosphorylation', 'electron transport', 'atp synthesis']
    
    if not pathways_list:
        return False
    
    for pathway in pathways_list:
        pathway_lower = pathway.lower()
        if any(keyword in pathway_lower for keyword in mito_keywords):
            return True
    return False


def determine_functional_coherence(pathway_data: Dict[str, Any]) -> str:
    """Determine functional coherence level based on pathway evidence."""
    from .schema_utilities import determine_confidence_level
    
    # Use existing confidence level determination logic
    confidence = determine_confidence_level(pathway_data)
    return confidence.value


def determine_biological_process_focus(enrichment_data: List[Dict[str, Any]]) -> str:
    """Determine primary biological process focus from GO enrichment."""
    if not enrichment_data:
        return "unknown"
    
    # Find GO biological process terms
    bp_terms = [
        pathway for pathway in enrichment_data 
        if pathway.get('category', '').lower() == 'biological_process' or
           pathway.get('pathway_id', '').startswith('GO:') and 'biological_process' in pathway.get('pathway_name', '').lower()
    ]
    
    if not bp_terms:
        return "unknown"
    
    # Get the most significant biological process
    top_bp = min(bp_terms, key=lambda x: x.get('p_value', 1.0))
    return top_bp.get('pathway_name', 'unknown')


def determine_molecular_function_class(enrichment_data: List[Dict[str, Any]]) -> str:
    """Determine molecular function class from GO enrichment."""
    if not enrichment_data:
        return "unknown"
    
    # Find GO molecular function terms
    mf_terms = [
        pathway for pathway in enrichment_data 
        if pathway.get('category', '').lower() == 'molecular_function' or
           pathway.get('pathway_id', '').startswith('GO:') and 'molecular_function' in pathway.get('pathway_name', '').lower()
    ]
    
    if not mf_terms:
        return "unknown"
    
    # Get the most significant molecular function
    top_mf = min(mf_terms, key=lambda x: x.get('p_value', 1.0))
    return top_mf.get('pathway_name', 'unknown')


def determine_cellular_component_location(enrichment_data: List[Dict[str, Any]]) -> str:
    """Determine cellular component location from GO enrichment."""
    if not enrichment_data:
        return "unknown"
    
    # Find GO cellular component terms
    cc_terms = [
        pathway for pathway in enrichment_data 
        if pathway.get('category', '').lower() == 'cellular_component' or
           pathway.get('pathway_id', '').startswith('GO:') and 'cellular_component' in pathway.get('pathway_name', '').lower()
    ]
    
    if not cc_terms:
        return "unknown"
    
    # Get the most significant cellular component
    top_cc = min(cc_terms, key=lambda x: x.get('p_value', 1.0))
    return top_cc.get('pathway_name', 'unknown')


def generate_pathway_summary(gene_symbol: str, pathway_data: Dict[str, Any], pathways_list: List[str]) -> str:
    """Generate human-readable pathway evidence summary."""
    interaction_count = pathway_data.get('interaction_count', 0)
    pathway_count = pathway_data.get('pathway_count', 0)
    
    if interaction_count == 0 and pathway_count == 0:
        return f"No pathway or interaction evidence found for {gene_symbol}"
    
    summary = f"{gene_symbol} shows"
    
    if interaction_count > 0:
        summary += f" {interaction_count} protein interactions"
    
    if pathway_count > 0:
        if interaction_count > 0:
            summary += f" and"
        summary += f" {pathway_count} enriched pathways"
    
    # Add disease relevance
    if assess_dopamine_pathway_involvement(pathways_list):
        summary += ". Participates in dopaminergic signaling pathways"
    elif assess_neurodegeneration_pathway_involvement(pathways_list):
        summary += ". Involved in neurodegeneration-related pathways"
    
    summary += "."
    return summary


def generate_mechanism_hypothesis(gene_symbol: str, pathway_data: Dict[str, Any], pathways_list: List[str]) -> str:
    """Generate mechanism of action hypothesis based on pathway evidence."""
    if not pathways_list:
        return f"Insufficient pathway data to determine mechanism for {gene_symbol}"
    
    mechanisms = []
    
    if assess_dopamine_pathway_involvement(pathways_list):
        mechanisms.append("dopaminergic neurotransmission regulation")
    
    if assess_mitochondrial_involvement(pathways_list):
        mechanisms.append("mitochondrial function and energy metabolism")
    
    if assess_neurodegeneration_pathway_involvement(pathways_list):
        mechanisms.append("protein aggregation and neurodegeneration pathways")
    
    if not mechanisms:
        mechanisms.append("general cellular processes")
    
    return f"{gene_symbol} likely functions through " + " and ".join(mechanisms) + "."


def assess_druggability_potential(pathway_data: Dict[str, Any]) -> str:
    """Assess druggability potential based on pathway characteristics."""
    interaction_count = pathway_data.get('interaction_count', 0)
    pathway_count = pathway_data.get('pathway_count', 0)
    
    if interaction_count >= 10 and pathway_count >= 5:
        return "high_druggability"
    elif interaction_count >= 5 and pathway_count >= 3:
        return "moderate_druggability"
    elif interaction_count > 0 or pathway_count > 0:
        return "limited_druggability"
    else:
        return "unknown"
    