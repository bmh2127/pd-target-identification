from pydantic import BaseModel
from typing import Optional

class Gene(BaseModel):
    """Gene entity for Graphiti knowledge graph"""
    symbol: str
    chromosome: str
    description: Optional[str] = None

class GWASSignal(BaseModel):
    """GWAS association entity"""
    variant_id: str
    gene_symbol: str
    p_value: float
    odds_ratio: float
    population: str
    study_type: str = "meta_analysis"

class EvidenceScore(BaseModel):
    """Multi-evidence scoring entity"""
    gene_symbol: str
    genetic_evidence_score: float
    total_evidence_sources: int
    analysis_date: str