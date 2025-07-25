from dagster import Config
from pydantic import BaseModel
from typing import List

class Gene(BaseModel):
    """Pydantic model for gene entities"""
    symbol: str
    ensembl_id: str
    chromosome: str
    start_position: int
    end_position: int

class GWASSignal(BaseModel):
    """Pydantic model for GWAS associations"""
    variant_id: str
    chromosome: str
    position: int
    p_value: float
    odds_ratio: float
    nearest_gene: str
    population: str
    
class GWASConfig(Config):
    """Configuration for GWAS data acquisition"""
    study_id: str = "PD_2019_meta"
    populations: List[str] = ["EUR", "EAS"]
    p_value_threshold: float = 5e-8
    max_variants: int = 10000

class ExpressionConfig(Config):
    """Configuration for expression data"""
    tissue_types: List[str] = ["brain_substantia_nigra", "brain_striatum"]
    min_expression_level: float = 1.0
    include_single_cell: bool = True

class IntegrationConfig(Config):
    """Configuration for evidence integration"""
    genetic_weight: float = 0.4
    expression_weight: float = 0.3
    pathway_weight: float = 0.2
    literature_weight: float = 0.1
    min_evidence_sources: int = 2

