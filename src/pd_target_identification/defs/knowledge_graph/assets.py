# from dagster import asset, AssetExecutionContext
# from typing import Dict, List
# import json

# # from .graphiti_integration import create_graphiti_episodes  # TODO: Implement this function
# from .entity_types import Gene, GWASSignal, ExpressionProfile

# @asset(
#     deps=["integrated_target_scores"],
#     group_name="knowledge_graph",
#     compute_kind="graphiti"
# )
# def target_discovery_episodes(
#     context: AssetExecutionContext,
#     integrated_target_scores: Dict
# ) -> List[Dict]:
#     """
#     Transform integrated evidence into Graphiti knowledge graph episodes
#     """
#     context.log.info("Creating Graphiti episodes for target evidence")
    
#     # TODO: Implement once create_graphiti_episodes is available
#     # episodes = create_graphiti_episodes(
#     #     target_scores=integrated_target_scores,
#     #     entity_types=[Gene, GWASSignal, ExpressionProfile]
#     # )
    
#     # Placeholder return for now
#     episodes = [{"source_description": "placeholder_episode", "content": "TODO: implement"}]
    
#     context.add_output_metadata({
#         "num_episodes": len(episodes),
#         "episode_types": list(set(ep["source_description"] for ep in episodes))
#     })
    
#     return episodes

from dagster import asset, AssetExecutionContext
import pandas as pd
import json
from typing import List, Dict
from datetime import datetime
import numpy as np
from .entity_types import Gene, GWASSignal, EvidenceScore

@asset(
    deps=["raw_gwas_data"],
    group_name="knowledge_graph",
    compute_kind="graphiti",
    tags={"output": "graphiti_episodes"}
)
def gwas_graphiti_episodes(
    context: AssetExecutionContext,
    raw_gwas_data: pd.DataFrame
) -> List[Dict]:
    """
    Convert GWAS data to Graphiti episodes with proper JSON structure
    """
    context.log.info("Creating Graphiti episodes from GWAS data")
    
    episodes = []
    
    # Create episodes for each GWAS signal
    for _, row in raw_gwas_data.iterrows():
        
        # Episode for the gene entity
        gene_episode = {
            "name": f"Gene {row['nearest_gene']}",
            "episode_body": json.dumps({
                "gene": {
                    "symbol": row['nearest_gene'],
                    "chromosome": row['chromosome'],
                    "description": f"Gene associated with Parkinson's disease"
                }
            }),
            "source": "json",
            "source_description": "PD gene annotations"
        }
        
        # Episode for the GWAS signal
        gwas_episode = {
            "name": f"GWAS Signal {row['variant_id']}",
            "episode_body": json.dumps({
                "gwas_signal": {
                    "variant_id": row['variant_id'],
                    "gene_symbol": row['nearest_gene'],
                    "p_value": float(row['p_value']),
                    "odds_ratio": float(row['odds_ratio']),
                    "population": row['population'],
                    "study_type": "parkinson_disease_meta_analysis"
                }
            }),
            "source": "json",
            "source_description": "GWAS association evidence"
        }
        
        # Episode for evidence scoring
        evidence_episode = {
            "name": f"Evidence Score {row['nearest_gene']}",
            "episode_body": json.dumps({
                "evidence_score": {
                    "gene_symbol": row['nearest_gene'],
                    "genetic_evidence_score": float(-np.log10(row['p_value']) / 10),  # Normalized
                    "total_evidence_sources": 1,  # Just GWAS for now
                    "analysis_date": datetime.now().isoformat()
                }
            }),
            "source": "json", 
            "source_description": "Multi-evidence target scoring"
        }
        
        episodes.extend([gene_episode, gwas_episode, evidence_episode])
    
    context.add_output_metadata({
        "num_episodes_created": len(episodes),
        "episode_types": ["gene", "gwas_signal", "evidence_score"],
        "genes_processed": raw_gwas_data['nearest_gene'].nunique()
    })
    
    return episodes