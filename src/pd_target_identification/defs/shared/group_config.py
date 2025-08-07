"""
Centralized knowledge graph group configuration.

This ensures all assets use the same group_id for consistent knowledge graph construction.
"""
import os
from typing import Optional

# Default group for unified multi-omics Parkinson's disease knowledge graph  
# Aligned with schema_constants.py for consistency
DEFAULT_GROUP_ID = "pd_target_discovery"

def get_knowledge_graph_group_id() -> str:
    """
    Get the current knowledge graph group ID.
    
    Priority:
    1. Environment variable PD_KNOWLEDGE_GRAPH_GROUP
    2. Default: pd_parkinson_multiomics_v1
    
    Returns:
        str: Group ID for knowledge graph operations
    """
    return os.getenv("PD_KNOWLEDGE_GRAPH_GROUP", DEFAULT_GROUP_ID)

def set_knowledge_graph_group_id(group_id: str) -> None:
    """
    Set the knowledge graph group ID via environment variable.
    
    Args:
        group_id: The group ID to use for knowledge graph operations
    """
    os.environ["PD_KNOWLEDGE_GRAPH_GROUP"] = group_id

def get_available_groups() -> list[str]:
    """
    Get list of recommended group naming patterns.
    
    Returns:
        List of suggested group ID patterns
    """
    return [
        "pd_parkinson_multiomics_v1",  # Current unified approach
        "alzheimers_discovery_v1",     # Future disease research
        "cancer_targets_v1",           # Future disease research  
        "test_development",            # Development/testing
    ]

def validate_group_id(group_id: str) -> bool:
    """
    Validate that a group ID follows recommended naming conventions.
    
    Args:
        group_id: Group ID to validate
        
    Returns:
        bool: True if valid naming pattern
    """
    # Basic validation: alphanumeric + underscores, reasonable length
    if not group_id or len(group_id) < 3 or len(group_id) > 50:
        return False
    
    # Allow alphanumeric characters and underscores
    return all(c.isalnum() or c == '_' for c in group_id)

# Module-level access for convenience
CURRENT_GROUP_ID = get_knowledge_graph_group_id()