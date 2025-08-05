"""
YAML-based Episode Type Configuration for PD Target Identification Pipeline

This module loads episode type configuration from YAML files, providing the same
interface as the Python-based configuration but with YAML-based storage.

This approach offers:
- Consistency with existing YAML configuration patterns
- Easy editing by non-developers
- Environment-specific configuration overrides
- External tooling compatibility
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from functools import lru_cache

@dataclass
class EpisodeTypeConfig:
    """Configuration for an episode type"""
    order: int
    description: str
    dependencies: List[str]
    group_name: str
    compute_kind: str
    tags: Dict[str, str]
    enabled: bool = True

class EpisodeConfigLoader:
    """Loads and manages episode configuration from YAML files"""
    
    def __init__(self, config_path: Optional[str] = None, environment: str = "production"):
        """
        Initialize the configuration loader
        
        Args:
            config_path: Path to the episode configuration YAML file
            environment: Environment name for configuration overrides
        """
        if config_path is None:
            # Default to config/episode_types.yaml in project root
            project_root = Path(__file__).parent.parent.parent.parent.parent
            config_path = project_root / "config" / "episode_types.yaml"
        
        self.config_path = Path(config_path)
        self.environment = environment
        self._config_cache = None
        
    @lru_cache(maxsize=1)
    def _load_config(self) -> Dict[str, Any]:
        """Load and cache the YAML configuration"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Episode configuration not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Apply environment-specific overrides
        if 'environments' in config and self.environment in config['environments']:
            env_config = config['environments'][self.environment]
            if 'episode_types' in env_config:
                for episode_type, overrides in env_config['episode_types'].items():
                    if episode_type in config['episode_types']:
                        config['episode_types'][episode_type].update(overrides)
        
        return config
    
    def get_episode_types(self) -> Dict[str, EpisodeTypeConfig]:
        """Get all episode type configurations"""
        config = self._load_config()
        episode_types = {}
        
        for name, type_config in config['episode_types'].items():
            # Only include enabled episode types
            if type_config.get('enabled', True):
                episode_types[name] = EpisodeTypeConfig(
                    order=type_config['order'],
                    description=type_config['description'],
                    dependencies=type_config.get('dependencies', []),
                    group_name=type_config.get('group_name', 'knowledge_graph'),
                    compute_kind=type_config.get('compute_kind', 'python'),
                    tags=type_config.get('tags', {}),
                    enabled=type_config.get('enabled', True)
                )
        
        return episode_types
    
    def reload_config(self):
        """Force reload of configuration (clears cache)"""
        self._load_config.cache_clear()

# ============================================================================
# GLOBAL CONFIGURATION INSTANCE
# ============================================================================

# Get environment from environment variable or default to production
ENVIRONMENT = os.getenv('PD_EPISODE_CONFIG_ENV', 'production')

# Create global configuration loader
_config_loader = EpisodeConfigLoader(environment=ENVIRONMENT)

# ============================================================================
# PUBLIC API - Same interface as Python config
# ============================================================================

def get_episode_types() -> Dict[str, EpisodeTypeConfig]:
    """Get all episode type configurations"""
    return _config_loader.get_episode_types()

def get_episode_config(episode_type: str) -> Optional[EpisodeTypeConfig]:
    """Get configuration for a specific episode type"""
    episode_types = get_episode_types()
    return episode_types.get(episode_type)

def validate_episode_type(episode_type: str) -> bool:
    """Check if episode type is valid and enabled"""
    return episode_type in get_episode_types()

def get_dependencies(episode_type: str) -> List[str]:
    """Get dependencies for an episode type"""
    config = get_episode_config(episode_type)
    return config.dependencies if config else []

def get_processing_order() -> List[str]:
    """Get the recommended processing order for all episode types"""
    episode_types = get_episode_types()
    return [
        episode_type for episode_type, config in 
        sorted(episode_types.items(), key=lambda x: x[1].order)
    ]

def get_episode_types_for_export() -> List[str]:
    """Get episode types in export order (same as processing order)"""
    return get_processing_order()

def get_all_episode_types() -> List[str]:
    """Get all episode type names"""
    return list(get_episode_types().keys())

# Backward compatibility aliases
INGESTION_ORDER = get_processing_order()
ALL_EPISODE_TYPES = get_all_episode_types()

def reload_configuration():
    """Reload configuration from YAML file"""
    global _config_loader
    _config_loader.reload_config()

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def print_episode_config_summary():
    """Print a summary of the current episode configuration"""
    episode_types = get_episode_types()
    processing_order = get_processing_order()
    
    print(f"📋 Episode Type Configuration Summary (Environment: {ENVIRONMENT})")
    print("=" * 60)
    print(f"Total Episode Types: {len(episode_types)}")
    print(f"Processing Order: {' → '.join(processing_order)}")
    print(f"Configuration Source: {_config_loader.config_path}")
    print("\nDetailed Configuration:")
    
    for episode_type in processing_order:
        config = episode_types[episode_type]
        deps = ', '.join(config.dependencies) if config.dependencies else 'None'
        print(f"  {config.order}. {episode_type}")
        print(f"     Description: {config.description}")
        print(f"     Dependencies: {deps}")
        print(f"     Source: {config.tags.get('source', 'unknown')}")
        print(f"     Enabled: {config.enabled}")
        print()

def switch_environment(environment: str):
    """Switch to a different environment configuration"""
    global _config_loader, ENVIRONMENT
    ENVIRONMENT = environment
    _config_loader = EpisodeConfigLoader(environment=environment)
    
    # Update module-level variables for backward compatibility
    global INGESTION_ORDER, ALL_EPISODE_TYPES
    INGESTION_ORDER = get_processing_order()
    ALL_EPISODE_TYPES = get_all_episode_types()

def validate_configuration() -> Dict[str, Any]:
    """Validate the current configuration and return validation results"""
    validation = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "stats": {}
    }
    
    try:
        episode_types = get_episode_types()
        processing_order = get_processing_order()
        
        # Check for duplicate orders
        orders = [config.order for config in episode_types.values()]
        if len(orders) != len(set(orders)):
            validation["errors"].append("Duplicate order numbers found")
            validation["valid"] = False
        
        # Check dependency validity
        for episode_type, config in episode_types.items():
            for dep in config.dependencies:
                if dep not in episode_types:
                    validation["errors"].append(f"{episode_type} depends on non-existent type: {dep}")
                    validation["valid"] = False
        
        # Check for circular dependencies (basic check)
        for episode_type, config in episode_types.items():
            if episode_type in config.dependencies:
                validation["errors"].append(f"{episode_type} has circular dependency on itself")
                validation["valid"] = False
        
        validation["stats"] = {
            "total_types": len(episode_types),
            "enabled_types": len([t for t in episode_types.values() if t.enabled]),
            "max_order": max(orders) if orders else 0,
            "total_dependencies": sum(len(config.dependencies) for config in episode_types.values())
        }
        
    except Exception as e:
        validation["valid"] = False
        validation["errors"].append(f"Configuration error: {str(e)}")
    
    return validation

if __name__ == "__main__":
    print_episode_config_summary()
    print("\n" + "=" * 60)
    validation = validate_configuration()
    if validation["valid"]:
        print("✅ Configuration is valid")
    else:
        print("❌ Configuration errors found:")
        for error in validation["errors"]:
            print(f"  - {error}")