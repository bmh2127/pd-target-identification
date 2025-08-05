# Adding New Evidence Sources to the Pipeline

This guide explains how to add new evidence sources and episode types to the PD Target Identification pipeline.

## Overview

The pipeline uses a **YAML-based centralized configuration system** for episode types defined in:
```
config/episode_types.yaml                                    # YAML configuration
src/pd_target_identification/defs/shared/episode_config_yaml.py  # Python loader
```

This eliminates hardcoding and makes adding new evidence sources straightforward, with support for environment-specific configurations.

## Step-by-Step Guide

### 1. Add Episode Type Configuration

Edit `config/episode_types.yaml`:

```yaml
episode_types:
  your_new_evidence:
    order: 8  # Choose appropriate order (after dependencies)
    description: "Your evidence source description"
    dependencies: ["gene_profile"]  # What episode types must exist first
    group_name: "knowledge_graph"
    compute_kind: "api"  # "python", "api", or other
    tags:
      data_type: "episodes"
      source: "your_source"
    enabled: true
```

**Environment-specific configuration** (optional):
```yaml
environments:
  development:
    episode_types:
      your_new_evidence:
        enabled: false  # Disable in dev for faster testing
```

### 2. Create the Episode Asset

Create your episode generation asset in the appropriate module:

```python
# Example: src/pd_target_identification/defs/ingestion/your_source/assets.py
from dagster import asset, AssetExecutionContext
import pandas as pd

@asset(
    deps=["multi_evidence_integrated"],  # or other dependencies
    group_name="knowledge_graph",
    compute_kind="api",
    tags={"data_type": "episodes", "source": "your_source"}
)
def your_new_evidence_episodes(
    context: AssetExecutionContext,
    multi_evidence_integrated: pd.DataFrame
) -> pd.DataFrame:
    """Generate episodes for your new evidence source"""
    # Your episode generation logic here
    episodes = []
    
    for _, row in multi_evidence_integrated.iterrows():
        # Create episode content
        episode_data = {
            'name': f"{row['gene_symbol']}_your_evidence",
            'episode_body': f"Your evidence content for {row['gene_symbol']}",
            'source': 'your_source',
            'source_description': 'Your evidence source description',
            'group_id': 'pd_target_identification'
        }
        
        episodes.append({
            'gene_symbol': row['gene_symbol'],
            'episode_name': episode_data['name'],
            'episode_data': episode_data,
            'validation_status': 'success',
            'episode_type': 'your_new_evidence',
            'data_completeness': 1.0,
            'created_timestamp': pd.Timestamp.now()
        })
    
    return pd.DataFrame(episodes)
```

### 3. Update Complete Episodes Asset

Add your new episode asset to the dependencies in:
`src/pd_target_identification/defs/knowledge_graph/assets.py`

```python
@asset(
    deps=["gene_profile_episodes", "gwas_evidence_episodes", "eqtl_evidence_episodes",
          "literature_evidence_episodes", "pathway_evidence_episodes", 
          "census_validation_episodes", "your_new_evidence_episodes"],  # Add here
    description="Combine all episode types into complete knowledge graph dataset"
)
def complete_knowledge_graph_episodes(
    context: AssetExecutionContext,
    # ... existing parameters ...
    your_new_evidence_episodes: pd.DataFrame  # Add parameter
) -> pd.DataFrame:
    # ... existing logic ...
    
    # Add to concatenation
    all_episodes = pd.concat([
        gene_profile_episodes,
        # ... existing episode types ...
        your_new_evidence_episodes  # Add here
    ], ignore_index=True)
```

### 4. Update Main Definitions

Add your new asset to `src/pd_target_identification/definitions.py`:

```python
# Add import
from .defs.ingestion.your_source.assets import your_new_evidence_episodes

# Add to all_assets list
all_assets = [
    # ... existing assets ...
    your_new_evidence_episodes,  # Add here
    # ... rest of assets ...
]
```

### 5. Test Your Integration

Run the configuration summary to verify your new episode type:

```bash
cd src/pd_target_identification/defs/shared
python episode_config.py
```

You should see your new episode type in the processing order.

## Configuration Features

### Automatic Processing

Once configured, your new episode type will automatically be:
- ✅ **Included in export order** (graphiti_export)
- ✅ **Recognized in tests** (test validation)
- ✅ **Available for ingestion** (Graphiti service)
- ✅ **Documented in metadata** (episode type configs)

### Dependency Management

The configuration system supports:
- **Processing order** based on dependencies
- **Parallel processing** for independent evidence types
- **Validation** of dependency relationships

### Metadata Support

Each episode type includes:
- **Description** for documentation
- **Dependencies** for ordering
- **Tags** for categorization
- **Compute type** for resource planning

## Examples

### Simple Independent Evidence
```python
"biobank_evidence": EpisodeTypeConfig(
    order=6,
    description="Biobank validation data",
    dependencies=["gene_profile"],  # Only needs gene profiles
    group_name="validation",
    compute_kind="api",
    tags={"data_type": "episodes", "source": "biobank"}
)
```

### Complex Dependent Evidence
```python
"multi_omics_integration": EpisodeTypeConfig(
    order=10,
    description="Multi-omics data integration",
    dependencies=["gene_profile", "gwas_evidence", "eqtl_evidence", 
                 "proteomics_evidence", "metabolomics_evidence"],
    group_name="integration",
    compute_kind="python",
    tags={"data_type": "episodes", "source": "multi_omics"}
)
```

## Migration Notes

If you have existing hardcoded episode types in:
- Custom scripts
- Test files  
- Configuration files

Update them to import from the YAML-based configuration:

```python
from pd_target_identification.defs.shared.episode_config_yaml import (
    get_all_episode_types,
    get_processing_order,
    get_episode_config
)
```

**Environment Configuration**:
```bash
# Set environment for different configurations
export PD_EPISODE_CONFIG_ENV=development  # or testing, production
```

## Troubleshooting

### Episode Type Not Exported
- Check that it's added to `config/episode_types.yaml`
- Verify the order number is unique
- Ensure dependencies are correct
- Check that `enabled: true` is set
- Verify environment configuration if using non-production environment

### Asset Not Found
- Check import in `definitions.py`
- Verify asset is in `all_assets` list
- Check asset decorator parameters

### Ingestion Issues
- Verify episode DataFrame format matches other episode assets
- Check that `episode_type` column matches config name exactly
- Ensure all required columns are present

This centralized configuration makes the pipeline much more extensible and maintainable! 🎉