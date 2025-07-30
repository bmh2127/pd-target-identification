# PD Target Identification Platform

A comprehensive data pipeline for Parkinson's Disease (PD) target identification and biomarker discovery using Dagster, integrating multiple data sources including GWAS, gene expression, literature, and biological pathways.

## 🧬 Overview

This platform combines multi-omics data integration with knowledge graph construction to systematically identify and prioritize potential therapeutic targets for Parkinson's Disease. Built on Dagster's orchestration framework, it provides reproducible, scalable data pipelines for biomarker discovery and target validation.

## ✨ Key Features

### 🔄 Data Integration Pipeline
- **GWAS Data Ingestion**: Processing genome-wide association studies for PD susceptibility loci
- **Gene Expression Analysis**: GTEx and disease-specific expression profiling
- **Literature Mining**: Automated extraction of target-related evidence from scientific literature
- **Pathway Analysis**: Integration of biological pathway data for mechanistic insights
- **Gene Mapping**: Comprehensive gene annotation and ortholog mapping

### 🧠 Knowledge Graph Construction
- **Graphiti Integration**: Advanced knowledge graph management for biological entities
- **Entity Recognition**: Automated identification of genes, proteins, and biological processes
- **Relationship Mapping**: Network analysis of gene-disease-pathway associations
- **Evidence Scoring**: Weighted integration of evidence from multiple data sources

### 📊 Target Prioritization
- **Multi-source Evidence Integration**: Combining GWAS, expression, and literature evidence
- **Scoring Algorithms**: Weighted scoring systems for target prioritization
- **Quality Control**: Comprehensive data validation and quality assessment
- **Interactive Visualization**: Dashboards for exploring target candidates

## 🏗️ Architecture

### Project Structure
```
pd-target-identification/
├── src/pd_target_identification/
│   ├── defs/
│   │   ├── ingestion/          # Data ingestion assets
│   │   │   ├── gwas/           # GWAS data processing
│   │   │   ├── expression/     # Gene expression analysis
│   │   │   ├── gene_mapping/   # Gene annotation and mapping
│   │   │   ├── literature/     # Literature mining
│   │   │   └── pathways/       # Pathway data integration
│   │   ├── integration/        # Data integration and scoring
│   │   ├── knowledge_graph/    # Graphiti knowledge graph management
│   │   ├── schedules/          # Pipeline scheduling
│   │   └── shared/             # Shared resources and configurations
│   ├── definitions.py          # Dagster definitions
│   └── assets.py              # Asset definitions
├── pd_target_identification_tests/  # Test suite
├── data/                      # Raw data storage
├── .venv/                     # Virtual environment
├── pyproject.toml            # Project configuration
└── uv.lock                   # Dependency lock file
```

### Data Pipeline Flow

1. **Ingestion Layer**
   - GWAS: Process genome-wide association data for PD
   - Expression: Analyze tissue-specific gene expression patterns
   - Literature: Extract target-related evidence from publications
   - Pathways: Import biological pathway annotations
   - Gene Mapping: Standardize gene identifiers and annotations

2. **Integration Layer**
   - Cross-reference data sources
   - Apply quality filters and validation
   - Calculate evidence scores and confidence metrics

3. **Knowledge Graph Layer**
   - Construct entity-relationship networks
   - Store structured biological knowledge
   - Enable graph-based queries and analysis

4. **Output Layer**
   - Prioritized target candidates
   - Evidence summaries and visualizations
   - Quality assessment reports

## 🚀 Getting Started

### Prerequisites
- Python ≥ 3.12
- uv (recommended) or pip for dependency management

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/bmh2127/pd-target-identification.git
cd pd-target-identification
```

2. **Install dependencies**
```bash
# Using uv (recommended)
uv sync

# Or activate virtual environment and use pip
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

3. **Configure environment**
Create a `.env` file with necessary API keys and database connections:
```env
# Database connections
DATABASE_URL=your_database_url

# API keys (if using external data sources)
NCBI_API_KEY=your_ncbi_api_key
GTEX_API_KEY=your_gtex_api_key

# Graphiti configuration
GRAPHITI_ENDPOINT=your_graphiti_endpoint
```

### Quick Start

1. **Launch Dagster UI**
```bash
dagster dev
```
Navigate to `http://localhost:3000` to access the Dagster web interface.

2. **Run the pipeline**
```bash
# Run all assets
dagster asset materialize --select "*"

# Run specific ingestion pipeline
dagster asset materialize --select "gwas_*"
```

3. **Check results**
```bash
# View materialized assets in the Dagster UI
# Or access data programmatically
python -c "
from pd_target_identification.defs.integration.assets import get_target_scores
scores = get_target_scores()
print(f'Top targets: {scores.head()}')
"
```

## 📊 Data Sources

### Integrated Databases
- **GWAS Catalog**: Genome-wide association study results for PD
- **GTEx**: Tissue-specific gene expression data
- **STRING**: Protein-protein interaction networks
- **KEGG/Reactome**: Biological pathway databases
- **PubMed**: Scientific literature for evidence mining
- **UniProt**: Protein annotation and functional data

### Data Processing
- **Quality Control**: Automated data validation and filtering
- **Standardization**: Consistent gene/protein identifier mapping
- **Evidence Integration**: Multi-source evidence aggregation
- **Statistical Analysis**: Association testing and significance assessment

## 🔧 Configuration

### Pipeline Configuration
Modify `src/pd_target_identification/defs/shared/configs.py`:
```python
# Data source priorities
DATA_SOURCE_WEIGHTS = {
    "gwas": 0.4,
    "expression": 0.3,
    "literature": 0.2,
    "pathways": 0.1
}

# Quality thresholds
QUALITY_THRESHOLDS = {
    "min_gwas_pvalue": 5e-8,
    "min_expression_fold_change": 1.5,
    "min_literature_score": 0.7
}
```

### Asset Configuration
Customize data processing in individual asset files:
- `ingestion/gwas/assets.py`: GWAS processing parameters
- `ingestion/expression/assets.py`: Expression analysis settings
- `integration/assets.py`: Integration and scoring algorithms

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest pd_target_identification_tests/

# Run specific test modules
pytest pd_target_identification_tests/test_assets.py -v

# Test data pipeline
dagster asset materialize --select "*" --partition-key "test"
```

### Test Coverage
- Unit tests for individual assets
- Integration tests for pipeline workflows
- Data quality validation tests
- Performance benchmarking

## 📈 Usage Examples

### Target Prioritization Analysis
```python
from pd_target_identification.defs.integration.scoring import calculate_target_scores
from pd_target_identification.defs.knowledge_graph.assets import query_knowledge_graph

# Get prioritized targets
targets = calculate_target_scores()
top_targets = targets.nlargest(10, 'composite_score')

# Query knowledge graph for target details
for target in top_targets.index:
    details = query_knowledge_graph(f"gene:{target}")
    print(f"{target}: {details}")
```

### Custom Analysis Pipeline
```python
from dagster import materialize
from pd_target_identification.defs import *

# Materialize specific asset groups
result = materialize([
    gwas_processed_data,
    expression_analysis_results,
    integrated_target_scores
])
```

### Data Export
```python
# Export results for external analysis
from pd_target_identification.defs.integration.assets import export_results

export_results(
    format="excel",
    include_evidence=True,
    output_path="pd_targets_analysis.xlsx"
)
```

## 🔍 Monitoring and Observability

### Dagster Features
- **Asset Lineage**: Track data dependencies and transformations
- **Run History**: Monitor pipeline execution and performance
- **Data Quality**: Built-in data validation and testing
- **Alerting**: Configure alerts for pipeline failures or data quality issues

### Metrics and Logging
- Pipeline execution metrics
- Data quality assessment reports
- Target scoring distribution analysis
- Evidence source contribution tracking

## 📚 Scientific Background

### Target Identification Strategy
This platform implements a systematic approach to PD target identification based on:

1. **Genetic Evidence**: GWAS-identified risk loci and causal genes
2. **Expression Evidence**: Disease-relevant expression changes
3. **Literature Evidence**: Published research on PD mechanisms
4. **Network Evidence**: Protein interaction and pathway analysis
5. **Druggability Assessment**: Potential for therapeutic intervention

### Validation Framework
- **Cross-validation**: Evidence consistency across data sources
- **Literature Support**: Published validation studies
- **Pathway Context**: Biological relevance and mechanism
- **Clinical Translation**: Potential for therapeutic development

## 🔒 Data Security and Compliance

- **Data Privacy**: No personal genomic data is stored
- **API Security**: Secure handling of API keys and credentials
- **Audit Trail**: Complete lineage tracking for reproducibility
- **Backup and Recovery**: Automated data backup and version control

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Install development dependencies: `uv sync --dev`
4. Make changes and add tests
5. Run tests: `pytest`
6. Submit a pull request

### Code Standards
- Follow PEP 8 style guidelines
- Add docstrings to all functions and classes
- Include unit tests for new features
- Update documentation as needed

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Check the documentation in `docs/`
- Review test examples in `pd_target_identification_tests/`

## 📖 References

- **Dagster Documentation**: https://docs.dagster.io/
- **Graphiti Framework**: Knowledge graph management
- **PD Genetics**: Latest research on Parkinson's Disease genetics
- **Multi-omics Integration**: Best practices for biological data integration

---

*Advancing Parkinson's Disease research through systematic target identification and validation.*