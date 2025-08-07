# PD Target Identification Platform

A **production-ready** comprehensive data pipeline for Parkinson's Disease (PD) target identification and biomarker discovery using Dagster, integrating multiple data sources with advanced knowledge graph construction via Graphiti and MCP (Model Context Protocol) integration.

## 🧬 Overview

This platform combines multi-omics data integration with **AI-powered knowledge graph construction** to systematically identify and prioritize potential therapeutic targets for Parkinson's Disease. Built on Dagster's orchestration framework with direct MCP integration, it provides reproducible, scalable data pipelines for biomarker discovery and target validation.

**🎯 Current Status**: **Production Ready** - Successfully processing 94 episodes across multiple evidence types with 1,000+ knowledge graph nodes. **Recent Updates (Jan 2025)**: Configuration consolidation for consistent gene discovery + CELLxGENE Census single cell RNA integration + Direct MCP integration for efficient knowledge graph construction.

## ✨ Key Features

### 🔄 **Multi-Source Data Integration Pipeline** 
- **GWAS Data**: GWAS Catalog integration for PD genetic associations (10,000 variant limit)
- **eQTL Data**: GTEx brain tissue expression quantitative trait loci
- **Single Cell RNA**: CELLxGENE Census integration for cell type-specific expression validation
- **Literature Mining**: PubMed therapeutic target evidence extraction
- **Pathway Analysis**: STRING protein-protein interactions and functional enrichment
- **Gene Mapping**: Comprehensive gene annotation and identifier standardization with biorosetta

### 🧠 **AI-Powered Knowledge Graph** 
- **Direct MCP Integration**: Efficient knowledge graph construction via Model Context Protocol
- **Graphiti Engine**: Advanced entity recognition and relationship mapping
- **Real-time Search**: Semantic search capabilities for biological entities and relationships
- **Evidence Integration**: Multi-source evidence aggregation with enhanced scoring
- **1,000+ Active Nodes**: Rich knowledge graph with genes, evidence, and relationships

### 📊 **Target Prioritization & Results** 
Current top-ranked therapeutic targets with enhanced integrated scores:
- **SNCA** (218.66): Alpha-synuclein, strongest PD genetic association, Lewy body formation
- **LRRK2** (185.43): Kinase target with clinical trials, familial PD mutations
- **HLA-DRA** (166.0): Novel immune target, neuroinflammation pathway
- **RIT2** (128.05): GTPase signaling with brain-specific regulation
- **DGKQ** (100.47): Diacylglycerol kinase, lipid metabolism target

## 🏗️ Architecture

### **Production Infrastructure**
- **Pipeline Orchestration**: Dagster with asset-based architecture
- **Data Storage**: DuckDB for intermediate processing, Neo4j for knowledge graph
- **Knowledge Graph**: Graphiti + MCP direct integration (bypassing service layer)
- **Graph Database**: Neo4j with 1,000+ nodes across multiple entity types
- **Transport**: Server-Sent Events (SSE) for efficient MCP communication

### **MCP Communication & Integration**
- **SSE Transport**: Server-Sent Events enable efficient real-time communication
- **Direct Integration**: Bypasses service layer for reduced latency and complexity
- **Cursor Compatibility**: Works seamlessly with Cursor IDE for development
- **Claude Desktop Support**: HTTP transport available for Claude Desktop users
- **Early Stage Technology**: SSE MCP is cutting-edge but stable for production use

### Project Structure
```
pd-target-identification/
├── src/pd_target_identification/
│   ├── defs/
│   │   ├── ingestion/              # Data ingestion assets
│   │   │   ├── gwas/               # GWAS Catalog processing
│   │   │   ├── expression/         # GTEx eQTL analysis  
│   │   │   ├── gene_mapping/       # Gene annotation mapping
│   │   │   ├── literature/         # PubMed literature mining
│   │   │   ├── pathways/           # STRING database integration
│   │   │   └── single_cell/        # CELLxGENE Census integration
│   │   ├── knowledge_graph/        # Knowledge graph construction
│   │   │   ├── assets.py           # Episode generation assets
│   │   │   └── mcp_assets.py       # Direct MCP integration
│   │   └── shared/                 # Resources and configurations
│   │   │   ├── configs.py          # Centralized configuration classes
│   │   │   └── resources.py        # Dagster resources
│   ├── definitions.py              # Complete asset definitions
│   └── assets.py                   # Legacy asset definitions
├── exports/                        # Knowledge graph export files
├── data/                          # Raw and processed data storage
├── .venv/                         # Virtual environment
├── pyproject.toml                 # Project configuration
├── README.md                      # This file
└── SINGLE_CELL_RNA_INTEGRATION_MISSION.md  # 🆕 Next phase roadmap
```

### **Data Pipeline Flow** 

```mermaid
graph TD
    A[GWAS Catalog] --> F[Gene Profiles]
    B[GTEx eQTLs] --> F
    C[PubMed Literature] --> F
    D[STRING Pathways] --> F
    E[CELLxGENE Census] --> F
    F --> G[Evidence Episodes]
    G --> H[Graphiti Export]
    H --> I[MCP Integration]
    I --> J[Neo4j Knowledge Graph]
    J --> K[Semantic Search & Analysis]
```

1. **Ingestion Layer** 
   - GWAS: Genome-wide association data for PD risk loci
   - eQTL: Brain-specific expression quantitative trait loci
   - Single Cell: CELLxGENE Census for cell type-specific expression validation
   - Literature: Target-related evidence from scientific publications
   - Pathways: Protein interactions and functional enrichment
   - Gene Mapping: Standardized identifiers and annotations

2. **Episode Generation** 
   - Gene profiles with multi-evidence integration
   - Evidence-specific episodes (GWAS, eQTL, literature, pathway, single cell)
   - Enhanced integrated scoring (base + evidence contributions)
   - Cell type-specific validation episodes (neurons, microglia, astrocytes)
   - Structured episode format for knowledge graph ingestion

3. **Knowledge Graph Construction** 
   - **Direct MCP Integration**: Efficient episode ingestion via SSE transport
   - **Entity Recognition**: Automated gene, protein, and pathway identification  
   - **Relationship Mapping**: Network analysis of gene-disease-pathway associations
   - **Evidence Scoring**: Weighted integration with enhanced scoring framework

4. **Analysis & Search** 
   - **Semantic Search**: Find targets by biological meaning and context
   - **Fact Queries**: Discover relationships and evidence connections
   - **Target Prioritization**: Enhanced integrated scores for ranking
   - **Real-time Access**: MCP tools for immediate knowledge graph interaction

## 🚀 Getting Started

### Prerequisites
- Python ≥ 3.12
- Docker (for Neo4j and MCP services)
- uv package manager (recommended)
- **Graphiti MCP Server**: Must be running for knowledge graph functionality

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

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Start required services**
```bash
# Start Neo4j and MCP services (ensure Docker is running)
cd /path/to/graphiti && docker-compose up -d

# Verify Graphiti MCP server is running
curl http://localhost:8000/health
```

4. **Configure environment**
Create a `.env` file:
```env
# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=demodemo

# API Keys
NCBI_API_KEY=your_ncbi_api_key
OPENAI_API_KEY=your_openai_api_key

# MCP Configuration
MCP_SERVER_URL=http://localhost:8000/sse
```

### Quick Start

1. **Launch Dagster UI**
```bash
dagster dev
```
Navigate to `http://localhost:3000` to access the Dagster web interface.

2. **Run the complete pipeline**
```bash
# Materialize all assets for fresh data (now with improved GWAS configuration)
dagster asset materialize --select "*"

# Or run specific components
dagster asset materialize --select "gwas_*"        # GWAS with 10K variant limit
dagster asset materialize --select "census_*"      # Single cell RNA validation
dagster asset materialize --select "graphiti_*"    # Knowledge graph construction
```

3. **Access the knowledge graph**
```python
# Example: Search for therapeutic targets
from mcp_graphiti_memory import search_memory_nodes, search_memory_facts

# Find top PD targets
targets = search_memory_nodes("SNCA LRRK2 therapeutic target Parkinson's disease")
print(f"Found {len(targets)} target entities")

# Query relationships
facts = search_memory_facts("enhanced integrated score clinical trials")
print(f"Found {len(facts)} evidence relationships")
```

## 📊 **Current Data & Results** 

### **Knowledge Graph Statistics**
- **Total Nodes**: 1,000+
- **Episodes**: 94 across multiple evidence types
- **Entity Types**: Genes, proteins, pathways, evidence scores
- **Groups**: Multiple knowledge graph groups for different data versions

### **Integrated Databases** 
- **GWAS Catalog**: PD genetic associations and risk loci
- **GTEx v8**: Brain tissue eQTL data (basal ganglia, substantia nigra focus)
- **CELLxGENE Census**: Single cell RNA-seq data for cell type-specific validation
- **PubMed**: Literature evidence for therapeutic targets
- **STRING v11**: Protein-protein interactions and pathway enrichment
- **Gene Mapping**: HGNC, Ensembl, UniProt identifier standardization

### **Top Validated Targets**
1. **SNCA (α-synuclein)**: Score 218.66
   - 10 genome-wide significant variants
   - Central to Lewy body formation  
   - Strongest PD genetic association
   - Active therapeutic development

2. **LRRK2 (Kinase)**: Score 185.43
   - Familial PD mutations (G2019S, R1441G/C/H)
   - Multiple clinical trials ongoing
   - Biomarker availability (pS935 dephosphorylation)
   - Druggable kinase target

3. **HLA-DRA (Immune)**: Score 166.0
   - Novel neuroinflammation target
   - Antigen presentation pathway
   - Immune-mediated therapeutic approach

## 🔧 **Knowledge Graph Integration**

### **MCP Direct Integration** 
```python
# New MCP asset for efficient knowledge graph construction
@asset(
    deps=["graphiti_export"],
    description="Direct MCP integration for knowledge graph ingestion"
)
def graphiti_mcp_direct_ingestion(context, graphiti_export):
    # Process episodes via MCP tools
    # No service layer overhead
    # Configurable group IDs for data organization
```

### **Search Capabilities**
```python
# Semantic node search
nodes = search_memory_nodes("therapeutic targets drug development")

# Relationship queries  
facts = search_memory_facts("LRRK2 kinase inhibitor clinical trials")

# Episode retrieval
episodes = get_episodes(group_id="pd_target_discovery", last_n=10)
```

### **Configuration Options**
- **GWAS Configuration**: Centralized config in `configs.py` (10,000 max variants, 5e-8 p-value threshold)
- **Group ID Management**: Organize knowledge graphs by project/version
- **Default Values**: No manual configuration required in Dagster UI
- **Error Handling**: Robust failure detection and retry logic
- **Container Coordination**: Smart health checking of MCP services

## ⚙️ **Configuration Management**

### **GWAS Configuration Consolidation** (Updated Jan 2025)
The platform now uses a **single source of truth** for GWAS configuration:

```python
# configs.py - Centralized configuration
class GWASConfig(Config):
    p_value_threshold: float = 5e-8    # Standard GWAS significance
    max_variants: int = 10000          # Comprehensive variant discovery
    populations: List[str] = ["EUR", "EAS"]
    study_id: str = "PD_2019_meta"
```

**Benefits:**
- ✅ **Consistent Results**: No more conflicting hardcoded limits
- ✅ **Comprehensive Discovery**: 10,000 variants vs previous 50-variant limit
- ✅ **Modern Dagster**: Uses Config classes as function parameters
- ✅ **Gene Cleaning**: Automatic deduplication of genes with formatting issues

**Expected Impact**: Significantly more genes discovered (50+ instead of 22) for comprehensive analysis.

## 🧪 Testing & Validation 

### **Run Tests**
```bash
# Test complete pipeline
pytest pd_target_identification_tests/

# Validate knowledge graph integration
python validate_knowledge_graph.py

# Run specific asset tests
dagster asset materialize --select "graphiti_mcp_direct_ingestion"
```

## 📈 **Usage Examples**

### **Target Analysis Workflow**
```python
from mcp_graphiti_memory import search_memory_nodes, search_memory_facts, get_episodes

# 1. Find top therapeutic targets
targets = search_memory_nodes("high priority therapeutic targets enhanced score")
for target in targets:
    print(f"{target['name']}: {target['summary']}")

# 2. Query specific target evidence
snca_facts = search_memory_facts("SNCA alpha-synuclein therapeutic target")
for fact in snca_facts:
    print(f"Evidence: {fact['fact']}")

# 3. Get recent analysis episodes
recent_episodes = get_episodes(group_id="pd_target_discovery_enhanced", last_n=5)
for episode in recent_episodes:
    print(f"Episode: {episode['name']} - {episode['source_description']}")
```

### **Custom Analysis Pipeline**
```python
from dagster import materialize
from pd_target_identification.defs import *

# Run end-to-end pipeline
result = materialize([
    raw_gwas_data,
    gtex_brain_eqtls,
    census_expression_validation,
    literature_evidence_extraction,
    multi_evidence_integrated,
    graphiti_mcp_direct_ingestion
])

print(f"Pipeline completed: {result.success}")
```

## 🔍 **Monitoring & Observability**

### **Dagster Features** 
- **Asset Lineage**: Complete data dependency tracking
- **Run History**: Pipeline execution monitoring and performance metrics
- **Data Quality**: Built-in validation and testing frameworks
- **Real-time Logs**: Detailed logging for debugging and monitoring

### **Knowledge Graph Monitoring**
- **Episode Processing**: Track ingestion success/failure rates
- **Node Growth**: Monitor knowledge graph expansion over time
- **Search Performance**: Query response time and accuracy metrics
- **MCP Health**: Container status and connection monitoring

## 🚀 **Future Roadmap**

### **✅ Phase Completed: Single Cell RNA Integration** 
- **Status**: **IMPLEMENTED** - CELLxGENE Census integration active
- **Databases**: CELLxGENE Census with 1.8M+ cells from human brain tissue
- **Cell Types**: Neurons, microglia, astrocytes validation
- **Integration**: Dagster asset patterns with configurable batch processing
- **Enhancement**: Census contribution to enhanced integrated scoring

### **🧬 Phase Next: Advanced Single Cell Analysis**

### **Planned Enhancements**
- **Multi-modal Integration**: Proteomics, metabolomics data sources
- **Drug Development Pipeline**: Compound screening and druggability assessment
- **Clinical Integration**: Patient data and biomarker validation
- **Advanced Analytics**: Machine learning models for target prediction

## 📚 **Scientific Validation**

### **Evidence Integration Framework**
```python
# Enhanced scoring methodology
enhanced_integrated_score = base_score + (
    gwas_contribution +           # Genetic evidence weight
    eqtl_contribution +           # Expression regulation evidence  
    literature_contribution +     # Published research evidence
    pathway_contribution +        # Functional network evidence
    census_contribution +         # Single cell expression validation
)
```

### **Current Validation Results**
- **Multi-source Convergence**: Targets validated across genetic, expression, literature, and single cell evidence
- **Cell Type Specificity**: Single cell validation in neurons, microglia, and astrocytes
- **Clinical Relevance**: Top targets have active therapeutic development programs
- **Network Context**: Pathway analysis confirms biological relevance
- **Literature Support**: Extensive publication evidence for target prioritization

## 🔒 **Data Security & Compliance**

- **Privacy**: No personal genomic data storage or processing
- **API Security**: Secure credential management and rate limiting
- **Audit Trail**: Complete data lineage and processing history
- **Reproducibility**: Deterministic pipeline execution and version control

## 🤝 **Contributing**

### **Development Setup**
1. Fork the repository
2. Install development dependencies: `uv sync --dev`
3. Run tests: `pytest`
4. Submit pull requests with tests and documentation

### **Architecture Guidelines**
- Follow established Dagster asset patterns
- Maintain compatibility with MCP integration
- Include comprehensive logging and error handling
- Document API changes and new functionality

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔧 **Troubleshooting**

### **Common Issues**

**GWAS Gene Count Variations**
- ✅ **Fixed**: Configuration consolidation ensures consistent gene discovery
- **Previous Issue**: Different runs returned 22 vs 27 genes due to conflicting `max_variants` settings
- **Solution**: All configurations now use centralized `configs.py` with 10,000 variant limit

**Configuration Conflicts**
```bash
# If you see inconsistent results, verify configuration:
python -c "from pd_target_identification.defs.shared.configs import GWASConfig; print(GWASConfig().max_variants)"
# Should output: 10000
```

**Gene Cleaning Issues**
- Platform automatically removes genes with formatting issues (e.g., carriage returns)
- This may slightly reduce final gene counts but improves data quality

**MCP Server Issues**
- **Ensure Graphiti MCP server is running**: `docker-compose up -d` in graphiti directory
- **Check SSE transport**: Verify `http://localhost:8000/sse` is accessible
- **Cursor IDE compatibility**: SSE MCP works seamlessly with Cursor
- **Claude Desktop users**: May need HTTP transport configuration

## 🆘 Support & Documentation

### **Key Resources**
- **Execution Guide**: `COMPLETE_EXECUTION_GUIDE.md`
- **Phase 7 Results**: `PHASE_7_COMPLETION_SUMMARY.md`
- **Next Mission**: `SINGLE_CELL_RNA_INTEGRATION_MISSION.md`
- **Research Queries**: `research_queries.cypher`
- **Configuration Guide**: See "Configuration Management" section above
- **MCP Integration Guide**: `MCP_INTEGRATION_GUIDE.md`

### **Support Channels**
- GitHub Issues for bug reports and feature requests
- Documentation in `docs/` directory
- Test examples in `pd_target_identification_tests/`

## 📖 References

- **Dagster Documentation**: https://docs.dagster.io/
- **Graphiti Framework**: AI-powered knowledge graph management
- **Model Context Protocol**: https://modelcontextprotocol.io/
- **PD Genetics Consortium**: Latest Parkinson's Disease genetic research
- **Multi-omics Integration**: Best practices for biological data integration

---



*Advancing Parkinson's Disease research through systematic target identification and AI-powered knowledge graph analysis.*
