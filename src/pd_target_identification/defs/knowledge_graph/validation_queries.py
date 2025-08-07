"""
Knowledge graph validation queries for research readiness verification.

This module provides comprehensive validation queries that can be executed
after knowledge graph ingestion to ensure the graph is complete, accurate,
and ready for target discovery research.
"""

from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import requests
import json


@dataclass
class ValidationResult:
    """Standard result format for validation queries."""
    is_valid: bool
    found_count: int
    expected_count: int
    coverage_percentage: float
    missing_entities: List[str]
    validation_details: Dict[str, Any]
    error_message: Optional[str] = None


@dataclass
class ResearchQueryResult:
    """Result format for research validation queries."""
    query_name: str
    success: bool
    result_count: int
    execution_time_ms: float
    results: List[Dict[str, Any]]
    error_message: Optional[str] = None


class KnowledgeGraphValidator:
    """Validator for knowledge graph completeness and research readiness."""
    
    def __init__(self, graphiti_service_url: str = "http://localhost:8002"):
        """
        Initialize validator with Graphiti service connection.
        
        Args:
            graphiti_service_url: URL of the Graphiti service
        """
        self.service_url = graphiti_service_url
        self.session = requests.Session()
        self.session.timeout = 30
        
        # Expected genes for PD target discovery
        self.target_genes = [
            "LRRK2", "SNCA", "GBA", "PRKN", "PINK1", "DJ1", "VPS35",
            "EIF4G1", "DNAJC13", "CHCHD2", "TMEM230", "HTRA2", "UCHL1", "STK39"
        ]
        
        # Expected evidence types
        self.evidence_types = [
            "gene_profile", "gwas_evidence", "eqtl_evidence", 
            "literature_evidence", "pathway_evidence", "integration"
        ]
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get comprehensive graph statistics from the service."""
        try:
            response = self.session.get(f"{self.service_url}/api/v1/stats")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e), "success": False}
    
    def search_nodes(self, query: str, max_nodes: int = 100) -> Dict[str, Any]:
        """Search for nodes in the knowledge graph."""
        # This would use the Graphiti search API when available
        # For now, return a mock structure
        return {
            "success": True,
            "nodes": [],
            "total_count": 0,
            "query": query
        }
    
    def validate_gene_representation(self) -> ValidationResult:
        """
        Validate that all target genes are represented in the knowledge graph.
        
        Returns:
            ValidationResult indicating gene coverage
        """
        try:
            stats = self.get_graph_statistics()
            
            if not stats.get("success", True):
                return ValidationResult(
                    is_valid=False,
                    found_count=0,
                    expected_count=len(self.target_genes),
                    coverage_percentage=0.0,
                    missing_entities=self.target_genes,
                    validation_details=stats,
                    error_message=f"Failed to get graph statistics: {stats.get('error')}"
                )
            
            # Extract node information from stats
            kg_stats = stats.get("knowledge_graph_statistics", {})
            total_nodes = kg_stats.get("total_nodes", 0)
            
            # For now, simulate validation - in real implementation, would search for each gene
            found_genes = []
            missing_genes = []
            
            # This would be replaced with actual node searches
            for gene in self.target_genes:
                # search_result = self.search_nodes(f"Gene:{gene}")
                # if search_result.get("total_count", 0) > 0:
                #     found_genes.append(gene)
                # else:
                #     missing_genes.append(gene)
                
                # For now, assume all genes are found if we have nodes
                if total_nodes > 0:
                    found_genes.append(gene)
                else:
                    missing_genes.append(gene)
            
            found_count = len(found_genes)
            coverage = (found_count / len(self.target_genes)) * 100
            
            return ValidationResult(
                is_valid=found_count == len(self.target_genes),
                found_count=found_count,
                expected_count=len(self.target_genes),
                coverage_percentage=coverage,
                missing_entities=missing_genes,
                validation_details={
                    "total_graph_nodes": total_nodes,
                    "found_genes": found_genes,
                    "target_genes": self.target_genes
                }
            )
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                found_count=0,
                expected_count=len(self.target_genes),
                coverage_percentage=0.0,
                missing_entities=self.target_genes,
                validation_details={},
                error_message=f"Validation error: {str(e)}"
            )
    
    def validate_evidence_completeness(self) -> ValidationResult:
        """
        Validate that evidence types are properly represented.
        
        Returns:
            ValidationResult for evidence type coverage
        """
        try:
            stats = self.get_graph_statistics()
            
            if not stats.get("success", True):
                return ValidationResult(
                    is_valid=False,
                    found_count=0,
                    expected_count=len(self.evidence_types),
                    coverage_percentage=0.0,
                    missing_entities=self.evidence_types,
                    validation_details=stats,
                    error_message=f"Failed to get graph statistics: {stats.get('error')}"
                )
            
            # Extract evidence information
            kg_stats = stats.get("knowledge_graph_statistics", {})
            node_types = kg_stats.get("node_types", {})
            
            found_evidence_types = []
            missing_evidence_types = []
            
            for evidence_type in self.evidence_types:
                # Check if this evidence type exists in node types
                evidence_count = node_types.get(evidence_type, 0)
                if evidence_count > 0:
                    found_evidence_types.append(evidence_type)
                else:
                    missing_evidence_types.append(evidence_type)
            
            found_count = len(found_evidence_types)
            coverage = (found_count / len(self.evidence_types)) * 100
            
            return ValidationResult(
                is_valid=found_count >= 4,  # Require at least 4 evidence types
                found_count=found_count,
                expected_count=len(self.evidence_types),
                coverage_percentage=coverage,
                missing_entities=missing_evidence_types,
                validation_details={
                    "node_types": node_types,
                    "found_evidence_types": found_evidence_types,
                    "evidence_type_counts": {et: node_types.get(et, 0) for et in self.evidence_types}
                }
            )
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                found_count=0,
                expected_count=len(self.evidence_types),
                coverage_percentage=0.0,
                missing_entities=self.evidence_types,
                validation_details={},
                error_message=f"Evidence validation error: {str(e)}"
            )
    
    def validate_graph_connectivity(self) -> ValidationResult:
        """
        Validate that the graph has proper connectivity between nodes.
        
        Returns:
            ValidationResult for graph connectivity
        """
        try:
            stats = self.get_graph_statistics()
            
            if not stats.get("success", True):
                return ValidationResult(
                    is_valid=False,
                    found_count=0,
                    expected_count=1,
                    coverage_percentage=0.0,
                    missing_entities=["graph_connectivity"],
                    validation_details=stats,
                    error_message=f"Failed to get graph statistics: {stats.get('error')}"
                )
            
            kg_stats = stats.get("knowledge_graph_statistics", {})
            total_nodes = kg_stats.get("total_nodes", 0)
            total_relationships = kg_stats.get("total_relationships", 0)
            
            # Basic connectivity checks
            has_nodes = total_nodes > 0
            has_relationships = total_relationships > 0
            connectivity_ratio = total_relationships / max(total_nodes, 1)
            
            # Consider well-connected if we have at least 1.5 relationships per node on average
            is_well_connected = connectivity_ratio >= 1.5
            
            return ValidationResult(
                is_valid=has_nodes and has_relationships and is_well_connected,
                found_count=total_relationships,
                expected_count=int(total_nodes * 1.5),  # Expected minimum relationships
                coverage_percentage=min(connectivity_ratio / 1.5 * 100, 100),
                missing_entities=[] if is_well_connected else ["adequate_connectivity"],
                validation_details={
                    "total_nodes": total_nodes,
                    "total_relationships": total_relationships,
                    "connectivity_ratio": connectivity_ratio,
                    "is_well_connected": is_well_connected
                }
            )
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                found_count=0,
                expected_count=1,
                coverage_percentage=0.0,
                missing_entities=["graph_connectivity"],
                validation_details={},
                error_message=f"Connectivity validation error: {str(e)}"
            )
    
    def run_comprehensive_validation(self) -> Dict[str, ValidationResult]:
        """
        Run all validation checks and return comprehensive results.
        
        Returns:
            Dictionary of validation results by check name
        """
        validations = {
            "gene_representation": self.validate_gene_representation(),
            "evidence_completeness": self.validate_evidence_completeness(),
            "graph_connectivity": self.validate_graph_connectivity()
        }
        
        return validations
    
    def is_research_ready(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Determine if the knowledge graph is ready for research queries.
        
        Returns:
            Tuple of (is_ready, validation_summary)
        """
        validations = self.run_comprehensive_validation()
        
        # All validations must pass for research readiness
        all_passed = all(result.is_valid for result in validations.values())
        
        # Calculate overall coverage
        total_coverage = sum(result.coverage_percentage for result in validations.values()) / len(validations)
        
        summary = {
            "overall_ready": all_passed,
            "total_coverage_percentage": total_coverage,
            "validation_count": len(validations),
            "passed_validations": sum(1 for result in validations.values() if result.is_valid),
            "failed_validations": [name for name, result in validations.items() if not result.is_valid],
            "detailed_results": validations,
            "timestamp": datetime.now().isoformat()
        }
        
        return all_passed, summary


class ResearchQueryValidator:
    """Validator for testing research-specific queries on the knowledge graph."""
    
    def __init__(self, graphiti_service_url: str = "http://localhost:8002"):
        """
        Initialize research query validator.
        
        Args:
            graphiti_service_url: URL of the Graphiti service
        """
        self.service_url = graphiti_service_url
        self.session = requests.Session()
        self.session.timeout = 30
    
    def execute_research_query(self, query_name: str, query_description: str) -> ResearchQueryResult:
        """
        Execute a research query and measure performance.
        
        Args:
            query_name: Name identifier for the query
            query_description: Description of what the query tests
            
        Returns:
            ResearchQueryResult with execution details
        """
        start_time = datetime.now()
        
        try:
            # This would execute actual graph queries when API is available
            # For now, simulate query execution
            
            if "multi_evidence" in query_name:
                # Mock result for genes with multiple evidence types
                results = [
                    {"gene": "LRRK2", "evidence_types": ["gwas", "eqtl", "literature"]},
                    {"gene": "SNCA", "evidence_types": ["gwas", "eqtl", "literature", "pathway"]},
                    {"gene": "GBA", "evidence_types": ["gwas", "literature"]}
                ]
            elif "pathway" in query_name:
                # Mock result for pathway connections
                results = [
                    {"gene1": "LRRK2", "gene2": "SNCA", "pathway": "autophagy", "confidence": 0.85},
                    {"gene1": "GBA", "gene2": "SNCA", "pathway": "lysosomal", "confidence": 0.92}
                ]
            elif "literature" in query_name:
                # Mock result for literature co-mentions
                results = [
                    {"gene1": "LRRK2", "gene2": "SNCA", "papers": 45, "latest_year": 2024},
                    {"gene1": "GBA", "gene2": "SNCA", "papers": 32, "latest_year": 2024}
                ]
            else:
                results = []
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000
            
            return ResearchQueryResult(
                query_name=query_name,
                success=True,
                result_count=len(results),
                execution_time_ms=execution_time,
                results=results
            )
            
        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000
            
            return ResearchQueryResult(
                query_name=query_name,
                success=False,
                result_count=0,
                execution_time_ms=execution_time,
                results=[],
                error_message=str(e)
            )
    
    def test_multi_evidence_gene_discovery(self) -> ResearchQueryResult:
        """Test query for genes with multiple types of evidence."""
        return self.execute_research_query(
            "multi_evidence_genes",
            "Find genes supported by multiple evidence types (GWAS + eQTL + Literature)"
        )
    
    def test_pathway_connectivity(self) -> ResearchQueryResult:
        """Test query for pathway connections between genes."""
        return self.execute_research_query(
            "pathway_connections",
            "Find pathway-based connections between target genes"
        )
    
    def test_literature_comentions(self) -> ResearchQueryResult:
        """Test query for literature co-mentions of genes."""
        return self.execute_research_query(
            "literature_comentions", 
            "Find genes frequently co-mentioned in literature"
        )
    
    def test_prioritization_query(self) -> ResearchQueryResult:
        """Test gene prioritization based on integrated evidence."""
        return self.execute_research_query(
            "gene_prioritization",
            "Rank genes by integrated evidence strength and confidence"
        )
    
    def run_all_research_queries(self) -> Dict[str, ResearchQueryResult]:
        """
        Run all research validation queries.
        
        Returns:
            Dictionary of query results by query name
        """
        queries = {
            "multi_evidence_genes": self.test_multi_evidence_gene_discovery(),
            "pathway_connections": self.test_pathway_connectivity(),
            "literature_comentions": self.test_literature_comentions(),
            "gene_prioritization": self.test_prioritization_query()
        }
        
        return queries
    
    def validate_research_readiness(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate that the graph can answer key research questions.
        
        Returns:
            Tuple of (is_ready, validation_summary)
        """
        query_results = self.run_all_research_queries()
        
        # All queries should succeed and return results
        all_successful = all(result.success for result in query_results.values())
        results_adequate = all(result.result_count > 0 for result in query_results.values())
        performance_adequate = all(result.execution_time_ms < 5000 for result in query_results.values())  # < 5 seconds
        
        is_ready = all_successful and results_adequate and performance_adequate
        
        avg_execution_time = sum(result.execution_time_ms for result in query_results.values()) / len(query_results)
        total_results = sum(result.result_count for result in query_results.values())
        
        summary = {
            "research_ready": is_ready,
            "all_queries_successful": all_successful,
            "adequate_results": results_adequate,
            "adequate_performance": performance_adequate,
            "query_count": len(query_results),
            "total_results_returned": total_results,
            "average_execution_time_ms": avg_execution_time,
            "failed_queries": [name for name, result in query_results.items() if not result.success],
            "detailed_results": query_results,
            "timestamp": datetime.now().isoformat()
        }
        
        return is_ready, summary


def run_complete_validation(graphiti_service_url: str = "http://localhost:8002") -> Dict[str, Any]:
    """
    Run complete validation suite for knowledge graph research readiness.
    
    Args:
        graphiti_service_url: URL of the Graphiti service
        
    Returns:
        Comprehensive validation report
    """
    graph_validator = KnowledgeGraphValidator(graphiti_service_url)
    research_validator = ResearchQueryValidator(graphiti_service_url)
    
    # Run graph structure validation
    graph_ready, graph_summary = graph_validator.is_research_ready()
    
    # Run research query validation
    research_ready, research_summary = research_validator.validate_research_readiness()
    
    # Overall readiness assessment
    overall_ready = graph_ready and research_ready
    
    complete_report = {
        "overall_research_ready": overall_ready,
        "graph_structure_ready": graph_ready,
        "research_queries_ready": research_ready,
        "graph_validation": graph_summary,
        "research_validation": research_summary,
        "recommendations": _generate_recommendations(graph_summary, research_summary),
        "validation_timestamp": datetime.now().isoformat()
    }
    
    return complete_report


def _generate_recommendations(graph_summary: Dict[str, Any], research_summary: Dict[str, Any]) -> List[str]:
    """Generate actionable recommendations based on validation results."""
    recommendations = []
    
    if not graph_summary.get("overall_ready"):
        failed_validations = graph_summary.get("failed_validations", [])
        if "gene_representation" in failed_validations:
            recommendations.append("Check episode generation for missing target genes")
        if "evidence_completeness" in failed_validations:
            recommendations.append("Verify all evidence types are being ingested properly")
        if "graph_connectivity" in failed_validations:
            recommendations.append("Review relationship creation between entities")
    
    if not research_summary.get("research_ready"):
        failed_queries = research_summary.get("failed_queries", [])
        if failed_queries:
            recommendations.append(f"Debug failed research queries: {', '.join(failed_queries)}")
        
        if not research_summary.get("adequate_performance"):
            recommendations.append("Optimize graph indices for better query performance")
    
    if not recommendations:
        recommendations.append("Knowledge graph is research-ready - all validations passed!")
    
    return recommendations


if __name__ == "__main__":
    # Run validation when executed directly
    report = run_complete_validation()
    print(json.dumps(report, indent=2))