// 🧬 Target Identification Research Queries
// Run these in Neo4j Browser (http://localhost:7475) after pipeline completion

// ============================================================================
// 🏆 TOP-LEVEL RESEARCH QUESTIONS
// ============================================================================

// 1️⃣ "Why is SNCA ranked #1?" - Evidence synthesis for top gene
MATCH (g:Gene {name: 'SNCA'})-[r]->(e:Evidence)
RETURN g.name as Gene,
       type(r) as RelationType,
       e.type as EvidenceType,
       e.strength as Strength,
       e.confidence as Confidence,
       e.source as Source
ORDER BY e.strength DESC, e.confidence DESC;

// 2️⃣ "Which genes have both strong GWAS and pathway evidence?"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(gwas:Evidence {type: 'GWAS'})
MATCH (g)-[:HAS_EVIDENCE]->(pathway:Evidence {type: 'Pathway'})
WHERE gwas.strength > 0.7 AND pathway.strength > 0.7
RETURN g.name as Gene,
       gwas.strength as GWAS_Strength,
       pathway.strength as Pathway_Strength,
       (gwas.strength + pathway.strength) / 2 as Combined_Score
ORDER BY Combined_Score DESC;

// 3️⃣ "Show me the complete evidence profile for each gene"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(e:Evidence)
WITH g, 
     collect({type: e.type, strength: e.strength, confidence: e.confidence}) as evidence_list,
     count(e) as evidence_count,
     avg(e.strength) as avg_strength,
     max(e.strength) as max_strength
RETURN g.name as Gene,
       evidence_count as Evidence_Types,
       round(avg_strength * 100) / 100 as Avg_Strength,
       round(max_strength * 100) / 100 as Max_Strength,
       evidence_list as Evidence_Details
ORDER BY avg_strength DESC, evidence_count DESC;

// ============================================================================
// 🔍 EVIDENCE-SPECIFIC ANALYSES
// ============================================================================

// 4️⃣ "Strongest GWAS associations"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(e:Evidence {type: 'GWAS'})
WHERE e.strength IS NOT NULL
RETURN g.name as Gene,
       e.strength as GWAS_Strength,
       e.confidence as Confidence,
       e.p_value as P_Value,
       e.odds_ratio as Odds_Ratio
ORDER BY e.strength DESC
LIMIT 10;

// 5️⃣ "Most cited literature evidence"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(e:Evidence {type: 'Literature'})
WHERE e.citation_count IS NOT NULL
RETURN g.name as Gene,
       e.strength as Literature_Strength,
       e.citation_count as Citations,
       e.publication_year as Year,
       e.title as Title
ORDER BY e.citation_count DESC
LIMIT 10;

// 6️⃣ "Pathway enrichment analysis"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(e:Evidence {type: 'Pathway'})
WHERE e.pathway_name IS NOT NULL
RETURN e.pathway_name as Pathway,
       count(g) as Gene_Count,
       collect(g.name) as Genes,
       avg(e.strength) as Avg_Strength
ORDER BY Gene_Count DESC, Avg_Strength DESC;

// ============================================================================
// 🧪 PROTEIN-PROTEIN INTERACTION ANALYSIS
// ============================================================================

// 7️⃣ "Protein interaction networks"
MATCH (g1:Gene)-[:HAS_EVIDENCE]->(ppi:Evidence {type: 'PPI'})<-[:HAS_EVIDENCE]-(g2:Gene)
WHERE g1.name <> g2.name
RETURN g1.name as Gene1,
       g2.name as Gene2,
       ppi.confidence as Interaction_Confidence,
       ppi.interaction_type as Interaction_Type
ORDER BY ppi.confidence DESC;

// 8️⃣ "Hub genes in PPI network"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(ppi:Evidence {type: 'PPI'})
WITH g, count(ppi) as interaction_count, avg(ppi.confidence) as avg_confidence
WHERE interaction_count > 3
RETURN g.name as Gene,
       interaction_count as PPI_Count,
       round(avg_confidence * 100) / 100 as Avg_PPI_Confidence
ORDER BY interaction_count DESC, avg_confidence DESC;

// ============================================================================
// 📊 EXPRESSION & eQTL ANALYSIS
// ============================================================================

// 9️⃣ "Brain-specific expression patterns"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(expr:Evidence {type: 'Expression'})
WHERE expr.tissue = 'Brain' OR expr.tissue CONTAINS 'brain'
RETURN g.name as Gene,
       expr.tissue as Brain_Region,
       expr.expression_level as Expression_Level,
       expr.strength as Evidence_Strength
ORDER BY expr.expression_level DESC;

// 🔟 "eQTL evidence for gene regulation"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(eqtl:Evidence {type: 'eQTL'})
WHERE eqtl.tissue IS NOT NULL
RETURN g.name as Gene,
       eqtl.tissue as Tissue,
       eqtl.p_value as eQTL_P_Value,
       eqtl.beta as Effect_Size,
       eqtl.strength as Evidence_Strength
ORDER BY eqtl.strength DESC;

// ============================================================================
// 🎯 INTEGRATED TARGET PRIORITIZATION
// ============================================================================

// 1️⃣1️⃣ "Multi-evidence target prioritization"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(e:Evidence)
WITH g,
     count(DISTINCT e.type) as evidence_types,
     avg(e.strength) as avg_strength,
     collect(DISTINCT e.type) as evidence_list,
     sum(CASE WHEN e.type = 'GWAS' AND e.strength > 0.8 THEN 1 ELSE 0 END) as strong_gwas,
     sum(CASE WHEN e.type = 'Literature' AND e.citation_count > 100 THEN 1 ELSE 0 END) as high_citation
WHERE evidence_types >= 4  // At least 4 evidence types
RETURN g.name as Target_Gene,
       evidence_types as Evidence_Count,
       round(avg_strength * 100) / 100 as Avg_Evidence_Strength,
       evidence_list as Evidence_Types,
       (strong_gwas > 0) as Has_Strong_GWAS,
       (high_citation > 0) as Has_High_Citation_Literature,
       round((evidence_types * 0.3 + avg_strength * 0.7) * 100) / 100 as Priority_Score
ORDER BY Priority_Score DESC;

// 1️⃣2️⃣ "Evidence convergence analysis"
MATCH (g:Gene)-[:HAS_EVIDENCE]->(e:Evidence)
WITH g,
     count(e) as total_evidence,
     sum(CASE WHEN e.strength > 0.7 THEN 1 ELSE 0 END) as strong_evidence_count,
     avg(e.strength) as avg_strength
WHERE total_evidence >= 3
WITH g, total_evidence, strong_evidence_count, avg_strength,
     (strong_evidence_count * 1.0 / total_evidence) as convergence_ratio
RETURN g.name as Gene,
       total_evidence as Total_Evidence,
       strong_evidence_count as Strong_Evidence,
       round(convergence_ratio * 100) as Convergence_Percentage,
       round(avg_strength * 100) / 100 as Avg_Strength
ORDER BY convergence_ratio DESC, avg_strength DESC;

// ============================================================================
// 📈 QUALITY ASSURANCE QUERIES
// ============================================================================

// 1️⃣3️⃣ "Data completeness check"
MATCH (g:Gene)
OPTIONAL MATCH (g)-[:HAS_EVIDENCE]->(e:Evidence)
WITH g, count(e) as evidence_count, collect(DISTINCT e.type) as evidence_types
RETURN g.name as Gene,
       evidence_count as Total_Evidence,
       size(evidence_types) as Unique_Evidence_Types,
       evidence_types as Evidence_Types_Present,
       (size(evidence_types) >= 6) as Complete_Profile
ORDER BY evidence_count DESC;

// 1️⃣4️⃣ "Database statistics"
MATCH (n)
RETURN labels(n)[0] as NodeType, count(n) as Count
ORDER BY Count DESC
UNION
MATCH ()-[r]->()
RETURN type(r) as RelationshipType, count(r) as Count
ORDER BY Count DESC;

// ============================================================================
// 🚀 READY-TO-USE RESEARCH COMMANDS
// ============================================================================

// Copy and paste any of these queries into Neo4j Browser at:
// http://localhost:7475
// 
// Login credentials:
// Username: neo4j
// Password: password
//
// Each query answers a specific research question and can be modified
// to explore different aspects of your target identification results.