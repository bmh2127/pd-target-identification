# Create this file: pd_target_identification/defs/ingestion/literature/assets.py

from dagster import asset, AssetExecutionContext
import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any
from datetime import datetime
from ...shared.resources import PubMedResource

@asset(
    deps=["gwas_eqtl_integrated"],
    group_name="data_acquisition",
    compute_kind="api",
    tags={"source": "pubmed", "data_type": "literature"}
)
def literature_analysis(
    context: AssetExecutionContext,
    gwas_eqtl_integrated: pd.DataFrame,
    pubmed: PubMedResource
) -> pd.DataFrame:
    """
    Consolidated literature analysis: search PubMed, extract evidence, and summarize by gene
    """
    context.log.info("Starting consolidated literature analysis")
    
    if len(gwas_eqtl_integrated) == 0:
        context.log.warning("No integrated genes to search literature for")
        return pd.DataFrame()
    
    # Get gene symbols from GWAS/eQTL integrated data (top candidates first)
    top_genes = gwas_eqtl_integrated.nlargest(40, 'integrated_score')
    gene_symbols = top_genes['gene_symbol'].tolist()
    
    context.log.info(f"Searching literature for {len(gene_symbols)} genes: {gene_symbols}")
    
    # Define evidence type keywords for scoring
    evidence_keywords = {
        'therapeutic_target': [
            'drug target', 'therapeutic target', 'treatment', 'therapy', 'medication',
            'inhibitor', 'agonist', 'antagonist', 'modulator', 'intervention'
        ],
        'biomarker': [
            'biomarker', 'diagnostic', 'prognostic', 'marker', 'indicator',
            'detection', 'screening', 'monitor', 'predict'
        ],
        'pathogenesis': [
            'pathogenesis', 'disease mechanism', 'neurodegeneration', 'pathology',
            'dysfunction', 'impairment', 'damage', 'loss', 'deficiency'
        ],
        'clinical': [
            'clinical trial', 'patient', 'clinical study', 'human study',
            'clinical data', 'case study', 'cohort', 'population'
        ],
        'functional': [
            'function', 'mechanism', 'pathway', 'signaling', 'regulation',
            'expression', 'activity', 'interaction', 'molecular'
        ]
    }
    
    try:
        # Step 1: Search PubMed for all genes
        literature_by_gene = pubmed.batch_gene_literature_search(gene_symbols, max_papers_per_gene=25)
        
        # Step 2: Process papers and extract evidence (combine search + scoring)
        all_papers_processed = []
        gene_summaries = []
        current_year = datetime.now().year
        
        for gene_symbol, papers in literature_by_gene.items():
            if not papers:
                continue
                
            gene_papers_with_scores = []
            
            for paper in papers:
                title = str(paper.get('title', '')).lower()
                abstract = str(paper.get('abstract', '')).lower()
                combined_text = f"{title} {abstract}"
                pub_year = paper.get('publication_year')
                
                # Extract evidence scores for each type
                evidence_scores = {}
                evidence_mentions = {}
                
                for evidence_type, keywords in evidence_keywords.items():
                    mentions = []
                    score = 0
                    
                    for keyword in keywords:
                        if keyword in combined_text:
                            mentions.append(keyword)
                            # Weight by keyword importance
                            if evidence_type == 'therapeutic_target':
                                score += 3
                            elif evidence_type == 'clinical':
                                score += 2.5
                            elif evidence_type == 'biomarker':
                                score += 2
                            else:
                                score += 1
                    
                    evidence_scores[evidence_type] = score
                    evidence_mentions[evidence_type] = mentions
                
                # Calculate recency bonus
                recency_bonus = 0
                if pub_year:
                    years_ago = current_year - pub_year
                    if years_ago <= 1:
                        recency_bonus = 2.0
                    elif years_ago <= 3:
                        recency_bonus = 1.5
                    elif years_ago <= 5:
                        recency_bonus = 1.0
                
                # Overall literature relevance score
                total_evidence_score = sum(evidence_scores.values()) + recency_bonus
                
                paper_record = {
                    'gene_symbol': gene_symbol,
                    'pmid': paper['pmid'],
                    'title': paper['title'],
                    'publication_year': pub_year,
                    'journal': paper.get('journal', ''),
                    'therapeutic_target_score': evidence_scores['therapeutic_target'],
                    'biomarker_score': evidence_scores['biomarker'],
                    'pathogenesis_score': evidence_scores['pathogenesis'],
                    'clinical_score': evidence_scores['clinical'],
                    'functional_score': evidence_scores['functional'],
                    'recency_bonus': recency_bonus,
                    'total_evidence_score': total_evidence_score,
                    'therapeutic_keywords': ', '.join(evidence_mentions['therapeutic_target'][:3]),
                    'clinical_keywords': ', '.join(evidence_mentions['clinical'][:3])
                }
                
                all_papers_processed.append(paper_record)
                gene_papers_with_scores.append(paper_record)
            
            # Step 3: Generate gene-level summary
            if gene_papers_with_scores:
                gene_df = pd.DataFrame(gene_papers_with_scores)
                
                summary = {
                    'gene_symbol': gene_symbol,
                    'total_papers': len(gene_papers_with_scores),
                    'avg_evidence_score': gene_df['total_evidence_score'].mean(),
                    'max_evidence_score': gene_df['total_evidence_score'].max(),
                    'therapeutic_target_papers': len(gene_df[gene_df['therapeutic_target_score'] > 0]),
                    'clinical_papers': len(gene_df[gene_df['clinical_score'] > 0]),
                    'biomarker_papers': len(gene_df[gene_df['biomarker_score'] > 0]),
                    'recent_papers': len(gene_df[gene_df['recency_bonus'] > 0]),
                    'literature_evidence_score': (
                        gene_df['total_evidence_score'].sum() * 0.3 +
                        len(gene_df[gene_df['therapeutic_target_score'] > 0]) * 5.0 +
                        len(gene_df[gene_df['clinical_score'] > 0]) * 3.0 +
                        len(gene_df[gene_df['recency_bonus'] > 0]) * 2.0
                    ),
                    'top_evidence_paper_pmid': gene_df.loc[gene_df['total_evidence_score'].idxmax(), 'pmid'],
                    'top_evidence_score': gene_df['total_evidence_score'].max(),
                    'most_recent_paper_year': gene_df['publication_year'].max(),
                    'journals_count': gene_df['journal'].nunique(),
                    'therapeutic_keywords_summary': '; '.join(gene_df[gene_df['therapeutic_target_score'] > 0]['therapeutic_keywords'].unique()[:5]),
                    'has_strong_literature_evidence': len(gene_df[gene_df['total_evidence_score'] >= 5]) > 0
                }
                
                gene_summaries.append(summary)
        
        # Create final DataFrame with gene summaries
        df = pd.DataFrame(gene_summaries)
        
        if len(df) > 0:
            # Sort by literature evidence score
            df = df.sort_values('literature_evidence_score', ascending=False).reset_index(drop=True)
            
            # Log comprehensive results
            total_papers = len(all_papers_processed)
            genes_with_literature = len(df)
            genes_with_therapeutic = len(df[df['therapeutic_target_papers'] > 0])
            genes_with_clinical = len(df[df['clinical_papers'] > 0])
            genes_with_strong_evidence = len(df[df['has_strong_literature_evidence']])
            recent_papers = sum(gene['recent_papers'] for gene in gene_summaries)
            
            context.log.info(f"Literature analysis results:")
            context.log.info(f"  Total papers processed: {total_papers}")
            context.log.info(f"  Genes with literature: {genes_with_literature}/{len(gene_symbols)}")
            context.log.info(f"  Genes with therapeutic target papers: {genes_with_therapeutic}")
            context.log.info(f"  Genes with clinical papers: {genes_with_clinical}")
            context.log.info(f"  Genes with strong literature evidence: {genes_with_strong_evidence}")
            context.log.info(f"  Recent papers (last 3 years): {recent_papers}")
            
            if len(df) >= 5:
                context.log.info("  Top genes by literature evidence:")
                for _, gene_row in df.head(5).iterrows():
                    context.log.info(f"    {gene_row['gene_symbol']}: Score {gene_row['literature_evidence_score']:.1f} "
                                    f"(Papers: {gene_row['total_papers']}, Therapeutic: {gene_row['therapeutic_target_papers']}, "
                                    f"Clinical: {gene_row['clinical_papers']})")
            
            context.add_output_metadata({
                "total_papers_processed": total_papers,
                "genes_with_literature": genes_with_literature,
                "genes_searched": len(gene_symbols),
                "genes_with_therapeutic_papers": genes_with_therapeutic,
                "genes_with_clinical_papers": genes_with_clinical,
                "genes_with_strong_evidence": genes_with_strong_evidence,
                "recent_papers": recent_papers,
                "avg_papers_per_gene": float(df['total_papers'].mean()),
                "avg_evidence_score": float(df['literature_evidence_score'].mean()),
                "top_literature_gene": df.iloc[0]['gene_symbol'] if len(df) > 0 else "None",
                "data_source": "PubMed E-utilities API",
                "processing_approach": "Consolidated search + scoring + summarization"
            })
        else:
            context.log.warning("No literature found for any genes")
        
        return df
        
    except Exception as e:
        context.log.error(f"Literature analysis failed: {e}")
        
        # Return empty DataFrame with expected structure
        return pd.DataFrame(columns=[
            'gene_symbol', 'total_papers', 'avg_evidence_score', 'max_evidence_score',
            'therapeutic_target_papers', 'clinical_papers', 'biomarker_papers',
            'recent_papers', 'literature_evidence_score', 'top_evidence_paper_pmid',
            'top_evidence_score', 'most_recent_paper_year', 'journals_count',
            'therapeutic_keywords_summary', 'has_strong_literature_evidence'
        ])