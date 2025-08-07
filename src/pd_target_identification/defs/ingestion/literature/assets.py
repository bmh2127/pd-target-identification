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
def pubmed_literature_search(
    context: AssetExecutionContext,
    gwas_eqtl_integrated: pd.DataFrame,
    pubmed: PubMedResource
) -> pd.DataFrame:
    """
    Search PubMed for literature about our integrated target genes
    """
    context.log.info("Searching PubMed for literature on integrated target genes")
    
    if len(gwas_eqtl_integrated) == 0:
        context.log.warning("No integrated genes to search literature for")
        return pd.DataFrame()
    
    # Get gene symbols from GWAS/eQTL integrated data (top candidates first)
    # Sort by integrated score to prioritize top candidates
    top_genes = gwas_eqtl_integrated.nlargest(40, 'integrated_score')  # Focus on top candidates
    gene_symbols = top_genes['gene_symbol'].tolist()
    
    context.log.info(f"Searching literature for {len(gene_symbols)} genes: {gene_symbols}")
    
    try:
        # Batch search for all genes
        literature_by_gene = pubmed.batch_gene_literature_search(gene_symbols, max_papers_per_gene=25)
        
        # Convert to DataFrame format
        literature_records = []
        total_papers = 0
        
        for gene_symbol, papers in literature_by_gene.items():
            for paper in papers:
                literature_records.append({
                    'gene_symbol': gene_symbol,
                    'pmid': paper.get('pmid', ''),
                    'title': paper.get('title', ''),
                    'abstract': paper.get('abstract', ''),
                    'publication_year': paper.get('publication_year'),
                    'journal': paper.get('journal', ''),
                    'first_author': paper.get('first_author', ''),
                    'search_query': paper.get('search_query', '')
                })
            
            total_papers += len(papers)
            context.log.info(f"  {gene_symbol}: {len(papers)} papers")
        
        df = pd.DataFrame(literature_records)
        
        if len(df) > 0:
            # Calculate summary statistics
            papers_per_gene = df.groupby('gene_symbol').size()
            genes_with_literature = len(papers_per_gene[papers_per_gene > 0])
            
            # Recent papers (last 3 years)
            current_year = datetime.now().year
            recent_papers = len(df[df['publication_year'] >= current_year - 3])
            
            context.log.info(f"Literature search results:")
            context.log.info(f"  Total papers retrieved: {len(df)}")
            context.log.info(f"  Genes with literature: {genes_with_literature}/{len(gene_symbols)}")
            context.log.info(f"  Recent papers (last 3 years): {recent_papers}")
            context.log.info(f"  Average papers per gene: {len(df)/len(gene_symbols):.1f}")
            
            # Log genes with most literature
            top_lit_genes = papers_per_gene.nlargest(5)
            context.log.info(f"  Genes with most literature: {top_lit_genes.to_dict()}")
            
            context.add_output_metadata({
                "total_papers": len(df),
                "genes_searched": len(gene_symbols),
                "genes_with_literature": genes_with_literature,
                "recent_papers": recent_papers,
                "avg_papers_per_gene": float(len(df)/len(gene_symbols)),
                "data_source": "PubMed E-utilities API",
                "search_period": "Last 5 years"
            })
        else:
            context.log.warning("No literature found for any genes")
            
        return df
        
    except Exception as e:
        context.log.error(f"Literature search failed: {e}")
        
        # Return empty DataFrame with expected structure
        return pd.DataFrame(columns=[
            'gene_symbol', 'pmid', 'title', 'abstract', 'publication_year',
            'journal', 'first_author', 'search_query'
        ])

@asset(
    deps=["pubmed_literature_search"],
    group_name="data_processing",
    compute_kind="python",
    tags={"data_type": "literature", "processing": "extract"}
)
def literature_evidence_extraction(
    context: AssetExecutionContext,
    pubmed_literature_search: pd.DataFrame
) -> pd.DataFrame:
    """
    Extract structured evidence from literature abstracts
    """
    context.log.info("Extracting evidence from literature abstracts")
    
    if len(pubmed_literature_search) == 0:
        context.log.warning("No literature data to process")
        return pd.DataFrame()
    
    # Define evidence type keywords
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
    
    evidence_records = []
    
    for _, paper in pubmed_literature_search.iterrows():
        gene_symbol = paper['gene_symbol']
        title = str(paper.get('title', '')).lower()
        abstract = str(paper.get('abstract', '')).lower()
        combined_text = f"{title} {abstract}"
        
        # Extract evidence for each type
        evidence_scores = {}
        evidence_mentions = {}
        
        for evidence_type, keywords in evidence_keywords.items():
            mentions = []
            score = 0
            
            for keyword in keywords:
                if keyword in combined_text:
                    mentions.append(keyword)
                    # Weight by keyword importance (therapeutic target highest)
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
        
        # Calculate recency bonus (more recent papers get higher scores)
        pub_year = paper.get('publication_year')
        recency_bonus = 0
        if pub_year:
            current_year = datetime.now().year
            years_ago = current_year - pub_year
            if years_ago <= 1:
                recency_bonus = 2.0
            elif years_ago <= 3:
                recency_bonus = 1.5
            elif years_ago <= 5:
                recency_bonus = 1.0
        
        # Overall literature relevance score
        total_evidence_score = sum(evidence_scores.values()) + recency_bonus
        
        evidence_records.append({
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
            'clinical_keywords': ', '.join(evidence_mentions['clinical'][:3]),
            'evidence_summary': f"Therapeutic: {evidence_scores['therapeutic_target']}, Clinical: {evidence_scores['clinical']}, Biomarker: {evidence_scores['biomarker']}"
        })
    
    df = pd.DataFrame(evidence_records)
    
    if len(df) > 0:
        # Summary statistics
        high_evidence_papers = len(df[df['total_evidence_score'] >= 5])
        therapeutic_papers = len(df[df['therapeutic_target_score'] > 0])
        clinical_papers = len(df[df['clinical_score'] > 0])
        recent_papers = len(df[df['recency_bonus'] > 0])
        
        context.log.info(f"Literature evidence extraction results:")
        context.log.info(f"  Papers processed: {len(df)}")
        context.log.info(f"  High evidence papers (score ≥5): {high_evidence_papers}")
        context.log.info(f"  Papers with therapeutic target evidence: {therapeutic_papers}")
        context.log.info(f"  Papers with clinical evidence: {clinical_papers}")
        context.log.info(f"  Recent papers with recency bonus: {recent_papers}")
        
        # Log top papers by evidence score
        top_papers = df.nlargest(5, 'total_evidence_score')[['gene_symbol', 'total_evidence_score', 'evidence_summary']]
        context.log.info(f"  Top evidence papers: {top_papers.to_dict('records')}")
        
        context.add_output_metadata({
            "papers_processed": len(df),
            "high_evidence_papers": high_evidence_papers,
            "therapeutic_target_papers": therapeutic_papers,
            "clinical_papers": clinical_papers,
            "recent_papers": recent_papers,
            "mean_evidence_score": float(df['total_evidence_score'].mean()),
            "max_evidence_score": float(df['total_evidence_score'].max())
        })
    
    return df

@asset(
    deps=["literature_evidence_extraction"],
    group_name="data_processing",
    compute_kind="python",
    tags={"data_type": "literature", "processing": "summarize"}
)
def literature_gene_summary(
    context: AssetExecutionContext,
    literature_evidence_extraction: pd.DataFrame
) -> pd.DataFrame:
    """
    Summarize literature evidence by gene
    """
    context.log.info("Summarizing literature evidence by gene")
    
    if len(literature_evidence_extraction) == 0:
        context.log.warning("No literature evidence to summarize")
        return pd.DataFrame()
    
    # Group by gene and calculate summary statistics
    gene_summaries = []
    
    for gene_symbol, gene_papers in literature_evidence_extraction.groupby('gene_symbol'):
        summary = {
            'gene_symbol': gene_symbol,
            'total_papers': len(gene_papers),
            'avg_evidence_score': gene_papers['total_evidence_score'].mean(),
            'max_evidence_score': gene_papers['total_evidence_score'].max(),
            'therapeutic_target_papers': len(gene_papers[gene_papers['therapeutic_target_score'] > 0]),
            'clinical_papers': len(gene_papers[gene_papers['clinical_score'] > 0]),
            'biomarker_papers': len(gene_papers[gene_papers['biomarker_score'] > 0]),
            'recent_papers': len(gene_papers[gene_papers['recency_bonus'] > 0]),
            'literature_evidence_score': (
                gene_papers['total_evidence_score'].sum() * 0.3 +  # Total evidence
                len(gene_papers[gene_papers['therapeutic_target_score'] > 0]) * 5.0 +  # Therapeutic papers
                len(gene_papers[gene_papers['clinical_score'] > 0]) * 3.0 +  # Clinical papers
                len(gene_papers[gene_papers['recency_bonus'] > 0]) * 2.0  # Recent papers
            ),
            'top_evidence_paper_pmid': gene_papers.loc[gene_papers['total_evidence_score'].idxmax(), 'pmid'],
            'top_evidence_score': gene_papers['total_evidence_score'].max(),
            'most_recent_paper_year': gene_papers['publication_year'].max(),
            'journals_count': gene_papers['journal'].nunique(),
            'therapeutic_keywords_summary': '; '.join(gene_papers[gene_papers['therapeutic_target_score'] > 0]['therapeutic_keywords'].unique()[:5]),
            'has_strong_literature_evidence': len(gene_papers[gene_papers['total_evidence_score'] >= 5]) > 0
        }
        
        gene_summaries.append(summary)
    
    df = pd.DataFrame(gene_summaries)
    
    if len(df) > 0:
        # Sort by literature evidence score
        df = df.sort_values('literature_evidence_score', ascending=False).reset_index(drop=True)
        
        # Calculate summary statistics
        genes_with_therapeutic = len(df[df['therapeutic_target_papers'] > 0])
        genes_with_clinical = len(df[df['clinical_papers'] > 0])
        genes_with_strong_evidence = len(df[df['has_strong_literature_evidence']])
        
        context.log.info(f"Literature gene summary results:")
        context.log.info(f"  Genes with literature: {len(df)}")
        context.log.info(f"  Genes with therapeutic target papers: {genes_with_therapeutic}")
        context.log.info(f"  Genes with clinical papers: {genes_with_clinical}")
        context.log.info(f"  Genes with strong literature evidence: {genes_with_strong_evidence}")
        
        if len(df) > 0:
            avg_papers = df['total_papers'].mean()
            context.log.info(f"  Average papers per gene: {avg_papers:.1f}")
            
            # Log top genes by literature evidence
            top_genes = df.head(5)[['gene_symbol', 'literature_evidence_score', 'therapeutic_target_papers', 'clinical_papers']]
            context.log.info(f"  Top genes by literature evidence:")
            for _, gene_row in top_genes.iterrows():
                context.log.info(f"    {gene_row['gene_symbol']}: Score {gene_row['literature_evidence_score']:.1f} "
                                f"(Therapeutic: {gene_row['therapeutic_target_papers']}, Clinical: {gene_row['clinical_papers']})")
        
        context.add_output_metadata({
            "genes_with_literature": len(df),
            "genes_with_therapeutic_papers": genes_with_therapeutic,
            "genes_with_clinical_papers": genes_with_clinical,
            "genes_with_strong_evidence": genes_with_strong_evidence,
            "avg_papers_per_gene": float(df['total_papers'].mean()),
            "avg_evidence_score": float(df['literature_evidence_score'].mean()),
            "top_literature_gene": df.iloc[0]['gene_symbol'] if len(df) > 0 else "None"
        })
    
    return df