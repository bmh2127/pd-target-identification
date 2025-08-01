# pd_target_identification/defs/ingestion/pathways/assets.py
from dagster import asset, AssetExecutionContext
import pandas as pd
from ...shared.resources import STRINGResource

@asset(
    deps=["gwas_eqtl_integrated"],
    group_name="data_acquisition",
    compute_kind="api",
    tags={"source": "string", "data_type": "pathways"}
)
def string_protein_interactions(
    context: AssetExecutionContext,
    gwas_eqtl_integrated: pd.DataFrame,
    string_db: STRINGResource
) -> pd.DataFrame:
    """
    Get protein interaction networks for integrated GWAS/eQTL genes
    """
    context.log.info("Fetching protein interactions from STRING database")
    
    if len(gwas_eqtl_integrated) == 0:
        context.log.warning("No integrated genes to process")
        return pd.DataFrame()
    
    # Get gene symbols from integrated data
    gene_symbols = gwas_eqtl_integrated['gene_symbol'].unique().tolist()
    context.log.info(f"Getting interactions for {len(gene_symbols)} genes: {gene_symbols}")
    
    try:
        # Get protein interaction network
        raw_interactions_df = string_db.get_protein_interactions(gene_symbols)
        
        if len(raw_interactions_df) > 0:
            context.log.info(f"Raw interactions DataFrame columns: {list(raw_interactions_df.columns)}")
            
            # Extract gene symbols from the dictionary structures
            raw_interactions_df['gene_a'] = raw_interactions_df['protein_a'].apply(
                lambda x: x.get('preferredName', '') if isinstance(x, dict) else str(x)
            )
            raw_interactions_df['gene_b'] = raw_interactions_df['protein_b'].apply(
                lambda x: x.get('preferredName', '') if isinstance(x, dict) else str(x)
            )
            
            # Add metadata about our query genes
            query_genes = set(gene_symbols)
            raw_interactions_df['query_gene_a'] = raw_interactions_df['gene_a'].isin(query_genes)
            raw_interactions_df['query_gene_b'] = raw_interactions_df['gene_b'].isin(query_genes)
            raw_interactions_df['both_query_genes'] = raw_interactions_df['query_gene_a'] & raw_interactions_df['query_gene_b']
            
            # Create final DataFrame with explicit column selection to ensure consistent schema
            interactions_df = pd.DataFrame({
                'protein_a': raw_interactions_df['protein_a'],
                'protein_b': raw_interactions_df['protein_b'],
                'combined_score': raw_interactions_df['combined_score'],
                'gene_a': raw_interactions_df['gene_a'],
                'gene_b': raw_interactions_df['gene_b'],
                'query_gene_a': raw_interactions_df['query_gene_a'],
                'query_gene_b': raw_interactions_df['query_gene_b'],
                'both_query_genes': raw_interactions_df['both_query_genes']
            })
            
            # Calculate interaction statistics
            total_interactions = len(interactions_df)
            internal_interactions = len(interactions_df[interactions_df['both_query_genes']])
            
            # Get unique proteins using the extracted gene symbols
            unique_proteins = set(interactions_df['gene_a'].tolist() + interactions_df['gene_b'].tolist())
            unique_proteins.discard('')  # Remove any empty strings
            
            context.log.info(f"STRING interaction results:")
            context.log.info(f"  Total interactions: {total_interactions}")
            context.log.info(f"  Internal interactions (gene-gene): {internal_interactions}")
            context.log.info(f"  Unique proteins in network: {len(unique_proteins)}")
            context.log.info(f"  Query genes in network: {len(unique_proteins.intersection(query_genes))}")
            
            # Log high-confidence interactions
            high_conf = interactions_df[interactions_df['combined_score'] >= 0.7]
            context.log.info(f"  High confidence interactions (>0.7): {len(high_conf)}")
            
            context.add_output_metadata({
                "total_interactions": total_interactions,
                "internal_interactions": internal_interactions,
                "unique_proteins": len(unique_proteins),
                "query_genes_in_network": len(unique_proteins.intersection(query_genes)),
                "high_confidence_interactions": len(high_conf),
                "mean_confidence_score": float(interactions_df['combined_score'].mean()),
                "data_source": "STRING Database v12",
                "output_columns": list(interactions_df.columns)
            })
            
        else:
            context.log.warning("No protein interactions found")
            # Return empty DataFrame with consistent schema
            interactions_df = pd.DataFrame(columns=[
                'protein_a', 'protein_b', 'combined_score', 'gene_a', 'gene_b', 
                'query_gene_a', 'query_gene_b', 'both_query_genes'
            ])
            
        return interactions_df
        
    except Exception as e:
        context.log.error(f"Failed to get protein interactions: {e}")
        
        # Return empty DataFrame with expected structure
        return pd.DataFrame(columns=[
            'protein_a', 'protein_b', 'combined_score', 'gene_a', 'gene_b', 
            'query_gene_a', 'query_gene_b', 'both_query_genes'
        ])

@asset(
    deps=["gwas_eqtl_integrated"],
    group_name="data_acquisition", 
    compute_kind="api",
    tags={"source": "string", "data_type": "pathways"}
)
def string_functional_enrichment(
    context: AssetExecutionContext,
    gwas_eqtl_integrated: pd.DataFrame,
    string_db: STRINGResource
) -> pd.DataFrame:
    """
    Get functional enrichment analysis for integrated genes
    """
    context.log.info("Performing functional enrichment analysis")
    
    if len(gwas_eqtl_integrated) == 0:
        context.log.warning("No integrated genes to process")
        return pd.DataFrame()
    
    # Get gene symbols
    gene_symbols = gwas_eqtl_integrated['gene_symbol'].unique().tolist()
    context.log.info(f"Enrichment analysis for {len(gene_symbols)} genes")
    
    try:
        # Get functional enrichment
        enrichment_df = string_db.get_functional_enrichment(gene_symbols)
        
        if len(enrichment_df) > 0:
            # Filter to significant results
            significant = enrichment_df[enrichment_df['fdr'] < 0.05]
            
            # Group by category
            categories = enrichment_df['category'].value_counts()
            
            context.log.info(f"Functional enrichment results:")
            context.log.info(f"  Total terms: {len(enrichment_df)}")
            context.log.info(f"  Significant terms (FDR < 0.05): {len(significant)}")
            context.log.info(f"  Categories: {dict(categories)}")
            
            if len(significant) > 0:
                # Log top significant terms
                top_terms = significant.nsmallest(5, 'fdr')[['category', 'term', 'description', 'fdr']]
                context.log.info(f"Top significant terms:")
                for _, row in top_terms.iterrows():
                    context.log.info(f"    {row['category']}: {row['description']} (FDR: {row['fdr']:.2e})")
            
            context.add_output_metadata({
                "total_terms": len(enrichment_df),
                "significant_terms": len(significant),
                "categories_found": len(categories),
                "top_category": categories.index[0] if len(categories) > 0 else "None",
                "min_fdr": float(enrichment_df['fdr'].min()) if len(enrichment_df) > 0 else 1.0,
                "data_source": "STRING Database functional annotation"
            })
            
        else:
            context.log.warning("No functional enrichment results found")
            
        return enrichment_df
        
    except Exception as e:
        context.log.error(f"Failed to get functional enrichment: {e}")
        
        # Return empty DataFrame with expected structure
        return pd.DataFrame(columns=[
            'category', 'term', 'description', 'p_value', 'fdr', 'proteins_in_term'
        ])

@asset(
    deps=["string_protein_interactions", "string_functional_enrichment"],
    group_name="data_processing",
    compute_kind="python",
    tags={"data_type": "pathways", "processing": "analyze"}
)
def pathway_network_summary(
    context: AssetExecutionContext,
    string_protein_interactions: pd.DataFrame,
    string_functional_enrichment: pd.DataFrame
) -> pd.DataFrame:
    """
    Summarize pathway and network data by gene with fixed pathway assignment
    """
    context.log.info("Summarizing pathway and network data by gene")
    
    gene_summaries = []
    
    if len(string_protein_interactions) > 0:
        # Analyze network properties for each gene
        # Get unique gene symbols from the interactions
        all_gene_symbols = set()
        all_gene_symbols.update(string_protein_interactions['gene_a'].dropna())
        all_gene_symbols.update(string_protein_interactions['gene_b'].dropna())
        all_gene_symbols.discard('')  # Remove empty strings
        
        for gene_symbol in all_gene_symbols:
            if gene_symbol not in [g['gene_symbol'] for g in gene_summaries]:
                
                # Count interactions for this gene
                gene_interactions = string_protein_interactions[
                    (string_protein_interactions['gene_a'] == gene_symbol) |
                    (string_protein_interactions['gene_b'] == gene_symbol)
                ]
                
                # Calculate network metrics
                interaction_count = len(gene_interactions)
                avg_confidence = gene_interactions['combined_score'].mean() if len(gene_interactions) > 0 else 0
                high_conf_interactions = len(gene_interactions[gene_interactions['combined_score'] >= 0.7])
                
                # Get enriched pathways for this gene - FIXED PARSING
                gene_pathways = []
                if len(string_functional_enrichment) > 0:
                    for _, enrich_row in string_functional_enrichment.iterrows():
                        # Parse the complex input_proteins structure
                        input_proteins = enrich_row.get('input_proteins', [])
                        
                        # Extract gene symbols from the complex protein objects
                        pathway_genes = []
                        if input_proteins is not None:
                            try:
                                # Handle both array and list formats
                                if hasattr(input_proteins, '__iter__'):
                                    for protein_obj in input_proteins:
                                        if isinstance(protein_obj, dict):
                                            preferred_name = protein_obj.get('preferredName', '')
                                            if preferred_name:
                                                pathway_genes.append(preferred_name)
                                        elif isinstance(protein_obj, str):
                                            # Fallback for simple string format
                                            pathway_genes.append(protein_obj)
                            except Exception as e:
                                context.log.warning(f"Error parsing input_proteins for {enrich_row['term']}: {e}")
                        
                        # Check if this gene is in this pathway
                        if gene_symbol in pathway_genes:
                            gene_pathways.append({
                                'category': enrich_row['category'],
                                'term': enrich_row['term'],
                                'description': enrich_row['description'],
                                'fdr': enrich_row['fdr']
                            })
                
                # Network centrality (simple degree centrality)
                degree_centrality = interaction_count / max(1, len(string_protein_interactions)) * 100
                
                gene_summaries.append({
                    'gene_symbol': gene_symbol,
                    'interaction_count': interaction_count,
                    'avg_interaction_confidence': round(avg_confidence, 3),
                    'high_confidence_interactions': high_conf_interactions,
                    'degree_centrality': round(degree_centrality, 2),
                    'pathway_count': len(gene_pathways),
                    'significant_pathways': len([p for p in gene_pathways if p['fdr'] < 0.05]),
                    'top_pathway': gene_pathways[0]['description'] if gene_pathways else None,
                    'pathway_evidence_score': len([p for p in gene_pathways if p['fdr'] < 0.05]) * 10,  # Simple scoring
                    'pathways_list': [p['description'] for p in gene_pathways if p['fdr'] < 0.05]  # Store pathway names
                })
    
    if gene_summaries:
        df = pd.DataFrame(gene_summaries)
        
        # Sort by combined network and pathway evidence
        df['combined_pathway_score'] = (
            df['interaction_count'] * 0.3 + 
            df['high_confidence_interactions'] * 0.4 + 
            df['pathway_evidence_score'] * 0.3
        )
        df = df.sort_values('combined_pathway_score', ascending=False).reset_index(drop=True)
        
        context.log.info(f"Pathway network summary:")
        context.log.info(f"  Genes with network data: {len(df)}")
        
        if len(df) > 0:
            avg_interactions = df['interaction_count'].mean()
            genes_with_pathways = len(df[df['pathway_count'] > 0])
            context.log.info(f"  Average interactions per gene: {avg_interactions:.1f}")
            context.log.info(f"  Genes with pathway annotations: {genes_with_pathways}")
            
            # Log pathway details for genes with significant pathways
            genes_with_sig_pathways = df[df['significant_pathways'] > 0]
            if len(genes_with_sig_pathways) > 0:
                context.log.info(f"  Genes with significant pathways:")
                for _, gene_row in genes_with_sig_pathways.iterrows():
                    pathways_str = ", ".join(gene_row['pathways_list'])
                    context.log.info(f"    {gene_row['gene_symbol']}: {pathways_str}")
            
            # Log top genes
            top_genes = df.head(3)[['gene_symbol', 'combined_pathway_score', 'interaction_count', 'pathway_count']]
            context.log.info(f"Top genes by pathway evidence: {top_genes.to_dict('records')}")
        
        context.add_output_metadata({
            "genes_with_network_data": len(df),
            "avg_interactions_per_gene": float(df['interaction_count'].mean()) if len(df) > 0 else 0,
            "genes_with_pathways": len(df[df['pathway_count'] > 0]) if len(df) > 0 else 0,
            "genes_with_significant_pathways": len(df[df['significant_pathways'] > 0]) if len(df) > 0 else 0,
            "top_pathway_gene": df.iloc[0]['gene_symbol'] if len(df) > 0 else "None"
        })
        
    else:
        df = pd.DataFrame()
        context.log.warning("No pathway/network data to summarize")
        
    return df

@asset(
    deps=["gwas_eqtl_integrated", "pathway_network_summary", "literature_gene_summary"],
    group_name="data_integration",
    compute_kind="python",
    tags={"data_type": "integrated"}
)
def multi_evidence_integrated(
    context: AssetExecutionContext,
    gwas_eqtl_integrated: pd.DataFrame,
    pathway_network_summary: pd.DataFrame,
    literature_gene_summary: pd.DataFrame
) -> pd.DataFrame:
    """
    Integrate GWAS, eQTL, pathway, and literature evidence for comprehensive gene scoring
    """
    context.log.info("Creating multi-evidence integration with pathway and literature data")
    
    if len(gwas_eqtl_integrated) == 0:
        context.log.warning("No GWAS/eQTL data to integrate")
        return pd.DataFrame()
    
    # Start with GWAS/eQTL data
    integrated_data = gwas_eqtl_integrated.copy()
    
    # Add pathway/network evidence
    if len(pathway_network_summary) > 0:
        # Merge pathway data
        pathway_dict = pathway_network_summary.set_index('gene_symbol').to_dict('index')
        
        integrated_data['interaction_count'] = integrated_data['gene_symbol'].map(
            lambda g: pathway_dict.get(g, {}).get('interaction_count', 0)
        )
        integrated_data['pathway_count'] = integrated_data['gene_symbol'].map(
            lambda g: pathway_dict.get(g, {}).get('pathway_count', 0)
        )
        integrated_data['degree_centrality'] = integrated_data['gene_symbol'].map(
            lambda g: pathway_dict.get(g, {}).get('degree_centrality', 0)
        )
        integrated_data['pathway_evidence_score'] = integrated_data['gene_symbol'].map(
            lambda g: pathway_dict.get(g, {}).get('pathway_evidence_score', 0)
        )
    else:
        # No pathway data available
        integrated_data['interaction_count'] = 0
        integrated_data['pathway_count'] = 0
        integrated_data['degree_centrality'] = 0
        integrated_data['pathway_evidence_score'] = 0
    
    # Add literature evidence
    if len(literature_gene_summary) > 0:
        # Merge literature data
        literature_dict = literature_gene_summary.set_index('gene_symbol').to_dict('index')
        
        integrated_data['literature_papers_count'] = integrated_data['gene_symbol'].map(
            lambda g: literature_dict.get(g, {}).get('total_papers', 0)
        )
        integrated_data['therapeutic_target_papers'] = integrated_data['gene_symbol'].map(
            lambda g: literature_dict.get(g, {}).get('therapeutic_target_papers', 0)
        )
        integrated_data['clinical_papers'] = integrated_data['gene_symbol'].map(
            lambda g: literature_dict.get(g, {}).get('clinical_papers', 0)
        )
        integrated_data['literature_evidence_score'] = integrated_data['gene_symbol'].map(
            lambda g: literature_dict.get(g, {}).get('literature_evidence_score', 0)
        )
        integrated_data['has_strong_literature_evidence'] = integrated_data['gene_symbol'].map(
            lambda g: literature_dict.get(g, {}).get('has_strong_literature_evidence', False)
        )
        
        # Update evidence types count to include literature
        integrated_data['evidence_types'] = (
            2 +  # GWAS + eQTL (base)
            (integrated_data['pathway_count'] > 0).astype(int) +  # Pathway evidence
            (integrated_data['literature_papers_count'] > 0).astype(int)  # Literature evidence
        )
        
        # Enhanced integrated score including literature evidence (5 evidence types max)
        integrated_data['enhanced_integrated_score'] = (
            integrated_data['integrated_score'] * 0.4 +  # GWAS + eQTL (reduced weight)
            integrated_data['interaction_count'] * 1.5 +  # Protein interactions
            integrated_data['pathway_evidence_score'] * 1.0 +  # Pathway evidence
            integrated_data['literature_evidence_score'] * 0.5 +  # Literature evidence
            integrated_data['therapeutic_target_papers'] * 3.0  # Bonus for therapeutic target papers
        )
        
    else:
        # No literature data available
        integrated_data['literature_papers_count'] = 0
        integrated_data['therapeutic_target_papers'] = 0
        integrated_data['clinical_papers'] = 0
        integrated_data['literature_evidence_score'] = 0
        integrated_data['has_strong_literature_evidence'] = False
        
        # Evidence types without literature
        integrated_data['evidence_types'] = 2 + (integrated_data['pathway_count'] > 0).astype(int)
        
        # Enhanced score without literature
        integrated_data['enhanced_integrated_score'] = (
            integrated_data['integrated_score'] * 0.5 +  # GWAS + eQTL
            integrated_data['interaction_count'] * 2.0 +  # Protein interactions
            integrated_data['pathway_evidence_score'] * 1.0  # Pathway evidence
        )
    
    # Sort by enhanced score
    integrated_data = integrated_data.sort_values('enhanced_integrated_score', ascending=False).reset_index(drop=True)
    
    # Calculate summary statistics
    genes_with_pathways = len(integrated_data[integrated_data['pathway_count'] > 0])
    genes_with_interactions = len(integrated_data[integrated_data['interaction_count'] > 0])
    genes_with_literature = len(integrated_data[integrated_data['literature_papers_count'] > 0])
    genes_with_therapeutic_literature = len(integrated_data[integrated_data['therapeutic_target_papers'] > 0])
    max_evidence_types = integrated_data['evidence_types'].max()
    
    context.log.info(f"Multi-evidence integration results:")
    context.log.info(f"  Total genes: {len(integrated_data)}")
    context.log.info(f"  Genes with pathway data: {genes_with_pathways}")
    context.log.info(f"  Genes with protein interactions: {genes_with_interactions}")
    context.log.info(f"  Genes with literature: {genes_with_literature}")
    context.log.info(f"  Genes with therapeutic target literature: {genes_with_therapeutic_literature}")
    context.log.info(f"  Maximum evidence types per gene: {max_evidence_types}")
    
    if len(integrated_data) >= 5:
        top_genes = integrated_data.head(5)[['gene_symbol', 'enhanced_integrated_score', 'evidence_types', 'therapeutic_target_papers']]
        context.log.info(f"Top multi-evidence genes:")
        for _, gene_row in top_genes.iterrows():
            context.log.info(f"  {gene_row['gene_symbol']}: Score {gene_row['enhanced_integrated_score']:.1f}, "
                            f"Evidence types: {gene_row['evidence_types']}, "
                            f"Therapeutic papers: {gene_row['therapeutic_target_papers']}")
    
    context.add_output_metadata({
        "total_genes": len(integrated_data),
        "genes_with_pathway_data": genes_with_pathways,
        "genes_with_protein_interactions": genes_with_interactions,
        "genes_with_literature": genes_with_literature,
        "genes_with_therapeutic_literature": genes_with_therapeutic_literature,
        "max_evidence_types": int(max_evidence_types) if len(integrated_data) > 0 else 0,
        "top_multi_evidence_gene": integrated_data.iloc[0]['gene_symbol'] if len(integrated_data) > 0 else "None",
        "max_enhanced_score": float(integrated_data['enhanced_integrated_score'].max()) if len(integrated_data) > 0 else 0
    })
    
    return integrated_data