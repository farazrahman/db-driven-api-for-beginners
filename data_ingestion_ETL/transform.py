import pandas as pd


def transform_analysis_data(filename: str):
    analysis_df = pd.read_csv(filename)
    analysis_df.rename(columns={'gold_id': 'analysis_gold_id',
                                'project_gold_ids': 'project_gold_id'},
                       inplace=True)
    analysis_df = analysis_df.loc[:, ~analysis_df.columns.str.contains('^Unnamed')]

    return analysis_df


def transform_projects_data(filename: str):
    sp_df = pd.read_csv(filename)
    sp_df = sp_df.loc[:, ~sp_df.columns.str.contains('^Unnamed')]
    sp_df.rename(columns={'gold_id': 'project_gold_id'}, inplace=True)

    ncbi_cols = ['ncbi_bioproject_accession', 'ncbi_biosample_accession', 'sra_experiment_ids', 'project_gold_id']
    ncbi_df = sp_df[ncbi_cols]

    projects_cols = ['project_gold_id', 'project_name', 'project_status', 'project_funding', 'legacy_gold_id',
                     'its_proposal_id', 'its_spid', 'its_sample_id', 'pmo_project_id',
                     'genome_publication_pubmed_ids', 'other_publication_pubmed_ids']
    projects_df = sp_df[projects_cols]

    sequence_cols = ['project_gold_id', 'sequencing_strategy', 'sequencing_status', 'sequencing_centers']
    sequence_df = sp_df[sequence_cols]

    studies_cols = ['project_gold_id', 'study_gold_ids', 'organism_gold_id', 'biosample_gold_id']
    studies_df = sp_df[studies_cols]

    return ncbi_df, projects_df, sequence_df, studies_df
