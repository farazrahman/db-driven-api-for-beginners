import pandas as pd
from data_ingestion_ETL.classification_mapper import analysis_type_mapper


def transform_analysis_data(filename: str):
    """
    Transforms the Ex_ap file by renaming the columns and removing unwanted columns
    :param filename:  path reference string of the csv file
    """
    analysis_df = pd.read_csv(filename)
    analysis_df.rename(columns={'gold_id': 'analysis_gold_id',
                                'project_gold_ids': 'project_gold_id'},
                       inplace=True)
    analysis_df = analysis_df.loc[:, ~analysis_df.columns.str.contains('^Unnamed')]
    analysis_df['analysis_type_id'] = analysis_df['gold_analysis_project_type'].map(analysis_type_mapper)
    analysis_df['analysis_attributes'] = analysis_df[['its_analysis_project_id', 'img_taxon_oid', 'genbanks',
                                                      'study_gold_id']].to_dict(orient='records')
    analysis_cols = ['analysis_gold_id', 'analysis_project_name', 'analysis_type_id',  'analysis_attributes', 'project_gold_id']
    analysis_df = analysis_df[analysis_cols]
    return analysis_df


def transform_projects_data(filename: str):
    """
    Transforms the Ex_sp file and splits into 4 different tables for ingestion
    :param filename: path reference string of the csv file
    """
    sp_df = pd.read_csv(filename)
    sp_df = sp_df.loc[:, ~sp_df.columns.str.contains('^Unnamed')]
    sp_df.rename(columns={'gold_id': 'project_gold_id'}, inplace=True)

    ncbi_cols = ['ncbi_bioproject_accession', 'ncbi_biosample_accession', 'sra_experiment_ids', 'project_gold_id']
    ncbi_df = sp_df[ncbi_cols]

    sp_df['project_attributes'] = sp_df[['legacy_gold_id', 'its_proposal_id', 'its_spid', 'its_sample_id',
                                         'pmo_project_id', 'genome_publication_pubmed_ids',
                                         'other_publication_pubmed_ids']].to_dict(orient='records')
    projects_cols = ['project_gold_id', 'project_name', 'project_status', 'project_funding', 'project_attributes']
    projects_df = sp_df[projects_cols]

    sequence_cols = ['project_gold_id', 'sequencing_strategy', 'sequencing_status', 'sequencing_centers']
    sequence_df = sp_df[sequence_cols]

    studies_cols = ['project_gold_id', 'study_gold_ids', 'organism_gold_id', 'biosample_gold_id']
    studies_df = sp_df[studies_cols]

    return ncbi_df, projects_df, sequence_df, studies_df
