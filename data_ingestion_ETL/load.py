import pandas as pd
import os
import sqlite3
from transform import transform_analysis_data, transform_projects_data

DB_NAME = '/Users/farazrahman/db-driven-api/interview.db'


def create_connection():
    sqlite_connection = sqlite3.connect(DB_NAME)
    cursor = sqlite_connection.cursor()

    return sqlite_connection, cursor


def load_ap_data(filename: str):
    connection, cursor = create_connection()
    analysis_df = transform_analysis_data(filename=filename)
    lst = analysis_df.values.tolist()
    try:
        cursor.executemany("INSERT INTO main.analysis(analysis_gold_id, analysis_project_name, "
                           "gold_analysis_project_type, its_analysis_project_id, img_taxon_oid, genbanks, "
                           "study_gold_id, project_gold_id) VALUES (?,?,?,?,?,?,?,?)", lst)
        connection.commit()
        print('data ingested')
    except Exception as e:
        print(str(e))

    connection.close()


def load_sp_data(filename: str):
    connection, cursor = create_connection()
    ncbi, projects, sequence, studies = transform_projects_data(filename=filename)

    def _load_ncbi_data():
        ncbi_lst = ncbi.values.tolist()
        try:
            cursor.executemany("INSERT INTO main.ncbi(ncbi_bioproject_accession, ncbi_biosample_accession, "
                               "sra_experiment_ids, project_gold_id) VALUES (?,?,?,?)", ncbi_lst)
            connection.commit()
            print('data ingested')
        except Exception as e:
            print(str(e))

    def _load_projects_data():
        project_lst = projects.values.tolist()
        try:
            cursor.executemany("INSERT INTO main.projects(project_gold_id, project_name, project_status, "
                               "project_funding, legacy_gold_id, its_proposal_id, its_spid, its_sample_id, "
                               "pmo_project_id, genome_publication_pubmed_ids, other_publication_pubmed_ids) VALUES ("
                               "?,?,?,?,?,?,?,?,?,?,?)", project_lst)
            connection.commit()
            print('data ingested')
        except Exception as e:
            print(str(e))

    def _load_sequence_data():
        sequence_lst = sequence.values.tolist()
        try:
            cursor.executemany("INSERT INTO main.sequence(project_gold_id, sequencing_strategy, sequencing_status, "
                               "sequencing_centers) VALUES ( "
                               "?,?,?,?)", sequence_lst)
            connection.commit()
            print('data ingested')
        except Exception as e:
            print(str(e))

    def _load_studies_data():
        studies_lst = studies.values.tolist()
        try:
            cursor.executemany("INSERT INTO main.studies(project_gold_id, study_gold_ids, organism_gold_id, "
                               "biosample_gold_id) VALUES ( "
                               "?,?,?,?)", studies_lst)
            connection.commit()
            print('data ingested')
        except Exception as e:
            print(str(e))

    # call all load functions
    _load_ncbi_data()
    _load_projects_data()
    _load_sequence_data()
    _load_studies_data()
    connection.close()


sp_ap_table = """ SELECT p.project_gold_id, p.project_name, p.project_status, 
                         a.gold_analysis_project_type, COUNT(a.analysis_gold_id) as analysis_count
                  FROM projects p
                  JOIN analysis a ON p.project_gold_id = a.project_gold_id
                  GROUP BY p.project_gold_id, a.gold_analysis_project_type"""


def load_sp_ap_table():
    connection, cursor = create_connection()

    agg_list = list(cursor.execute(sp_ap_table))
    print(agg_list)
    try:
        cursor.executemany("INSERT INTO main.sp_ap_table(project_gold_id, project_name, project_status, "
                           "gold_analysis_project_type,analysis_count) VALUES (?,?,?,?,?)", agg_list)
        connection.commit()
        print('data ingested')
    except Exception as e:
        print(str(e))
    connection.close()


if __name__ == '__main__':
    load_ap_data(filename='data/20220125_Ex_ap_data.csv')

    load_sp_data(filename='data/20220125_Ex_sp_data.csv')
    load_sp_ap_table()
