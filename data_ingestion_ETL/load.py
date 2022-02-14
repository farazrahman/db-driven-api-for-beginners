import os
import sqlite3
import pandas as pd
from data_ingestion_ETL.transform import transform_analysis_data, transform_projects_data
from data_ingestion_ETL.classification_mapper import analysis_type_mapper

# change the directory to access the db location for data ingestion
path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
DB_NAME = 'interview.db'
DB_PATH = os.path.join(path_parent, DB_NAME)


# create a connection with already built sqlite db by giving the db path
def create_connection():
    """
    creates a sqlite db connection and cursor object to execute sql commands in python
    :return: DB connection and cursor object
    """
    sqlite_connection = sqlite3.connect(DB_PATH)
    cursor = sqlite_connection.cursor()

    return sqlite_connection, cursor


# load the ap csv file as one table in the db
def load_ap_data(filename: str):
    """
    Populates the analysis table with given data
    :param filename: path reference string of the csv file
    """
    connection, cursor = create_connection()
    analysis_df = transform_analysis_data(filename=filename)
    analysis_df['analysis_attributes'] = analysis_df['analysis_attributes'].astype(str)
    lst = analysis_df.values.tolist()
    try:
        cursor.executemany("INSERT INTO main.analysis(analysis_gold_id, analysis_project_name, "
                           "analysis_type_id, analysis_attributes, project_gold_id) VALUES (?,?,?,?,?)", lst)
        connection.commit()
        print('data ingested')
    except Exception as e:
        print(str(e))

    connection.close()


# load the sp csv file into 4 different tables
def load_sp_data(filename: str):
    """
    Populates the projects table, ncbi table, sequence table and studies table with given data
    :param filename: path reference string of the csv file
    """
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
        projects['project_attributes'] = projects['project_attributes'].astype(str)
        project_lst = projects.values.tolist()
        try:
            cursor.executemany("INSERT INTO main.projects(project_gold_id, project_name, project_status, "
                               "project_funding, project_attributes) VALUES (?,?,?,?,?)", project_lst)
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


def load_mapping_tables():
    """
    Load the analysis_type mapping table
    """
    connection, cursor = create_connection()

    def _load_analysis_type():

        analysis_type = pd.DataFrame(analysis_type_mapper.items(), columns=['analysis_type', 'analysis_type_id'])

        analysis_type_lst = analysis_type.values.tolist()
        try:
            cursor.executemany("INSERT INTO main.analysis_type(analysis_type, analysis_type_id) VALUES (?,?)", analysis_type_lst)
            connection.commit()
            print('data ingested')
        except Exception as e:
            print(str(e))

    # call all load functions
    _load_analysis_type()


if __name__ == '__main__':
    load_ap_data(filename='data_ingestion_ETL/data/20220125_Ex_ap_data.csv')

    load_sp_data(filename='data_ingestion_ETL/data/20220125_Ex_sp_data.csv')

    load_mapping_tables()
