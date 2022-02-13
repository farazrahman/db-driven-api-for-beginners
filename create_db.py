import sqlite3
from typing import List

# Declare the DB name
DB_NAME = "interview.db"


# The below code tries to break the Ex_sp_data into 5 different tables like by project_gold_id:
# sequence table- contains details of sequencing strategy, sequencing status and centers for each project id
sequence_table = """CREATE TABLE IF NOT EXISTS sequence ([project_gold_id] VARCHAR PRIMARY KEY NOT NULL,
                                                        [sequencing_strategy] VARCHAR NULL,
                                                        [sequencing_status] VARCHAR NULL,
                                                        [sequencing_centers] VARCHAR NULL);"""

# project table- contains details of project name, status, funding etc.
project_table = """CREATE TABLE IF NOT EXISTS projects ([project_gold_id] VARCHAR PRIMARY KEY NOT NULL,
                                                        [project_name] VARCHAR NULL,
                                                        [project_status] VARCHAR NULL,
                                                        [project_funding] VARCHAR NULL,
                                                        [legacy_gold_id] VARCHAR NULL,
                                                        [its_proposal_id] VARCHAR NULL,
                                                        [its_spid] VARCHAR,
                                                        [its_sample_id] VARCHAR NULL,
                                                        [pmo_project_id] VARCHAR NULL,
                                                        [genome_publication_pubmed_ids] VARCHAR NULL,
                                                        [other_publication_pubmed_ids] VARCHAR NULL);"""

# ncbi table- contains the details of ncbi sra experiments for each project_id
ncbi_table = """CREATE TABLE IF NOT EXISTS ncbi ([ncbi_bioproject_accession] VARCHAR PRIMARY KEY NOT NULL,
                                                 [ncbi_biosample_accession] VARCHAR NULL,
                                                 [sra_experiment_ids] VARCHAR NULL,
                                                 [project_gold_id] VARCHAR NULL);"""

# study table- contains details of studies, organism and biosample of each project id
study_table = """CREATE TABLE IF NOT EXISTS studies ([project_gold_id] VARCHAR PRIMARY KEY NOT NULL,
                                                     [study_gold_ids] VARCHAR NULL,
                                                     [organism_gold_id] VARCHAR NULL,
                                                     [biosample_gold_id] VARCHAR NULL);"""

# analysis table- has all columns from the EX_ap_data csv file
analysis_table = """CREATE TABLE IF NOT EXISTS analysis ([analysis_gold_id] VARCHAR PRIMARY KEY NOT NULL,
                                                        [analysis_project_name] VARCHAR NULL,
                                                        [gold_analysis_project_type] VARCHAR NULL,
                                                        [its_analysis_project_id] VARCHAR NULL,
                                                        [img_taxon_oid] VARCHAR NULL,
                                                        [genbanks] VARCHAR NULL,
                                                        [study_gold_id] VARCHAR NULL,
                                                        [project_gold_id] VARCHAR NULL);"""


# Make a list of tables that needs to be created in our database. This will be an input to our create_tables function
tables_lst = [project_table, sequence_table, study_table, ncbi_table, analysis_table]


def create_tables(list_of_tables: List[str]):
    """

    :param list_of_tables: list of tables created with SQL command
    """
    sqlite_connection = sqlite3.connect(DB_NAME)
    cursor = sqlite_connection.cursor()
    print("Connected to the database")
    for table in list_of_tables:
        cursor.execute(table)
    sqlite_connection.commit()
    sqlite_connection.close()
    print("created_tables")


if __name__ == '__main__':
    # first drop tables if required else keep it commented
    # sqlite_connection = sqlite3.connect(DB_NAME)
    # cursor = sqlite_connection.cursor()
    # cursor.execute('''DROP TABLE IF EXISTS analysis;''')
    # cursor.execute('''DROP TABLE IF EXISTS ncbi;''')
    # cursor.execute('''DROP TABLE IF EXISTS projects;''')
    # cursor.execute('''DROP TABLE IF EXISTS sequence;''')
    # cursor.execute('''DROP TABLE IF EXISTS studies;''')
    # cursor.execute('''DROP VIEW IF EXISTS sp_ap_rpt;''')
    # sqlite_connection.close()

    # create tables in the db
    create_tables(list_of_tables=tables_lst)
