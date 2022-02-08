# To use SQLite, we must import sqlite3.
import os
from os.path import dirname, join
import sqlite3
from typing import List

# #Then create a connection using connect() method and pass the name of the database you want to access if there is a
# # file with that name, it will open that file. Otherwise, Python will create a file with the given name.
# sqlite_connection = sqlite3.connect('interview.db')
#
# # After this, a cursor object is called to be capable to send commands to the SQL.
# cursor = sqlite_connection.cursor()
# cursor.execute('''DROP TABLE sequencing;''')


DB_NAME = os.path.dirname(os.path.abspath("interview.db"))
print(DB_NAME)
# create the sequencing table
print("creating sequence table")
sequence_table = """CREATE TABLE IF NOT EXISTS sequence ([project_gold_id] VARCHAR(10) PRIMARY KEY NOT NULL,
                                                        [sequencing_strategy] VARCHAR NULL,
                                                        [sequencing_status] VARCHAR NULL,
                                                        [sequencing_centers] VARCHAR NULL);"""

print("creating project table")
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

print("creating ncbi table")
ncbi_table = """CREATE TABLE IF NOT EXISTS ncbi ([ncbi_bioproject_accession] VARCHAR PRIMARY KEY NOT NULL,
                                                 [ncbi_biosample_accession] VARCHAR NULL,
                                                 [sra_experiment_ids] VARCHAR NULL,
                                                 [project_gold_id] VARCHAR NULL);"""

print("creating study table")
study_table = """CREATE TABLE IF NOT EXISTS studies ([project_gold_id] VARCHAR PRIMARY KEY NOT NULL,
                                                     [study_gold_ids] VARCHAR NULL,
                                                     [organism_gold_id] VARCHAR NULL,
                                                     [biosample_gold_id] VARCHAR NULL);"""

print("creating analysis table")
analysis_table = """CREATE TABLE IF NOT EXISTS analysis ([analysis_gold_id] VARCHAR PRIMARY KEY NOT NULL,
                                                        [analysis_project_name] VARCHAR NULL,
                                                        [gold_analysis_project_type] VARCHAR NULL,
                                                        [its_analysis_project_id] VARCHAR NULL,
                                                        [img_taxon_oid] VARCHAR NULL,
                                                        [genbanks] VARCHAR NULL,
                                                        [study_gold_id] VARCHAR NULL,
                                                        [project_gold_id] VARCHAR NULL);"""


sp_ap_table = """CREATE TABLE IF NOT EXISTS sp_ap_table ([project_gold_id] VARCHAR NOT NULL,
                                                   [project_name] VARCHAR NULL,
                                                   [project_status] VARCHAR NULL,
                                                   [gold_analysis_project_type] VARCHAR NULL,
                                                   [analysis_count] INTEGER NULL);"""

# Make a list of tables that needs to be created in our database. This will be an input to our create_tables function
tables_lst = [project_table, sequence_table, study_table, ncbi_table, analysis_table, sp_ap_table]


def create_tables(list_of_tables: List[str]):
    """

    :param list_of_tables: list of tables created with SQL command to create a table in the database
    """
    sqlite_connection = sqlite3.connect(DB_NAME)
    cursor = sqlite_connection.cursor()
    print("Connected to the database")
    for table in list_of_tables:
        cursor.execute(table)
    sqlite_connection.commit()
    sqlite_connection.close()
    print("created_tables")


# if __name__ == '__main__':
#     # # first drop tables if required else keep it commented
#     # sqlite_connection = sqlite3.connect(DB_NAME)
#     # cursor = sqlite_connection.cursor()
#     # cursor.execute('''DROP TABLE analysis;''')
#     # cursor.execute('''DROP TABLE ncbi;''')
#     # cursor.execute('''DROP TABLE projects;''')
#     # cursor.execute('''DROP TABLE sequence;''')
#     # cursor.execute('''DROP TABLE studies;''')
#     # cursor.execute('''DROP TABLE sp_ap_table;''')
#     # sqlite_connection.close()
#
#     # create tables in the db
#     create_tables(list_of_tables=tables_lst)
