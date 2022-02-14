import sqlite3

DB_NAME = 'interview.db'

# The following code block attempts to follow the instructions in the part-2 of the project i.e.
# denormalizing the table and creating a combined report from the original tables as sp_ap_rpt.
# This new table or view joins the projects table and the analysis table on project_gold_id, analysis_type_id and groups
# the project by analysis type and counts the total number of analysis undertaken under each project and analysis type
sp_ap_rpt = """ CREATE VIEW sp_ap_rpt 
                AS 
                SELECT p.project_gold_id, p.project_name, p.project_status, at.analysis_type,
                       COUNT(a.analysis_gold_id) as analysis_count
                FROM projects p
                JOIN analysis a ON p.project_gold_id = a.project_gold_id
                JOIN analysis_type at ON at.analysis_type_id = a.analysis_type_id
                GROUP BY p.project_gold_id, at.analysis_type"""


def create_sp_ap_view(sql_command: str):
    """
    This function connects to the database and creates a view table by using the required sql command
    :param sql_command: sql command string
    """
    sqlite_connection = sqlite3.connect(DB_NAME)
    cursor = sqlite_connection.cursor()

    try:
        cursor.execute(sql_command)
        sqlite_connection.commit()
        print('sp_ap_view created')
    except Exception as e:
        print(str(e))
    sqlite_connection.close()


if __name__ == '__main__':
    create_sp_ap_view(sp_ap_rpt)
