# db-driven-api
### About the Project:

1. This project is about building a local database in sqlite. 
2. Using DDL and DML to create, load, query, and manipulate database tables and views. 
3. Developing a web service to expose the database table for further analysis and use.

### About the Data:
The data provided includes 2 csv files;

20220125_Ex_ap_data.csv:
Data containing details of analysis.

20220125_Ex_sp_data.csv:
Data containing details of projects.

### Programming Language and Libraries used:
1. Python- The code should run using Python versions 3.8
2. sqlite3 for SQL database Manangement- Python comes with a built-in sqlite database in the form of a library to provide a complete database management system without the need for downloading an additional software. Works best for practising. 
3. pandas for doing transformation tasks on the csv files before loading to db.
4. os- python module for interacting with operating system
5. Flask- python web framework used to build a web service

### Project Structure:
The project has following three elements:

1. ETL Pipeline:
   1. create_db.py - creates a sqlite db by normalizing the data provided in the csv files.
   2. transform.py- used as an import in the subsequent step, performs steps like renaming columns, eliminating unwanted or additional columns.
   3. load.py- connects to the db created in step 1 and loads the data. The DDL and DML functions are integrated in python code. 
2. DB denormalization to create a view:
   1. view.py - creates a combined view/ report from the original tables as sp_ap_table.
3. create a web service:
   1. app.py - web service using Flask framework  that hooks up to ‘sp_ap_rpt’ and take an input project id (Gpxxxxx) to return a JSON output containing the values from the  underlying table

### File Structure and Description:
db-driven-api:
1. README.md: read me file
2. requirements.txt
3. create_db.py
4. view.py
5. app.py
6. data_ingestion_ETL: package containing the ETL pipeline preparation code
   1. transform.py
   2. load.py

### Instructions for execution
To execute the app follow the steps below:
NOTE- IDE used Pycharm

1. Clone the repository
2. Set up a virtual environment in the project's root directory
   1. Open a terminal in the project directory and run:
      1. python3 -m virtualenv venv
      2. source venv/bin/activate
      3. pip3 install -r requirements.txt
3. To create a db and required tables run create_db.py in the project's root directory
4. To populate the db with data run load.py in the directory /data_ingestion_ETL/load.py
5. To create a view of the sp_ap_rpt run view.py in the project's root directory
6. To create a web service, run app.py in the project's root directory
   1. Go to http://127.0.0.1:5000/api/projects
   2. To select a project_id provide the project_gold_id in the link,
      1. example- http://127.0.0.1:5000/api/projects?project_gold_id=Gp0072752