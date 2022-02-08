import os
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
import sqlite3

DB_NAME = '/Users/farazrahman/db-driven-api/interview.db'

sqlite_connection = sqlite3.connect(DB_NAME)
cursor = sqlite_connection.cursor()

app = Flask(__name__)
api = Api(app)

@app.route('/', methods=['GET'])
def get_project():
    with sqlite3.connect(DB_NAME) as connection:
        project_id = 'Gp0072749'
        cursor = connection.cursor()
        project = cursor.execute("select * from sp_ap_table where project_gold_id = (?)", (project_id,))

        return jsonify(list(project))


if __name__ == '__main__':
    app.debug = True
    app.run()
