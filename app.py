import sqlite3
from flask import Flask, request, jsonify
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

DB_NAME = 'interview.db'


class ProjectManager(Resource):
    """
    Class for API development that inherits from flask-restful Resource and has methods to get/fetch one or all
    project_gold_ids
    """
    @staticmethod
    def get():
        """
        Connects to the sqlite3 db created at the start of the project and fetches the details of a given
        project_gold_id based on the sp_ap_rpt view/table.
        """
        with sqlite3.connect(DB_NAME) as con:
            cursor = con.cursor()
            try:
                project_gold_id = request.args['project_gold_id']
            except Exception as _:
                project_gold_id = None

            if not project_gold_id:
                projects = cursor.execute("select * from sp_ap_rpt").fetchall()
                return jsonify(list(projects))
            project = cursor.execute("select * from sp_ap_rpt where project_gold_id = (?)", (project_gold_id,))
            return jsonify(list(project))


api.add_resource(ProjectManager, '/api/projects')

if __name__ == '__main__':
    app.run(debug=True)

    # Note: after running the app, click on the following link for result
    # http://127.0.0.1:5000/api/projects?project_gold_id=Gp0072752
