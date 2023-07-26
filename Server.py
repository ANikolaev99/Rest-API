# coding: utf-8

from flask import Flask
from flask_restful import Api, Resource, reqparse

# создать приложение
app = Flask(__name__)

api = Api()

courses = {
    #1: {"name": "Python", "value": 1},
    #2: {"name": "Java", "value": 2}
    0: {"name": "", "value": 0}
    }

parser = reqparse.RequestParser()
parser.add_argument("name", type=str, required=True, location='form')
parser.add_argument("value", type=int, required=True, location='form')

class Main(Resource):
    def get(self, course_id):
        # здесь взаимодействие с БД 
        #return {"info": "Some info", "num": 56}
        if course_id == 0:
            return courses
        else:
            return courses[course_id]

    def delete(self, course_id):
        del courses[course_id]
        return courses

    def post(self, course_id):
        courses[course_id] = parser.parse_args()
        return courses

    def put(self, course_id):

        courses[course_id] = parser.parse_args()
        return courses


#api.add_resource(Main, "/api/main")
api.add_resource(Main, "/api/courses/<int:course_id>")
api.init_app(app)

if __name__ == "__main__":
    port = 3000
    host = "127.0.0.1"
    app.run(debug=True, port=port, host = host)
