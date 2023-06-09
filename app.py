from flask import Flask
from flask import request
import json
import NlToSql as nls
from flask_cors import CORS

app = Flask(__name__)
app.config.from_object(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})

nlToSql = nls.NlToSql('speakql_university')

@app.route('/answer_question', methods=['POST'])
def answer_question():
    print("received request")
    question = request.get_json()['question']
    print(question)
    answer = nlToSql.get_sql(question)
    response = {'answer': answer}
    return json.dumps(response)