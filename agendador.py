import flask
from flask import request
from flask import Response
import time
import pymysql
import json
import requests

headers = {'content-type': 'application/json'}
url = 'http://localhost:4000/disparador'


app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/agendador', methods=['GET'])
def home():

    data = {"id_projeto": 100, "sequencia": 0}
    
    r = requests.post(url, data=json.dumps(data), headers=headers)

    print(r)

    return {'b': 'json.dumps(r)'}


app.run(host='0.0.0.0', port=3000)