import flask
from flask import request
from flask import Response
import time
import pymysql
import json
import requests
from database import gravaStatus, statusExecucao

headers = {'content-type': 'application/json'}
url = 'http://localhost:4000/disparador'

app = flask.Flask(__name__)
app.config["DEBUG"] = True


def disparador(id_projeto, sequencia):

    data = {"id_projeto":id_projeto, "sequencia": sequencia}
    r = requests.post(url, data=json.dumps(data), headers=headers)

    return json.dumps({'status': 200, 'success':"Acabou a goiaba - disparador", 'ContentType':'application/json'} )


@app.route('/callback', methods=['GET', 'POST'])
def callback():

    id_projeto = json.loads(request.data)["id_projeto"]
    num_job = json.loads(request.data)["num_job"]
    script = json.loads(request.data)['script']
    sequencia = json.loads(request.data)['sequencia']
    objValidacao = json.loads(request.data)['objValidacao']

    sucesso_erro = statusExecucao(num_job, script)

    print(objValidacao[script])

    print(sucesso_erro)


    # {"10.hql": 'sucesso', "20.hql": 'erro', "30.hql": 'any'}
    if(objValidacao[script] == sucesso_erro):

        lista = sequencia.pop(0)
    
        disparador(id_projeto, sequencia)
    
    return False

app.run(host='0.0.0.0', port=5000)
