import flask
from flask import request
from flask import Response
import uuid
import time
import pymysql
import json
import requests
from database import gravaStatus, controleSequencia, buscaSequencia


headers = {'content-type': 'application/json'}
url = 'http://localhost:5000/callback'


app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/disparador', methods=['GET', 'POST'])
def disparador():

    id_projeto = json.loads(request.data)['id_projeto'] # Recebe por parametro
    sequencia = json.loads(request.data)['sequencia']
    num_job = uuid.uuid4()

    if sequencia == 0:
        keys = buscaSequencia(id_projeto)
        sequencia = list(keys)[0].split(",")
    
    objValidacao = controleSequencia(id_projeto)

    print(objValidacao)
    # {10: 'sucesso', 20: 'erro', 30: 'any'}

    if len(sequencia) < 1:
        return json.dumps({'status': 200, 'success':"Finalizou os Arquivos em Sequencia", 'ContentType':'application/json'})

    script = sequencia[0].strip()
    context = open("scripts/" + script,"r").read()

    status = "Processando" # Status inicial

    processamento(id_projeto, num_job, script, status, context, sequencia, objValidacao)
     

# SIMULACAO DA API ONPREM - CDP
def processamento(id_projeto, num_job, script, status, context, sequencia, objValidacao):

    # INCIO DO PROCESSAMENTO -> "Processando"
    gravaStatus(id_projeto, num_job, script, status)

    time.sleep( 2 )

    # Finalizado -> "Erro / Sucesso"
    
    
    if(script == "10.hql"):
        gravaStatus(id_projeto, num_job, script, "sucesso")
        data = {"id_projeto": id_projeto, "num_job": str(num_job), "script": script, "sequencia": sequencia, "objValidacao": objValidacao}
    
    if (script == "20.hql"):
        gravaStatus(id_projeto, num_job, script, "erro")
        

    data = {"id_projeto": id_projeto, "num_job": str(num_job), "script": script, "sequencia": sequencia, "objValidacao": objValidacao}
    r = requests.post(url, data=json.dumps(data), headers=headers)

    
app.run(host='0.0.0.0', port=4000)
