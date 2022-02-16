import time
import pymysql
import json


def buscaSequencia(id_projeto):

    connection = conectaBanco()

    with connection:

        with connection.cursor() as cursor:

            sql = "select new_scripts, tipo from sequencia where id_projeto = {};".format(id_projeto)
            obj = {}
            cursor.execute(sql)

            for row in cursor:
                obj[row['new_scripts']] = row['tipo']

    return obj



def statusExecucao(num_job, script):

    connection = conectaBanco()

    with connection:

        with connection.cursor() as cursor:

            sql = "select status from controle_execucao where numjob = '{}' and script = '{}';".format(num_job, script)
            cursor.execute(sql)
            status = cursor.fetchone()

    return status



def controleSequencia(id_projeto):

    connection = conectaBanco()

    with connection:

        with connection.cursor() as cursor:

            sql = "select id_script, termino from sucesso_erro where id_projeto = {};".format(id_projeto)
            obj = {}
            cursor.execute(sql)

            for row in cursor:
                obj[row['id_script']] = row['termino']

    return obj



def gravaStatus(id_projeto, num_job, script, status):

    connection = conectaBanco()

    with connection:

        with connection.cursor() as cursor:

            # sql = "insert into controle_execucao values (100, 'c303282d-f2e6-46ca-a04a-35d3d873712d', '10.hql', 'Finalizado', now())"
            sql = "insert into controle_execucao values ({}, '{}', '{}', '{}', now())".format(id_projeto, num_job, script, status)
            cursor.execute(sql)
        connection.commit()

    return True



def conectaBanco():

    return pymysql.connect(host=os.get('host'),
                            user=os.get('user'),
                            password=os.get('pwd'),
                            database=os.get('database'),
                            cursorclass=pymysql.cursors.DictCursor)
