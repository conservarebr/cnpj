
import duckdb
import os
from app.core.config import Settings

settings = Settings()

def execute_sql_file(file_path, data_path):
    with open(file_path, 'r') as file:
       
        sql_commands = file.read().replace('${data_path}', data_path).split(';')


    conn = duckdb.connect(database=':memory:') 

    for command in sql_commands:
        command = command.strip()
        if command: 
            try:
                conn.execute(command)
            except Exception as e:
                print(f"Erro ao executar o comando: {command}")
                print(e)


sql_file_path = os.path.join('/home/fribeiro/src/cnpj/app/scripts', 'CNPJ.sql')

execute_sql_file(sql_file_path, settings.data_path)
