#import os
#from app.core.config import Settings
#from app.sco import settings

# Carregar as configurações
#settings = Settings()

# Listar os arquivos CSV que você espera encontrar
#csv_files = [
    #"cnaes.csv",
    #"municipios.csv",
    #"estabelecimentos0.csv",
    #"estabelecimentos1.csv",
    #"estabelecimentos2.csv",
    #"estabelecimentos3.csv",
    #"estabelecimentos4.csv",
    #"estabelecimentos5.csv",
   # "estabelecimentos6.csv",
   # "estabelecimentos7.csv",
   # "estabelecimentos8.csv",
   # "estabelecimentos9.csv",
#]

# Verificar se cada arquivo existe
#for csv_file in csv_files:
    #file_path = os.path.join(settings.data_path, csv_file)
    #if os.path.exists(file_path):
       # print(f"Arquivo encontrado: {file_path}")
    #else:
        #print(f"Arquivo não encontrado: {file_path}")


# testes.py
import duckdb

from app.core.config import settings
import os

def execute_sql_file(file_path):
    # Cria uma conexão com o DuckDB
    conn = duckdb.connect()

    # Lê o conteúdo do arquivo SQL
    with open(file_path, 'r') as file:
        sql_script = file.read()

    # Executa o script SQL
    conn.execute(sql_script)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    sql_file_path = os.path.join(settings.data_path, 'scripts', 'CNPJ.sql')
    execute_sql_file(sql_file_path)
