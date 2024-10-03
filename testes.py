
import duckdb
from app.core.config import settings
import os

def execute_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()
        
    conn = duckdb.connect(database=':memory:')
    conn.execute(sql_script)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    sql_file_path = os.path.join('/home/fribeiro/src/cnpj/app/scripts', 'CNPJ.sql')
    execute_sql_file(sql_file_path)
    print("SQL script executed successfully.")
