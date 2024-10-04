
import duckdb
from app.core.config import settings
import os
from jinja2 import Environment, FileSystemLoader

def execute_sql_file(file_path, data_path):
    env = Environment(loader=FileSystemLoader(os.path.dirname(file_path)))
    template = env.get_template(os.path.basename(file_path))
    sql_script = template.render(data_path=data_path)
    
    conn = duckdb.connect(database=':memory:')
    conn.execute(sql_script)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    sql_file_path = os.path.join('/home/fribeiro/src/cnpj/app/scripts', 'CNPJ_template.sql')
    execute_sql_file(sql_file_path, settings.data_path)
    print("SQL script executed successfully.")
