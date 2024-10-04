import duckdb
from app.core.config import settings
import os
from jinja2 import Environment, FileSystemLoader

def execute_sql_file(file_path, data_path):
    env = Environment(loader=FileSystemLoader(os.path.dirname(file_path)))
    template = env.get_template(os.path.basename(file_path))
    sql_script = template.render(data_path=data_path)

    conn = duckdb.connect(database=':memory:')
    
    # Separar as instruções SQL
    sql_commands = sql_script.split(';')
    for command in sql_commands:
        command = command.strip()
        if command:  
            try:
                conn.execute(command)
            except Exception as e:
                print(f"Error executing command: {command}")
                print(e)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    sql_file_path = os.path.join('/home/fribeiro/src/cnpj/app/scripts', 'CNPJ.SQL')  
    execute_sql_file(sql_file_path, settings.data_path)
    print("SQL script executed successfully.")

