import duckdb
import os

data_path = "/mnt/disk1/data/cnpj"
conn = duckdb.connect(database=':memory:')

# Municipios 

conn.execute("""
CREATE TABLE municipios (
    codigo VARCHAR PRIMARY KEY,
    descricao VARCHAR
);
""")

municipios_file_path = os.path.join(data_path, 'municipios.csv')
conn.execute(f"""
COPY municipios FROM '{municipios_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

result = conn.execute("SELECT * FROM municipios LIMIT 10").fetchall()
for row in result:
    print(row)

conn.close()
