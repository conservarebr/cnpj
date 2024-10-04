import duckdb
import os

data_path = "/mnt/disk1/data/cnpj"

conn = duckdb.connect()

conn.execute("""
CREATE TABLE cnae (
    codigo VARCHAR PRIMARY KEY,
    descricao VARCHAR
);
""")

cnae_file_path = os.path.join(data_path, 'cnaes.csv')
conn.execute(f"""
COPY cnae FROM '{cnae_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

cnae_count = conn.execute("SELECT COUNT(*) FROM cnae").fetchone()[0]
print(f"Count of records in cnae: {cnae_count}")

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

municipios_select = conn.execute("SELECT * FROM municipios limit 10").fetchone()[0]
print(f"As primeiras 10 linhas de municipios: {municipios_select}")

conn.close()
