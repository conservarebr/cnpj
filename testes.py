import duckdb
import os

# Define the data path
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
COPY cnae FROM '{cnae_file_path}' WITH (FORMAT 'csv', DELIMITER ';', HEADER false, ENCODING 'UTF8');
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
COPY municipios FROM '{municipios_file_path}' WITH (FORMAT 'csv', DELIMITER ';', HEADER false, ENCODING 'UTF8');
""")

municipios_count = conn.execute("SELECT COUNT(*) FROM municipios").fetchone()[0]
print(f"Count of records in municipios: {municipios_count}")

conn.close()
