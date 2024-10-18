import duckdb
import os

data_ibge = "/home/fribeiro/bases/IBGE_CNEFE"
conn = duckdb.connect(database=':memory:')

#### IBGE ####
conn.execute("""CREATE TABLE ibge (
    uf VARCHAR,
    cep VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR
);""")

# Listando os arquivos CSV no diretório
ibge_files = [os.path.join(data_ibge, f) for f in os.listdir(data_ibge) if f.endswith('.csv')]
ibge_files_str = ', '.join([f"'{file}'" for file in ibge_files])

# Inserindo dados na tabela ibge
conn.execute(f"""
INSERT INTO ibge
SELECT DISTINCT
    e.column01 AS uf,
    e.column08 AS cep,
    e.column25 AS latitude,
    e.column26 AS longitude
FROM read_csv_auto(
    [{ibge_files_str}],
    sep = ';',
    header = true,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS e
WHERE e.column01 IS NOT NULL AND e.column08 IS NOT NULL;  -- Filtro para garantir que UF e CEP não sejam nulos
""")

#### Salvando em csv ####
saida_ibge = os.path.join(data_ibge, 'IBGE.csv')
conn.execute(f"""
COPY ibge TO '{saida_ibge}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela 'ibge' foi salva em {saida_ibge}")

conn.close()
