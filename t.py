import duckdb
import os

data_ibge = "/home/fribeiro/bases/IBGE_CNEFE"
conn = duckdb.connect(database=':memory:')

#### IBGE ####
# Criação da tabela ibge
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
    e.UF,
    e.CEP,
    e.LATITUDE,
    e.LONGITUDE
FROM read_csv_auto(
    [{ibge_files_str}],
    sep = ';',
    header = true,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS e
WHERE e.UF IS NOT NULL AND e.CEP IS NOT NULL;  -- Filtro para garantir que UF e CEP não sejam nulos
""")

# Criando uma nova tabela para armazenar as médias das coordenadas por CEP e UF
conn.execute("""CREATE TABLE ibge_avg AS
SELECT
    UF,
    CEP,
    AVG(CAST(LATITUDE AS FLOAT)) AS avg_latitude,
    AVG(CAST(LONGITUDE AS FLOAT)) AS avg_longitude
FROM
    ibge
WHERE
    CEP IS NOT NULL
GROUP BY
    UF, CEP;""")

# Salvando a tabela com as médias em um novo arquivo CSV
saida_ibge_avg = os.path.join(data_ibge, 'IBGE_avg.csv')
conn.execute(f"""
COPY ibge_avg TO '{saida_ibge_avg}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela com a média das coordenadas por CEP e UF foi salva em {saida_ibge_avg}")

conn.close()
