import duckdb
import os

data_fribeiro = "/home/fribeiro/bases/CNPJ"
data_ibge = "/home/fribeiro/bases/IBGE_CNEFE"
conn = duckdb.connect(database=':memory:')

#### CNAE ####
conn.execute("""CREATE TABLE cnae (
    codigo VARCHAR PRIMARY KEY,
    descricao VARCHAR
);""")
cnae_file_path = os.path.join(data_fribeiro, 'cnae.csv')
conn.execute(f"""
COPY cnae FROM '{cnae_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

#### Municípios ####
conn.execute("""CREATE TABLE municipios (
    codigo VARCHAR PRIMARY KEY,
    descricao VARCHAR
);""")
municipios_file_path = os.path.join(data_fribeiro, 'municipios.csv')
conn.execute(f"""
COPY municipios FROM '{municipios_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

#### IBGE ####
conn.execute("""CREATE TABLE ibge (
    uf VARCHAR,
    cep VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR
);""")
ibge_files = [os.path.join(data_ibge, f) for f in os.listdir(data_ibge) if f.endswith('.csv')]
for ibge_file in ibge_files:
    conn.execute(f"""
    COPY ibge FROM '{ibge_file}' 
        (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
    """)

conn.execute("""CREATE TABLE ibge_avg AS
SELECT
    UF,
    CEP,
    ROUND(AVG(CAST(LATITUDE AS FLOAT)), 6) AS avg_latitude,
    ROUND(AVG(CAST(LONGITUDE AS FLOAT)), 6) AS avg_longitude
FROM
    ibge
WHERE
    CEP IS NOT NULL
GROUP BY
    UF, CEP;""")

#### Estabelecimentos ####
estabelecimentos_files = [os.path.join(data_fribeiro, f'estabelecimentos_{i}.csv') for i in range(10)]
estabelecimentos_files_str = ', '.join([f"'{file}'" for file in estabelecimentos_files])

conn.execute(f"""
CREATE TABLE CNPJ AS
SELECT
    CONCAT(e.column00, e.column01, e.column02) AS cnpj_completo,
    CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19, ' ', e.column18) AS endereco,
    CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19) AS endereco_editado,
    e.column18 AS cep,
    e.column11 AS cnae_primaria,
    TRIM(value) AS cnae_secundaria,
    CONCAT(
        CONCAT(e.column00, e.column01, e.column02),
        '|http://venus.iocasta.com.br:8080/search.php?q=',
        TRIM(CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19))
    ) AS colecao,
    i.avg_latitude,
    i.avg_longitude
FROM read_csv_auto(
    [{estabelecimentos_files_str}],
    sep = ';',
    header = false,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS e
JOIN municipios m ON e.column20 = m.codigo
LEFT JOIN ibge_avg i ON e.column18 = i.CEP  
CROSS JOIN UNNEST(string_split(e.column12, ',')) AS cnae_secundaria(value)
WHERE e.column05 = '02' 
GROUP BY 
    cnpj_completo, endereco, endereco_editado, cep, cnae_primaria, cnae_secundaria, colecao, avg_latitude, avg_longitude, e.column18, TRIM(value);
""")

#### Salvando em CSV ####
saida = os.path.join(data_fribeiro, 'CNPJ.csv')
conn.execute(f"""
COPY CNPJ TO '{saida}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela 'CNPJ' foi salva em {saida}")

conn.close()


# scp fribeiro@209.126.127.15:/home/fribeiro/bases/CNPJ/CNPJ.csv C:/Users/RibeiroF/Downloads/