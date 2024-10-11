import duckdb
import os

data_fribeiro = "/home/fribeiro/bases"
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

#### Municipios ####
conn.execute("""CREATE TABLE municipios (
    codigo VARCHAR PRIMARY KEY,
    descricao VARCHAR
);""")
municipios_file_path = os.path.join(data_fribeiro, 'municipios.csv')
conn.execute(f"""
COPY municipios FROM '{municipios_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

#### Estabelecimentos Com CNAES ####
conn.execute(f"""
CREATE TABLE csv_02 AS
SELECT DISTINCT
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
    ) AS colecao
FROM read_csv_auto(
    [{estabelecimentos_files_str}],
    sep = ';',
    header = false,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS e
JOIN municipios m ON e.column20 = m.codigo
CROSS JOIN UNNEST(string_split(e.column12, ',')) AS cnae_secundaria(value)
WHERE e.column05 = '02';  -- Removido o filtro de cnae_filtro
""")

#### Salvando em csv ####
saida_02 = os.path.join(data_fribeiro, 'Teste_02.csv')
conn.execute(f"""
COPY csv_02 TO '{saida_02}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela 'csv_02' foi salva em {saida_02}")

conn.close()
