import duckdb
import os

data_path = "/mnt/disk1/data/cnpj"
data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

#### Municipios ####
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

#### Estabelecimentos ####
estabelecimentos_files = [os.path.join(data_path, f'estabelecimentos{i}.csv') for i in range(10)]

conn.execute(f"""
CREATE TABLE estabelecimentos AS 
SELECT 
    column00 AS cnpj_basico,
    column01 AS cnpj_ordem,
    column02 AS cnpj_dv,
    column05 AS situacao_cadastral,
    column11 AS cnae_fiscal_principal,
    TRIM(split.value) AS cnae_secundaria,
    column13 AS tipo_de_logradouro,
    column14 AS logradouro,
    column15 AS numero,
    column16 AS complemento,
    column17 AS bairro,
    column18 AS cep,
    column19 AS uf,
    column19 AS municipio
FROM read_csv_auto(
    ['{0}'],
    sep = ';',
    header = false,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS subquery
CROSS JOIN UNNEST(string_split(column12, ',')) AS split(value);
""".format("', '".join(estabelecimentos_files)))

result = conn.execute("SELECT * FROM estabelecimentos LIMIT 10").fetchall()
for row in result:
    print(row)

result = conn.execute("SELECT count(*) FROM estabelecimentos").fetchall()
for row in result:
    print(row)

#### Cnae ####
conn.execute("""
CREATE TABLE cnae (
    codigo VARCHAR PRIMARY KEY,
    descricao VARCHAR
);
""")

cnae_file_path = os.path.join(data_fribeiro, 'cnae.csv')
conn.execute(f"""
COPY cnae FROM '{cnae_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

result = conn.execute("SELECT * FROM cnae LIMIT 10").fetchall()
for row in result:
    print(row)
    
conn.close()