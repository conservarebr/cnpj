import duckdb
import os

data_path = "/mnt/disk1/data/cnpj"
data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

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

#### Estabelecimentos ####
estabelecimentos_files = [os.path.join(data_path, f'estabelecimentos{i}.csv') for i in range(10)]

conn.execute(f"""
CREATE TABLE csv AS
SELECT 
    CONCAT(column00, column01, column02) AS cnpj_completo,
    CONCAT(tipo_de_logradouro, ' ', logradouro, ' ', numero, ' ', bairro, ' ', m.descricao, ' ', uf, ' ', cep) AS endereco,
    CONCAT(tipo_de_logradouro, ' ', logradouro, ' ', numero, ' ', bairro, ' ', m.descricao, ' ', uf) AS endereco_editado,
    cep
FROM read_csv_auto(
    ['{0}'],
    sep = ';',
    header = false,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS e
JOIN municipios m ON e.column20 = m.codigo
CROSS JOIN UNNEST(string_split(e.column12, ',')) AS cnae_secundaria
WHERE e.column05 = '02' AND (
    e.column11 IN ('{cnae_filtro_str}') OR 
    TRIM(cnae_secundaria) IN ('{cnae_filtro_str}')
);
""".format("', '".join(estabelecimentos_files), "'".join(cnae_filtro)))

result = conn.execute("SELECT * FROM csv LIMIT 10").fetchall()
for row in result:
    print(row)

result = conn.execute("SELECT count(*) FROM csv").fetchall()
for row in result:
    print(row)

#### Salvando em csv ####
saida = os.path.join(data_fribeiro, 'Teste.csv')
conn.execute(f"""
COPY csv TO '{saida}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela 'csv' foi salva em {saida}")
conn.close()
