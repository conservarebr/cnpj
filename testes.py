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

conn.execute("""
CREATE TABLE estabelecimentos AS 
SELECT 
    column00 AS cnpj_basico,
    column01 AS cnpj_ordem,
    column02 AS cnpj_dv,
    column05 AS situacao_cadastral,
    column11 AS cnae_fiscal_principal,
    column12 AS cnae_fiscal_secundaria,
    column13 AS tipo_de_logradouro,
    column14 AS logradouro,
    column15 AS numero,
    column16 AS complemento,
    column17 AS bairro,
    column18 AS cep,
    column19 AS uf,
    column20 AS municipio
FROM read_csv_auto(
    ['{0}'],
    sep = ';',
    header = false,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS subquery
WHERE column05 = '02';
""".format("', '".join(estabelecimentos_files)))
    
#### Ajuste 1: Realizando o split em Estabelecimentos ####

conn.execute("""
CREATE TABLE temp_estabelecimentos AS
SELECT 
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    situacao_cadastral,
    cnae_fiscal_principal,
    TRIM(split.value) AS cnae_secundaria,
    tipo_de_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    cep,
    uf, 
    municipio
FROM estabelecimentos
CROSS JOIN UNNEST(string_split(cnae_fiscal_secundaria, ',')) AS split(value);

""")

conn.execute("DROP TABLE estabelecimentos;")
conn.execute("ALTER TABLE temp_estabelecimentos RENAME TO estabelecimentos;")

#### Ajuste 2: Realizando o Join Estabelecimentos x Municipios ####
    
conn.execute(""" 
ALTER TABLE estabelecimentos ADD COLUMN municipio_descricao VARCHAR; 
""")

conn.execute(""" 
UPDATE estabelecimentos e 
SET municipio_descricao = m.descricao 
FROM municipios m 
WHERE e.municipio = m.codigo; 
""")

#### Ajuste 3: Filtrando os Cnaes desejados ####

cnae_filtro = [
    '4110700', '6435201', '6470101', '6470103', 
    '6810201', '6810202', '6810203', '6821801', 
    '6821802', '6822600', '7490104'
]

cnae_filtro_str = "', '".join(cnae_filtro)

conn.execute(f"""
CREATE TABLE temp AS
SELECT DISTINCT *
FROM estabelecimentos
WHERE cnae_fiscal_principal IN ('{cnae_filtro_str}')
   OR cnae_secundaria IN ('{cnae_filtro_str}');
""")

#### Ajuste 4: Concatenando variaveis ####

conn.execute("""ALTER TABLE temp ADD COLUMN cnpj_completo VARCHAR;""")
conn.execute("""ALTER TABLE temp ADD COLUMN endereco VARCHAR;""")
conn.execute("""ALTER TABLE temp ADD COLUMN endereco_editado VARCHAR;""")

conn.execute(""" 
UPDATE temp
SET 
    cnpj_completo = CONCAT(cnpj_basico, cnpj_ordem, cnpj_dv),
    endereco = CONCAT(tipo_de_logradouro, ' ', logradouro, ' ', numero, ' ', bairro, ' ', municipio_descricao, ' ', uf, ' ', cep),
    endereco_editado = CONCAT(tipo_de_logradouro, ' ', logradouro, ' ', numero, ' ', bairro, ' ', municipio_descricao, ' ', uf);
""")

#### Ajuste 5: Selecionando apenas os campos necessarios ####

conn.execute("""
CREATE TABLE csv AS
SELECT cnpj_completo, endereco, endereco_editado, cep
FROM temp;
""")

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