import duckdb
import os

data_path = "/mnt/disk1/data/cnpj"
data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

#### Filtro de CNAEs ####
cnae_filtro = [
    '4110700', '6435201', '6470101', '6470103', 
    '6810201', '6810202', '6810203', '6821801', 
    '6821802', '6822600', '7490104'
]
cnae_filtro_str = "', '".join(cnae_filtro)

#### CNAE ####
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
estabelecimentos_files_str = ', '.join([f"'{file}'" for file in estabelecimentos_files])

conn.execute(f"""
CREATE TABLE csv AS
SELECT 
    CONCAT(e.column00, e.column01, e.column02) AS cnpj_completo,
    CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19, ' ', e.column18) AS endereco,
    CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19) AS endereco_editado,
    e.column18 AS cep
FROM read_csv_auto(
    [{estabelecimentos_files_str}],
    sep = ';',
    header = false,
    ignore_errors = true,
    union_by_name = true,
    filename = true
) AS e
JOIN municipios m ON e.column20 = m.codigo
CROSS JOIN UNNEST(string_split(e.column12, ',')) AS cnae_secundaria(value)  -- Altera para dar um alias
WHERE e.column05 = '02' AND (
    e.column11 IN ('{cnae_filtro_str}') OR 
    TRIM(value) IN ('{cnae_filtro_str}')  -- Usa o alias aqui
);
""")

#### Salvando em csv ####
saida = os.path.join(data_fribeiro, 'Teste.csv')
conn.execute(f"""
COPY csv TO '{saida}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela 'csv' foi salva em {saida}")
conn.close()
