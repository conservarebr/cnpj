import duckdb
import os
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Função para geocodificar um endereço
def geocode_address(address):
    geolocator = Nominatim(user_agent="geoapiExercises")
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except GeocoderTimedOut:
        return geocode_address(address) 

# Caminhos para os dados
data_path = "/mnt/disk1/data/cnpj"
data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

# Filtro de CNAEs
cnae_filtro = [
    '4110700', '6435201', '6470101', '6470103', 
    '6810201', '6810202', '6810203', '6821801', 
    '6821802', '6822600', '7490104'
]
cnae_filtro_str = "', '".join(cnae_filtro)

# Criar tabela CNAE
conn.execute("""CREATE TABLE cnae (codigo VARCHAR PRIMARY KEY, descricao VARCHAR);""")
cnae_file_path = os.path.join(data_fribeiro, 'cnae.csv')
conn.execute(f"""
COPY cnae FROM '{cnae_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

# Criar tabela Municipios
conn.execute("""CREATE TABLE municipios (codigo VARCHAR PRIMARY KEY, descricao VARCHAR);""")
municipios_file_path = os.path.join(data_path, 'municipios.csv')
conn.execute(f"""
COPY municipios FROM '{municipios_file_path}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
""")

# Criar tabela Estabelecimentos
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
CROSS JOIN UNNEST(string_split(e.column12, ',')) AS cnae_secundaria(value)
WHERE e.column05 = '02' AND (
    e.column11 IN ('{cnae_filtro_str}') OR 
    TRIM(value) IN ('{cnae_filtro_str}')
);
""")

# Carregar os dados da tabela CSV em um DataFrame
df = conn.execute("SELECT endereco_editado FROM csv").fetchdf()

# Geocodificar os endereços
df['latitude'], df['longitude'] = zip(*df['endereco_editado'].apply(geocode_address))

# Adicionar colunas geográficas de volta na tabela CSV
conn.execute("CREATE TABLE csv_geocoded AS SELECT *, latitude, longitude FROM csv JOIN (SELECT * FROM df) AS d ON csv.endereco_editado = d.endereco_editado")

# Salvando em CSV
saida = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
conn.execute(f"""
COPY csv_geocoded TO '{saida}' 
    (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
""")

print(f"A tabela 'csv_geocoded' foi salva em {saida}")
conn.close()

