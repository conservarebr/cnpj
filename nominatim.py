import duckdb
import pandas as pd
import requests
import time
import os

# Caminhos dos arquivos
data_fribeiro = "/home/fribeiro"
saida_georeferenciado = os.path.join(data_fribeiro, 'Teste.csv')

# Conectar ao banco de dados DuckDB
conn = duckdb.connect(database=':memory:')

# Carregar a tabela csv na memória
df = conn.execute("SELECT * FROM csv").fetchdf()

# Função para obter coordenadas usando Nominatim
def get_coordinates(address):
    try:
        response = requests.get('https://nominatim.openstreetmap.org/search', 
                                 params={'q': address, 'format': 'json', 'limit': 1})
        response.raise_for_status()
        data = response.json()
        if data:
            return data[0]['lat'], data[0]['lon']
        else:
            return None, None
    except Exception as e:
        print(f"Erro ao buscar o endereço {address}: {e}")
        return None, None

# Lista para armazenar os resultados
coordinates = []

# Iterar sobre cada endereço e buscar coordenadas
for index, row in df.iterrows():
    address = row['endereco_editado']
    lat, lon = get_coordinates(address)
    coordinates.append((row['cnpj_completo'], lat, lon))
    time.sleep(1)  # Para evitar sobrecarga na API

# Criar um DataFrame com os resultados
coords_df = pd.DataFrame(coordinates, columns=['cnpj_completo', 'latitude', 'longitude'])

# Exibir resultados
print(coords_df)

# Salvar os resultados em CSV
coords_df.to_csv(saida_georeferenciado, sep=';', index=False)

print(f"Coordenadas georeferenciadas salvas em {saida_georeferenciado}")
