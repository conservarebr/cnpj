import duckdb
import os
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

def geocode_address(address):
    geolocator = Nominatim(user_agent="SeuNomeOuIdentificacao")
    try:
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else (None, None)
    except GeocoderTimedOut:
        return geocode_address(address) 

def geocode_dataframe(df, num_rows=None):
    latitudes = []
    longitudes = []
    
    rows_to_process = df['endereco_editado'] if num_rows is None else df['endereco_editado'][:num_rows]
    
    for endereco in rows_to_process:
        lat, lon = geocode_address(endereco)
        latitudes.append(lat)
        longitudes.append(lon)
        time.sleep(1)
        
    df['latitude'] = pd.Series(latitudes)
    df['longitude'] = pd.Series(longitudes)
    
    return df

data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

saida = os.path.join(data_fribeiro, 'Teste.csv')
df = pd.read_csv(saida, sep=';', encoding='UTF-8')

# Chame a função de geocodificação com o número de linhas desejado (ou None para todas)
num_rows_to_geocode = 10  # Altere este valor conforme necessário
df_geocodificado = geocode_dataframe(df, num_rows=num_rows_to_geocode)

saida_geocodificado = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
df_geocodificado.to_csv(saida_geocodificado, sep=';', index=False, encoding='UTF-8')

print(f"O arquivo geocodificado foi salvo em {saida_geocodificado}")
conn.close()

