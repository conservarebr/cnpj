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

data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

saida = os.path.join(data_fribeiro, 'Teste.csv')
df = pd.read_csv(saida, sep=';', encoding='UTF-8')

latitudes = []
longitudes = []

for endereco in df['endereco_editado']:
    lat, lon = geocode_address(endereco)
    latitudes.append(lat)
    longitudes.append(lon)
    time.sleep(1) 

df['latitude'] = latitudes
df['longitude'] = longitudes

saida_geocodificado = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
df.to_csv(saida_geocodificado, sep=';', index=False, encoding='UTF-8')

print(f"O arquivo geocodificado foi salvo em {saida_geocodificado}")
conn.close()
