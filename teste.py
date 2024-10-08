import duckdb
import os
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import json

def geocode_address(address):
    geolocator = Nominatim(user_agent="SeuNomeOuIdentificacao")
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return {
                'address': location.address,
                'latitude': location.latitude,
                'longitude': location.longitude,
                'raw': location.raw 
            }
        return None
    except GeocoderTimedOut:
        return geocode_address(address) 

data_fribeiro = "/home/fribeiro"
conn = duckdb.connect(database=':memory:')

saida = os.path.join(data_fribeiro, 'Teste.csv')
df = pd.read_csv(saida, sep=';', encoding='UTF-8')

results = []

for endereco in df['endereco_editado']:
    result = geocode_address(endereco)
    results.append(result)
    time.sleep(1) 

df['resultado_geocodificacao'] = [json.dumps(result) for result in results]

saida_geocodificado = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
df.to_csv(saida_geocodificado, sep=';', index=False, encoding='UTF-8')

print(f"O arquivo geocodificado foi salvo em {saida_geocodificado}")
conn.close()
