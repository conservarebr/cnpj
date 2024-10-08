
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

def geocode_addresses(file_path, num_rows=None):
    data_fribeiro = os.path.dirname(file_path)
    conn = duckdb.connect(database=':memory:')
    
    df = pd.read_csv(file_path, sep=';', encoding='UTF-8')
    
    # Se num_rows não for especificado, geocodifica todo o DataFrame
    if num_rows is not None:
        df = df.head(num_rows)

    results = []

    for endereco in df['endereco_editado']:
        result = geocode_address(endereco)
        results.append(result)
        time.sleep(1) 

    df['resultado_geocodificacao'] = [json.dumps(result, ensure_ascii=False) for result in results]

    saida_geocodificado = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
    df.to_csv(saida_geocodificado, sep=';', index=False, encoding='ISO-8859=1')

    print(f"O arquivo geocodificado foi salvo em {saida_geocodificado}")
    conn.close()

# Exemplo de chamada da função
geocode_addresses("/home/fribeiro/Teste.csv", num_rows=1000)  # Para geocodificar o número de linhas desejado
# geocode_addresses("/home/fribeiro/Teste.csv")  # Para geocodificar todo o arquivo
