
import duckdb
import os
import pandas as pd
import requests
import time
import json

def geocode_address(address):
    url = f"http://venus.iocasta.com.br:8080/search.php?q={address}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data:
            location = data[0]
            return {
                'address': location.get('display_name'),
                'latitude': location.get('lat'),
                'longitude': location.get('lon'),
                'raw': location 
            }
        return None
    except (requests.exceptions.RequestException, IndexError) as e:
        print(f"Erro ao geocodificar o endereço: {address}. Erro: {e}")
        return None

def geocode_addresses(file_path, num_rows=None):
    data_fribeiro = os.path.dirname(file_path)
    conn = duckdb.connect(database=':memory:')
    
    df = pd.read_csv(file_path, sep=';', encoding='UTF-8')
    
    if num_rows is not None:
        df = df.head(num_rows)

    results = []

    for endereco in df['endereco_editado']:
        result = geocode_address(endereco)
        results.append(result)
        time.sleep(1)  

    df['resultado_geocodificacao'] = [json.dumps(result, ensure_ascii=False) for result in results]

    saida_geocodificado = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
    df.to_csv(saida_geocodificado, sep=';', index=False, encoding='ISO-8859-1')

    print(f"O arquivo geocodificado foi salvo em {saida_geocodificado}")
    conn.close()

geocode_addresses("/home/fribeiro/Teste.csv", num_rows=100)  # Para geocodificar um número específico de linhas
# geocode_addresses("/home/fribeiro/Teste.csv")  # Para geocodificar todo o arquivo
