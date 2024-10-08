import os
import pandas as pd
import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)

def geocode_address(endereco):
    url = f"http://venus.iocasta.com.br:8080/search.php?q={endereco}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()[0] if response.json() else None
    except (requests.exceptions.RequestException, IndexError) as e:
        logging.error(f"Erro ao geocodificar o endereço: {endereco}. Erro: {e}")
        return None 

def geocode_addresses(caminho_arquivo, num_linhas=None):
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8')
    if num_linhas is not None:
        df = df.head(num_linhas)

    with ThreadPoolExecutor() as executor:
        df['resultado_geocodificacao'] = list(executor.map(geocode_address, df['endereco_editado']))
    
    df['resultado_geocodificacao'] = df['resultado_geocodificacao'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else None)

    caminho_saida = os.path.join(os.path.dirname(caminho_arquivo), 'Teste_geocodificado.csv')
    df.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8')

    logging.info(f"O arquivo geocodificado foi salvo em {caminho_saida}")

geocode_addresses("/home/fribeiro/Teste.csv", num_linhas=100000)
