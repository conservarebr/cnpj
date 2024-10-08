
import os
import pandas as pd
import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)

def geocode_address(endereco):
    try:
        response = requests.get(f"http://venus.iocasta.com.br:8080/search.php?q={endereco}")
        response.raise_for_status()
        return response.json()[0] if response.json() else None
    except Exception as e:
        logging.error(f"Erro ao geocodificar o endereço: {endereco}. Erro: {e}")
        return None 

def geocode_addresses(caminho_arquivo, num_linhas=None):
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8').head(num_linhas)
    with ThreadPoolExecutor() as executor:
        df['resultado_geocodificacao'] = list(executor.map(geocode_address, df['endereco_editado']))
    
    df['resultado_geocodificacao'] = df['resultado_geocodificacao'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else None)
    df.to_csv(os.path.join(os.path.dirname(caminho_arquivo), 'Teste_geocodificado.csv'), sep=';', index=False, encoding='utf-8')
    
    logging.info(f"O arquivo geocodificado foi salvo em {caminho_arquivo}")

geocode_addresses("/home/fribeiro/Teste.csv", num_linhas=10)
