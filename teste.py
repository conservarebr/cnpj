import os
import pandas as pd
import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_fribeiro():
    return "/home/fribeiro"

def get_cnae_filtro():
    return [
        '4110700', '6435201', '6470101', '6470103', 
        '6810201', '6810202', '6810203', '6821801', 
        '6821802', '6822600', '7490104'
    ]

def geocode_address(endereco):
    try:
        response = requests.get(f"http://venus.iocasta.com.br:8080/search.php?q={endereco}")
        response.raise_for_status()
        return response.json()[0] if response.json() else None
    except Exception as e:
        logging.error(f"Erro ao geocodificar o endereço: {endereco}. Erro: {e}")
        return None 

def geocode_addresses(caminho_arquivo, num_linhas=None):
    logging.info(f"Iniciando a geocodificação de endereços a partir do arquivo: {caminho_arquivo}")
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8').head(num_linhas)
    
    with ThreadPoolExecutor() as executor:
        df['resultado_geocodificacao'] = list(executor.map(geocode_address, df['endereco_editado']))
        df['resultado_geocodificacao'] = df['resultado_geocodificacao'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else None)
    
    output_file = os.path.join(os.path.dirname(caminho_arquivo), 'Teste_geocodificado.csv')
    df.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    logging.info(f"O arquivo geocodificado foi salvo em {output_file}")

def main():
    data_fribeiro = get_data_fribeiro()
    cnae_filtro = get_cnae_filtro()
    geocode_addresses(os.path.join(data_fribeiro, 'Teste.csv'), num_linhas=10)

if __name__ == "__main__":
    main()
