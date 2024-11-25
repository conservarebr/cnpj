import os
import pandas as pd
import requests
import json
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from config import load_settings 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def geocode_address(endereco, geocode_url):
    try:
        response = requests.get(f"{geocode_url}{endereco}")
        response.raise_for_status()
        return response.json()[0] if response.json() else None
    except Exception as e:
        logging.error(f"Erro ao geocodificar o endereço: {endereco}. Erro: {e}")
        return None 

def cnae_filtro(df, cnaes):
    return df[df['cnae_primaria'].isin(cnaes) | df['cnae_secundaria'].isin(cnaes)]

def geocode_addresses(caminho_arquivo, cnaes_desejados, geocode_url, num_linhas=None):
    logging.info(f"Iniciando a geocodificação a partir do arquivo: {caminho_arquivo}")
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', dtype=str)
    df_filtrado = cnae_filtro(df, cnaes_desejados).drop(columns=['cnae_primaria', 'cnae_secundaria'], errors='ignore').drop_duplicates()
    if num_linhas: df_filtrado = df_filtrado.head(num_linhas)

    logging.info(f"Número de registros restantes: {df_filtrado.shape[0]}")
    
    for column in ['endereco_editado', 'cep']:
        logging.info(f"Iniciando a verificação por {column}")
        with ThreadPoolExecutor() as executor:
            df_filtrado[f'resultado_geocodificacao_{column}'] = list(executor.map(lambda endereco: geocode_address(endereco, geocode_url), df_filtrado[column]))
        logging.info(f"Concluída verificação por {column}")

    df_filtrado[[f'resultado_geocodificacao_{col}' for col in ['endereco_editado', 'cep']]] = df_filtrado[[f'resultado_geocodificacao_{col}' for col in ['endereco_editado', 'cep']]].apply(lambda x: x.apply(lambda y: json.dumps(y, ensure_ascii=False) if y else None))

    output_file = os.path.join(os.path.dirname(caminho_arquivo), 'nominatim.csv')
    df_filtrado.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    logging.info(f"Arquivo geocodificado salvo em {output_file}")

async def main():
    
    settings = await load_settings()
    
    caminho_arquivo = settings.path_file_endereco
    cnaes_desejados = settings.cnaes_desejados
    geocode_url = settings.geocode_url
    
    geocode_addresses(caminho_arquivo, cnaes_desejados, geocode_url)

if __name__ == "__main__":
    asyncio.run(main())
