import os
import pandas as pd
import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def geocode_address(endereco):
    try:
        response = requests.get(f"http://venus.iocasta.com.br:8080/search.php?q={endereco}")
        response.raise_for_status()
        return response.json()[0] if response.json() else None
    except Exception as e:
        logging.error(f"Erro ao geocodificar o endereço: {endereco}. Erro: {e}")
        return None 

def cnae_filtro(df, cnaes_desejados):
    return df[df['cnae_primaria'].isin(cnaes_desejados) | df['cnae_secundaria'].isin(cnaes_desejados)]

def geocode_addresses(caminho_arquivo, cnaes_desejados, num_linhas=None):
    logging.info(f"Iniciando a geocodificação a partir do arquivo: {caminho_arquivo}")
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', dtype=str)
    df_filtrado = cnae_filtro(df, cnaes_desejados).drop(columns=['cnae_primaria', 'cnae_secundaria'], errors='ignore').drop_duplicates()
    if num_linhas: df_filtrado = df_filtrado.head(num_linhas)

    logging.info(f"Número de registros restantes: {df_filtrado.shape[0]}")   

    with ThreadPoolExecutor() as executor:
        for column in ['endereco_editado', 'cep']:
            df_filtrado[f'resultado_geocodificacao_{column}'] = list(executor.map(geocode_address, df_filtrado[column]))
    
    for column in ['resultado_geocodificacao_endereco_editado', 'resultado_geocodificacao_cep']:
        df_filtrado[column] = df_filtrado[column].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else None)

    output_file = os.path.join(os.path.dirname(caminho_arquivo), 'Teste_geocodificado_03.csv')
    df_filtrado.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    logging.info(f"Arquivo geocodificado salvo em {output_file}")

caminho_arquivo = "/home/fribeiro/bases/Teste_02.csv"
cnaes_desejados = ['4110700', '6435201', '6470101', '6470103', '6810201', '6810202', '6810203', '6821801', '6821802', '6822600', '7490104']
geocode_addresses(caminho_arquivo, cnaes_desejados)