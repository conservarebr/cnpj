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
    return df[(df['cnae_primaria'].isin(cnaes_desejados)) | (df['cnae_secundaria'].isin(cnaes_desejados))]

def geocode_addresses(caminho_arquivo, cnaes_desejados, num_linhas=None):
    logging.info(f"Iniciando a geocodificação de endereços a partir do arquivo: {caminho_arquivo}")
    
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', dtype={'cnae_primaria': str, 'cnae_secundaria': str})
    df_filtrado = cnae_filtro(df, cnaes_desejados)
    
    if num_linhas is not None:
        df_filtrado = df_filtrado.head(num_linhas)
        
    df_filtrado = df_filtrado.drop(columns=['cnae_primaria', 'cnae_secundaria'], errors='ignore')
    df_filtrado = df_filtrado.drop_duplicates()

    num_registros = df_filtrado.shape[0]
    logging.info(f"Número de registros restantes a serem geocodificados: {num_registros}")   
    
    with ThreadPoolExecutor() as executor:
        df_filtrado['resultado_geocodificacao'] = list(executor.map(geocode_address, df_filtrado['endereco_editado']))
        df_filtrado['resultado_geocodificacao'] = df_filtrado['resultado_geocodificacao'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else None)
    
    output_file = os.path.join(os.path.dirname(caminho_arquivo), 'Teste_geocodificado_02.csv')
    df_filtrado.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    logging.info(f"O arquivo geocodificado foi salvo em {output_file}")

caminho_arquivo = "/home/fribeiro/bases/Teste_02.csv"
cnaes_desejados = [ '4110700', '6435201', '6470101', '6470103', '6810201', '6810202', '6810203', '6821801', '6821802', '6822600', '7490104']  
geocode_addresses(caminho_arquivo, cnaes_desejados, num_linhas=None)