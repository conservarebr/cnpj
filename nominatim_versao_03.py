import os
import pandas as pd
import aiohttp
import asyncio
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def geocode_address(session, endereco):
    headers = {
        "User-Agent": "SeuNome/SeuEmail"  # Substitua pelo seu nome e email
    }
    try:
        async with session.get(f"https://nominatim.openstreetmap.org/search?addressdetails=1&q={endereco}&format=json", headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data[0] if data else None
    except Exception as e:
        logging.error(f"Erro ao geocodificar o endereço: {endereco}. Erro: {e}")
        return None 

def cnae_filtro(df, cnaes_desejados):
    return df[df['cnae_primaria'].isin(cnaes_desejados) | df['cnae_secundaria'].isin(cnaes_desejados)]

async def geocode_addresses(caminho_arquivo, cnaes_desejados, num_linhas=None):
    logging.info(f"Iniciando a geocodificação de endereços a partir do arquivo: {caminho_arquivo}")
    
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', dtype=str)
    df_filtrado = cnae_filtro(df, cnaes_desejados)

    if num_linhas:
        df_filtrado = df_filtrado.head(num_linhas)

    df_filtrado = df_filtrado.drop(columns=['cnae_primaria', 'cnae_secundaria'], errors='ignore')
    df_filtrado = df_filtrado.drop_duplicates()

    num_registros = df_filtrado.shape[0]
    logging.info(f"Número de registros restantes a serem geocodificados: {num_registros}")

    async with aiohttp.ClientSession() as session:
        tasks = []
        for endereco in df_filtrado['endereco_editado']:
            tasks.append(geocode_address(session, endereco))
            await asyncio.sleep(1)  # Atraso de 1 segundo entre as requisições
        df_filtrado['resultado_geocodificacao'] = await asyncio.gather(*tasks)
        
        print(df_filtrado[['endereco_editado', 'resultado_geocodificacao']])

    df_filtrado['resultado_geocodificacao'] = df_filtrado['resultado_geocodificacao'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if x else None
    )
    
    output_file = os.path.join(os.path.dirname(caminho_arquivo), 'Teste_geocodificado_02.csv')
    df_filtrado.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    logging.info(f"O arquivo geocodificado foi salvo em {output_file}")

def main(caminho_arquivo, cnaes_desejados, num_linhas=None):
    asyncio.run(geocode_addresses(caminho_arquivo, cnaes_desejados, num_linhas))

if __name__ == "__main__":
    caminho_arquivo = "/home/fribeiro/bases/Teste_02.csv"
    cnaes_desejados = [
        '4110700', '6435201', '6470101', '6470103', '6810201',
        '6810202', '6810203', '6821801', '6821802', '6822600', 
        '7490104'
    ]  
    main(caminho_arquivo, cnaes_desejados)
