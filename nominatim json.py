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
    
    df = pd.read_csv(file_path, sep=';', encoding='utf-8')
    
    if num_rows is not None:
        df = df.head(num_rows)

    results = []

    for endereco in df['endereco_editado']:
        result = geocode_address(endereco)
        results.append(result)
        time.sleep(1)  # Aguarda 1 segundo entre as requisições

    df['resultado_geocodificacao'] = [json.dumps(result, ensure_ascii=False) for result in results]

    saida_geocodificado = os.path.join(data_fribeiro, 'Teste_geocodificado.csv')
    df.to_csv(saida_geocodificado, sep=';', index=False, encoding='utf-8')

    print(f"O arquivo geocodificado foi salvo em {saida_geocodificado}")
    conn.close()
    
geocode_addresses("/home/fribeiro/Teste.csv", num_rows=400)  # Para geocodificar um número específico de linhas
# geocode_addresses("/home/fribeiro/Teste.csv")  # Para geocodificar todo o arquivo





import asyncio
from aiohttp import ClientSession

async def fetch(url, session):
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return await response.text()

async def bound_fetch(sem, url, session):
    async with sem:
        await fetch(url, session)

async def run(r):
    tasks = []
    sem = asyncio.Semaphore(1000)
    
    async with ClientSession() as session:
        for i in (r):
            task = asyncio.ensure_future(bound_fetch(sem, i, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses

number = 10000
asyncio.run(run(number))

geocode_addresses("/home/fribeiro/Teste.csv", num_rows=400) 

nessa planilha teste.csv tenho os campos cnpj_concatenado, endereco, endereco_editado e cep, vou utiçizar cnpj_concatenado e para georeferenciar vou utilizar o endereco_editado no nominatim utilize  o codigo acima para fazer isso