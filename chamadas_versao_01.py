from flask import Flask, request, jsonify
import asyncio
from aiohttp import ClientSession
from requests.utils import requote_uri

app = Flask(__name__)

async def fetch(data, session):
    campos = data.split('|')
    cnpj = campos[0]
    url = requote_uri(campos[1])
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return await response.text()

async def bound_fetch(sem, data, session):
    async with sem:
        return await fetch(data, session)

async def run(data_list):
    tasks = []
    sem = asyncio.Semaphore(1000)

    async with ClientSession() as session:
        for data in data_list:
            task = asyncio.ensure_future(bound_fetch(sem, data, session))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        return responses

@app.route('/process', methods=['POST'])
async def process_data():
    data = request.json.get('data', [])
    if not isinstance(data, list):
        return jsonify({'error': 'Formato invalido'}), 400
    
    results = await run(data)
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)



#pip install Flask aiohttp quart
