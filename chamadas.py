
import asyncio
from aiohttp import ClientSession


async def fetch(data, session):
    campos = data.split('|')
    cnpj = campos[0]
    url = campos[1]    
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return f"{cnpj}|{await response.text()}" 


async def bound_fetch(sem, url, session):
    async with sem:
        await fetch(url, session)


async def run(r):
    tasks = []
    sem = asyncio.Semaphore(1000)

    async with ClientSession() as session:
        for i in r:
            task = asyncio.ensure_future(bound_fetch(sem, i, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        return await responses


def processa(colecao):
    return asyncio.run(run(colecao))
    