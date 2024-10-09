
import asyncio
from aiohttp import ClientSession
import colecao

async def fetch(data, session):
    campos = data.split('|')
    cnpj = campos[0]
    url = campos[1]    
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return await f"{cnpj}|{response.text()}" 


async def bound_fetch(sem, url, session):
    async with sem:
        await fetch(url, session)


async def run(r):
    tasks = []
    sem = asyncio.Semaphore(1000)

    async with ClientSession() as session:
        for i in r:
            task = asyncio.ensure_future(bound_fetch(sem, r, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        print()


def processa(colecao):
    asyncio.run(run(colecao))