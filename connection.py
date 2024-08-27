import os
import json
import aiohttp
import requests
from rich import print
from loguru import logger
from dotenv import load_dotenv

load_dotenv()
key = os.getenv("SCRAPPEY_KEY")
headers = { 'Content-Type' : 'application/json' }
scrappey = f"https://publisher.scrappey.com/api/v1?key={key}"

async def scrape(data, site):
    logger.info(f"Scraping from {site}")
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(scrappey, headers=headers, json=data) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(response)
                logger.error("ERROR")
                return None    