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

"""async def scrape(data, site):
    logger.info(f"Scraping from {site}")
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(scrappey, headers=headers, json=data) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(response)
                logger.error("ERROR")
                return None""" 

async def scrape(data, site):
    logger.info(f"Scraping from {site}")
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(site, headers=headers, json=data) as response:
            if response.status == 200:
                try:
                    # Attempt to decode the response as JSON
                    return await response.json()
                except aiohttp.ContentTypeError:
                    # Handle case where the response is not JSON
                    logger.error(f"Expected JSON response but got a different content type from {site}")
                    return None
                except UnicodeDecodeError:
                    # Handle decoding error by reading the raw content and logging
                    raw_content = await response.read()
                    logger.error(f"UnicodeDecodeError: could not decode response from {site}. Raw content: {raw_content}")
                    return None
            else:
                logger.error(f"Received {response.status} status from {site}")
                return None