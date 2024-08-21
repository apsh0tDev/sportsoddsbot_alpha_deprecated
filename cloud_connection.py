import os
import json
import asyncio
import aiohttp
import cloudscraper
from rich import print
from loguru import logger
from dotenv import load_dotenv
from scrapingant_client import ScrapingAntClient

load_dotenv()
key = os.getenv("SCRAPPEY_KEY")
ant_key = os.getenv("SCRAPING_ANT_TOKEN")

headers = { 'Content-Type' : 'application/json' }
scrappey = f"https://publisher.scrappey.com/api/v1?key={key}"
scraper = cloudscraper.create_scraper()
ant_client = ScrapingAntClient(token=ant_key)

 
async def scrape(data, site):
    logger.info(f"Scraping from {site}")
    response = await asyncio.to_thread(scraper.post, scrappey, headers=headers, json=data)
    return response.json()

async def scrape_b_u(url, site):
    logger.info(f"Scraping from {site} - Using Alt")
    try:
        result = ant_client.general_request(url, proxy_country='US', browser=False)
        if result.status_code == 200:
            return result.content
        else:
            print(result.content)
            return None 
    except Exception as e:
        print(e)
        return None