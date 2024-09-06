import os
import json
import asyncio
import aiohttp
import cloudscraper
from rich import print
from loguru import logger
from dotenv import load_dotenv
from dev_notifier import notification
from scrapingant_client import ScrapingAntClient

load_dotenv()
key = os.getenv("SCRAPPEY_KEY")
ant_key = os.getenv("SCRAPING_ANT_TOKEN")

headers = { 'Content-Type' : 'application/json' }
scrappey = f"https://publisher.scrappey.com/api/v1?key={key}"
scraper = cloudscraper.create_scraper()
ant_client = ScrapingAntClient(token=ant_key)
    
async def scrape_by_site(url, site, headless):
    logger.info(f"Scraping from {site} - Using Alt")
    token = await get_token(site)
    client = ScrapingAntClient(token=token)
    try:
        result = client.general_request(url, proxy_country='US', browser=headless)
        if result.status_code == 200:
            return result.content
        else:
            print(result.content)
            notification(f"{site} - {result.content}")
            return None 
    except Exception as e:
        print(e)
        notification(f"{site} - {e}")
        return None    
    
async def get_token(site):
    name = site.upper()
    match name:
        case "DRAFTKINGS":
            key = os.getenv("DRAFTKINGS_SAT")
            return key
        case "FANDUEL":
            key = os.getenv("FANDUEL_SAT")
            return key