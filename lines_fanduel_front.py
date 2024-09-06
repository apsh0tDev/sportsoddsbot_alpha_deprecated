import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from loguru import logger
from cloud_connection import scrape_by_site
from utils import verifier_alt, extract_players

async def scrape_events():
    matches = db.table("matches_list").select("*").execute()
    for match in matches.data:
        url = await url_generator(match[''])

async def scrape_event():
    response = await scrape_by_site("https://ny.sportsbook.fanduel.com/tennis/women's-us-open-2024/kichenok-ostapenko-v-mladenovic-zhang-33556248?tab=all", "FANDUEL", True)
    soup = bs4.BeautifulSoup(response, 'html.parser')
    spans = soup.find_all("span", class_=["cl", "cv", "ae", "af", "la", "lb", "lc", "ld", "hv", "hw", "hx", "ib", "le", "s", "er", "br", "bd", "h", "i", "j", "ah", "ai", "m", "aj", "o", "ak", "q", "al", "fq"])

    for span in spans:
        print(span.text) 

#-- Utils
async def url_generator(players, id, tournament):
    print("Url generator")


if __name__ == "__main__":
    asyncio.run((scrape_event()))


