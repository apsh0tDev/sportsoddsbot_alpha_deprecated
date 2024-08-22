import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from utils import verifier, verifier_alt
from actions import exists, upload
from cloud_connection import scrape_by_site

async def scrape_data():
    response = await scrape_by_site(constants.fanduel_url, "FANDUEL", False)
    if response != None or response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            load = json.loads(response)
            await sort_matches(load=load)

async def sort_matches(load):
    matches_in_set = db.table("sets").select("*").execute()
    
    if 'attachments' in load:
        if 'events' in load['attachments']:
            evs = load['attachments']['events']

        if 'markets' in load['attachments']:
            markets = load['attachments']['markets']
            market_keys = [key for key in markets]
            for key in market_keys:
                if 'inPlay' in markets[key] and markets[key]['inPlay'] == True:
                    event = markets[key]
                    info = {
                        "match_id" : event['eventId'],
                        "match_name" : f"{event['runners'][0]['runnerName']} vs {event['runners'][1]['runnerName']}",
                    }

                    for item in matches_in_set.data:
                        fuzz_ratio = fuzz.token_sort_ratio(item['match_name'], info['match_name'])
                        if fuzz_ratio >= 70:
                            info['uuID'] = item['uuID']

                    value_exists = await exists(table="fanduel", to_match={"uuID" : info['uuID'], "match_name" : info['match_name']})
                    if value_exists:
                        print("Exists. Skip")
                    else:
                        response = await upload(table="fanduel", info=info)
                        print(response)

async def scrape_events():
    matches = db.table("fanduel").select("*").execute()
    matches_ids = [item['match_id'] for item in matches.data]

    for id in matches_ids:
        await scrape_event(id=id)
        await asyncio.sleep(4)

async def scrape_event(id):
    url = constants.fanduel_event_url.format(id=id, tab="all")
    print(url)
    response = await scrape_by_site(url=url, site="FANDUEL", headless=True)
    if response != None and response != '':
       is_valid = await verifier_alt(response)
       if is_valid:
           print(response)
           soup = bs4.BeautifulSoup(response, 'html.parser')
           pre = soup.find("pre")
           print(pre)
            #load = json.loads(response)
            #await add_lines(load=load)

async def add_lines(load):
    print(load)

if __name__ == "__main__":
    asyncio.run(scrape_events())