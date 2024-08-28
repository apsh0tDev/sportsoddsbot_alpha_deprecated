import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from actions import exists, upload
from utils import verifier_alt
from cloud_connection import scrape_by_site

async def scrape_data_alt():
    response = await scrape_by_site(constants.betrivers_url, "BETRIVERS", True)
    if response == None or response == '':
        logger.error(f"Could not retrieve data from Betrivers.")
        db.table("sportsbooks").update({'available' : False}).eq("name", "Betrivers").execute()
    else:
        is_valid = await verifier_alt(response)
        if is_valid:
            soup = bs4.BeautifulSoup(response, 'html.parser')
            pre = soup.find("pre")
            if pre != None:
               load = json.loads(pre.text)
               await tidy_up_matches_partial(load)
        else:
            logger.error(f"Invalid response from Betrivers.")
            db.table("sportsbooks").update({'available' : False}).eq("name", "Betrivers").execute()            

async def tidy_up_matches_partial(load):
    matches_list = db.table("matches_list").select("*").eq("source", "BetMGM").execute()
    matches_names = [item['match_name'] for item in matches_list.data]
    ids = []
    if 'items' in load:
        for item in load['items']:
            if item['sport'] == "TENNIS" and item['state'] == "STARTED":
               info = await set_default_info(item)
               ids.append(info['match_id'])
               for name in matches_names:
                   fuzz_ratio = fuzz.token_sort_ratio(info['match_name'], name)
                   if fuzz_ratio >= 70:
                        value_exists = await exists(table="matches_list", to_match={"match_id" : info['match_id'], "match_name" : info['match_name'], "source" : "Betrivers"})
                        if value_exists:
                           print("Already exists. Skip")
                        else:
                            response = await upload(table="matches_list", info=info)
                            print(response)
    await cleaners(ids)

#-- Cleaners 🧹
async def cleaners(data):
    print("Run cleaners 🧹")
    matches_list = db.table("matches_list").select("*").eq("source", "Betrivers").execute()
    matches_ids = [int(item['match_id']) for item in matches_list.data]

    for record_id in matches_ids:
        if record_id not in data:
            response = db.table("matches_list").delete().match({"match_id" : record_id, "source" : "Betrivers"}).execute()
            logger.info(f"Deleting record {record_id} from matches list table: {response}")            

    print("Done cleaning 🧹")

#-- Utils
async def set_default_info(event):
    info = {
        "match_id" : event['id'],
        "match_name" : event['name'],
        "competition" : event['eventInfo'][2]['name'],
        "tournament" : event['eventInfo'][1]['name'],
        "source" : "Betrivers"
    }
    return info                                               


if __name__ == "__main__":
    asyncio.run(scrape_data_alt())