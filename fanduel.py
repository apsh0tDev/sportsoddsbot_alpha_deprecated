import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from utils import verifier_alt
from actions import exists, upload
from cloud_connection import scrape_by_site

async def scrape_data():
    print(constants.fanduel_live_url)
    response = await scrape_by_site(constants.fanduel_url, "FANDUEL", True)
    if response != None and response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            soup = bs4.BeautifulSoup(response, 'html.parser')
            pre = soup.find("pre")
            load = json.loads(pre.text)
            await tidy_up_matches(load)
        else:
            print("Invalid response, try again")
    else:
        print("None response, try again")

async def tidy_up_matches(load):
    live_matches = db.table("live_matches").select("*").execute()
    matches_names = [{"name" : item['match_name'], "uuID" : item['uuID']} for item in live_matches.data]

    #for cleaners
    matches_ids = []
    if 'attachments' in load:
        if 'competitions' in load['attachments']:
            competitions = load['attachments']['competitions']

        if 'events' in load['attachments']:
            evs = load['attachments']['events']

        if 'markets' in load['attachments']:
            markets = load['attachments']['markets']
            market_keys = [key for key in markets]

        for key in market_keys:
            if 'inPlay' in markets[key] and markets[key]['inPlay'] == True:
                event = markets[key]
                info = {
                    "match_id": event['eventId'],
                    "match_name" : find_value(event['eventId'], evs),
                    "competition" : find_value(event['competitionId'], competitions),
                    "source" : "FanDuel"
                }
                matches_ids.append(f"{event['eventId']}")
                for item in matches_names:
                    fuzz_ratio = fuzz.token_sort_ratio(item['name'], info['match_name'])
                    if fuzz_ratio >= 60:
                        info['uuID'] = item['uuID']
                        to_match = { "match_id" : info['match_id'], "match_name" : info['match_name'], "source" : "FanDuel" }
                        value_exists = await exists("matches_list", to_match)
                        if value_exists:
                                print("Already exists, skip")
                        else:
                                response = await upload(table="matches_list", info=info)
                                print(response)

        await cleaners(matches_ids)

# -- Cleaners 🧹
async def cleaners(data):
    print("Run cleaners 🧹")

    matches_table = db.table("matches_list").select("*").eq("source", "FanDuel").execute()
    matches_ids = [item['match_id'] for item in matches_table.data]

    for record_id in matches_ids:
        if record_id not in data:
            response = db.table("matches_list").delete().match({"match_id" : record_id, "source" : "FanDuel"}).execute()
            logger.info(f"Deleting record {record_id} from live matches table: {response}")
    print("Done cleaning 🧹")

# -- Utils
def find_value(id, group):
    group_keys = [key for key in group]
    for key in group_keys:
        if str(id) == key:
            return group[key]['name']  

if __name__ == "__main__":
    asyncio.run(scrape_data())