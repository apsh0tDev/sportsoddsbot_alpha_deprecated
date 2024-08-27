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
    tournaments_table = db.table("featured_tournaments").select("*").eq("source", "Draftkings").execute()
    matches_table = db.table("matches_list").select("*").execute()
    matches_list = list(set(item['competition'] for item in matches_table.data))
    
    #Compare tournaments from Draftkings table to tournaments from matches list
    tournaments = []
    tasks = []
    for tournament in tournaments_table.data:
        for item in matches_list:
            fuzz_ratio = fuzz.token_sort_ratio(tournament['display_name'], item)
            if fuzz_ratio >= 80:
                tournaments.append(tournament['key'])

    tournaments = list(set(tournaments))
    
    #Scrape each tournament
    for tournament_id in tournaments:
        tasks.append(scrape_tournament(tournament_id))

    await asyncio.gather(*tasks)

async def scrape_tournament(id):
    url = constants.draftkings_tournaments.format(id=id)
    response = await scrape_by_site(url, "DRAFTKINGS", True)
    if response != None and response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            soup = bs4.BeautifulSoup(response, 'html.parser')
            pre = soup.find("pre")
            load = json.loads(pre.text)
            await tidy_up_matches(load=load)
    else:
        print("None response, try again")

async def tidy_up_matches(load):
    matches_list = db.table("matches_list").select("*").eq("source", "BetMGM").execute()
    matches_names = [item['match_name'] for item in matches_list.data]

    try:
        current_matches = []
        if 'eventGroup' in load:
            if 'events' in load['eventGroup']:
                events = load['eventGroup']['events']
                for event in events:
                    if event['eventStatus']['state'] == "STARTED":
                        event_name = event['name']
                        for item in matches_names:
                            fuzz_ratio = fuzz.token_sort_ratio(item, event_name)
                            if fuzz_ratio >= 70:
                                info = await set_default_info(event=event)
                                current_matches.append(info['match_id'])
                                to_match = { "match_id" : info['match_id'], "match_name" : info['match_name'], "source" : "Draftkings" }
                                #DB Actions
                                value_exists = await exists("matches_list", to_match)
                                if value_exists:
                                    print("Already exists, skip")
                                else:
                                    response = await upload(table="matches_list", info=info)
                                    print(response)
        await cleaners(current_matches)
    except Exception as e:
        logger.error(f"Error parsing Draftkings - {e}")


#-- Cleaners 🧹
async def cleaners(data):
    print("Run cleaners 🧹")
    #Clean matches list
    matches_tables = db.table("matches_list").select("*").eq("source", "Draftkings").execute()
    matches_ids = [item['match_id'] for item in matches_tables.data]

    for record_id in matches_ids:
        if record_id not in data:
            response = db.table("matches_list").delete().match({"match_id" : record_id, "source" : "Draftkings"}).execute()
            logger.info(f"Deleting record {record_id} from matches list table: {response}")

    print("Done cleaning 🧹")


#-- Utils
async def set_default_info(event):
    info = {
        "match_id" : event['eventId'],
        "match_name" : event['name'],
        "source" : "Draftkings",
        "competition" : event['eventGroupName'], 
    }

    return info

if __name__ == "__main__":
    asyncio.run(scrape_data())

