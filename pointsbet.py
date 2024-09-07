#This display is the same as Fanatics/Resort Worlds Bet
import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from actions import exists, upload
from connection import scrape
from cloud_connection import scrape_by_site
from utils import verifier, verifier_alt

async def scrape_data():
    data = {
        'cmd' : 'request.get',
        'url' : constants.pointsbet_competitions_url,
        'requestType' : 'request',
        'proxyCountry' : 'UnitedStates'
    }
    response = await scrape(data, "Pointsbet")
    if response != None:
        db.table("sportsbooks").update({'available' : False}).eq("name", "Pointsbet").execute()
        print("None response, try backup.")
        #await scrape_data_backup(constants.pointsbet_competitions_url) 
    else:
        if 'statusCode' in response and 'statusCode' == 200:
            is_valid = await verifier(response)
            if is_valid:
                load = json.loads(response['solution']['response'])
                await scrape_matches(load)
            else:
                db.table("sportsbooks").update({'available' : False}).eq("name", "Pointsbet").execute()
                print("Response not valid from Pointsbet, try backup.")
                #await scrape_data_backup(constants.pointsbet_competitions_url) 
        else:
            print("Forbidden response to Pointsbet, try backup.")
            #await scrape_data_backup(constants.pointsbet_competitions_url)
            
async def scrape_data_backup(url):
    response = await scrape_by_site(url, "Fanduel", True)
    is_valid = await verifier_alt(response)
    if is_valid:
        load = json.loads(response)
        await scrape_matches(load)
    else:
        logger.error(f"Could not retrieve anything from Pointsbet.")
        db.table("sportsbooks").update({'available' : False}).eq("name", "Pointsbet").execute()        

async def scrape_matches(load):
    #Get featured events:
    competitions_ids = []
    if 'locales' in load:
        for item in load['locales']:
            if item['key'] == 'featured':
                if 'competitions' in item:
                    for competition in item['competitions']:
                        competitions_ids.append(competition['key'])

    competitions_ids = list(set(competitions_ids))

    for id in competitions_ids:
        url = constants.pointsbet_url.format(competitionId = id)
        response = await scrape_by_site(url, "Pointsbet")
        is_valid = await verifier_alt(response)
        if is_valid:
            load = json.loads(response)
            await tidy_up_matches_partial(load)

    db.table("sportsbooks").update({'available' : True}).eq("name", "Pointsbet").execute()

async def tidy_up_matches_partial(load):
    matches_list = db.table("matches_list").select("*").eq("source", "BetMGM").execute()
    matches_names = [item['match_name'] for item in matches_list.data]

    events_ids = []
    if 'events' in load:
        for event in load['events']:
            if event['isLive'] == True:
                events_ids.append(events_ids)
                info = await set_default_info(event)
                to_match = { "match_id" : info['match_id'], "match_name" : info['match_name'], "source" : "Pointsbet" }
                for item in matches_names:
                    fuzz_ratio = fuzz.token_sort_ratio(item, info['match_name'])
                    if fuzz_ratio >= 70:
                        value_exists = await exists("matches_list", to_match)
                        if value_exists:
                            print("Already exists, skip")
                        else:
                            response = await upload(table="matches_list", info=info)
                            print(response)
   # await cleaners(events_ids)
    

# -- Cleaners 🧹
async def cleaners(data):
    print("Run cleaners 🧹")
    #Clean matches list
    matches_table = db.table("matches_list").select("*").eq("source", "Pointsbet").execute()
    matches_ids = [item['match_id'] for item in matches_table.data]

    for record_id in data:
        if record_id not in matches_ids:
            response = db.table("matches_list").delete().match({"match_id"}).execute()
            logger.info(f"Deleting record {record_id} from matches list table: {response}")            
    
# -- Utils
async def set_default_info(event):
    info = {
        "match_name" : event['name'],
        "match_id" : event['key'],
        "competition" : event['competitionName'],
        "source" : "Pointsbet",
        "tournament_id" : event['competitionKey']
    }
    return info

if __name__ == "__main__":
    asyncio.run(scrape_data())
        