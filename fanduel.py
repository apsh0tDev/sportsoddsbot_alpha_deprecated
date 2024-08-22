import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from utils import verifier, verifier_alt
from actions import exists, upload
from connection import scrape
from cloud_connection import scrape_b_u

async def scrape_data():
    data = {
        'cmd' : 'request.get',
        'url' : constants.fanduel_url,
        'requestType' : 'request',
        'proxyCountry' : 'UnitedStates'
    }
    response = await scrape(data, "FanDuel")
    if response == None:
       db.table("sportsbooks").update({'available' : False}).eq("name", "FanDuel").execute() 
       print("None response, try backup.")
       await scrape_data_backup()
    else:
        if 'statusCode' in response['solution'] and response['solution']['statusCode'] == 403:
            print("Forbidden response to FanDuel, try backup.")
            await scrape_data_backup()
        else:
            #Not forbidden - Check validity
            is_valid = await verifier(response)
            if is_valid:
                load = json.loads(response['solution']['response'])
                await tidy_up_matches_partial(load)
                await tidy_up_all_matches(load)
                db.table("sportsbooks").update({'available' : True}).eq("name", "FanDuel").execute()
            else:
                logger.error(f"Response not valid from FanDuel, try backup.")
                await scrape_data_backup()


async def scrape_data_backup():
    response = await scrape_b_u(constants.fanduel_url, "FanDuel")
    is_valid = await verifier_alt(response)
    if is_valid:
        load = json.loads(response)
        await tidy_up_matches_partial(load)
        #await tidy_up_all_matches(load)
        db.table("sportsbooks").update({'available' : True}).eq("name", "FanDuel").execute()
    else:
        logger.error(f"Could not retrieve anything from FanDuel.")
        db.table("sportsbooks").update({'available' : False}).eq("name", "FanDuel").execute()

async def tidy_up_matches_partial(load):
    matches_table = db.table("matches_list").select("*").match({"source" : "BetMGM"}).execute()
    table_names = [item['match_name'] for item in matches_table.data]
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
                    for item in table_names:
                        fuzz_ratio = fuzz.token_sort_ratio(item, info['match_name'])
                        if fuzz_ratio >= 70:
                            to_match = { "match_id" : info['match_id'], "match_name" : info['match_name'], "source" : "FanDuel" }
                            value_exists = await exists("matches_list", to_match)
                            if value_exists:
                                print("Already exists, skip")
                            else:
                                response = await upload(table="matches_list", info=info)
                                print(response) 
    await cleaners(matches_ids, "matches_list")            

async def tidy_up_all_matches(load):
    uuid_table = db.table("sets").select("*").execute()

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
                        "match_id": event['eventId'],
                        "match_name" : find_value(event['eventId'], evs),
                    }
                    for item in uuid_table.data:
                        fuzz_ratio = fuzz.token_sort_ratio(item['match_name'], info['match_name'])
                        if fuzz_ratio >= 70:
                            info['uuID'] = item['uuID']
                            to_match = { "match_id" : info['match_id'], "match_name" : info['match_name'], "uuID" : info['uuID'] }
                            value_exists = await exists("fanduel", to_match)
                            if value_exists:
                                print("Already exists, skip")
                            else:
                                response = await upload(table="fanduel", info=info)
                                print(response) 
                    
# -- Cleaners 🧹
async def cleaners(data, table):
    print("Run cleaners 🧹")
    if table == "matches_list":
        matches_table = db.table("matches_list").select("*").eq("source", "FanDuel").execute()
        matches_ids = [item['match_id'] for item in matches_table.data]

        for record_id in matches_ids:
            if record_id not in data:
                response = db.table("matches_list").delete().match({"match_id" : record_id, "source" : "FanDuel"}).execute()
                logger.info(f"Deleting record {record_id} from live matches table: {response}")
        print("Done cleaning 🧹")
    elif table == "fanduel":
        fanduel_table = db.table("matches_list").select("*").eq("source", "FanDuel").execute()
        fanduel_names = [item['match_name'] for item in fanduel_table.data]

        for record_name in fanduel_names:
            if record_name not in data:
                response = db.table("fanduel").delete().match({"match_name" : record_name}).execute()
                logger.info(f"Deleting record {record_name} from fanduel table: {response}")
        print("Done cleaning 🧹")
   
# -- Utils
def find_value(id, group):
    group_keys = [key for key in group]
    for key in group_keys:
        if str(id) == key:
            return group[key]['name']  

if __name__ == "__main__":
    asyncio.run(scrape_data())