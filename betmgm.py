import json
import asyncio
import constants
from db import db
from rich import print
from loguru import logger
from connection import scrape
from utils import remove_parentheses, verifier

#==== General call
async def scrape_data():
    data = {
        'cmd' : 'request.get',
        'url' : constants.betmgm_url,
        'requestType' : 'request'
    }

    response = await scrape(data, "BetMGM")
    if response == None:
        print(None)
    else:
        try:
            is_valid = await verifier(response)
            if is_valid:
                load = json.loads(response['solution']['response'])

                #Tables
                matches_list = db.table("matches_list").select("*").match({"source" : "BetMGM"}).execute()
                schedule = db.table("schedule").select("*").execute()
                #Tables Ids
                matches_list_ids = [item['match_id'] for item in matches_list.data]
                schedule_list_ids = [item['match_id'] for item in schedule.data]

                if 'widgets' in load and 'payload' in load['widgets'][0] and 'fixtures' in load['widgets'][0]['payload']:
                    #Fixtures are all the matches
                    fixtures = load['widgets'][0]['payload']['fixtures']

                    for match in fixtures:
                        #Live matches
                        if match['stage'] == "Live":
                            info = {
                                "match_name" : remove_parentheses(match['name']['value']),
                                "match_id" : match['id'],
                                "tournament" : match['tournament']['name']['value'],
                                "competition" : match['competition']['name']['value'],
                                "source" : "BetMGM",
                            }

                            if info['match_id'] not in matches_list_ids:
                                response = db.table("matches_list").insert(info).execute()
                                print(response)
                            else:
                                print(f"{info['match_id']} already in table. Skipping.")

                    await cleaners(fixtures, schedule_list_ids, matches_list_ids)


        except Exception as e:
            logger.error(f"An error occurred while parsing BetMGM: {e}")
            db.table("sportsbooks").update({"available" : False}).eq("name", "BetMGM").execute()

# -- Cleaners 🧹
async def cleaners(data, schedule, matches):
    print("Run cleaners 🧹")
    betmgm_table = db.table("betmgm").select("*").execute()
    betmgm_ids = [item['match_id'] for item in betmgm_table.data]
    prematch_ids = []
    live_ids = []
    
    for match in data:
                if match['stage'] == "PreMatch":
                    prematch_ids.append(match['id'])
                elif match['stage'] == "Live":
                    live_ids.append(match['id'])

    for record_id in schedule:
        if record_id not in prematch_ids:
            response = db.table("schedule").delete().eq("match_id", record_id).execute()
            logger.info(f"Deleting record {record_id} from schedule table: {response}")

    for record_id in matches:
        if record_id not in live_ids:
            response = db.table("matches_list").delete().eq("match_id", record_id).execute()
            logger.info(f"Deleting record {record_id} from matches list table: {response}")

    for record_id in betmgm_ids:
        if record_id not in live_ids:
            response = db.table("betmgm").delete().eq("match_id", record_id).execute()
            logger.info(f"Deleting record {record_id} from betmgm table: {response}")

    print("Done cleaning 🧹")


# -- Scores 📆
async def handle_scores():
    print("scores")       

if __name__ == "__main__":
    asyncio.run(scrape_data())
