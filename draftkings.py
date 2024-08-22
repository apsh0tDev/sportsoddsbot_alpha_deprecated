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
from cloud_connection import scrape_by_site

async def scrape_data():
    tournaments = db.table("featured_tournaments").select("*").eq("source", "Draftkings").execute()
    matches = db.table("matches_list").select("*").execute()

    matches_tournaments = list(set([item['competition'] for item in matches.data]))
    
    tournament_ids = []
    for tournament in tournaments.data:
        tournament_name = tournament['display_name']
        for item in matches_tournaments:
            fuzz_ratio = fuzz.token_sort_ratio(item, tournament_name)
            if fuzz_ratio >= 70:
                tournament_ids.append(tournament['key'])

    tasks = []
    for id in tournament_ids:
        tasks.append(scrape_tournament(id))

    await asyncio.gather(*tasks)

async def scrape_tournament(id):
    url = constants.draftkings_tournaments.format(id=id)
    data = {
        'cmd' : 'request.get',
        'url' : url,
        'requestType' : 'request',
        'proxyCountry' : 'UnitedStates'
    }
    response = await scrape(data, "FanDuel")
    if response != None and response != '':
        if 'statusCode' in response['solution'] and response['solution']['statusCode'] != 403:
            is_valid = await verifier(response)
            if is_valid:
                load = json.loads(response['solution']['response'])
                await tidy_up_tournaments(load=load)
            else:
                print("Not valid, try backup")
        else:
            print("Forbidden, try backup")
    else:
        print("None response, try backup")

async def tidy_up_tournaments(load):
    matches = db.table("matches_list").select("*").execute()
    matches_name = [item['match_name'] for item in matches.data]
    if 'eventGroup' in load and 'events' in load['eventGroup']:
        events = load['eventGroup']['events']
        for event in events:
            info = await set_match_default_info(event=event)
            for item in matches_name:
                fuzz_ratio = fuzz.token_sort_ratio()

async def set_match_default_info(event):
    info = {
        "match_id" : event['eventId'],
        "match_name" : event['name'],
        "source" : "Draftkings",
        "competition" : event['eventGroupName'],
        "tournament_id" : event['eventGroupId']
    }
    return info


if __name__ == "__main__":
    asyncio.run(scrape_data())

