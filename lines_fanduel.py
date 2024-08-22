import json 
import asyncio
import constants
from db import db
from rich import print 
from loguru import logger
from connection import scrape
from cloud_connection import scrape_b_u
from actions import exists, update, upload
from utils import verifier, extract_players, verifier_alt

async def scrape_events():
    table = db.table("matches_list").select("*").eq("source", "FanDuel").execute()
    tasks = []
    tasks_status = []

    if len(table.data) > 0:
        for task in table.data:
            tasks.append(scrape_event(task['match_id']))
            await asyncio.sleep(5)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            tasks_status.append("ERROR")
        else:
            tasks_status.append(result)

    logger.info(f"Task statuses: {tasks_status}")
    error_count = tasks_status.count("ERROR")
    if len(tasks_status) > 0:
        error_percentage = (error_count / len(tasks_status)) * 100
        if error_percentage > 45:
            error_sum = db.table("sportsbooks").select("not_available_sum").eq("name", "FanDuel").execute()
            num = error_sum.data[0]['not_available_sum'] + 1        
            db.table("sportsbooks").update({"available" : False, "not_available_sum" : num}).eq("name", "FanDuel").execute()
        else:
            available_sum = db.table("sportsbooks").select("available_sum").eq("name", "FanDuel").execute()
            num = available_sum.data[0]['available_sum'] + 1
            db.table("sportsbooks").update({"available" : True, "available_sum" : num}).eq("name", "FanDuel").execute()
    else:
        logger.info(f"No tasks for FanDuel at the moment.")

async def scrape_event(match_id):
    logger.info(f"Starting task {match_id} - FanDuel")
    url = constants.fanduel_event_url.format(id=match_id, tab=constants.fanduel_tabs[3]['case'])
    data = {
        'cmd' : 'request.get',
        'url' : constants.fanduel_url,
        'requestType' : 'request',
        'proxyCountry' : 'UnitedStates'
    }
    response = await scrape(data, "FanDuel")
    if response != None and response != '':
        if 'statusCode' in response['solution'] and response['solution']['statusCode'] == 403:
            print("Forbidden, try backup")
            alt = await scrape_event_alt(match_id)
            return alt
        else:
            is_valid = await verifier(response)
            if is_valid:
                load = json.loads(response['solution']['response'])
                await tidy_up_and_sort(load)
                return "DONE"
            else:
                print("Not valid, try backup")
                alt = await scrape_event_alt(match_id)
                return alt
    else:
        print("None response, try backup")
        alt = await scrape_event_alt(match_id)
        return alt



async def scrape_event_alt(match_id):
    url = constants.fanduel_event_url.format(id=match_id, tab=constants.fanduel_tabs[3]['case'])
    response = await scrape_b_u(url, "FanDuel")
    if response != None and response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            load = json.loads(response)
            await tidy_up_and_sort(load)
            return "DONE" 
        else:
            print("Not valid response from Fanduel.")
            return "ERROR"
    else:
        print("None response from Fanduel.")
        return "ERROR" 

async def tidy_up_and_sort(load):
    if 'attchments' in load:
        if 'attachments' in load and 'events' in load['attachments']:
            event_key = [key for key in load['attachments']['events']]
            match_name = load['attachments']['events'][event_key[0]]['name']
            players = extract_players(load['attachments']['events'][event_key[0]]['name'])

            if 'markets' in load['attachments']:
                markets = load['attachments']['markets']
                markets_keys = [key for key in markets]
                for key in markets_keys:
                    market = markets[key]
                    await market_sorter(market, players, match_name)
                
        if 'events' not in load['attachments']:
            logger.info(f"Match {match_name} ended - Deleting...")
            res = db.table("matches_list").delete().match({"match_name" : match_name, "source" : "FanDuel"}).execute()
            print(res)

# === Markets
async def market_sorter(event, players, match_name):
    game_name = event['marketName']
    game_type = event['marketType']

    match game_name:
        #--- Set Winner
        case "Set 1 Winner":
            await handle_set_winner(event, players, match_name, "Set 1")
        case "Set 2 Winner":
            await handle_set_winner(event, players, match_name, "Set 2")
        case "Set 3 Winner":
            await handle_set_winner(event, players, match_name, "Set 3")

#---- Set Winners
async def handle_set_winner(event, players, match_name, set_number):
    info = await set_default_info(event=event, match_name=match_name)
    info['teamA'] = {"name": players[0], "odds" : await set_default_odds(event['runners'], 0)}
    info['teamB'] = {"name": players[1], "odds" : await set_default_odds(event['runners'], 1)}

    to_match, to_update = await get_default_options(info)
    if set_number == "Set 1":
        table = "set_one_winner"
    elif set_number == "Set 2":
        table = "set_two_winner"
    elif set_number == "Set 3":
        table = "set_three_winner"

    await db_actions(to_match=to_match, to_update=to_update, info=info, table=table)


#---- Utils

async def set_default_info(event, match_name):
    info = {
        "match_id" : event['eventId'],
        "match_name" : match_name,
        "isOpen" : True if event['marketStatus'] == "OPEN" else False,
        "source" : "FanDuel"
    }

    return info

async def set_default_odds(odds, team_number):
    win_runner_odds = odds[team_number].get('winRunnerOdds')
    if win_runner_odds:
        american_odds = win_runner_odds.get('americanDisplayOdds', {}).get('americanOdds')
        decimal_odds = win_runner_odds.get('trueOdds', {}).get('decimalOdds', {}).get('decimalOdds')
        if decimal_odds is not None:
            decimal_odds = round(decimal_odds, 2)
    else:
        american_odds = None
        decimal_odds = None

    return {
        "americanOdds": american_odds,
        "decimalOdds" : decimal_odds
    }

#---- Db Actions
async def get_default_options(info):
    to_match = { "match_name" : info['match_name'], "match_id" : info['match_id'], "source" : "FanDuel" }
    to_update = { "teamA" : info['teamA'], "teamB" : info['teamB'], "isOpen": info['isOpen']}

    return to_match, to_update

async def db_actions(to_match, to_update, info, table):
    value_exists = await exists(to_match=to_match, table=table)
    if value_exists:
        response = await update(to_match=to_match, info=to_update, table=table)
        print(response)
    else:
        response = await upload(info=info, table=table)
        print(response)
    
if __name__ == "__main__":
    asyncio.run(scrape_events())