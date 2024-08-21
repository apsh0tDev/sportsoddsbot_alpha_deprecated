import json
import asyncio
import constants
from db import db
from rich import print
from loguru import logger
from connection import scrape
from cloud_connection import scrape_b_u
from actions import exists, update, upload
from utils import verifier, decimal_to_american, verifier_alt

async def scrape_events():
    table = db.table("matches_list").select("*").match({"source": "Pointsbet"}).execute()
    tasks = []
    tasks_status = []

    if len(table.data) > 0:
        for task in table.data:
            tasks.append(scrape_event(task['match_id']))
    
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
            error_sum = db.table("sportsbooks").select("not_available_sum").eq("name", "Pointsbet").execute()
            num = error_sum.data[0]['not_available_sum'] + 1        
            db.table("sportsbooks").update({"available" : False, "not_available_sum" : num}).eq("name", "Pointsbet").execute()
        else:
            available_sum = db.table("sportsbooks").select("available_sum").eq("name", "Pointsbet").execute()
            num = available_sum.data[0]['available_sum'] + 1
            db.table("sportsbooks").update({"available" : True, "available_sum" : num}).eq("name", "Pointsbet").execute()
    else:
        logger.info(f"No tasks for Pointsbet at the moment.")

async def scrape_event(match_id):
    url = constants.pointsbet_event_url.format(eventId=match_id)
    data = {
        'cmd' : 'request.get',
        'url' : url,
        'requestType' : 'request',
        'proxyCountry' : 'UnitedStates'
    }
    response = await scrape(data, "Pointsbet")
    if response == None:
        print("None value, retry")
        db.table("sportsbooks").update({"available" : False}).eq("name", "Pointsbet").execute()
        return "ERROR"
    else:
        if 'statusCode' in response['solution'] and response['solution']['statusCode'] == 200:
            is_valid = await verifier(response)
            if is_valid:
                load = json.loads(response['solution']['response'])
                await tidy_up_and_sort(load)
                return "DONE"
            else:
                print("Not valid response in Pointsbet, retry")
                return "ERROR"
        else:
            print("Forbidden, retry")
            alt = await scrape_alt(match_id)
            return alt
    
async def scrape_alt(match_id):
    url = constants.pointsbet_event_url.format(eventId=match_id)
    response = await scrape_b_u(url, "Pointsbet")
    if response == '' or None:
        logger.error("Could not scrape anything in PointsBet")
        return "ERROR"
    else:
        is_valid = await verifier_alt(response)
        if is_valid:
            load = json.loads(response)
            await tidy_up_and_sort(load)
            return "DONE"
        else:
            logger.error("Not a valid response in PointsBet")
            db.table("sportsbooks").update({"available" : False}).eq("name", "Pointsbet").execute()
            return "ERROR"
        
async def tidy_up_and_sort(load):
    match_name = load['name']
    players = [load['homeTeam'], load['awayTeam']]
    if 'fixedOddsMarkets' in load:
        for market in load['fixedOddsMarkets']:
            await market_sorter(market, players, match_name)

#==== Market sorter
async def market_sorter(game, players, match_name):
    game_name = game['eventName']

    match game_name:
        case "Match Result":
            await handle_match_winner(game, match_name)
        case "1st Set Winner":
            await handle_set_winner(game, match_name, "Set 1")
        case "Set Winner (Set 2)":
            await handle_set_winner(game, match_name, "Set 2")
        case "Set Winner (Set 3)":
            await handle_set_winner(game, match_name, "Set 3")

#--- Match Result
async def handle_match_winner(game, match_name):
    info = await set_default_info(game, match_name)
    info['teamA'] = await set_default_odds(game['outcomes'], "teamA")
    info['teamB'] = await set_default_odds(game['outcomes'], "teamB" )

    to_match, to_udpdate = await get_default_options(info)
    await db_actions(table="match_winner", to_match=to_match, to_update=to_udpdate, info=info)
    
#--- Set Winners
async def handle_set_winner(game, match_name, set_number):

    info = await set_default_info(game, match_name)
    info['teamA'] = await set_default_odds(game['outcomes'], "teamA")
    info['teamB'] = await set_default_odds(game['outcomes'], "teamB" )

    if set_number == "Set 1":
        table = "set_one_winner"
    elif set_number == "Set 2":
        table = "set_two_winner"
    elif set_number == "Set 3":
        table = "set_three_winner"
    to_match, to_udpdate = await get_default_options(info)
    await db_actions(table=table, to_match=to_match, to_update=to_udpdate, info=info)

#--- Correct Score
#--- Set Betting

#--- Utils
async def set_default_info(game, match_name):
    info = {
        "match_id" : game['eventKey'],
        "match_name" : match_name,
        "source" : "Pointsbet",
        "isOpen" : True if game['isOpenForBetting'] == True else False
    }
    return info    

async def set_default_odds(outcomes, team):
    info = {}
    if team == "teamA":
        info = {
                "name" : outcomes[0]['name'],
                "americanOdds" : round(decimal_to_american(outcomes[0]['price'])),
                "decimalOdds" :  outcomes[0]['price']            
        }
    elif team == "teamB":
        info = {
                "name" : outcomes[1]['name'],
                "americanOdds" : round(decimal_to_american(outcomes[1]['price'])),
                "decimalOdds" :  outcomes[1]['price']            
        }        
    return info
    
#---- DB Actions
async def get_default_options(info):
    to_match = { "match_name" : info['match_name'], "match_id" : info['match_id'], "source" : "Pointsbet" }
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