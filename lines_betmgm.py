import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from connection import scrape
from actions import exists, update, upload
from utils import verifier, remove_parentheses

async def scrape_events():
    table = db.table("matches_list").select("*").match({"source" : "BetMGM"}).execute()
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

    # Log the task statuses or handle them as needed
    logger.info(f"Task statuses: {tasks_status}")
    error_count = tasks_status.count("ERROR")
    error_percentage = (error_count / len(tasks_status)) * 100
    if error_percentage > 45:
        error_sum = db.table("sportsbooks").select("not_available_sum").eq("name", "BetMGM").execute()
        num = error_sum.data[0]['not_available_sum'] + 1
        db.table("sportsbooks").update({"available" : False, "not_available_sum" : num}).eq("name", "BetMGM").execute()
    else:
        available_sum = db.table("sportsbooks").select("available_sum").eq("name", "BetMGM").execute()
        num = available_sum.data[0]['available_sum'] + 1
        db.table("sportsbooks").update({"available" : True, "available_sum" : num}).eq("name", "BetMGM").execute()

async def scrape_event(match_id):
    try:
        logger.info(f"Starting task {match_id} - BetMGM")
        url = constants.betmgm_events.format(id=match_id)
        data = {
            'cmd' : 'request.get',
            'url' : url,
            'requestType' : 'request'
        }
        response = await scrape(data, "BetMGM")
        if response == None:
            logger.info(f"Ending task {match_id} with error - BetMGM")
            return "ERROR"
        else:
            is_valid = await verifier(response)
            if is_valid:
                await tidy_up_and_sort(response['solution']['response'])
            else:
                return "ERROR"
        
        logger.info(f"Ending task {match_id} - BetMGM")
        return f"DONE"        
    except Exception as e:
        logger.error(f"Task {match_id} encountered an error: {e}")
        return "ERROR"
    
async def tidy_up_and_sort(match):
    load = json.loads(match)
    if 'fixture' in load:
        match_id = load['fixture']['id']
        match_name = remove_parentheses(load['fixture']['name']['value'])
        games = load['fixture']['games']
        match_players = [remove_parentheses(load['fixture']['participants'][0]['name']['value']), remove_parentheses(load['fixture']['participants'][1]['name']['value'])]
        for game in games:
            await market_sorter(game=game,
                                match_name=match_name,
                                match_id=match_id,
                                match_players=match_players)


#==== Market sorter
async def market_sorter(game, match_name, match_id, match_players):
    game_name = game['name']['value']

    match game_name:
       # -- Set Winners
       # -- Match Winner
        case "Match Winner":
            await handle_match_winner(game, match_name, match_id, match_players)         

#-- Match Winner
async def handle_match_winner(game, match_name, match_id, match_players):
    info = await set_default_info(match_name=match_name, match_id=match_id, game=game)
    info['teamA'] = await set_default_odds(match_players=match_players, game=game, team=0)
    info['teamB'] = await set_default_odds(match_players=match_players, game=game, team=1)

    to_match, to_update = await get_default_options(info)
    await db_actions(to_match=to_match, to_update=to_update, info=info, table="match_winner")

#-- Set Betting
#-- Correct Score - Set 1
#-- Correct Score - Set 2
#-- Correct Score - Set 3
#-- Set Winners
async def handle_set_winner(game, match_name, match_id, match_players, set_number):
    info = await set_default_info(match_name=match_name, match_id=match_id, game=game)
    info['teamA'] = await set_default_odds(match_players=match_players, game=game, team=0)
    info['teamB'] = await set_default_odds(match_players=match_players, game=game, team=1)

    if set_number == "Set 1":
        table = "set_one_winner"
    elif set_number == "Set 2":
        table = "set_two_winner"
    elif set_number == "Set 3":
        table = "set_three_winner"

    to_match, to_update = await get_default_options(info)
    await db_actions(to_match=to_match, to_update=to_update, info=info, table=table)

#---- Utils
async def set_default_info(match_name, match_id, game):
    info = {
        "match_id" : match_id,
        "match_name" : match_name,
        "source" : "BetMGM",
        "isOpen" : True if game['visibility'] == "Visible" else False
    }
    return info

async def set_default_odds(match_players, game, team):
    odds = {
        "name" : match_players[team],
        "americanOdds" : game['results'][team]['americanOdds'],
        "decimalOdds" : game['results'][team]['odds']
    }

    return odds

#----- Db Actions
async def get_default_options(info):
    to_match = { "match_name" : info['match_name'], "match_id" : info['match_id'], "source" : "BetMGM" }
    to_update = { "teamA" : info['teamA'], "teamB" : info['teamB'], "isOpen": info['isOpen']}

    return to_match, to_update

async def db_actions(to_match, to_update, info, table):
    value_exists = await exists(to_match=to_match, table=table)
    if value_exists:
        response = await update(table=table, to_match=to_match, info=to_update)
        print(response)
    else:
        response = await upload(table=table, info=info)
        print(response)

if __name__ == "__main__":
    asyncio.run(scrape_events())