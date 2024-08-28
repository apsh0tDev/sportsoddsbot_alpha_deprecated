import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from loguru import logger
from cloud_connection import scrape_by_site
from actions import exists, update, upload
from utils import verifier_alt

async def scrape_events():
    table = db.table("matches_list").select("*").eq("source", "Betrivers").execute()
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
            error_sum = db.table("sportsbooks").select("not_available_sum").eq("name", "Betrivers").execute()
            num = error_sum.data[0]['not_available_sum'] + 1        
            db.table("sportsbooks").update({"available" : False, "not_available_sum" : num}).eq("name", "Betrivers").execute()
        else:
            available_sum = db.table("sportsbooks").select("available_sum").eq("name", "Betrivers").execute()
            num = available_sum.data[0]['available_sum'] + 1
            db.table("sportsbooks").update({"available" : True, "available_sum" : num}).eq("name", "Betrivers").execute()
    else:
        logger.info(f"No tasks for Betrivers at the moment.")

async def scrape_event(id):
    logger.info(f"Starting task {id} - Betrivers")
    url = constants.betrivers_event_url.format(eventId=id)
    print(url)
    response = await scrape_by_site(url, "BETRIVERS", True)
    if response != None and response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            soup = bs4.BeautifulSoup(response, 'html.parser')
            pre = soup.find("pre")
            if pre and "No event with eventId" not in pre.text:
                load = json.loads(pre.text)
                await tidy_up_and_sort(load)
                return "DONE"
            elif "No event with eventId" in pre.text:
                response = db.table("matches_list").delete().match({"match_id" : id, "source" : "Betrivers"}).execute()
                logger.info(f"Deleting match {id} from matches list: {response}")
                return "DONE"
            else:
                print("Error in bs4")
                return "ERROR"                                    
    else:
        print("None response.")
        return "ERROR"
    
async def tidy_up_and_sort(load):
    if 'offeringGroups' in load:
        match_id = load['id']
        match_name = load['name']
        players = [load['participants'][0]['name'], load['participants'][1]['name']]
        for offeringGroup in load['offeringGroups']:
            if 'criterionGroups' in offeringGroup:
                for offer in offeringGroup['criterionGroups']:
                    await market_sorter(offer, match_name, players, match_id)
#--- Markets
async def market_sorter(market, match_name, players, match_id):
    market_name = market['criterionName']
    
    match market_name:
        case "Moneyline":
            await handle_moneyline(market, match_name, players, match_id)

#--- Moneyline
async def handle_moneyline(market, match_name, players, match_id):
                 
    info = await set_default_info(match_name, match_id)
    info['teamA'] = await set_default_odds(market['betOffers'][0], players, "teamA")
    info['teamB'] = await set_default_odds(market['betOffers'][0], players, "teamB")

    to_match, to_update = await get_default_options(info)
    await(db_actions(to_match=to_match, table="moneyline", to_update=to_update, info=info))

#--- Utils
async def set_default_info(match_name, match_id):
    info = {
        "match_id" : match_id,
        "match_name" : match_name,
        "source" : "Betrivers",
        "isOpen" : True #TODO change this and all to individual
    }
    return info

async def set_default_odds(offers, players, team):
    if 'outcomes' in offers:
        if team == "teamA":
            american_odds = None
            decimal_odds = None

            for outcome in offers['outcomes']:
                if outcome['label'] == players[0]:
                    american_odds = outcome['oddsAmerican']
                    decimal_odds = outcome['odds']

            info = {
                "name" : players[0],
                "odds" : {
                    "americanOdds" : american_odds,
                    "decimal_odds" : decimal_odds
                }
            }
            return info
        elif team == "teamB":
            american_odds = None
            decimal_odds = None

            for outcome in offers['outcomes']:
                if outcome['label'] == players[1]:
                    american_odds = outcome['oddsAmerican']
                    decimal_odds = outcome['odds']

            info = {
                "name" : players[1],
                "odds" : {
                    "americanOdds" : american_odds,
                    "decimal_odds" : decimal_odds
                }
            }
            return info
        
#----Db Actions
async def get_default_options(info):
    to_match = { "match_name" : info['match_name'], "match_id" : info['match_id'], "source" : "Draftkings" }
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