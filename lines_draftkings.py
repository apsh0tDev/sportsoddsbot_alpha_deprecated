import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from loguru import logger
from connection import scrape
from cloud_connection import scrape_by_site
from actions import exists, update, upload
from utils import verifier_alt, verifier

async def scrape_events():
    table = db.table("matches_list").select("*").eq("source", "Draftkings").execute()
    events_ids = [item['match_id'] for item in table.data]

    tasks = []
    tasks_status = []
    for id in events_ids:
        tasks.append(scrape_event(id))
    
    results = await asyncio.gather(*tasks)
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
            error_sum = db.table("sportsbooks").select("not_available_sum").eq("name", "Draftkings").execute()
            num = error_sum.data[0]['not_available_sum'] + 1        
            db.table("sportsbooks").update({"available" : False, "not_available_sum" : num}).eq("name", "Draftkings").execute()
        else:
            available_sum = db.table("sportsbooks").select("available_sum").eq("name", "Draftkings").execute()
            num = available_sum.data[0]['available_sum'] + 1
            db.table("sportsbooks").update({"available" : True, "available_sum" : num}).eq("name", "Draftkings").execute()
    else:
        logger.info(f"No tasks for Draftkings at the moment.")

async def scrape_event(id):
    url = constants.draftkings_event.format(eventId=id)
    print(url)
    response = await scrape_by_site(url=url, site="DRAFTKINGS", headless=True)
    if response != None and response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            soup = bs4.BeautifulSoup(response, 'html.parser')
            pre = soup.find("pre")
            load = json.loads(pre.text)
            await tidy_up_and_sort(load)
            return "DONE"
    else:
        logger.error(f"None response in Draftkings")
        return "ERROR"
    

async def tidy_up_and_sort(load):
    #First get event information
    if 'events' in load and 'markets' in load and 'selections' in load:
        event = load['events'][0]
        players = [event['participants'][0]['name'], event['participants'][1]['name']]

        for market in load['markets']:
           market_info = await get_odds_for_market(market=market, odds=load['selections'], players=players)
           await market_sorter(event=event, market=market_info, players=players)


#--- Markets
async def market_sorter(event, market, players):
    market_name = market['market_name']
    print(market_name)
    match market_name:
        case "Moneyline":
            await handle_moneyline(event=event, market=market, players=players)

async def handle_moneyline(event, market, players):
    info = await set_default_info(event=event, market=market)
    info['teamA'] = {
        "name" : players[0],
        "odds" : await set_default_odds(market=market, team="teamA")
    }
    info['teamB'] = {
        "name" : players[1],
        "odds" : await set_default_odds(market=market, team="teamB")
    }
    
    to_match, to_update = await get_default_options(info)
    await db_actions(to_match=to_match, to_update=to_update, info=info, table="moneyline")
    
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

#--- Utils
async def set_default_info(event, market):
    info = {
        "match_id" : event['id'],
        "match_name" : event['name'],
        "source" : "Draftkings",
        "isOpen" : market['isOpen']
    }
    return info

async def set_default_odds(market, team):
    info = {
        "americanOdds" : market[team]['americanOdds'],
        "decimalOdds" : market[team]['decimalOdds']
    }
    return info


async def get_odds_for_market(market, odds, players):
    market_info = {
        "market_name" : market['name'],
        "id" : market['id'],
        "isOpen" : False if 'isSupended' in market and market['isSupended'] == True else True
    }

    for odd in odds:
        if odd['marketId'] == market_info['id']:
            #Get team odds
            if 'participants' in odd:
                if odd['participants'][0]['name'] == players[0]:
                    market_info['teamA'] = {
                        "americanOdds" : odd['displayOdds']['american'],
                        "decimalOdds" : odd['displayOdds']['decimal']
                    }

                if odd['participants'][0]['name'] == players[1]:
                    market_info['teamB'] = {
                        "americanOdds" : odd['displayOdds']['american'],
                        "decimalOdds" : odd['displayOdds']['decimal']
                    }
            elif 'participants' not in odd and odd['label'] == "Under":
                market_info['Under'] = {
                    "americanOdds" : odd['displayOdds']['american'],
                    "decimalOdds" : odd['displayOdds']['decimal']
                }
            elif 'participants' not in odd and odd['label'] == "Over":
                market_info['Over'] = {
                    "americanOdds" : odd['displayOdds']['american'],
                    "decimalOdds" : odd['displayOdds']['decimal']
                }

    return market_info



if __name__ == "__main__":
    asyncio.run(scrape_events())
