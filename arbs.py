import json
import asyncio
import shortuuid
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from constants import available_markets
from datetime import datetime, timezone, timedelta
from notifier import arbitrage_notification, edit_message

logger.add("arbitrage.log", filter=lambda record: "arbitrage" in record["extra"])
logger.add("notifications.log", filter=lambda record: "notification" in record["extra"])

async def call_all_markets():
    tasks = []
    for market in available_markets:
        tasks.append(get_market(market))
    
    asyncio.gather(*tasks)

async def get_market(marke_name):
    match marke_name:
        case "SET_ONE_WINNER":
            await set_winner("Set 1 Winner")
        case "SET_TWO_WINNER":
            await set_winner("Set 2 Winner")
        case "SET_THREE_WINNER":
            await set_winner("Set 3 Winner")
        case "MATCH_WINNER":
            await match_winner()

async def set_winner(set_name):
    if set_name == "Set 1 Winner":
        table = "set_one_winner"
    elif set_name == "Set 2 Winner":
        table = "set_two_winner"
    elif set_name == "Set 3 Winner":
        table = "set_three_winner"
    response = db.table(table).select("*").execute()
    
    #Group matches by name for later comparison
    groups = await group_matches(response.data)
    arbitrages = await calculate_arbitrage(odds=groups, market=set_name)

    if len(arbitrages) > 0:
        await db_actions(arbitrages)

async def match_winner():
    response = db.table("match_winner").select("*").execute()
     #Group matches by name for later comparison
    groups = await group_matches(response.data)

    arbitrages = await calculate_arbitrage(odds=groups, market="Match Winner")
    if len(arbitrages) > 0:
        await db_actions(arbitrages)

#======== Utils
async def group_matches(matches, similarity_threshold=70):
    grouped_matches = []

    for match in matches:
        match_found = False
        match_name = match['match_name']
        # Try to find an existing group that this match is similar to
        for group in grouped_matches:
            if fuzz.token_sort_ratio(match_name, group['match_name']) >= similarity_threshold:
                group['odds'].append({
                    'source': match['source'],
                    'teamA': match['teamA'],
                    'teamB': match['teamB'],
                    'isOpen' : match['isOpen']
                })
                # If BetMGM is in this match, update the group's match name to BetMGM's match name
                if match['source'] == 'BetMGM':
                    group['match_name'] = match_name
                match_found = True
                break

        # If no similar group is found, create a new group
        if not match_found:
            grouped_matches.append({
                'match_name': match_name.strip(),
                'odds': [{
                    'source': match['source'],
                    'teamA': match['teamA'],
                    'teamB': match['teamB'],
                    'isOpen' : match['isOpen']
                }]
            })
    return grouped_matches

#======== ARBITRAGE CALCULATOR =======
async def calculate_arbitrage(odds, market):
    availability = db.table("sportsbooks").select("name").match({"available" : True}).execute()
    available_sportsbooks = [item['name'] for item in availability.data]
    arbitrages = []
    market_name = market
    for match in odds:
        match_name = match['match_name']
        best_odds_teamA = None
        best_odds_teamB = None
        
        for odds in match['odds']:
            if odds['source'] in available_sportsbooks:
                # Safely extract the decimal odds
                teamA_odds = odds['teamA'].get('odds', odds['teamA']).get('decimalOdds')
                teamB_odds = odds['teamB'].get('odds', odds['teamB']).get('decimalOdds')
                
                # Ensure that teamA_odds and teamB_odds are not None and are valid numbers
                if teamA_odds is not None and (best_odds_teamA is None or teamA_odds > best_odds_teamA['decimalOdds']):
                    best_odds_teamA = {
                        'source': odds['source'],
                        'decimalOdds': round(teamA_odds, 2),
                        'isOpen' : odds['isOpen']
                    }

                if teamB_odds is not None and (best_odds_teamB is None or teamB_odds > best_odds_teamB['decimalOdds']):
                    best_odds_teamB = {
                        'source': odds['source'],
                        'decimalOdds': round(teamB_odds, 2),
                        'isOpen' : odds['isOpen']
                    }


        if best_odds_teamA and best_odds_teamB:
            inv_teamA = 1 / best_odds_teamA['decimalOdds']
            inv_teamB = 1 / best_odds_teamB['decimalOdds']
            total_inverse = inv_teamA + inv_teamB
            if total_inverse < 1:
                arbitrage_opportunity = {
                    'match_name': match_name.strip(),
                    'teamA': best_odds_teamA,
                    'teamB': best_odds_teamB,
                    'arbitrage_percentage': (1 - total_inverse) * 100,
                    'market' : market_name
                }
                arbitrages.append(arbitrage_opportunity)

    return arbitrages

async def check_arbitrages():
    # Fetch all records from the arbitrages table
    response = db.table("arbitrages").select("*").execute()
    arbs_table = response.data if response.data else []

    current_time = datetime.now(timezone.utc)
    two_minutes_ago = current_time - timedelta(minutes=2)

    for arbitrage in arbs_table:
        created_at = datetime.fromisoformat(arbitrage['created_at'].replace('Z', '+00:00'))
        arbitrage_percentage = float(arbitrage['arbitrage_percentage'])
        if created_at > two_minutes_ago and arbitrage['teamA']['isOpen'] and arbitrage['teamB']['isOpen'] and arbitrage['notification_id'] == None:
            print(f"Arbitrage opportunity for match '{arbitrage['match_name']}' has been there for more than 2 minutes.")
            print(arbitrage)
            logger.bind(arbitrage=True).info(f'Arbitrage opportunity for match: {arbitrage['match_name']} {arbitrage['id']}')
            await arbitrage_notification(arbitrage_data=arbitrage)

async def clean_arbitrages():
    arbitrages = db.table("arbitrages").select("*").execute()
    matches = db.table("matches_list").select("*").execute()
    matches_names = [item['match_name'].strip() for item in matches.data]
    print("Run arbitrages cleaners 🧹")
    for arb in arbitrages.data:
        arb_match_name = arb['match_name'].split('vs')
        arb_match_name = ('-').join(arb_match_name)
        if arb_match_name not in matches_names:
            await edit_message(arb, True)
        
    

#======== DB Actions =======
async def db_actions(arbitrages):
    arbs_table = db.table("arbitrages").select("*").execute()
    arbs_data = arbs_table.data
    arbs_names = [item['match_name'] for item in arbs_data]

    for arbitrage in arbitrages:
        if arbitrage['match_name'] in arbs_names:
            # Find the corresponding record in arbs_data
            matching_arbitrage = next(item for item in arbs_data if item['match_name'] == arbitrage['match_name'])
            
            # Update existing arbitrage record
            res = db.table("arbitrages").update({
                'teamA': arbitrage['teamA'],
                'teamB': arbitrage['teamB'],
                'arbitrage_percentage': arbitrage['arbitrage_percentage']
            }).eq('match_name', arbitrage['match_name']).execute()
            
            print(f"Update record - ", res)
            
            # Now call the edit_message function with the updated data
            updated_arbitrage_data = matching_arbitrage
            updated_arbitrage_data['teamA'] = arbitrage['teamA']
            updated_arbitrage_data['teamB'] = arbitrage['teamB']
            updated_arbitrage_data['arbitrage_percentage'] = arbitrage['arbitrage_percentage']
            
            # Ensure the isOpen status is correctly passed along
            updated_arbitrage_data['teamA']['isOpen'] = arbitrage['teamA']['isOpen']
            updated_arbitrage_data['teamB']['isOpen'] = arbitrage['teamB']['isOpen']
            
            # Pass the updated arbitrage data to edit_message
            await edit_message(updated_arbitrage_data, False)
        else:
            # Insert new arbitrage record
            res = db.table("arbitrages").insert({
                'match_name': arbitrage['match_name'],
                'teamA': arbitrage['teamA'],
                'teamB': arbitrage['teamB'],
                'market': arbitrage['market'],
                'arbitrage_percentage': arbitrage['arbitrage_percentage'],
                'uuID': shortuuid.uuid()
            }).execute()
            print(f"New record - ", res)


async def call_get_markets_every_20_seconds():
    
    while True:
        await call_all_markets()
        await check_arbitrages()
        await clean_arbitrages()
        print("Running...")
        await asyncio.sleep(20)


if __name__ == "__main__":
    print("Run arbs...")
    asyncio.run(call_get_markets_every_20_seconds())