import json
import asyncio
import shortuuid
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from utils import verifier
from datetime import datetime
from connection import scrape
from actions import exists, update, upload

async def scrape_data():
    today = datetime.today().strftime('%d/%m/%Y')
    url = constants.scores365_url.format(startDate = today, endDate = today)
    print(url)
    data = {
        'cmd' : 'request.get',
        'url' : url,
        'requestType' : 'request'
    }
    response = await scrape(data, "365Scores")
    if response == None:
        logger.error("Could not get live scores - 365Scores")
    else:
        is_valid = await verifier(response)
        if is_valid:
            try:
                load = json.loads(response['solution']['response'])
                games = load['games']
                competitions = load['competitions']
                await tidy_up_matches_partial(games, competitions)
                await tidy_up_all_matches(games, competitions)
            except Exception as e:
                logger.error(f"Error Parsing 365Scores - {e}")
        else:
            logger.error(f"Invalid response - 365Scores")

#---- Handle Live Scores Table
async def tidy_up_matches_partial(games, tournaments):
    matches = db.table("matches_list").select("*").match({"source" : "BetMGM"}).execute()
    matches_names = [item['match_name'] for item in matches.data]

    matches_to_handle = []
    for game in games:
        if game['statusText'] != "Scheduled":
            match_name = await get_match_name(game)
            for item in matches_names:
                fuzz_ratio = fuzz.token_sort_ratio(item, match_name)
                if fuzz_ratio >= 80:
                    matches_to_handle.append(game)
        else:
            print("Scheduled. Skip")

    matches_ids = []
    for match in matches_to_handle:
        scoresTeamA = await get_match_scores(match['stages'], "teamA")
        scoresTeamB = await get_match_scores(match['stages'], "teamB")

        info = await set_match_info(match, tournaments)
        matches_ids.append(info['match_id'])
        scores_info = await set_scores_info(info, scoresTeamA, scoresTeamB, match['statusText'])
        match_exists_in_live_matches = await exists("live_matches", {"source" : "365Scores", "match_id" : info['match_id'], "match_name" : info['match_name']})
        match_exists_in_scoreboard = await exists("scoreboard", {"source" : "365Scores", "match_id" : info['match_id']})

        if match_exists_in_live_matches and match_exists_in_scoreboard:
            response_scoreboard = await update(table="scoreboard", to_match={"match_id" : info['match_id']}, info={"teamA" : scores_info['teamA'], "teamB" : scores_info['teamB'], "period" : scores_info['period']})
            print(response_scoreboard)
        else:
            response_live = await upload("live_matches", info)
            response_scoreboard = await upload("scoreboard", scores_info)
            print(response_live)
            print(response_scoreboard)

    
    await cleaners(matches_ids, table="live_matches")

#---- Handle Set Checker
async def tidy_up_all_matches(games, tournaments):
    games_all = []
    for game in games:
        if game['statusText'] != "Scheduled":
            match_name = await get_match_name(game)
            current_set = await get_current_set(game['stages'])
            if current_set == None:
                if game['statusText'] == "Final":
                    current_set = "Final" 
            info = {
                "uuID" : shortuuid.uuid(),
                "match_name" : match_name,
                "tournament" : await get_tournament(tournaments, game['competitionId']),
                "current_set" : current_set }
            games_all.append(info['match_name'])
            value_exists = await exists(table="sets", to_match={"match_name" : info['match_name']})
            if value_exists:
                response = await update(table="sets", to_match={"match_name" : info['match_name']}, info={'current_set' : info['current_set']})
                print(response)
            else:
                response = await upload(table="sets", info=info)
                print(response)

        else:
            print("Not live. Skip")

    await cleaners(games_all, "sets")


# -- Cleaners 🧹
async def cleaners(data, table):
    print("Run cleaners 🧹")
    if table == "live_matches":
        matches_table = db.table("live_matches").select("*").execute()
        matches_ids = [item['match_id'] for item in matches_table.data]

        for record_id in matches_ids:
            if record_id not in data:
                response = db.table("live_matches").delete().match({"match_id" : record_id}).execute()
                logger.info(f"Deleting record {record_id} from live matches table: {response}")

        print("Done cleaning 🧹")
    if table == "sets":
        sets_table = db.table("sets").select("*").execute()
        sets_names = [item['match_name'] for item in sets_table.data]

        for record_name in sets_names:
            if record_name not in data:
                response = db.table("live_matches").delete().match({"match_name" : record_name}).execute()
                logger.info(f"Deleting record {record_name} from sets table: {response}")
        print("Done cleaning 🧹")

# -- Utils
async def get_match_name(game):
    match_name = f"{game['homeCompetitor']['name']} vs {game['awayCompetitor']['name']}"
    return match_name

async def get_players_names(match):
    teamA = match['homeCompetitor']['name'] if match['homeCompetitor']['name'] else "Unknown"
    teamB = match['awayCompetitor']['name'] if match['awayCompetitor']['name'] else "Unknown"
    return [teamA, teamB]

async def get_tournament(tournaments, id):
    for tournament in tournaments:
        if id == tournament['id']:
            return tournament['name']

async def get_match_scores(scores, teamName):
    teamA_scores = []
    teamB_scores = []
    if teamName == "teamA":
        for score in scores:
            if score['name'] == "Set 1" or score['name'] == "Set 2" or score['name'] == "Set 3":
                teamA_scores.append(int(score['homeCompetitorScore']) if score['homeCompetitorScore'] > 0 else 0)
        return teamA_scores
    elif teamName == "teamB":
        for score in scores:
            if score['name'] == "Set 1" or score['name'] == "Set 2" or score['name'] == "Set 3":
                teamB_scores.append(int(score['awayCompetitorScore']) if score['awayCompetitorScore'] > 0 else 0)
        return teamB_scores
    
async def set_match_info(game, tournaments):
    info = {
        "match_id" : game['id'],
        "source" : "365Scores",
        "status" : "Live",
        "match_name" : f"{game['homeCompetitor']['name']} vs {game['awayCompetitor']['name']}",
        "tournament" : await get_tournament(tournaments=tournaments, id=game['competitionId']),
        "tournament_display_name" : game['competitionDisplayName'],
        "date" : game['startTime'],
        "teamA" : game['homeCompetitor']['name'],
        "teamB" : game['awayCompetitor']['name']
    }

    return info

async def set_scores_info(game, scoresTeamA, scoresTeamB, statusText):
    period = "Unknown"
    match statusText:
        case "Set 1":
            period = "Set 1"
        case "Set 2":
            period = "Set 2"
        case "Set 3":
            period = "Set 3"
 
    info = {
        "match_id" : game['match_id'],
        "period" : period,
        "teamA" : scoresTeamA,
        "teamB" : scoresTeamB,
        "source" : "365Scores"
        }
    
    return info

async def get_current_set(stages):
    current_set = None

    for stage in stages:
        # Check if the stage is a set and is live
        if "Set" in stage['name'] and stage.get('isLive', False) and not stage.get('isEnded', False):
            current_set = stage['name']
            break  # Once we find the live set, we can break the loop

    return current_set

# -- End of Utils

if __name__ == "__main__":
    asyncio.run(scrape_data())