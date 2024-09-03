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
                await tidy_up_all_matches(games, competitions)
            except Exception as e:
                logger.error(f"Error Parsing 365Scores - {e}")
        else:
            logger.error(f"Invalid response - 365Scores")

#---- Handle Live Scores Table
async def tidy_up_all_matches(games, tournaments):
    matches_to_handle = []
    matches_to_schedule = []
    live_sets = ["Set 1", "Set 2", "Set 3", "Set 4", "Set 5"]
    for game in games:
        if game['statusText'] == "Scheduled":
            matches_to_schedule.append(game)
        elif game['statusText'] in live_sets:
            matches_to_handle.append(game)
        elif game['statusText'] == "Final" and 'justEnded' in game and game['justEnded'] == True:
            matches_to_handle.append(game)

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
    
    await cleaners(matches_ids, "live_matches")

    #Schedule
    await handle_schedule(matches_to_schedule)

#---- Handle Schedule
async def handle_schedule(matches):
    for match in matches:
        info = await set_schedule_info(match)
        to_match = { "match_name" : info['match_name'], "match_id" : info['match_id'] }
        match_exists = await exists(table="schedule", to_match=to_match)
        if match_exists:
            print("Match exists, skip")
        else:
            response = await upload(table="schedule", info=info)
            print(response)

    matches_ids = [item['id'] for item in matches]
    await cleaners(matches_ids, "schedule")

# -- Cleaners 🧹
async def cleaners(data, table):
    table_name = table
    print("Run cleaners 🧹")
    table = db.table(table_name).select("*").execute()
    ids = [int(item['match_id']) for item in table.data]

    for record_id in ids:
        if record_id not in data:
            logger.info(f"Deleting {record_id} from {table_name} table")
            db.table(table_name).delete().eq("match_id", record_id).execute()
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
            if score['name'] == "Set 1" or score['name'] == "Set 2" or score['name'] == "Set 3" or score['name'] == "Set 4" or score['name'] == "Set 5":
                teamA_scores.append(int(score['homeCompetitorScore']) if score['homeCompetitorScore'] > 0 else 0)
        return teamA_scores
    elif teamName == "teamB":
        for score in scores:
            if score['name'] == "Set 1" or score['name'] == "Set 2" or score['name'] == "Set 3" or score['name'] == "Set 4" or score['name'] == "Set 5":
                teamB_scores.append(int(score['awayCompetitorScore']) if score['awayCompetitorScore'] > 0 else 0)
        return teamB_scores
    
async def set_match_info(game, tournaments):
    info = {
        "match_id" : game['id'],
        "source" : "365Scores",
        "status" : "Final" if game['statusText'] == "Final" else "Live",
        "match_name" : f"{game['homeCompetitor']['name']} vs {game['awayCompetitor']['name']}",
        "tournament" : await get_tournament(tournaments=tournaments, id=game['competitionId']),
        "tournament_display_name" : game['competitionDisplayName'],
        "date" : game['startTime'],
        "teamA" : game['homeCompetitor']['name'],
        "teamB" : game['awayCompetitor']['name'],
        "uuID" : shortuuid.uuid()
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
        case "Set 4":
            period = "Set 4"
        case "Set 5":
            period = "Set 5"
        case "Final":
            period = "Final"
 
    info = {
        "match_id" : game['match_id'],
        "period" : period,
        "teamA" : scoresTeamA,
        "teamB" : scoresTeamB,
        "source" : "365Scores"
        }
    
    return info

async def set_schedule_info(game):
    players = await get_players_names(game)
    match_info = {
        "match_id" : game['id'],
        "match_name" : f"{players[0]} vs {players[1]}",
        "tournament" : game['competitionDisplayName'],
        "tournament_display_name" : game['competitionDisplayName'],
        "date" : game['startTime'],
        "teamA" : players[0].strip(),
        "teamB" : players[1].strip()
    }

    return match_info

# -- End of Utils

if __name__ == "__main__":
    asyncio.run(scrape_data())