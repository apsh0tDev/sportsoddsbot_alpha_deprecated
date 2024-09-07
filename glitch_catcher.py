import asyncio
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from notifier import glitch_notifier_fanduel

logger.add("glitches.log", filter=lambda record: "glitches" in record["extra"], rotation="2 weeks")

async def glitch_catcher_fanduel(markets=[], match_name="", uuID=""):
    print("RUNNING GLITCH CATCHER 👾")
    live_match = db.table("scoreboard").select("*").eq("uuID", uuID).execute()
    if len(live_match.data) > 0:
        current_set = live_match.data[0]['period']
        glitches = []
        for item in markets:
            if current_set == "Final" and "Set" in item:
                glitches.append(item)
            elif current_set == "Set 2":
                if "Set 1" in item:
                    glitches.append(item)
            elif current_set == "Set 3":
                if "Set 1" in item or "Set 2" in item:
                    glitches.append(item)
            elif current_set == "Set 4":
                if "Set 1" in item or "Set 2" in item or "Set 3" in item:
                    glitches.append(item)
            elif current_set == "Set 5":
                if "Set 1" in item or "Set 2" in item or "Set 3" in item or "Set 4" in item:
                    glitches.append(item)
 
        if len(glitches) > 0:
            await glitch_notifier_fanduel(glitches=glitches, match_name=match_name, current_set=current_set)
            logger.bind(glitches=True).info(f"""Glitch was found for match: {match_name} {uuID}\nLines = {glitches}""")
        else:
            print("No glitches found.")
    else:
        logger.info(f"Match by uuID {uuID} not found")

if __name__ == "__main__":
    asyncio.run(glitch_catcher_fanduel())



    """data = [
    'Set 2 Game 11 - Server to win to Love/15',
    'Most Aces Handicap 5.5',
    'Set 3 Total Aces 3.5',
    'Set 3 Aces',
    'Total Karolina Muchova Aces 6.5',
    'Set 3 Game 10 Karolina Muchova Points 2.5',
    'Correct Score 3rd Set',
    'Set 3 Game 9 - To Win the First Point and Win The Game',
    'Service Break Number 9',
    'Set Betting',
    'Set 3 Game 9 - Server to win to Love/15',
    'Set 3 Game 9 Karolina Muchova Points 2.5',
    'Set 3 Game 10 - To Win the First Point and Win The Game',
    'Both Players to Win Points in Set 3 Game 8',
    'Moneyline',
    'Set 2 Game 8 Aces',
    'Set 3 Game 9 Jessica Pegula Points 2.5',
    'Set 3 Game 9 Aces',
    'Set 3 Game 10 - Server to win to Love/15',
    'Set 3 Game 9 Deuces',
    'Total Aces 7.5',
    'Set 3 Game 8 Break points',
    'Set 3 Game 9 Break points',
    'Set 2 Game 8 Point 7 Winner',
    'Both Players to Win Points in Set 3 Game 9',
    'Set 3 Game 10 Point Handicap',
    'Total Jessica Pegula Aces 1.5',
    'Set 3 Game 10 Jessica Pegula Points 2.5',
    'Set 3 Game 8 Point 4 Winner',
    'Set 3 Game 9 Point Handicap',
    'Total Match Games 25.5',
    'Set 1 Game 8 Point 5 Winner',
    'Set 3 Game 8 Winner',
    'Set 3 Game 8 Point 6 Winner',
    'Set 3 Game 8 Point 3 Winner',
    'Set 1 Game 9 Winner'
    ]"""