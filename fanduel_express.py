import bs4
import json
import asyncio
import constants
from db import db
from rich import print
from thefuzz import fuzz
from loguru import logger
from utils import verifier_alt
from actions import exists, upload
from notifier import glitch_notifier_fanduel
from cloud_connection import scrape_by_site
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

async def scrape_data():
    response = await scrape_by_site(constants.fanduel_live_url, "FANDUEL", False)
    if response != None or response != '':
        is_valid = await verifier_alt(response)
        if is_valid:
            load = json.loads(response)
            await sort_matches(load=load)


async def sort_matches(load):
    matches_in_set = db.table("sets").select("*").execute()
    
    if 'attachments' in load:
        if 'events' in load['attachments']:
            evs = load['attachments']['events']

        if 'markets' in load['attachments']:
            markets = load['attachments']['markets']
            market_keys = [key for key in markets]
            for key in market_keys:
                if 'inPlay' in markets[key] and markets[key]['inPlay'] == True:
                    event = markets[key]
                    info = {
                        "match_id" : event['eventId'],
                        "match_name" : f"{event['runners'][0]['runnerName']} vs {event['runners'][1]['runnerName']}",
                    }
                    for item in matches_in_set.data:
                        fuzz_ratio = fuzz.token_sort_ratio(item['match_name'], info['match_name'])
                        if fuzz_ratio >= 70:
                            info['uuID'] = item['uuID']
                            value_exists = await exists(table="fanduel", to_match={"uuID" : info['uuID'], "match_name" : info['match_name']})
                            if value_exists:
                                print("Exists. Skip")
                            else:
                                response = await upload(table="fanduel", info=info)
                                print(response)

async def scrape_events():
    matches = db.table("fanduel").select("*").execute()
    matches_ids = [{"match_id" : item['match_id'], "uuID" : item['uuID']} for item in matches.data]

    for item in matches_ids:
        await scrape_event(id=item['match_id'], uuID=item['uuID'])
        await asyncio.sleep(4)

async def scrape_event(id, uuID):
    url = constants.fanduel_event_url.format(id=id, tab="all")
    print(url)
    response = await scrape_by_site(url=url, site="FANDUEL", headless=True)
    if response != None and response != '':
       is_valid = await verifier_alt(response)
       if is_valid:
           soup = bs4.BeautifulSoup(response, 'html.parser')
           pre = soup.find("pre")
           load = json.loads(pre.text)
           await add_lines(load, uuID)

async def add_lines(load, uuID):
    sets_table = db.table("sets").select("*").eq("uuID", uuID).execute()
    print(sets_table.data)
    glitches = []
    if len(sets_table.data) == 1:
        if 'attachments' in load:
            if 'markets' in load['attachments']:
                markets = load['attachments']['markets']
                market_keys = [key for key in markets]
                for key in market_keys:
                    line = {
                        "market_name" : markets[key]['marketName'],
                        "isOpen" : True if markets[key]['marketStatus'] == 'OPEN' else False,
                    }
                    match_name = sets_table.data[0]['match_name']
                    current_set = sets_table.data[0]['current_set']
                    glitch_in_line = await compare_lines(current_set=current_set, line=line, finished=sets_table.data[0]['itEnded'])
                    if glitch_in_line:
                        glitches.append(line)
            elif len(load['attachments']) == 0:
               response = db.table("fanduel").delete().eq("uuID", uuID).execute()
               print(f"Deleting record {uuID} from fanduel table: {response}")
               await scrape_data()

    if len(glitches) > 0:
        await glitch_notifier_fanduel(glitches, match_name, current_set)

async def compare_lines(current_set, line, finished):
    print(current_set)
    print(line)
    #Match is finished but lines are still open
    if finished == True and line['isOpen'] == True:
        return True

    #On Set 2 but there are Set 1 lines open
    if current_set == "Set 2" and "Set 1" in line and line['isOpen'] == True:
        return True
    if current_set == "Set 2" and "1rst Set" in line and line['isOpen'] == True:
        return True
    
    #On Set 3 but there are Set 2 lines open
    if current_set == "Set 3" and "Set 2" in line and line['isOpen'] == True:
        return True
    if current_set == "Set 3" and "2nd Set" in line and line['isOpen'] == True:
        return True
    
    return False

#----- For scheduler
async def get_lines():
    await scrape_events()

def lines():
    jobs = scheduler.get_jobs()
    jobs_names = [job.name for job in jobs]
    if 'get_scores' not in jobs_names:
        scheduler.add_job(get_lines, 'interval', minutes=1.5)

job_defaults = {
    'coalesce': False,
    'max_instances': 10
}

scheduler.configure(job_defaults=job_defaults)
scheduler.start()

"""if __name__ == "__main__":
    asyncio.run(scrape_data())"""

try:
    lines()
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    pass