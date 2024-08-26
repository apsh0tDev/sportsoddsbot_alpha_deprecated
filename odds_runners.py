import asyncio
import betmgm
import pointsbet
import fanduel
import lines_betmgm
import lines_fanduel
import lines_pointsbet
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()
logger.add("info.log", level="INFO")
logger.add("errors.log", level="WARNING")

async def get_matches():
    tasks = [
        betmgm.scrape_data(),
        pointsbet.scrape_data(),
        #fanduel.scrape_data()
    ]

    await asyncio.gather(*tasks)

async def live_odds():
    tasks = [
        lines_betmgm.scrape_events(),
        lines_pointsbet.scrape_events(),
        #lines_fanduel.scrape_events()
    ]

    await asyncio.gather(*tasks)

def odds():
    print("Get odds and matches")
    jobs = scheduler.get_jobs()
    jobs_names = [job.name for job in jobs]
    if 'live_odds' not in jobs_names:
        scheduler.add_job(live_odds, 'interval', minutes=1)
    if 'matches_list' not in jobs_names:
        scheduler.add_job(get_matches, 'interval', minutes=5)

    print(jobs_names)

job_defaults = {
    'coalesce': False,
    'max_instances': 10
}

scheduler.configure(job_defaults=job_defaults)
scheduler.start()

try:
    odds()
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    pass