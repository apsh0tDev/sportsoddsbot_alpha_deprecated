from db import db
import asyncio
import scores365
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

async def get_scores():
    await scores365.scrape_data()

def sleep_bot():
    print("RUNNING SLEEP BOT FUNCTION")
    table = db.table("live_matches").select("*").execute()
    if len(table.data) == 0:
        jobs = scheduler.get_jobs()
        jobs_names = [job.name for job in jobs]
        for job in jobs:
            print(job.id)
            print(job.name)
            if job.name == "get_scores":
                scheduler.remove_job(job_id=job.id)
    
        print(jobs_names)           
            

def live():
    jobs = scheduler.get_jobs()
    jobs_names = [job.name for job in jobs]
    if 'get_scores' not in jobs_names:
        scheduler.add_job(get_scores, 'interval', seconds=40)

job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

scheduler.configure(job_defaults=job_defaults)
scheduler.add_job(sleep_bot, 'interval', minutes=2)
scheduler.start()

try:
    live()
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    pass