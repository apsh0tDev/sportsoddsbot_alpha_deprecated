import asyncio
import scores365
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

async def get_scores():
    await scores365.scrape_data()

def live():
    jobs = scheduler.get_jobs()
    jobs_names = [job.name for job in jobs]
    if 'get_scores' not in jobs_names:
        scheduler.add_job(get_scores, 'interval', seconds=30)   

job_defaults = {
    'coalesce': False,
    'max_instances': 10
}

scheduler.configure(job_defaults=job_defaults)
scheduler.start()

try:
    live()
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    pass