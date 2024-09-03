import pytz
import asyncio
import scores365
from db import db
from datetime import datetime, timedelta
from dateutil.parser import parse
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

def wake_up_bot():
    print("RUNNING WAKE UP ⛅ BOT FUNCTION")
    table = db.table("schedule").select("*").execute()
    dates = [parse(item['date']) for item in table.data]
    ny_tz = pytz.timezone("America/New_York")
    now_ny = datetime.now(ny_tz)
    dates_ny = [date.astimezone(ny_tz) for date in dates]
    closest_date_ny = min(dates_ny, key=lambda date: abs(date - now_ny))
    closest_date_str = closest_date_ny.strftime('%Y-%m-%d %H:%M:%S %Z%z')

    with open('next_game.txt', 'w') as file:
        file.write(f"The closest game is scheduled for: {closest_date_str}")

    jobs = scheduler.get_jobs()
    jobs_names = [job.name for job in jobs]
    if 'get_scores' not in jobs_names:
        run_time = closest_date_ny - timedelta(minutes=5)
        scheduler.add_job(get_scores, 'date', run_date=run_time)


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
scheduler.add_job(wake_up_bot, 'interval', minutes=2)
scheduler.start()

try:
    wake_up_bot()
    live()
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    pass