import pytz
import asyncio
import scores365
from db import db
from glitch_catcher import glitch_catcher_fanduel
from datetime import datetime,timedelta
from dateutil.parser import parse
from loguru import logger
from dev_notifier import notification
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()
logger.add("info.log", level="INFO", rotation="2 weeks")
logger.add("errors.log", level="WARNING", rotation="2 weeks")

async def running():
    await scores365.scrape_data()

async def glitch_catchers():
    catchers = [
        glitch_catcher_fanduel()
    ]
    await asyncio.gather(*catchers)


def schedule():
    print("CHECKING SCHEDULED DATE")
    table = db.table("schedule").select("*").execute()
    dates = [parse(item['date']) for item in table.data]
    if len(dates) > 0:
        ny_tz = pytz.timezone("America/New_York")
        now_ny = datetime.now(ny_tz)
        dates_ny = [date.astimezone(ny_tz) for date in dates]
        closest_date_ny = min(dates_ny, key=lambda date: abs(date - now_ny))
        closest_date_str = closest_date_ny.strftime('%Y-%m-%d %H:%M:%S %Z%z')

        with open('next_game.txt', 'r') as file:
            if file.read().strip() != closest_date_str:
                print(f"Changing schedule data to: {closest_date_str}")
                with open('next_game.txt', 'w') as file_out:
                    file_out.write(closest_date_str)

def runners_status():
    jobs = scheduler.get_jobs()
    jobs_names = [job.name for job in jobs]
    print(jobs_names)

    if 'running' not in jobs_names:
        wake_up()
    elif 'running' in jobs_names:
        sleep()
     

def wake_up():
    print("RUNNING WAKE UP ⛅ FUNCTION")
    table = db.table("live_matches").select("*").execute()
    if len(table.data) > 0:
        scheduler.add_job(running, 'interval', seconds=40)
        notification("Starting runners.")
    elif len(table.data) == 0:
        ny_tz = pytz.timezone("America/New_York")
        now_ny = datetime.now(ny_tz)
        with open('next_game.txt', 'r') as file:
            next_date_str = file.read().strip()
            next_date = datetime.strptime(next_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ny_tz)
            time_diff = (next_date - now_ny).total_seconds()
            
            if time_diff <= 0:
                print("Time for schedule already passed. Run.")
                scheduler.add_job(running, 'interval', seconds=40)
                scheduler.add_job(glitch_catchers, 'interval', seconds=40)
                notification(f"Starting runners. Next match in schedule started at {next_date_str}")
            elif 0 < time_diff <= 300:  # 5 minutes = 300 seconds
                print("Next match is about to start in 5 minutes.")
                scheduler.add_job(running, 'interval', seconds=40)
                scheduler.add_job(glitch_catchers, 'interval', seconds=40)
                notification(f"Next match is about to start at {next_date_str}. Starting runners.")
            
def sleep():
    print("RUNNING SLEEP ⛅ FUNCTION")
    table = db.table("live_matches").select("*").execute()
    if len(table.data) == 0:
        jobs = scheduler.get_jobs()
        for job in jobs:
            print(f"ID: {job.id} NAME: {job.name}")
            if job.name == "running":
                scheduler.remove_job(job_id=job.id)
                notification(f"No more matches left at the moment. Stop runners.")
            if job.name == "glitcher_catchers":
                scheduler.remove_job(job_id=job.id)


job_defaults = {
    'coalesce': False,
    'max_instances': 10
}

scheduler.configure(job_defaults=job_defaults)
scheduler.add_job(runners_status, 'interval', minutes=1)
scheduler.add_job(schedule, 'interval', minutes=5)


try:
    schedule()
    scheduler.start()
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    pass
