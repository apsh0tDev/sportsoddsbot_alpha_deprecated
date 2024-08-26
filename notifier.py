import os
import pytz
import discord
import asyncio
import aiohttp
from db import db
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta
from utils import get_current_ny_time

load_dotenv()

current_branch = "PROD"

def get_token():
    if current_branch == "DEV":
        DISCORD_API = os.getenv("DISCORD_WEBHOOK_DEV")
    elif current_branch == "PROD":
        DISCORD_API = os.getenv("DISCORD_WEBHOOK_PROD")
    return DISCORD_API

WEBHOOK_URL = get_token()

async def arbitrage_notification(arbitrage_data):
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
        text = await format_message(arbitrage_data)
        message = await webhook.send(text, username='Odds Bot', wait=True)
        arbitrage_uuID = arbitrage_data['uuID']
        print(arbitrage_uuID)
        search = db.table("arbitrages").select("*").match({"uuID" : arbitrage_uuID}).execute()
        if len(search.data) > 0:
            db.table("arbitrages").update({"notification_id" : message.id}).match({"uuID" : arbitrage_uuID}).execute()

async def edit_message(arbitrage_data, close_match=False):
    if arbitrage_data['notification_id'] != None:
        if close_match == False:
            
                async with aiohttp.ClientSession() as session:
                    webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
                    past_message = await webhook.fetch_message(arbitrage_data['notification_id'])
                    updated_message = await format_message(arbitrage_data)
                    await past_message.edit(content=updated_message)
        else:
            await close_match_action(arbitrage_data['notification_id'])
  

async def format_message(arbitrage_data):
    match_name = arbitrage_data['match_name']
    teamA_name, teamB_name = match_name.split('-')
    teamA_odds = arbitrage_data['teamA']['decimalOdds']
    teamA_source = await get_source(arbitrage_data['teamA']['source']) 
    teamB_odds = arbitrage_data['teamB']['decimalOdds']
    teamB_source = await get_source(arbitrage_data['teamB']['source']) 
    teamA_status = arbitrage_data['teamA']['isOpen']
    teamB_status = arbitrage_data['teamB']['isOpen']
    market = arbitrage_data['market']
    utc_time = datetime.strptime(arbitrage_data['created_at'], '%Y-%m-%dT%H:%M:%S.%f%z')
    arbitrage_percentage = float(arbitrage_data['arbitrage_percentage'])
    ny_tz = pytz.timezone('America/New_York')
    ny_time = utc_time.astimezone(ny_tz)
    created_at = ny_time.strftime('%Y-%m-%d %I:%M %p')
    teamA_message = f"- **{teamA_name.strip()}:** {teamA_odds} ({teamA_source})\n" if teamA_status == True else f"- ~~**{teamA_name}:** {teamA_odds} ({teamA_source})~~ 🔒\n"
    teamB_message = f"- **{teamB_name.strip()}:** {teamB_odds} ({teamB_source})\n" if teamB_status == True else f"- ~~**{teamB_name}:** {teamB_odds} ({teamB_source})~~ 🔒\n"
    message = (
        "🎯 **New Arbitrage Opportunity Detected!**\n\n"
        f"**🎾 Tennis**\n"
        f"**Match:** {teamA_name} vs. {teamB_name}\n"
        f"**Market:** {market}\n"
        "**Odds Breakdown:**\n"
        f"{teamA_message}"
        f"{teamB_message}"
        f"**Arbitrage Percentage:** {arbitrage_percentage:.2f}%\n\n"
        f"**Time:** {created_at}"
    )

    return message

async def close_match_action(notification_id):
    print("Close this match: ", notification_id)
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
        past_message = await webhook.fetch_message(notification_id)
        lines = past_message.content.split('\n')
        lines.remove("🎯 **New Arbitrage Opportunity Detected!**")
        lines.insert(0,"**⛔ Match ended. **")
        output = "\n".join(lines)
        await past_message.edit(content=output)
    response = db.table("arbitrages").delete().eq("notification_id", notification_id).execute()
    print(response)

async def glitch_notifier_fanduel(glitches, match_name, current_set):
    markets = [item['market_name'] for item in glitches]
    output = "\n".join(markets)
    text = (
        "👾 **Glitch found in FanDuel!**\n\n"
        f"**Match:** {match_name}\n"
        f"**Current Set on 365Scores: {current_set}**\n"
        f"**Line(s):** \n"
        f"{output}\n\n"
        f"**Time:** {get_current_ny_time()}"
    )
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
        message = await webhook.send(text, username='Odds Bot', wait=True)
        print(message.id)

#-- Utils
async def get_source(source_name):
    if source_name == "Pointsbet":
        return "Resorts World Bet (Fanatics)"
    else:
        return source_name

if __name__ == "__main__":
    asyncio.run()
