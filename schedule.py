import discord
from db import db
from loguru import logger
from utils import format_datetime

async def get_schedule():
    result = db.table("schedule").select("*").execute()
    response = await format_schedule(result.data)
    return response

async def format_schedule(data):
    logger.info("Formatting schedule for discord")
    if len(data) > 0:
        try:
            fields_added = 0
            embeds = []
            current_embed = discord.Embed(title="Schedule 📅")

            for event in data:
                event_name = event.get('match_name', '')
                event_tournament = event.get('tournament', '')
                event_date = format_datetime(event.get('date', ''))
                if isinstance(event_name, str) and event_name.strip() != '' and isinstance(event_tournament, str) and event_tournament.strip() != '' and isinstance(event_date, str) and event_date.strip() != '':
                    field_value = f"{event_tournament} - {event_date}"
                    if len(current_embed) + len(event_name) + len(field_value) <= 6000 and fields_added < 25:
                            current_embed.add_field(name=event_name, value=field_value, inline=False)
                            fields_added += 1
                    else:
                            embeds.append(current_embed)
                            current_embed = discord.Embed(title="Schedule 📅")
                            fields_added = 0

            embeds.append(current_embed)
            for index, embed in enumerate(embeds):
                if index < 10:  # Limit to 10 embeds per message
                    return embed
                else:
                    logger.warning("Maximum number of embeds per message reached")
                    break 
        except Exception as e:
            logger.error(f"There was an Error formatting the message: {e}")       
    else:
        return "No events scheduled."        