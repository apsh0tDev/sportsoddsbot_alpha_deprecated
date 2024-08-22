import os
import discord
import textwrap
from db import db
from loguru import logger
from tabulate import tabulate
from dotenv import load_dotenv
from discord.ext import commands, tasks
from live import get_live_matches
from schedule import get_schedule


#---- Init 
load_dotenv()

current_branch = "PROD"
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

def get_token():
    DISCORD_API = ''
    if current_branch == "DEV":
        DISCORD_API = os.getenv("TOKEN_DEV")
    elif current_branch == "PROD":
        DISCORD_API = os.getenv("TOKEN_PROD")
    return DISCORD_API

logger.add("discord_errors.log", filter=lambda record:"discord_error" in record["extra"])

#======= Bot Events =========

@bot.event
async def on_ready():
    print("Bot ready to go!")
    status_checker.start()

@bot.event
async def on_command_error(ctx: commands.Context, error):
    try:
            # Log all unhandled errors
            logger.bind(discord_error=True).error(f'Ignoring exception in command {ctx.command}: {type(error).__name__}: {error}')
            await ctx.send("An error occurred. Please try again later.")
    except Exception as e:
        # Catch any exception that occurs within the error handler itself
        logger.bind(discord_error=True).error(f'Exception in error handler: {type(e).__name__}: {e}')
        

#==== End of Bot Events =====

#======= Bot Tasks =========
@tasks.loop(minutes=15)
async def status_checker():
    matches = db.table("matches_list").select("*").execute()
    if len(matches.data) == 0:
        await bot.change_presence(
            status=discord.Status.idle,
        )
    else:
        await bot.change_presence(
            status=discord.Status.online, 
            activity=discord.Activity(type=discord.ActivityType.watching, name="live matches")
        ) 

#==== End of bot Tasks =====

#====== Bot Commands ========
@bot.command()
async def commands(ctx):
    commands_message = f"""
                        🤖 **Commands:**
                        `!live`: Displays a full list of today's live matches and current scores
                        `!schedule`: Displays a list of scheduled events
                        `!sportsbooks` : Displays a list of available sportsbooks and their current availability
                        """
    await ctx.send(commands_message)

@bot.command()
async def live(ctx):
    response = await get_live_matches()
    
    if response is not None:
        if isinstance(response, list):
            for embed in response:
                await ctx.send(embed=embed)
        else:
            await ctx.send(embed=response)
    else:
        await ctx.send("There are no live matches at the moment.")

@bot.command()
async def schedule(ctx):
    message = await get_schedule()
    if isinstance(message, str):
        await ctx.send(message)
    else:
        await ctx.send(embed=message)

@bot.command()
async def sportsbooks(ctx):
    res = db.table("sportsbooks").select("*").execute()
    if len(res.data) > 0:
        header = ["Sportsbook", "Is available", "Availability Avg"]
        body = [header]  # Start with the header
        # Add each sportsbook and its availability as a new row

                # Define ANSI color codes for Available and Not Available
        available_format = "\u001b[1;40;32m Available \u001b[0m"
        not_available_format = "\u001b[1;40;31m Not Available \u001b[0m"  # Bright red text
        
        for item in res.data:
            total_count = item['available_sum'] + item['not_available_sum']
            avg = round((item['available_sum'] / total_count) * 100)
            row = [item['name'], available_format if item['available'] == True else not_available_format, f"\u001b[1;40;32m {avg}% \u001b[0m" if avg > 60 else f"\u001b[1;40;31m {avg}% \u001b[0m"]
            body.append(row)
        table = tabulate(body, headers="firstrow", tablefmt="simple")
        message = f"```ansi\n{table}\n```"
        message = textwrap.dedent(message)
        await ctx.send(message)
        await ctx.send("For real-time information, please visit [this link](https://apsh0tdev.github.io/spAvailability/)")


#=== End of Bot Commands ====

if __name__ == "__main__":
    bot.run(get_token())