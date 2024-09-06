import re
import uuid
import pytz
from loguru import logger
from datetime import datetime, timezone

async def verifier(value):
    if value != None and 'solution' in value and 'verified' in value['solution'] and value['solution']['verified'] == True:
        if "<title>Just a moment...</title>" not in value['solution']['response'] and "Request blocked." not in value['solution']['response'] and "Sorry, you have been blocked" not in value['solution']['response']:
            return True
        else:
            logger.error("CLOUDFARE BLOCK.")
            return False
    else:
        logger.error(value)
        return False
    
async def verifier_alt(value):
    if value != None and "Request blocked." not in value and "Just a moment..." not in value and "Sorry, you have been blocked" not in value:
        return True
    return False
    
def remove_parentheses(text):
    return re.sub(r'\([^)]*\)', '', text)

def format_datetime(input_datetime_str):
    input_datetime = datetime.fromisoformat(input_datetime_str.replace('Z', '+00:00'))
    ny_timezone = pytz.timezone('America/New_York')
    input_datetime = input_datetime.astimezone(ny_timezone)
    current_datetime = datetime.now(ny_timezone)
    day_difference = (input_datetime.date() - current_datetime.date()).days
    if day_difference == 0:
        day_str = "Today"
    elif day_difference == 1:
        day_str = "Tomorrow"
    else:
        day_str = input_datetime.strftime("%A")
    time_str = input_datetime.strftime("%I:%M %p")

    return f"{day_str} - {time_str}"

def american_to_decimal(american_odds):
    if american_odds > 0:
        return (american_odds / 100) + 1
    else:
        return (100 / abs(american_odds)) + 1
    
def decimal_to_american(decimal_odds):
    if decimal_odds >= 2.0:
        return (decimal_odds - 1) * 100
    else:
        return -100 / (decimal_odds - 1)

    
def extract_players(matchup):
    return matchup.split(" v ")

def generate_session_id():
    return str(uuid.uuid4())

def get_current_ny_time():
    # Define New York timezone
    ny_tz = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_tz)
    return ny_time.strftime('%Y-%m-%d %I:%M %p')


#-------