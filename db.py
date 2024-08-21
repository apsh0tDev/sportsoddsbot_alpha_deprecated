import os
from dotenv import load_dotenv
from supabase import create_client

#Database connection with Supabase

load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

supabase = create_client(url, key)
db = supabase