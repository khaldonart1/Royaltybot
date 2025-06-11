import os

print("--- Starting Environment Variable Check ---")

bot_token = os.environ.get("7950170561:AAER-L3TyzKll--bl4n7FyPVxLxsFju6wSs")
supabase_url = os.environ.get("https://jofxsqsgarvzolgphqjg.supabase.co")
supabase_key = os.environ.get("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg")

if bot_token:
    print("BOT_TOKEN: Found!")
else:
    print("BOT_TOKEN: !!! NOT FOUND !!!")

if supabase_url:
    print("SUPABASE_URL: Found!")
else:
    print("SUPABASE_URL: !!! NOT FOUND !!!")

if supabase_key:
    print("SUPABASE_KEY: Found!")
else:
    print("SUPABASE_KEY: !!! NOT FOUND !!!")

print("--- Check Complete ---")