import os
import json
import time
import yfinance as yf
import pandas as pd
import requests
import discord
import asyncio
from datetime import datetime, timedelta

# Load environment variables
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))  # Default to 0 if not set

POSTED_EARNINGS_FILE = "posted_earnings.json"
TICKER_FILE = "nasdaq_tickers.csv"

intents = discord.Intents.default()
client = discord.Client(intents=intents)
message_queue = asyncio.Queue()


# ---- Ticker Download und Laden ----

def download_nasdaq_ticker_list():
    url = "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
    response = requests.get(url)
    if response.status_code == 200:
        with open(TICKER_FILE, 'wb') as f:
            f.write(response.content)
    else:
        raise Exception("Tickerliste konnte nicht geladen werden.")

def load_tickers():
    if not os.path.exists(TICKER_FILE):
        download_nasdaq_ticker_list()
    df = pd.read_csv(TICKER_FILE)
    return df['Symbol'].dropna().unique().tolist()


# ---- Earnings-Daten abrufen ----

def get_next_earnings_for_ticker(ticker):
    try:
        stock = yf.Ticker(ticker)
        cal = stock.calendar
        if not cal.empty:
            earnings_date = cal.loc['Earnings Date'][0]
            if pd.notnull(earnings_date):
                return earnings_date.to_pydatetime() if isinstance(earnings_date, pd.Timestamp) else earnings_date
    except:
        pass
    return None

def get_earnings_calendar(for_tomorrow=False):
    tickers = load_tickers()
    target_date = datetime.today().date() + timedelta(days=1 if for_tomorrow else 0)
    earnings = []

    for ticker in tickers:
        earnings_dt = get_next_earnings_for_ticker(ticker)
        if earnings_dt and earnings_dt.date() == target_date:
            earnings.append({
                'ticker': ticker,
                'company': yf.Ticker(ticker).info.get('shortName', 'Unbekannt'),
                'datetime': earnings_dt.strftime('%Y-%m-%d %H:%M')
            })

    return earnings


# ---- Datenbank speichern/laden ----

def load_posted(file):
    if os.path.exists(file):
        with open(file, "r") as f:
            return set(json.load(f))
    return set()

def save_posted(data, file):
    with open(file, "w") as f:
        json.dump(list(data), f)


# ---- Discord-Post und Monitor-Loop ----

async def post_earnings_to_discord(earnings):
    channel = client.get_channel(CHANNEL_ID)
    if not channel:
        print("❌ Discord-Channel nicht gefunden.")
        return

    for e in earnings:
        msg = f"📣 **Earnings Alert!**\n`{e['ticker']}` ({e['company']}) berichtet am `{e['datetime']}`"
        await channel.send(msg)

async def handle_earnings_summary(earnings, already_posted):
    new_earnings = [e for e in earnings if e['ticker'] not in already_posted]
    if new_earnings:
        print(f"📣 Neue Earnings gefunden ({len(new_earnings)}):")
        for e in new_earnings:
            print(f" - {e['ticker']} ({e['company']}) um {e['datetime']}")
            already_posted.add(e['ticker'])
        await message_queue.put(new_earnings)
    else:
        print("🔍 Keine neuen Earnings.")
    return already_posted

async def earnings_monitor_loop():
    already_posted = load_posted(POSTED_EARNINGS_FILE)
    print("🚀 Earnings-Monitor gestartet...")

    while True:
        now = datetime.now()
        if now.minute in [0, 30] and now.second < 5:
            print(f"\n⏰ Abfrage um {now.strftime('%H:%M:%S')}")
            for_tomorrow = now.hour >= 20
            earnings = get_earnings_calendar(for_tomorrow=for_tomorrow)
            already_posted = await handle_earnings_summary(earnings, already_posted)
            save_posted(already_posted, POSTED_EARNINGS_FILE)
            await asyncio.sleep(61)
        else:
            await asyncio.sleep(1)

async def discord_message_sender():
    while True:
        earnings = await message_queue.get()
        await post_earnings_to_discord(earnings)


# ---- Discord Events ----

@client.event
async def on_ready():
    print(f"✅ Eingeloggt als {client.user}")
    asyncio.create_task(earnings_monitor_loop())
    asyncio.create_task(discord_message_sender())


# ---- Start ----

if __name__ == "__main__":
    if not DISCORD_TOKEN or CHANNEL_ID == 0:
        print("❌ Bitte DISCORD_TOKEN und DISCORD_CHANNEL_ID als Umgebungsvariablen setzen.")
    else:
        client.run(DISCORD_TOKEN)
