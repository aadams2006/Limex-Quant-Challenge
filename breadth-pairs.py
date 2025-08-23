import asyncio
import aiohttp
import pandas as pd
import numpy as np
import json
import time
import csv

# Load Credentials
with open('credentials.json') as f:
    creds = json.load(f)

CLIENT_ID = creds['client_id']
CLIENT_SECRET = creds['client_secret']
USERNAME = 'alexglobe20@gmail.com'
PASSWORD = 'Frayuuio_209'
ACCOUNT_NUMBER = 'dmo-c432'

AUTH_URL = creds['auth_url'] + '/connect/token'
BASE_URL = creds['base_url']
ORDER_URL = BASE_URL + '/orders/place'
PRICE_HIST_URL = BASE_URL + '/marketdata/history'

# Asset Pairs - Breadth Scalping Focused
ASSET_PAIRS = [
    ("TSLA", "NVDA"),
    ("META", "SNAP"),
    ("AMD", "NVDA"),
    ("SPY", "QQQ"),
    ("AAPL", "MSFT"),
    ("GOOGL", "META"),
    ("XOM", "CVX"),
    ("KO", "PEP"),
    ("BA", "LMT"),
    ("JPM", "BAC"),
    # Add up to 50 pairs like these...
]

FIXED_THRESHOLD = 0.001  # 0.1% price differential trigger
POSITION_SIZE = 1
POLL_INTERVAL = 1  # seconds

access_token = None

# ---------------- API Functions ----------------
async def get_access_token():
    global access_token
    async with aiohttp.ClientSession() as session:
        payload = {
            'grant_type': 'password',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'username': USERNAME,
            'password': PASSWORD
        }
        headers = {'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
        async with session.post(AUTH_URL, headers=headers, data=payload) as resp:
            if resp.status == 200:
                token_data = await resp.json()
                access_token = token_data['access_token']
            else:
                print(f"Failed to get token: {resp.status}")
                exit()

async def fetch_price_data(session, symbol):
    now = int(time.time())
    from_time = now - (2 * 24 * 60 * 60)  # Last 2 days
    params = {'symbol': symbol, 'period': 'minute_1', 'from': from_time, 'to': now}
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(PRICE_HIST_URL, params=params, headers=headers) as resp:
        if resp.status != 200:
            print(f"Failed fetching {symbol}: {resp.status}")
            return None
        data = await resp.json()
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('timestamp', inplace=True)
        return df['close']

async def place_order(session, symbol, side):
    payload = {
        "account_number": ACCOUNT_NUMBER,
        "symbol": symbol,
        "quantity": POSITION_SIZE,
        "time_in_force": "day",
        "order_type": "market",
        "side": side,
        "exchange": "auto",
    }
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    async with session.post(ORDER_URL, json=payload, headers=headers) as resp:
        if resp.status == 200:
            print(f"Order Placed: {side.upper()} {symbol}")
        else:
            print(f"Failed Order {symbol}: {resp.status}")

# ---------------- Trading Logic ----------------
async def trade_pair(session, symbol1, symbol2, state, log_writer):
    price1 = await fetch_price_data(session, symbol1)
    price2 = await fetch_price_data(session, symbol2)

    if price1 is None or price2 is None:
        return

    latest_price1 = price1.iloc[-1]
    latest_price2 = price2.iloc[-1]

    price_diff = abs(latest_price1 - latest_price2)
    avg_price = (latest_price1 + latest_price2) / 2
    spread_pct = price_diff / avg_price

    print(f"[{time.strftime('%H:%M:%S')}] {symbol1}/{symbol2} Spread%: {spread_pct:.4f}")

    pair_key = f"{symbol1}_{symbol2}"

    if state.get(pair_key) is None:
        state[pair_key] = {'position_open': False, 'current_position': None}

    if not state[pair_key]['position_open']:
        if spread_pct > FIXED_THRESHOLD:
            if latest_price1 > latest_price2:
                # Short expensive, long cheap
                await place_order(session, symbol1, 'sell')
                await place_order(session, symbol2, 'buy')
                state[pair_key] = {'position_open': True, 'current_position': 'SHORT_1'}
            else:
                await place_order(session, symbol1, 'buy')
                await place_order(session, symbol2, 'sell')
                state[pair_key] = {'position_open': True, 'current_position': 'LONG_1'}
            # Log Entry
            log_writer.writerow([time.strftime('%Y-%m-%d %H:%M:%S'), symbol1, symbol2, 'ENTER', spread_pct])
    else:
        if spread_pct < FIXED_THRESHOLD * 0.5:
            # Close Position
            if state[pair_key]['current_position'] == 'SHORT_1':
                await place_order(session, symbol1, 'buy')
                await place_order(session, symbol2, 'sell')
            elif state[pair_key]['current_position'] == 'LONG_1':
                await place_order(session, symbol1, 'sell')
                await place_order(session, symbol2, 'buy')
            state[pair_key] = {'position_open': False, 'current_position': None}
            # Log Exit
            log_writer.writerow([time.strftime('%Y-%m-%d %H:%M:%S'), symbol1, symbol2, 'EXIT', spread_pct])

# ---------------- Main Bot Loop ----------------
async def pairs_scalping_bot():
    await get_access_token()
    state = {}

    # Setup CSV Logger
    with open('trade_log.csv', 'w', newline='') as csvfile:
        log_writer = csv.writer(csvfile)
        log_writer.writerow(['Timestamp', 'Symbol1', 'Symbol2', 'Action', 'SpreadPct'])

        async with aiohttp.ClientSession() as session:
            while True:
                tasks = [trade_pair(session, s1, s2, state, log_writer) for s1, s2 in ASSET_PAIRS]
                await asyncio.gather(*tasks)
                await asyncio.sleep(POLL_INTERVAL)

# ---------------- Execute ----------------
if __name__ == "__main__":
    asyncio.run(pairs_scalping_bot())
