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
USERNAME = creds['username']
PASSWORD = creds['password']
ACCOUNT_NUMBER = USERNAME.split("@")[0] + "@demo"

AUTH_URL = creds['auth_url'] + '/connect/token'
BASE_URL = creds['base_url']
ORDER_URL = BASE_URL + '/orders/place'
PRICE_HIST_URL = BASE_URL + '/marketdata/history'

# Asset Pairs Configuration
ASSET_PAIRS = [
    ("AAPL", "MSFT"),
    ("KO", "PEP"),
    ("XOM", "CVX"),
    ("SPY", "QQQ"),
]

THRESHOLD = 2  # Standard Deviations
POLL_INTERVAL = 1  # seconds
POSITION_SIZE = 1

# Global Token Variable
access_token = None

# ------------------ API Functions ------------------
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
    from_time = now - (3 * 24 * 60 * 60)
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

# ------------------ Trading Logic ------------------
async def trade_pair(session, symbol1, symbol2, state):
    price1 = await fetch_price_data(session, symbol1)
    price2 = await fetch_price_data(session, symbol2)

    if price1 is None or price2 is None:
        return

    # Calculate Spread
    spread = np.log(price1) - np.log(price2)
    mean_spread = spread.mean()
    std_spread = spread.std()
    latest_spread = spread.iloc[-1]

    print(f"[{time.strftime('%H:%M:%S')}] {symbol1}/{symbol2} Spread: {latest_spread:.5f} | Mean: {mean_spread:.5f}")

    pair_key = f"{symbol1}_{symbol2}"

    if state.get(pair_key) is None:
        state[pair_key] = {'position_open': False, 'current_position': None}

    # Trading Signals
    if not state[pair_key]['position_open']:
        if latest_spread > mean_spread + THRESHOLD * std_spread:
            await place_order(session, symbol1, 'sell')
            await place_order(session, symbol2, 'buy')
            state[pair_key] = {'position_open': True, 'current_position': 'SHORT_1'}
        elif latest_spread < mean_spread - THRESHOLD * std_spread:
            await place_order(session, symbol1, 'buy')
            await place_order(session, symbol2, 'sell')
            state[pair_key] = {'position_open': True, 'current_position': 'LONG_1'}
    else:
        if abs(latest_spread - mean_spread) < 0.2 * std_spread:
            # Close positions
            if state[pair_key]['current_position'] == 'SHORT_1':
                await place_order(session, symbol1, 'buy')
                await place_order(session, symbol2, 'sell')
            elif state[pair_key]['current_position'] == 'LONG_1':
                await place_order(session, symbol1, 'sell')
                await place_order(session, symbol2, 'buy')
            state[pair_key] = {'position_open': False, 'current_position': None}

# ------------------ Main Async Bot ------------------
async def pairs_trading_bot():
    await get_access_token()
    state = {}

    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [trade_pair(session, s1, s2, state) for s1, s2 in ASSET_PAIRS]
            await asyncio.gather(*tasks)
            await asyncio.sleep(POLL_INTERVAL)

# ------------------ Run Bot ------------------
if __name__ == "__main__":
    asyncio.run(pairs_trading_bot())
