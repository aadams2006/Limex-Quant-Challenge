import requests
import json
import pandas as pd

# Load credentials
with open('credentials.json') as f:
    creds = json.load(f)

CLIENT_ID = creds['client_id']
CLIENT_SECRET = creds['client_secret']
USERNAME = 'alexglobe20@gmail.com'
PASSWORD = 'Frayuuio_209'
ACCOUNT_NUMBER = 'dmo-c432'
AUTH_URL = creds['auth_url'].rstrip('/') + '/connect/token'
BASE_URL = creds['base_url'].rstrip('/')
SYMBOLS_URL = BASE_URL + '/marketdata/symbols'

# Get Access Token
def get_access_token():
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'password',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'username': USERNAME,
        'password': PASSWORD
    }
    response = requests.post(AUTH_URL, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Failed to get token: {response.status_code}, {response.text}")

# Fetch supported symbols
def fetch_symbols(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(SYMBOLS_URL, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch symbols: {response.status_code}, {response.text}")
    return pd.DataFrame(response.json())

if __name__ == "__main__":
    token = get_access_token()
    symbols_df = fetch_symbols(token)

    # Save full symbol list
    symbols_df.to_csv("lime_symbols_full.csv", index=False)
    print(f"✅ Saved full symbol list to lime_symbols_full.csv with {len(symbols_df)} entries.")

    # Optional: filter to liquid stocks/ETFs
    liquid_df = symbols_df[
        (symbols_df['symbolType'].isin(['Stock', 'ETF'])) &
        (symbols_df['status'] == 'Active')
    ]
    liquid_df.to_csv("lime_symbols_liquid.csv", index=False)
    print(f"✅ Saved liquid symbols to lime_symbols_liquid.csv with {len(liquid_df)} entries.")

    print("💡 Tip: Use 'lime_symbols_liquid.csv' for your bot's ticker list.")
