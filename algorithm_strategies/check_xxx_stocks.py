import os
import time
import datetime
import requests
from clients.database import Database

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
RATE_LIMIT_PER_MIN = 60
SLEEP_INTERVAL = 60 / RATE_LIMIT_PER_MIN

db = Database()
X = 5
Years = 1


def fetch_with_retry(url, params, retries=3):
    for i in range(retries):
        response = requests.get(url, params=params)
        if response.status_code == 429:
            wait = 2 ** i
            print(f"Rate limited. Retrying in {wait}s...")
            time.sleep(wait)
        elif response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            time.sleep(SLEEP_INTERVAL)
        else:
            return response.json()
    raise Exception("Max retries reached")


def main():
    try:
        data = db.run_query("SELECT symbol FROM stocks.nyse_stocks;")
        symbols = [row['symbol'] for row in data]
        print(f"Retrieved {len(symbols)} symbols from the database.")
    except Exception as e:
        print("Error retrieving symbols from the database:", e)
        symbols = []

    end_date = datetime.datetime.today()
    start_date = end_date - datetime.timedelta(days=Years * 365)
    start_unix = int(start_date.timestamp())
    end_unix = int(end_date.timestamp())
    print(f"Checking historical data from {start_date.date()} to {end_date.date()}.")

    count = 0
    for symbol in symbols:
        try:
            url = "https://finnhub.io/api/v1/stock/candle"
            params = {
                "symbol": symbol,
                "resolution": "D",
                "from": start_unix,
                "to": end_unix,
                "token": FINNHUB_API_KEY,
            }
            data = fetch_with_retry(url, params)
            time.sleep(SLEEP_INTERVAL)  # Respect rate limit

            if data.get("s") != "ok" or not data.get("c"):
                continue

            first_close = data["c"][0]
            last_close = data["c"][-1]

            if last_close >= X * first_close:
                count += 1
                print(f"{symbol}: {first_close:.2f} -> {last_close:.2f}")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    print(f"\nTotal stocks with price >= {X}x increase in the last {Years} years: {count}")


if __name__ == "__main__":
    main()