__author__ = "Dimitri Cunning"

import os
import pytz
import time
import requests
import psycopg2
import pandas as pd
from top_stocks import ALL_STOCKS
from datetime import datetime, timedelta, timezone

polygon_api_key = os.getenv("POLYGON_API_KEY")

conn = psycopg2.connect(
    dbname=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_USER"),
    password=os.getenv("DATABASE_PASSWORD"),
    host=os.getenv("DATABASE_HOME_HOST"),
    port=os.getenv("DATABASE_PORT")
)
cur = conn.cursor()


def convert_ms(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).astimezone(pytz.timezone("America/New_York")).strftime(
        '%Y-%m-%d %H:%M:00')


def copy_to_db():
    copy_query = f"""
        COPY stocks.polygon_data(market_time, stock, open, close, high, low, volume, volume_weighted_avg_price)
        FROM '/Users/dcunning/PycharmProjects/EventsAppBackend/backend/stonks.csv'
        DELIMITER ',' CSV HEADER;
    """
    cur.execute(copy_query)
    conn.commit()


def populate_polygon_data_backfill():
    for company in ALL_STOCKS:
        stock = company[2]
        end = datetime(2024, 12, 12).date()
        start = end - timedelta(days=50)

        print(stock)
        for i in range(0): # 15: ~2 years

            url = f'https://api.polygon.io/v2/aggs/ticker/{stock}/range/1/minute/{start}/{end}?limit=50000&apiKey={polygon_api_key}'
            response = requests.get(url=url).json()
            if not 'results' in response:
                print(f'Skip: {response}')
                if (i+1) % 5 == 0: # API limit 5 calls / 60s
                    time.sleep(60)
                end = start - timedelta(days=1)
                start = end - timedelta(days=50)
                continue

            df = pd.DataFrame(data=response['results'])
            df = df.rename(columns={
                'v': 'volume',
                'vw': 'volume_weighted_avg_price',
                'o': 'open',
                'c': 'close',
                'h': 'high',
                'l': 'low',
                't': 'market_time'
            })
            df['stock'] = stock
            df = df[['market_time', 'stock', 'open', 'close', 'high', 'low', 'volume', 'volume_weighted_avg_price']]
            df['market_time'] = df['market_time'].apply(convert_ms)
            
            df.to_csv(f'/Users/dcunning/PycharmProjects/EventsAppBackend/backend/stonks.csv', index=False)

            copy_to_db()
            end = start - timedelta(days=1)
            start = end - timedelta(days=50)

            if (i+1) % 5 == 0: # API limit 5 calls / 60s
                time.sleep(60)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    populate_polygon_data_backfill()

