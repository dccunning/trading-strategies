__author__ = 'Dimitri Cunning'

import os
import sys
import time
import pytz
import requests
import psycopg2
import logging
import pandas as pd
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime, timezone


TEMP_BULK_COPY_FILE = 'temp/daily_stocks.csv'
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_API_CALLS_PER_MINUTE = 5
POLYGON_TICKER_BASE_URL = 'https://api.polygon.io/v2/aggs/ticker'
relative_dir = os.path.dirname(__file__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

conn = psycopg2.connect(
    dbname=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_USER"),
    password=os.getenv("DATABASE_PASSWORD"),
    host=os.getenv("DATABASE_HOME_HOST"),
    port=os.getenv("DATABASE_PORT")
)
cur = conn.cursor()


def fetch_stock_symbols() -> List[str]:
    """Fetch stock symbols from the database."""

    cur.execute("SELECT symbol FROM stocks.companies;")
    return [row[0] for row in cur.fetchall()]


def fetch_polygon_data(symbol: str, date: datetime.date) -> List[dict]:
    """Fetch daily data for a stock symbol."""

    url = f'{POLYGON_TICKER_BASE_URL}/{symbol}/range/1/minute/{date}/{date}?limit=50000&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        response_json = response.json()
        if 'results' in response_json:
            results = response_json['results']
            for entry in results:
                for key in ['v', 'vw', 'o', 'c', 'h', 'l']:
                    entry[key] = float(entry.get(key, 0))
            return results
        
        else:
            return []
    else:
        logging.error(f"Error fetching data for {symbol}: {response.text}")
        return []
    

def parse_dataframe(df: DataFrame, stock: str) -> DataFrame:
    """Parse the dataframe for saving to the database."""

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

    return df


def convert_ms(t: int) -> str:
    """Convert milliseconds to string timestamp."""
    return datetime.fromtimestamp(t / 1000, timezone.utc).astimezone(pytz.timezone("America/New_York")).strftime(
        '%Y-%m-%d %H:%M:00')


def write_to_csv(df: DataFrame) -> None:
    """Write the DataFrame to a temporary CSV file."""

    df.to_csv(f'{relative_dir}/{TEMP_BULK_COPY_FILE}', index=False)


def copy_data_to_db(path: str) -> None:
    """Load data from the CSV file into the database."""

    copy_query = f"""
        COPY stocks.polygon_data(market_time, stock, open, close, high, low, volume, volume_weighted_avg_price)
        FROM '{path}'
        DELIMITER ',' CSV HEADER;
    """
    cur.execute(copy_query)
    conn.commit()


def main(run_date: datetime.date) -> None:
    """Main Spark job for fetching data for existing stocks and copying prices data to the Database."""

    spark_session = SparkSession.builder \
        .appName("PolygonIO Daily Stocks Data Update Job") \
        .getOrCreate()

    all_stock_symbols = fetch_stock_symbols()
    logging.info(f"Fetched {len(all_stock_symbols)} stock symbols.")

    count_stocks_updated = 0
    count_api_calls = 0
    for symbol in all_stock_symbols:
        polygon_data = fetch_polygon_data(symbol=symbol, date=run_date)

        if polygon_data:
            polygon_data_df = pd.DataFrame(data=polygon_data)

            parsed_polygon_data_df = parse_dataframe(df=polygon_data_df, stock=symbol)
            write_to_csv(df=parsed_polygon_data_df)
            copy_data_to_db(path=f'{relative_dir}/{TEMP_BULK_COPY_FILE}')

            count_stocks_updated += 1
            logging.info(f"Rows added for {symbol}: {len(parsed_polygon_data_df)}")
        else:
            logging.info(f"No data for {symbol}")
        count_api_calls += 1
        if count_api_calls%POLYGON_API_CALLS_PER_MINUTE == 0:
            time.sleep(60)

    logging.info(f"Stocks updated on {run_date}: {count_stocks_updated}")
    cur.close()
    conn.close()
    spark_session.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.log("Usage: spark-submit polygon_job.py <data_time>")
        sys.exit(1)

    try:
        run_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    main(run_date=run_date)
