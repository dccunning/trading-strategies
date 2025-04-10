import os
import json
import time
import logging
from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'binance-api-price-book-1m'
BATCH_INTERVAL_SECONDS = 60.0
LOG_INTERVAL_SECONDS = 3600

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='192.168.1.67')

insert_query = """
INSERT INTO crypto.futures_price_book_1m (symbol, timestamp, last_trade_price, bid_price, ask_price, mid_price, bid_qty, ask_qty, ts_last_trade_price, ts_book_ticker)
VALUES %s
ON CONFLICT (symbol, timestamp) DO NOTHING;
"""


def safe_float(val):
    return float(val) if val is not None else None


hourly_insert_count = 0
hour_start_time = time.time()
last_batch_time = time.time()
buffer = []

for message in consumer:
    data = message.value
    ts = data.get('timestamp')[:19]
    symbol = data.get('symbol')
    row = (
        symbol,
        ts,
        safe_float(data.get('last_trade_price')),
        safe_float(data.get('bid_price')),
        safe_float(data.get('ask_price')),
        safe_float(data.get('mid_price')),
        safe_float(data.get('bid_qty')),
        safe_float(data.get('ask_qty')),
        data.get('ts_last_trade_price'),
        data.get('ts_book_ticker')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            db.run_query(insert_query, buffer)
            hourly_insert_count += len(buffer)
            buffer = []
        last_batch_time = time.time()

        if time.time() - hour_start_time >= LOG_INTERVAL_SECONDS:
            logging.info(f"{TOPIC}: consumed and inserted {hourly_insert_count} rows")
            hourly_insert_count = 0
            hour_start_time = time.time()
