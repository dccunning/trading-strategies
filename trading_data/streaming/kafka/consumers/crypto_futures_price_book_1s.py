__name__ = "Dimitri Cunning"

import os
import json
import time
import logging
from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'crypto-futures-price-book-1s'
BATCH_INTERVAL_SECONDS = 60.0


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='192.168.1.67')

insert_query = """
INSERT INTO crypto.futures_price_book_1s (symbol, timestamp, last_trade_price, bid_price, ask_price, mid_price, bid_qty, ask_qty, ts_last_trade_price, ts_book_ticker)
VALUES %s
ON CONFLICT (symbol, timestamp) DO NOTHING;
"""

buffer = []
last_batch_time = time.time()
for message in consumer:
    data = message.value
    ts = data.get('timestamp')[:19]
    symbol = data.get('symbol')
    row = (
        symbol,
        ts,
        float(data.get('last_trade_price')),
        float(data.get('bid_price')),
        float(data.get('ask_price')),
        float(data.get('mid_price')),
        float(data.get('bid_qty')),
        float(data.get('ask_qty')),
        data.get('ts_last_trade_price'),
        data.get('ts_book_ticker')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            logging.log(logging.INFO, f"Inserting: {buffer}")
            db.run_query(insert_query, buffer)
            logging.log(logging.INFO, f"Inserted {len(buffer)} rows...")
            buffer = []
        last_batch_time = time.time()
