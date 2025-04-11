import os
import json
import time
import logging
from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'binance-api-all-book-ticker'
GROUP = 'binance-api'
BATCH_INTERVAL_SECONDS = 10.0

consumer = KafkaConsumer(
    TOPIC,
    group_id=GROUP,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='75.155.166.60') # '192.168.1.67'

insert_query = """
INSERT INTO crypto.binance_api_book_ticker
(symbol, time, bidPrice, bidQty, askPrice, askQty, lastUpdateId)
VALUES %s
ON CONFLICT (symbol, time) DO NOTHING;
"""
create_table = """
CREATE TABLE crypto.binance_api_book_ticker (
    symbol TEXT NOT NULL,
    time BIGINT NOT NULL,
    bidPrice NUMERIC,
    bidQty NUMERIC,
    askPrice NUMERIC,
    askQty NUMERIC,
    lastUpdateId BIGINT,
    UNIQUE (symbol, time)
);
"""

last_batch_time = time.time()
buffer = []

for message in consumer:
    data = message.value
    consumed_time = time.time() * 1000

    row = (
        data.get('symbol'),
        data.get('time'),
        data.get('bidPrice'),
        data.get('bidQty'),
        data.get('askPrice'),
        data.get('askQty'),
        data.get('lastUpdateId')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            try:
                db.run_query(insert_query, buffer)
                logging.log(logging.INFO, f"Inserted {len(buffer)} rows")
                buffer = []
            except Exception as e:
                logging.warning(f"Insert query failed: {e}")

        last_batch_time = time.time()
