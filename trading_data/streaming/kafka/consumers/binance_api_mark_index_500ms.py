import os
import json
import time
import logging
from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'crypto-futures-mark-index-500ms'
BATCH_INTERVAL_SECONDS = 60.0
LOG_INTERVAL_SECONDS = 30 #3600

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='192.168.1.67')

insert_query = """
INSERT INTO crypto.futures_mark_index_500ms (symbol, timestamp, mark_price, index_price, last_funding_rate, interest_rate, ts_next_funding_time, ts_premium_index)
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
    ts = data.get('timestamp')[:22]
    symbol = data.get('symbol')
    row = (
        symbol,
        ts,
        safe_float(data.get('mark_price')),
        safe_float(data.get('index_price')),
        safe_float(data.get('last_funding_rate')),
        safe_float(data.get('interest_rate')),
        data.get('ts_next_funding_time'),
        data.get('ts_premium_index')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            # db.run_query(insert_query, buffer)
            hourly_insert_count += len(buffer)
            buffer = []
        last_batch_time = time.time()

        if time.time() - hour_start_time >= LOG_INTERVAL_SECONDS:
            logging.info(f"{TOPIC}: consumed and inserted {hourly_insert_count} rows")
            hourly_insert_count = 0
            hour_start_time = time.time()
