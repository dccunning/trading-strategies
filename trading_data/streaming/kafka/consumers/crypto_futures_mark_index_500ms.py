__name__ = "Dimitri Cunning"

import os
import json
import time
import logging
from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'crypto-futures-mark-index-500ms'
BATCH_INTERVAL_SECONDS = 60.0


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

buffer = []
last_batch_time = time.time()
for message in consumer:
    data = message.value
    ts = data.get('timestamp')[:22]
    symbol = data.get('symbol')
    row = (
        symbol,
        ts,
        float(data.get('mark_price')),
        float(data.get('index_price')),
        float(data.get('last_funding_rate')),
        float(data.get('interest_rate')),
        data.get('ts_next_funding_time'),
        data.get('ts_premium_index')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            logging.log(logging.INFO, f"Inserting: {buffer}")
            db.run_query(insert_query, buffer)
            logging.log(logging.INFO, f"Inserted {len(buffer)} rows...")
            buffer = []
        last_batch_time = time.time()
