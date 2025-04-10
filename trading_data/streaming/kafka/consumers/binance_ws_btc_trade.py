import os
import json
import time
import logging
from venv import create

from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'binance-ws-btc-trade'
GROUP = 'binance-trade-consumer'
BATCH_INTERVAL_SECONDS = 10.0
LOG_INTERVAL_SECONDS = 20 # 3600

consumer = KafkaConsumer(
    TOPIC,
    group_id=GROUP,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='75.155.166.60')  # '192.168.1.67'

insert_query = """
INSERT INTO crypto.binance_ws_btc_trade 
(trade_id, symbol, price, quantity, is_buyer_maker, drift, consumed_time, 
produced_time, event_time, trade_time)
VALUES %s
ON CONFLICT (trade_id) DO NOTHING;
"""
create_table = """
CREATE TABLE crypto.binance_ws_btc_trade (
    trade_id BIGINT PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC,
    quantity NUMERIC,
    is_buyer_maker BOOLEAN,
    drift BIGINT,
    consumed_time BIGINT, 
    produced_time BIGINT,  
    event_time BIGINT,    
    trade_time BIGINT  
);
"""

hourly_insert_count = 0
hour_start_time = time.time()
last_batch_time = time.time()
buffer = []

for message in consumer:
    data = message.value
    consumed_time = time.time() * 1000

    row = (
        data.get('trade_id'),
        data.get('symbol'),
        data.get('price'),
        data.get('quantity'),
        data.get('is_buyer_maker'),
        round(consumed_time - data.get('trade_time')),
        consumed_time,
        data.get('produced_time'),
        data.get('event_time'),
        data.get('trade_time')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            db.run_query(insert_query, buffer)
            drifts = [r[5] for r in buffer]
            drift_stats = {
                "max": round(max(drifts)),
                "avg": round(sum(drifts) / len(drifts)),
                "p95": sorted(drifts)[int(len(drifts) * 0.95) - 1]
            }
            logging.log(logging.INFO, f"{TOPIC}: Inserted {len(buffer)} rows - drift_stats: {drift_stats}")
            hourly_insert_count += len(buffer)
            buffer = []
        last_batch_time = time.time()

        if time.time() - hour_start_time >= LOG_INTERVAL_SECONDS:
            logging.info(f"{TOPIC}: consumed and inserted {hourly_insert_count} rows")
            hourly_insert_count = 0
            hour_start_time = time.time()
