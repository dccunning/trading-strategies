import os
import json
import time
import logging
from kafka import KafkaConsumer
from clients.database import Database

TOPIC = 'binance-ws-eth-trade'
GROUP = 'binance-ws-trade'
BATCH_INTERVAL_SECONDS = 10.0

consumer = KafkaConsumer(
    TOPIC,
    group_id=GROUP,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='192.168.1.67')

insert_query = """
INSERT INTO crypto.binance_ws_trade 
(trade_id, symbol, price, quantity, is_buyer_maker, drift, consumed_time, 
produced_time, event_time, trade_time)
VALUES %s
ON CONFLICT (trade_id) DO NOTHING;
"""
create_table = """
CREATE TABLE crypto.binance_ws_trade (
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
            try:
                pass
                # db.run_query(insert_query, buffer)
            except Exception as e:
                logging.warning(f"Insert query failed: {e}")
            drifts = [r[5] for r in buffer]
            drift_stats = {
                "max": round(max(drifts)),
                "avg": round(sum(drifts) / len(drifts)),
                "p95": sorted(drifts)[int(len(drifts) * 0.95) - 1]
            }
            logging.log(logging.INFO, f"Consumed and inserted {len(buffer)} rows - drift_stats: {drift_stats}")
            buffer = []
        last_batch_time = time.time()
