import ssl
import json
import time
import asyncio
import logging
import websockets
from typing import List
from collections import deque
from aiokafka import AIOKafkaProducer
from clients.database import Database

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


async def websocket_producer_stream(
        url: str,
        response_mapping: dict,
        producer: AIOKafkaProducer,
        key: str,
        topic: str = None
):
    fallback_buffer = deque(maxlen=10000)
    asyncio.create_task(retry_fallback_buffer(producer, fallback_buffer))
    ws_retry_wait = 1

    while True:
        try:
            async with websockets.connect(url, ssl=ssl_context) as ws:
                logging.info(f"WebSocket connection established")
                ws_retry_wait = 1
                async for msg in ws:
                    trade = json.loads(msg)
                    if 'stream' in trade:  # multi-stream
                        stream_type = trade['stream'].split('@')[-1]
                        topic = f'binance-ws-{stream_type}'
                        trade = trade['data']
                        trade_data = {
                            key: trade[value] for key, value in response_mapping[stream_type].items()
                        }
                    else:  # single-stream
                        trade_data = {
                            key: trade[value] for key, value in response_mapping.items()
                        }

                    trade_data["produced_time"] = time.time() * 1000
                    symbol = str(trade_data.get(key, 'unknown'))
                    try:
                        await producer.send_and_wait(
                            topic=topic,
                            key=symbol,
                            value=trade_data
                        )
                    except Exception as e:
                        logging.error(f"{topic}: Kafka error: {e}")
                        fallback_buffer.append((topic, symbol, trade_data))
        except Exception as ws_error:
            logging.error(f"WebSocket error: {ws_error}. Reconnecting in 5 seconds...")
            await asyncio.sleep(ws_retry_wait)
            ws_retry_wait = min(ws_retry_wait + 2, 30)


async def retry_fallback_buffer(producer, fallback_buffer):
    while True:
        if fallback_buffer:
            logging.info(f"Retrying {len(fallback_buffer)} buffered trades...")
            retry_success = []
            for topic, symbol, trade in list(fallback_buffer):
                try:
                    await producer.send_and_wait(topic=topic, key=symbol, value=trade)
                    retry_success.append((topic, symbol, trade))
                except Exception as retry_err:
                    logging.error(f"{topic}: Retry failed: {retry_err}")
                    break  # if Kafka is still down, stop retrying and wait 1s

            for item in retry_success:
                fallback_buffer.remove(item)

        await asyncio.sleep(1)


def insert_batched_data(db: Database, data: List[tuple], insert_query: str, topic: str):
    try:
        if data:
            db.run_query(insert_query, data)
        drifts = [r[-1] for r in data]
        drift_stats = {
            "max": round(max(drifts)),
            "avg": round(sum(drifts) / len(drifts)),
            "p95": sorted(drifts)[int(len(drifts) * 0.95) - 1]
        }
        drift_stats_str = f"{{'max': {drift_stats['max']:>4}, 'avg': {drift_stats['avg']:>4}, 'p95': {drift_stats['p95']:>4}}}"
        logging.log(logging.INFO, f"Inserted {len(data):>5} rows - drift: {drift_stats_str} ({topic})")

    except Exception as e:
        logging.warning(f"Insert query failed: {e}")


INSERT_WS_TRADE = """
INSERT INTO crypto.binance_ws_trade 
(trade_id, symbol, price, quantity, is_buyer_maker, event_time, trade_time, 
produced_time, consumed_time, drift)
VALUES %s
ON CONFLICT (symbol, trade_id) DO NOTHING;
"""
CREATE_WS_TRADE = """
CREATE TABLE crypto.binance_ws_trade (
    trade_id BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    quantity NUMERIC,
    is_buyer_maker BOOLEAN,
    drift BIGINT,
    consumed_time BIGINT, 
    produced_time BIGINT,  
    event_time BIGINT,    
    trade_time BIGINT,
    PRIMARY KEY (symbol, trade_id)
);
"""
INSERT_WS_BOOK_TICKER = """
INSERT INTO crypto.binance_ws_book_ticker
(book_update_id, symbol, bid_price, bid_quantity, ask_price, ask_quantity, 
event_time, transaction_time, produced_time, consumed_time, drift)
VALUES %s
ON CONFLICT (symbol, book_update_id) DO NOTHING;
"""
CREATE_WS_BOOK_TICKER = """
CREATE TABLE crypto.binance_ws_book_ticker (
    book_update_id BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    bid_price NUMERIC,
    bid_quantity NUMERIC,
    ask_price NUMERIC,
    ask_quantity NUMERIC,
    drift NUMERIC,
    consumed_time NUMERIC, 
    produced_time NUMERIC,  
    event_time BIGINT,    
    transaction_time BIGINT,
    PRIMARY KEY (symbol, book_update_id)
);
"""
INSERT_WS_MARK_PRICE = """
INSERT INTO crypto.binance_ws_mark_price
(symbol, mark_price, index_price, funding_rate, event_time,
next_funding_rate_est, next_funding_time, produced_time, consumed_time, drift)
VALUES %s
ON CONFLICT (symbol, event_time) DO NOTHING;
"""
CREATE_WS_MARK_PRICE = """
CREATE TABLE crypto.binance_ws_mark_price (
    symbol TEXT NOT NULL,
    mark_price NUMERIC,
    index_price NUMERIC,
    funding_rate NUMERIC,
    drift NUMERIC,
    consumed_time NUMERIC, 
    produced_time NUMERIC,  
    event_time BIGINT,    
    next_funding_rate_est NUMERIC,
    next_funding_time BIGINT,
    PRIMARY KEY (symbol, event_time)
);
"""
