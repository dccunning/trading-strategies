import os
import json
import logging
import asyncio
from aiokafka import AIOKafkaProducer
from utils.kafka_utils.binance_ws import websocket_producer_stream

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

TOPIC = 'binance-ws-btc-trade'
TRADE_ID_KEY = 'trade_id'
BTC_TRADE_WS = "wss://fstream.binance.com/ws/btcusdt@trade"
RESPONSE_MAP = {
    "trade_id": "t",
    "symbol": "s",
    "price": "p",
    "quantity": "q",
    "is_buyer_maker": "m",
    "event_time": "E",
    "trade_time": "T"
}
"""
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic binance-ws-btc-trade \
  --partitions 12 \
  --replication-factor 1

kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic binance-ws-btc-trade
"""


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )
    await producer.start()
    logging.log(logging.INFO, f"{TOPIC}: Started streaming")
    try:
        await websocket_producer_stream(
            url=BTC_TRADE_WS,
            response_mapping=RESPONSE_MAP,
            producer=producer,
            topic=TOPIC,
            key=TRADE_ID_KEY
        )
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
