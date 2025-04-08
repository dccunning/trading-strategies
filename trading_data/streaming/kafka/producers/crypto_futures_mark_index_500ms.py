import logging
import asyncio
from kafka_streams.utils.helpers import producer_async
from kafka_streams.utils.binance_crypto import get_futures_mark_index

TOPIC = 'crypto-futures-mark-index-500ms'
PRICE_DELAY_IN_SECONDS = 0.5
SYMBOLS = ["BTCUSDT", "ETHUSDT", "XMRUSDT"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

asyncio.run(
    producer_async(
        frequency=PRICE_DELAY_IN_SECONDS,
        topic=TOPIC,
        key_field="symbol",
        get_data_func=lambda: get_futures_mark_index(symbols=SYMBOLS)
    )
)